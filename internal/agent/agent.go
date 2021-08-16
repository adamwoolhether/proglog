package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	api "github.com/adamwoolhether/proglog/api/v1"
	"github.com/adamwoolhether/proglog/internal/auth"
	"github.com/adamwoolhether/proglog/internal/discovery"
	"github.com/adamwoolhether/proglog/internal/log"
	"github.com/adamwoolhether/proglog/internal/server"
)


// Agent runs on every service instance, it sets up
// and connects all the different components.
type Agent struct {
	Config
	log *log.Log
	server *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator
	shutdown bool
	shutdowns chan struct{}
	shutdownLock sync.Mutex
}

// Config compromises the components' parameters
// and gets passed through to the components.
type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig *tls.Config
	DataDir string
	BindAddr string
	RPCPort int
	NodeName string
	StartJoinAddrs []string
	ACLModeFile string
	ACLPolicyFile string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// New creates and Agent, running a slice of methods
// to set up and run the agent's components. It sets up
// a running, functioning service.
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config: config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupLog,
		a.setupServer,
		a.setupMemberShip,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupLog() error {
	var err error
	a.log, err = log.NewLog(a.Config.DataDir, log.Config{})
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(a.Config.ACLModeFile, a.Config.ACLPolicyFile)
	serverConfig := &server.Config{
		CommitLog: a.log,
		Authorizer: authorizer,
	}
	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}
	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}
	rpcAddr, err := a.RPCAddr()
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	go func() {
		if err := a.server.Serve(ln); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

// setupMembership setups up a Replicator with the gRPC dial options
// needed to connect to other servers and a client for the replicator
// to connect to other servers, consume data, and produce a copy of
// data to the local server. It also creates a Membership, passing
// in the replicator and its handler to notify the replicator when
// servers join or leave the cluster.
func (a *Agent) setupMemberShip() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	var opts [] grpc.DialOption
	if a.Config.PeerTLSConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
	}
	conn, err := grpc.Dial(rpcAddr, opts...)
	if err != nil {
		return err
	}
	client := api.NewLogClient(conn)
	a.replicator = &log.Replicator{
		DialOptions: opts,
		LocalServer: client,
	}
	a.membership, err = discovery.New(a.replicator, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{"rpc_addr": rpcAddr},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

// Shutdown shuts down the agent. It leaves membership,
// allowing other members to see this server has left the
// cluster, closes the replicator to prevent duplicates,
// gracefully stops the server to prevent new connections
// and blocking until the pending RPCS have finished,
// and finally closes the log.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()
	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		a.membership.Leave,
		a.replicator.Close,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}