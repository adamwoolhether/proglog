package loadbalance_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/adamwoolhether/proglog/api/v1"
	"github.com/adamwoolhether/proglog/internal/config"
	"github.com/adamwoolhether/proglog/internal/loadbalance"
	"github.com/adamwoolhether/proglog/internal/server"
)

func TestResolver(t *testing.T) {
	// Setup a server for test resolver to discover some servers from
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(tlsConfig)

	srv, err := server.NewGRPCServer(&server.Config{GetServerer: &getServers{}}, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go srv.Serve(l)

	// create and build the test resolver, configuring the endpoint pointing to the server set up above.
	conn := &clientConn{}
	tlsConfig, err = config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: "127.0.0.1",
		Server:        false,
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(tlsConfig)
	opts := resolver.BuildOptions{DialCreds: clientCreds}
	r := &loadbalance.Resolver{}
	_, err = r.Build(
		resolver.Target{
			Endpoint: l.Addr().String(),
		}, conn, opts)
	require.NoError(t, err)

	// check that the reolver updated the client connection with servers & data expected.
	wantState := resolver.State{
		Addresses:     []resolver.Address{{
			Addr: "localhost:9001",
			Attributes: attributes.New("is_leader", true),
		}, {
			Addr: "localhost:9002",
			Attributes: attributes.New("is_leader", false),
		}},
	}
	require.Equal(t, wantState, conn.state)

	conn.state.Addresses = nil
	r.ResolveNow(resolver.ResolveNowOptions{})
	require.Equal(t, wantState, conn.state)
}

// getServers implements GetServers. it returns a known server set for the resolver to find.
type getServers struct {}

func (s *getServers) GetServers() ([]*api.Server, error) {
	return []*api.Server{{
		Id: "leader",
		RpcAddr: "localhost:9001",
		IsLeader: true,
	}, {
		Id: "follower",
		RpcAddr: "localhost:9002",
	}}, nil
}

// clientConn implements resolver.ClientConn, it keeps a reference to the state that the
// resolver updated it with, allowing verification that the resolver updates the client
// connection with the correct data.
type clientConn struct {
	resolver.ClientConn
	state resolver.State
}

func (c *clientConn) UpdateState(state resolver.State) {
	c.state = state
}
func (c *clientConn) ReportError(err error) {}
func (c *clientConn) NewAddress(addrs []resolver.Address) {}
func (c *clientConn) NewServiceConfig(config string) {}
func (c *clientConn) ParseServiceConfig (config string) *serviceconfig.ParseResult {
	return nil
}