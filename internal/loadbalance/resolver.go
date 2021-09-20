package loadbalance

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"

	api "github.com/adamwoolhether/proglog/api/v1"
)

// Resolver implements gRPC's resolver.Builder and resolver.Resolver interface.
// clientConn represents the user's client connection, gRPC passes it to the
// resolver to be updated with the discovered servers. resolverConn is the
// resolver's own client connection to the server, allowing it to get servers.
type Resolver struct {
	mu            sync.Mutex
	clientConn    resolver.ClientConn
	resolverConn  *grpc.ClientConn
	serviceConfig *serviceconfig.ParseResult
	logger        *zap.Logger
}

// resolver.Builder is made of Build() and Scheme(0
var _ resolver.Builder = (*Resolver)(nil)

// Build receives data needed ot build a resolver that's able to discover servers
// and client connections the resolver will update with discovered servers.
// It sets up a client connection to the server, allowing the resolver to call
// the GetServers API.
func (r *Resolver) Build(target resolver.Target, cc resolver.ClientConn,
	opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.logger = zap.L().Named("resolver")
	r.clientConn = cc
	var dialOpts []grpc.DialOption
	if opts.DialCreds != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(opts.DialCreds))
	}
	r.serviceConfig = r.clientConn.ParseServiceConfig(fmt.Sprintf(`{"loadBalancingConfig":[{"%s":{}}]}`, Name))

	var err error
	r.resolverConn, err = grpc.Dial(target.Endpoint, dialOpts...)
	if err != nil {
		return nil, err
	}
	r.ResolveNow(resolver.ResolveNowOptions{})
	return r, nil
}

const Name = "proglog"

// Scheme returns the resolver's scheme identifier. This resolver is
// registered with init to implement the resolver.Resolver interface.
func (r *Resolver) Scheme() string {
	return Name
}

func init() {
	resolver.Register(&Resolver{})
}

// resolver.Resolver is made of ResolveNow and Close.
var _ resolver.Resolver = (*Resolver)(nil)

// ResolveNow is called by gRPC to resolve the target, discover servers, and update
// the client connection with servers. How resolver discovers servers depends on the
// resolver and the service it's working with. (Kubernetes).
func (r *Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	r.mu.Lock()
	defer r.mu.Unlock()
	client := api.NewLogClient(r.resolverConn)

	// get cluser and set on the cc attributes
	ctx := context.Background()
	res, err := client.GetServers(ctx, &api.GetServersRequest{})
	if err != nil {
		r.logger.Error("failed to resolve server", zap.Error(err))
		return
	}
	var addrs []resolver.Address
	for _, server := range res.Servers {
		addrs = append(addrs, resolver.Address{
			Addr: server.RpcAddr,
			Attributes: attributes.New("is_leader", server.IsLeader),
		})
	}
	r.clientConn.UpdateState(resolver.State{
		Addresses: addrs,
		ServiceConfig: r.serviceConfig,
	})
}

// Close closes the resolver created with Build.
func (r *Resolver) Close() {
	if err := r.resolverConn.Close(); err != nil {
		r.logger.Error("failed to close conn", zap.Error(err))
	}
}

