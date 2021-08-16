package log

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "github.com/adamwoolhether/proglog/api/v1"
)

// Replicator connects to other servers via gRPC client.
// It passes options to configure the client, holds a map
// of servers to channel, used to stop replicating from a
// server when the server fails or leaves. Produce func
// is called to save a copy of messages consumed from
// other servers.
type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

// Join adds a given server address to the list of
// servers for replication, it runs the replicate
// goroutine to run the replication logic.
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		//replication already, so return
		return nil
	}
	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])

	return nil
}

// replicate creates a client, opening up a stream to consume all
// the logs on the server. The loop consumes logs from the added
// server and produces a copy to the local server. Messages are
// replicated until the server leaves or fails and is closed by
// the replicator goroutine. Replicator closes the channel when
// Serf receives an event saying that the server has left.
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx,
		&api.ConsumeRequest{
			Offset: 0,
		},
	)
	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}
	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		select {
		case <-r.close:
			return
		case <-leave:
			return
			case record := <- records:
				_, err = r.LocalServer.Produce(ctx, &api.ProduceRequest{Record: record})
				if err != nil {
					r.logError(err, "failed to produce", addr)
					return
				}
		}
	}

}

// Leave handles the server leaving byt removing it from the
// list of servers for replication, and closes its channel,
// which signals the receiver to replicate goroutines to stop
// replicated from that server.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name])
	delete(r.servers, name)
	return nil
}

// init lazy intializes the server map. Lazy initialization
// should give structs a useful zero value, reducing the API's
// size and complexity while maintaining the same functionality.
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close closes the replicator to prevent replication
// of new servers joining the cluster, stopping replication
// of existing servers by having the goroutines replicate.
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// logError logs errors that have no other use.
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(msg, zap.String("addre", addr), zap.Error(err))
}