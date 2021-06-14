package server

import (
	"context"

	"google.golang.org/grpc"

	api "github.com/adamwoolhether/proglog/api/v1"
)

type Config struct {
	CommitLog CommitLog
}

var _ api.LogServer = (*grpcServer)(nil)

// NewGRPCServer allows users a way to instantiate the service, create
// a gRPC server, and register the service to that server.
func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

// grpcServer implements the methods defined in the service
// definition of our protobuf.
type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

// newgrpcServer creates a new instance of the server.
func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	return srv, nil
}

// Produce and Consume handlers must be implemented
// to implement the API generated in log_grpc.pb.go, the
// The below methods handle requests made by clients to
// produce and consume the server's log.
func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

// Now implement the streaming APIs:
// ProduceStream implements a bidirectional streaming RPC, allowing the client
// to stream data into the server's log, and the server responds to whether
// the request succeeded or not.
func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

// ConsumeStream implements a server side streaming RPC so the client can
// tell the server where in the log to read records. The server then
// streams every record that follows, even if they aren't in the log yet.
// When it reaches the end, the server waits until another log is
// appended to continue streaming records to the client.
func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}
			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

// CommitLog allows us to pass a log implementation based on our needs
// at the time of use. By having our service depend on a log interface
// instead of a concrete type, the service can use any log implementation
// that satisfies the log interface. This allows ease of use in different
// environemtns (like production, testing).
type CommitLog interface {
	Append(record *api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}
