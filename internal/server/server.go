package server

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	api "github.com/adamwoolhether/proglog/api/v1"
)

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

// These constans will match the values used in our ACL policy.
const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

var _ api.LogServer = (*grpcServer)(nil)

// NewGRPCServer allows users a way to instantiate the service, create
// a gRPC server, and register the service to that server. For opts, see:
// https://pkg.go.dev/google.golang.org/grpc?utm_source=godoc#ServerOption
func NewGRPCServer(config *Config, grpcOpts ...grpc.ServerOption) (*grpc.Server, error) {
	// configure the Zap logger. Logger is named to differentiate
	// from other logs in our service. 'grpc.time_ns' field is added
	// to log duration of each request.
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64("grpc.time_ns", duration.Nanoseconds())
			},
		),
	}
	// configure how Open-Census collects metrics and traces.
	// we always sample traces for development and want all
	// requests to be traced, this may affect performance in prod.
	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}
	// hook up authenticate interceptor to gRPC server to
	// identify the subject of each RPC to initiate authorization.
	// also configure gRPC to apply Zap interceptors for logging
	// gRPC calls, and attach OpenCensus as server's stat handler,
	// allowing stats recording on server request handling.
	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
			grpc_auth.UnaryServerInterceptor(authenticate),
		)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)
	gsrv := grpc.NewServer(grpcOpts...)
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
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, produceAction); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(subject(ctx), objectWildcard, consumeAction); err != nil {
		return nil, err
	}

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

// Authorizer is an interface that allows switching out the
// authorization implementation, similar to CommitLog.
type Authorizer interface {
	Authorize(subject, object, action string) error
}

// authenticate and subject are helper functions that takes the
// "subject" out of the client's cert. authenticate is an interceptor,
// it reads the client's "subject" and writes it to the RPC's context.
// Interceptors allow interception and modification of each RPC call's
// execution to break the request into smaller, reusable chunks.
func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(
			codes.Unknown,
			"couldn't find peer info",
		).Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

// subject returns the client's cert's 'subject' to identify
// a client and check their access.
func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
