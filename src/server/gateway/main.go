package main

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"os"
	"strconv"
	"tp1/health"
	"tp1/server/gateway/internal"

	"github.com/op/go-logging"
	"tp1/globalconfig"
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"
)

var (
	reportGrpcAddr = "report:50052"
)

func main() {
	logger := logging.MustGetLogger("controller")

	healthSrv := health.New(logger)
	healthSrv.Start()

	conn, err := rabbitmq.ConnectRabbitMQ(logger)
	if err != nil {
		logger.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitmq.ShutdownConnection(conn)

	ch, err := conn.Channel()
	if err != nil {
		logger.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	defer rabbitmq.ShutdownChannel(ch)

	err = rabbitmq.DeclareFanoutExchanges(ch, globalconfig.Exchanges...)
	if err != nil {
		logger.Fatalf("Failed to declare exchanges: %v", err)
	}

	err = rabbitmq.DeclareDirectQueues(ch, globalconfig.MoviesQueues...)
	if err != nil {
		logger.Fatalf("Failed to declare movies: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logger.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()

	var reportConn *grpc.ClientConn
	reportConn, err = internal.RetryWithBackoff(
		func() (*grpc.ClientConn, error) {
			return grpc.NewClient(reportGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		})
	if err != nil {
		logger.Fatalf("Failed to connect to report generator: %v", err)
	}
	defer internal.ShutdownGRPCConnection(reportConn)

	reportClient := protopb.NewReportServiceClient(reportConn)
	clientRegistry := internal.NewClientRegistry()

	shardsEnv := os.Getenv("SHARDS")
	if shardsEnv == "" {
		logger.Fatal("SHARDS environment variable must be set")
	}

	shards, err := strconv.ParseInt(shardsEnv, 10, 64)
	if err != nil {
		logger.Fatalf("Invalid SHARDS value '%s': %v", shardsEnv, err)
	}

	if shards <= 0 {
		logger.Fatalf("SHARDS must be a positive integer, got: %d", shards)
	}

	ctrl := internal.NewController(ch, reportClient, clientRegistry, shards)

	protopb.RegisterMovieServiceServer(grpcServer, ctrl)
	protopb.RegisterRatingServiceServer(grpcServer, ctrl)
	protopb.RegisterCreditServiceServer(grpcServer, ctrl)
	protopb.RegisterControllerServer(grpcServer, ctrl)

	logger.Info("gRPC server listening on :50051")

	healthSrv.MarkReady()

	err = grpcServer.Serve(lis)
	if err != nil {
		logger.Fatalf("Failed to serve gRPC: %v", err)
	}
}
