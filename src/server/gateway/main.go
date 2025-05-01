package main

import (
	"google.golang.org/grpc"
	"net"
	"tp1/server/gateway/internal"

	"github.com/op/go-logging"
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"
)

func main() {
	logger := logging.MustGetLogger("controller")

	conn, err := rabbitmq.ConnectRabbitMQ(logger)
	if err != nil {
		logger.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		logger.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	defer ch.Close()

	err = rabbitmq.DeclareFanoutExchanges(ch, "ratings_exchange", "credits_exchange")
	if err != nil {
		logger.Fatalf("Failed to declare exchanges: %v", err)
	}

	err = rabbitmq.DeclareDirectQueues(ch, "movies1", "movies2")
	if err != nil {
		logger.Fatalf("Failed to declare movies: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		logger.Fatalf("Failed to listen on port 50051: %v", err)
	}

	grpcServer := grpc.NewServer()

	ctrl := internal.NewController(ch)
	protopb.RegisterMovieServiceServer(grpcServer, ctrl)
	protopb.RegisterRatingServiceServer(grpcServer, ctrl)
	protopb.RegisterCreditServiceServer(grpcServer, ctrl)

	logger.Info("gRPC server listening on :50051")

	err = grpcServer.Serve(lis)
	if err != nil {
		logger.Fatalf("Failed to serve gRPC: %v", err)
	}
}
