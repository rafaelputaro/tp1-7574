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
	// Connect to RabbitMQ
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

	// Declare required queues
	queues := []string{"movies", "ratings", "credits"}
	for _, name := range queues {
		_, err := ch.QueueDeclare(name, true, false, false, false, nil)
		if err != nil {
			logger.Fatalf("Failed to declare queue '%s': %v", name, err)
		}
	}

	// Declare fanout exchanges
	exchanges := []string{"movies_exchange", "ratings_exchange", "credits_exchange"}
	for _, name := range exchanges {
		err := ch.ExchangeDeclare(
			name,
			"fanout", // exchange type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			logger.Fatalf("Failed to declare exchange '%s': %v", name, err)
		}
	}

	// Start gRPC server
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
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve gRPC: %v", err)
	}
}
