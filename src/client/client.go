package main

import (
	"context"
	"fmt"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/types/known/emptypb"
	"os"
	"os/signal"
	"syscall"
	"tp1/client/internal"
	pb "tp1/protobuf/protopb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	batchSize          = 100
	controllerGrpcAddr = "controller:50051"
	reportGrpcAddr     = "report:50052"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if len(os.Args) != 4 {
		fmt.Println("Usage: client <movies> <ratings> <credits>")
		os.Exit(1)
	}

	moviesPath := os.Args[1]
	ratingsPath := os.Args[2]
	creditsPath := os.Args[3]

	fmt.Println("Reading files:")
	fmt.Println(" - Movies:", moviesPath)
	fmt.Println(" - Ratings:", ratingsPath)
	fmt.Println(" - Credits:", creditsPath)

	logger := logging.MustGetLogger("client")

	// Create Parsers
	moviesParser, err := internal.NewMoviesParser(moviesPath, batchSize)
	if err != nil {
		logger.Fatalf("Failed to create movies parser: %v", err)
	}
	defer moviesParser.Close()

	creditsParser, err := internal.NewCreditsParser(creditsPath, batchSize)
	if err != nil {
		logger.Fatalf("Failed to create credits parser: %v", err)
	}
	defer creditsParser.Close()

	ratingsParser, err := internal.NewRatingsParser(ratingsPath, batchSize)
	if err != nil {
		logger.Fatalf("Failed to create ratings parser: %v", err)
	}
	defer ratingsParser.Close()

	// Send Data
	var controllerConn *grpc.ClientConn
	controllerConn, err = internal.RetryWithBackoff(
		func() (*grpc.ClientConn, error) {
			return grpc.NewClient(controllerGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		})
	if err != nil {
		logger.Fatalf("Failed to connect to controller after retries: %v", err)
	}
	defer controllerConn.Close()

	moviesClient := pb.NewMovieServiceClient(controllerConn)
	creditsClient := pb.NewCreditServiceClient(controllerConn)
	ratingsClient := pb.NewRatingServiceClient(controllerConn)

	internal.SendMovies(ctx, moviesClient, moviesParser)
	internal.SendCredits(ctx, creditsClient, creditsParser)
	internal.SendRatings(ctx, ratingsClient, ratingsParser)

	// Get Results
	var reportConn *grpc.ClientConn
	reportConn, err = internal.RetryWithBackoff(
		func() (*grpc.ClientConn, error) {
			return grpc.NewClient(reportGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		})
	if err != nil {
		logger.Fatalf("Failed to connect to report generator: %v", err)
	}
	defer reportConn.Close()

	reportClient := pb.NewReportServiceClient(reportConn)

	resp, err := reportClient.GetReport(ctx, &emptypb.Empty{})
	if err != nil {
		logger.Fatalf("Failed to generate report: %v", err)
	}

	logger.Infof("Received report response: %+v", resp)
}
