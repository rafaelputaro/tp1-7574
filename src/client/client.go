package main

import (
	"fmt"
	"log"
	"os"
	"tp1/client/internal"
	pb "tp1/protobuf/protopb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	batchSize = 100
	grpcAddr  = "controller:50051"
)

func main() {
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

	// Parsers
	moviesParser, err := internal.NewMoviesParser(moviesPath, batchSize)
	if err != nil {
		log.Fatalf("Failed to create movies parser: %v", err)
	}
	defer moviesParser.Close()

	creditsParser, err := internal.NewCreditsParser(creditsPath, batchSize)
	if err != nil {
		log.Fatalf("Failed to create credits parser: %v", err)
	}
	defer creditsParser.Close()

	ratingsParser, err := internal.NewRatingsParser(ratingsPath, batchSize)
	if err != nil {
		log.Fatalf("Failed to create ratings parser: %v", err)
	}
	defer ratingsParser.Close()

	// Senders
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	moviesClient := pb.NewMovieServiceClient(conn)
	creditsClient := pb.NewCreditServiceClient(conn)
	ratingsClient := pb.NewRatingServiceClient(conn)

	internal.SendMovies(moviesClient, moviesParser)
	internal.SendCredits(creditsClient, creditsParser)
	internal.SendRatings(ratingsClient, ratingsParser)
}
