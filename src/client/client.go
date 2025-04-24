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

	printReport(resp)
}

func printReport(report *pb.ReportResponse) {
	fmt.Println("=== Report Summary ===")

	// Answer 1: Movies
	fmt.Println("\n[1] Movies:")
	for _, movie := range report.Answer1.GetMovies() {
		fmt.Printf(" - ID: %d | Title: %s | Genres: %v\n", movie.GetId(), movie.GetTitle(), movie.GetGenres())
	}

	// Answer 2: Budgets by Country
	fmt.Println("\n[2] Budgets by Country:")
	for _, country := range report.Answer2.GetCountries() {
		fmt.Printf(" - %s: $%d\n", country.GetName(), country.GetBudget())
	}

	// Answer 3: Rating Extremes
	fmt.Println("\n[3] Rating Extremes:")
	fmt.Printf(" - Lowest: ID %d | Title: %s | Rating: %.2f\n",
		report.Answer3.GetMin().GetId(), report.Answer3.GetMin().GetTitle(), report.Answer3.GetMin().GetRating())
	fmt.Printf(" - Highest: ID %d | Title: %s | Rating: %.2f\n",
		report.Answer3.GetMax().GetId(), report.Answer3.GetMax().GetTitle(), report.Answer3.GetMax().GetRating())

	// Answer 4: Actors
	fmt.Println("\n[4] Frequent Actors:")
	for _, actor := range report.Answer4.GetActors() {
		fmt.Printf(" - ID: %d | Name: %s | Appearances: %d\n", actor.GetId(), actor.GetName(), actor.GetCount())
	}

	// Answer 5: Sentiment
	fmt.Println("\n[5] Sentiment Analysis:")
	fmt.Printf(" - Positive (%s): %.2f\n", report.Answer5.GetPositive().GetType(), report.Answer5.GetPositive().GetScore())
	fmt.Printf(" - Negative (%s): %.2f\n", report.Answer5.GetNegative().GetType(), report.Answer5.GetNegative().GetScore())
}
