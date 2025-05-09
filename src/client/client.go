package main

import (
	"context"
	"fmt"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/types/known/emptypb"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
	"tp1/client/internal"
	pb "tp1/protobuf/protopb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	batchSize          = 1000
	ratingsBatchSize   = 10000
	controllerGrpcAddr = "controller:50051"
)

func main() {
	time.Sleep(10*time.Second + time.Duration(rand.Intn(20000000000)))

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
	defer internal.ShutdownParser[pb.Movie](moviesParser)

	creditsParser, err := internal.NewCreditsParser(creditsPath, batchSize)
	if err != nil {
		logger.Fatalf("Failed to create credits parser: %v", err)
	}
	defer internal.ShutdownParser[pb.Credit](creditsParser)

	ratingsParser, err := internal.NewRatingsParser(ratingsPath, ratingsBatchSize)
	if err != nil {
		logger.Fatalf("Failed to create ratings parser: %v", err)
	}
	defer internal.ShutdownParser[pb.Rating](ratingsParser)

	// Send Data
	var controllerConn *grpc.ClientConn
	controllerConn, err = internal.RetryWithBackoff(
		func() (*grpc.ClientConn, error) {
			return grpc.NewClient(controllerGrpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		})
	if err != nil {
		logger.Fatalf("Failed to connect to controller after retries: %v", err)
	}
	defer internal.ShutdownGRPCConnection(controllerConn)

	moviesClient := pb.NewMovieServiceClient(controllerConn)
	creditsClient := pb.NewCreditServiceClient(controllerConn)
	ratingsClient := pb.NewRatingServiceClient(controllerConn)

	internal.SendMovies(ctx, moviesClient, moviesParser)
	internal.SendCredits(ctx, creditsClient, creditsParser)
	internal.SendRatings(ctx, ratingsClient, ratingsParser)

	controllerReportClient := pb.NewControllerClient(controllerConn)

	resp, err := controllerReportClient.GetReport(ctx, &emptypb.Empty{})

	if err != nil {
		logger.Fatalf("Failed to get report: %v", err)
	}

	// logger.Infof("Received report response: %+v", resp)

	printReport(resp)
}

func printReport(report *pb.ReportResponse) {
	var output string
	output += "=== Report Summary ===\n"

	// Answer 1: Movies
	output += "\n[1] Movies:\n"
	for _, movie := range report.Answer1.GetMovies() {
		output += fmt.Sprintf(" - ID: %d | Title: %s | Genres: %v\n", movie.GetId(), movie.GetTitle(), movie.GetGenres())
	}

	// Answer 2: Budgets by Country
	output += "\n[2] Budgets by Country:\n"
	for _, country := range report.Answer2.GetCountries() {
		output += fmt.Sprintf(" - %s: $%d\n", country.GetName(), country.GetBudget())
	}

	// Answer 3: Rating Extremes
	output += "\n[3] Rating Extremes:\n"
	output += fmt.Sprintf(" - Highest: ID %d | Title: %s | Rating: %.2f\n",
		report.Answer3.GetMin().GetId(), report.Answer3.GetMin().GetTitle(), report.Answer3.GetMin().GetRating())
	output += fmt.Sprintf(" - Lowest: ID %d | Title: %s | Rating: %.2f\n",
		report.Answer3.GetMax().GetId(), report.Answer3.GetMax().GetTitle(), report.Answer3.GetMax().GetRating())

	// Answer 4: Actors
	output += "\n[4] Frequent Actors:\n"
	for _, actor := range report.Answer4.GetActors() {
		output += fmt.Sprintf(" - Name: %s | Appearances: %d\n", actor.GetName(), actor.GetCount())
	}

	// Answer 5: Sentiment
	output += "\n[5] Sentiment Analysis:\n"
	output += fmt.Sprintf(" - Positive (%s): %.2f\n", report.Answer5.GetPositive().GetType(), report.Answer5.GetPositive().GetScore())
	output += fmt.Sprintf(" - Negative (%s): %.2f\n", report.Answer5.GetNegative().GetType(), report.Answer5.GetNegative().GetScore())

	// Print to console
	fmt.Println(output)

	// Save to file
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	filename := fmt.Sprintf("/app/report_%s.txt", timestamp)
	err := os.WriteFile(filename, []byte(output), 0644)
	if err != nil {
		fmt.Printf("Failed to write report to file: %v\n", err)
	} else {
		fmt.Printf("Report saved to %s\n", filename)
	}
}
