package internal

import (
	"context"
	"log"
	"time"
	pb "tp1/protobuf/protopb"
)

func SendMovies(client pb.MovieServiceClient, parser Parser[pb.Movie]) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamMovies(ctx)
	if err != nil {
		log.Fatalf("failed to open movie stream: %v", err)
	}

	for {
		batch, err := parser.NextBatch()
		if err != nil {
			break
		}
		for _, item := range batch {
			if err := stream.Send(item); err != nil {
				log.Printf("failed to send movie: %v", err)
			}
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		log.Printf("movie stream close error: %v", err)
	}
}

func SendRatings(client pb.RatingServiceClient, parser Parser[pb.Rating]) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamRatings(ctx)
	if err != nil {
		log.Fatalf("failed to open ratings stream: %v", err)
	}

	for {
		batch, err := parser.NextBatch()
		if err != nil {
			break
		}
		for _, item := range batch {
			if err := stream.Send(item); err != nil {
				log.Printf("failed to send rating: %v", err)
			}
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		log.Printf("rating stream close error: %v", err)
	}
}

func SendCredits(client pb.CreditServiceClient, parser Parser[pb.Credit]) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamCredits(ctx)
	if err != nil {
		log.Fatalf("failed to open credits stream: %v", err)
	}
	for {
		batch, err := parser.NextBatch()
		if err != nil {
			break
		}
		for _, item := range batch {
			if err := stream.Send(item); err != nil {
				log.Printf("failed to send credit: %v", err)
			}
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		log.Printf("credit stream close error: %v", err)
	}
}
