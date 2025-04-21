package internal

import (
	"context"
	"github.com/op/go-logging"
	"time"
	pb "tp1/protobuf/protopb"
)

var logger = logging.MustGetLogger("client")

func SendMovies(client pb.MovieServiceClient, parser Parser[pb.Movie]) {
	count := 0

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamMovies(ctx)
	if err != nil {
		logger.Fatalf("failed to open movie stream: %v", err)
	}

	for {
		batch, err := parser.NextBatch()
		if err != nil {
			break
		}
		for _, item := range batch {
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send movie: %v", err)
			}
		}
		count += len(batch)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		logger.Errorf("movie stream close error: %v", err)
	}

	logger.Infof("Sent %d movies", count)
}

func SendRatings(client pb.RatingServiceClient, parser Parser[pb.Rating]) {
	count := 0

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamRatings(ctx)
	if err != nil {
		logger.Fatalf("failed to open ratings stream: %v", err)
	}

	for {
		batch, err := parser.NextBatch()
		if err != nil {
			break
		}
		for _, item := range batch {
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send rating: %v", err)
			}
		}
		count += len(batch)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		logger.Errorf("rating stream close error: %v", err)
	}

	logger.Infof("Sent %d ratings", count)
}

func SendCredits(client pb.CreditServiceClient, parser Parser[pb.Credit]) {
	count := 0

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	stream, err := client.StreamCredits(ctx)
	if err != nil {
		logger.Fatalf("failed to open credits stream: %v", err)
	}
	for {
		batch, err := parser.NextBatch()
		if err != nil {
			break
		}
		for _, item := range batch {
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send credit: %v", err)
			}
		}
		count += len(batch)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		logger.Errorf("credit stream close error: %v", err)
	}

	logger.Infof("Sent %d credits", count)
}
