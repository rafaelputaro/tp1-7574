package internal

import (
	"context"
	"github.com/op/go-logging"
	"io"
	pb "tp1/protobuf/protopb"
)

var logger = logging.MustGetLogger("client")

func SendMovies(ctx context.Context, client pb.MovieServiceClient, parser Parser[pb.Movie]) {
	count := 0

	stream, err := RetryWithBackoff(
		func() (pb.MovieService_StreamMoviesClient, error) {
			return client.StreamMovies(ctx)
		})
	if err != nil {
		logger.Fatalf("Failed to open movies stream after retries: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Infof("Context canceled, stopping movies stream")
			stream.CloseSend()
			return
		default:
		}

		batch, err := parser.NextBatch()
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Failed get next batch for movies: %v", err)
			} else {
				logger.Infof("End of movies stream")
			}
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

func SendRatings(ctx context.Context, client pb.RatingServiceClient, parser Parser[pb.Rating]) {
	count := 0

	stream, err := RetryWithBackoff(
		func() (pb.RatingService_StreamRatingsClient, error) {
			return client.StreamRatings(ctx)
		})
	if err != nil {
		logger.Fatalf("Failed to open ratings stream after retries: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Infof("Context canceled, stopping ratings stream")
			stream.CloseSend()
			return
		default:
		}

		batch, err := parser.NextBatch()
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Failed get next batch for ratings: %v", err)
			} else {
				logger.Infof("End of ratings stream")
			}
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

func SendCredits(ctx context.Context, client pb.CreditServiceClient, parser Parser[pb.Credit]) {
	count := 0

	stream, err := RetryWithBackoff(
		func() (pb.CreditService_StreamCreditsClient, error) {
			return client.StreamCredits(ctx)
		})
	if err != nil {
		logger.Fatalf("Failed to open credits stream after retries: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			logger.Infof("Context canceled, stopping credits stream")
			stream.CloseSend()
			return
		default:
		}

		batch, err := parser.NextBatch()
		if err != nil {
			if err != io.EOF {
				logger.Errorf("Failed get next batch for credits: %v", err)
			} else {
				logger.Infof("End of credits stream")
			}
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
