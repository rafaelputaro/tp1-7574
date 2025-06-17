package internal

import (
	"context"
	"io"
	pb "tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"

	"github.com/op/go-logging"
)

var (
	logger        = logging.MustGetLogger("client")
	ValidMovieIDs = make(map[int64]struct{})
)

func SendMovies(ctx context.Context, client pb.MovieServiceClient, parser Parser[pb.Movie]) {
	count := 0

	stream, err := RetryWithBackoff(
		func() (pb.MovieService_StreamMoviesClient, error) {
			return client.StreamMovies(ctx)
		})
	if err != nil {
		logger.Fatalf("Failed to open movies stream after retries: %v", err)
	}

	ValidMovieIDs = make(map[int64]struct{}, parser.GetSize())

	for {
		select {
		case <-ctx.Done():
			logger.Infof("Context canceled, stopping movies stream")
			_ = stream.CloseSend()
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
			movieID := int64(item.GetId())

			if _, exists := ValidMovieIDs[movieID]; exists {
				continue
			}

			ValidMovieIDs[movieID] = struct{}{}

			protoUtils.SetMessageIdMovie(item, int64(count))
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send movie: %v", err)
			}
			count++
		}
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

	filteredBatch := make([]*pb.Rating, 0, parser.GetSize())

	for {
		select {
		case <-ctx.Done():
			logger.Infof("Context canceled, stopping ratings stream")
			_ = stream.CloseSend()
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

		filteredBatch = filteredBatch[:0]

		for _, item := range batch {
			movieID := item.GetMovieId()
			if _, exists := ValidMovieIDs[movieID]; !exists {
				continue
			}

			protoUtils.SetMessageIdRating(item, int64(count))
			filteredBatch = append(filteredBatch, item)
			count++

			if count%1000000 == 0 {
				logger.Infof("Progress: sent %d ratings", count)
			}
		}

		for _, item := range filteredBatch {
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send rating: %v", err)
			}
		}
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

	// Track which movie IDs we've already processed to avoid duplicates
	processedMovieIDs := make(map[int64]struct{}, parser.GetSize())

	filteredBatch := make([]*pb.Credit, 0, parser.GetSize())

	for {
		select {
		case <-ctx.Done():
			logger.Infof("Context canceled, stopping credits stream")
			_ = stream.CloseSend()
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

		filteredBatch = filteredBatch[:0]

		for _, item := range batch {
			movieID := item.GetId()

			// Skip credits for non-existent movie IDs
			if _, exists := ValidMovieIDs[movieID]; !exists {
				continue
			}

			// Skip duplicate credits for the same movie ID
			if _, exists := processedMovieIDs[movieID]; exists {
				continue
			}

			// Mark this movie ID as processed
			processedMovieIDs[movieID] = struct{}{}

			protoUtils.SetMessageIdCredit(item, int64(count))
			filteredBatch = append(filteredBatch, item)
			count++
		}

		for _, item := range filteredBatch {
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send credit: %v", err)
			}
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		logger.Errorf("credit stream close error: %v", err)
	}

	logger.Infof("Sent %d credits", count)
}
