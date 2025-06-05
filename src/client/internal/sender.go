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

	ValidMovieIDs = make(map[int64]struct{})

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

			ValidMovieIDs[movieID] = struct{}{}

			protoUtils.SetMessageIdMovie(item, int64(count))
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send movie: %v", err)
			}
		}
		count += len(batch)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		logger.Errorf("movie stream close error: %v", err)
	}

	logger.Infof("Processed %d movies, collected %d unique movie IDs", count, len(ValidMovieIDs)) // todo we are sending movies with repeated ids
}

func SendRatings(ctx context.Context, client pb.RatingServiceClient, parser Parser[pb.Rating]) {
	count := 0
	totalRatings := 0

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
		totalRatings += len(batch)

		for _, item := range batch {
			movieID := item.GetMovieId()
			if _, exists := ValidMovieIDs[movieID]; !exists {
				continue
			}

			protoUtils.SetMessageIdRating(item, int64(count))
			filteredBatch = append(filteredBatch, item)
			count++
		}

		for _, item := range filteredBatch {
			if err := stream.Send(item); err != nil {
				logger.Errorf("failed to send rating: %v", err)
			}
		}

		if totalRatings%1000000 == 0 {
			logger.Infof("Progress: processed %d ratings", totalRatings)
		}
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		logger.Errorf("rating stream close error: %v", err)
	}
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

		for _, item := range batch {
			protoUtils.SetMessageIdCredit(item, int64(count))
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
