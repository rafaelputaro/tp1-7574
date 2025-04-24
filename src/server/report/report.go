package main

import (
	"context"
	"fmt"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"sync"
	pb "tp1/protobuf/protopb"
	"tp1/rabbitmq"
)

var logger = logging.MustGetLogger("report")

type ReportGenerator struct {
	pb.UnimplementedReportServiceServer
	ch *amqp.Channel
}

func NewReportGenerator(ch *amqp.Channel) *ReportGenerator {
	return &ReportGenerator{ch: ch}
}

func (r *ReportGenerator) GetReport(_ context.Context, _ *emptypb.Empty) (*pb.ReportResponse, error) {
	queues := []queueConfig{
		{
			Name:    "movies_report",
			IsEOF:   isMoviesEOF,
			Process: processMovies,
		},
		{
			Name:    "top_5_report",
			IsEOF:   isTop5EOF,
			Process: processTop5,
		},
		{
			Name:    "top_and_bottom_report",
			IsEOF:   isTopAndBottomEOF,
			Process: processTopAndBottom,
		},
		/*{
			Name:    "top_10_report",
			IsEOF:   isTopAndBottomEOF,
			Process: processTopAndBottom,
		},
		{
			Name:    "",
			IsEOF:   isTopAndBottomEOF,
			Process: processTopAndBottom,
		},*/
	}

	response := pb.ReportResponse{}
	wg := sync.WaitGroup{}

	for _, q := range queues {
		wg.Add(1)
		go func(config queueConfig) {
			defer wg.Done()

			msgsCh, err := r.ch.Consume(config.Name, "", true, false, false, false, nil)
			if err != nil {
				logger.Errorf("failed to consume from %s: %v", config.Name, err)
				return
			}

			var allMsgs [][]byte

			for d := range msgsCh {
				if config.IsEOF(d.Body) {
					logger.Infof("received EOF for %s", config.Name)
					break
				}
				allMsgs = append(allMsgs, d.Body)
			}

			config.Process(allMsgs, &response)
		}(q)
	}

	wg.Wait()

	return &pb.ReportResponse{
		Answer1: response.Answer1,
		Answer2: response.Answer2,
		Answer3: response.Answer3,
		Answer4: &pb.Answer4{
			Actors: []*pb.ActorEntry{
				{Id: proto.Int64(123), Name: proto.String("Keanu Reeves"), Count: proto.Int32(3)},
			},
		},
		Answer5: &pb.Answer5{
			Positive: &pb.SentimentScore{Type: proto.String("positive"), Score: proto.Float32(0.82)},
			Negative: &pb.SentimentScore{Type: proto.String("negative"), Score: proto.Float32(0.13)},
		},
	}, nil

}

type queueConfig struct {
	Name    string
	IsEOF   func([]byte) bool
	Process func([][]byte, *pb.ReportResponse)
}

func isMoviesEOF(data []byte) bool {
	var movie pb.MovieSanit
	_ = proto.Unmarshal(data, &movie)
	return movie.GetEof()
}

func processMovies(data [][]byte, response *pb.ReportResponse) {

	answer1 := pb.Answer1{}

	for _, d := range data {
		var movie pb.MovieSanit
		_ = proto.Unmarshal(d, &movie)

		entry := pb.MovieEntry{
			Id:     movie.Id,
			Title:  movie.Title,
			Genres: movie.Genres,
		}

		answer1.Movies = append(answer1.Movies, &entry)
	}

	response.Answer1 = &answer1
}

func isTop5EOF(data []byte) bool {
	var country pb.Top5Country
	_ = proto.Unmarshal(data, &country)
	return country.GetEof()
}

func processTop5(data [][]byte, response *pb.ReportResponse) {
	answer2 := pb.Answer2{}

	for _, d := range data {
		var country pb.Top5Country
		_ = proto.Unmarshal(d, &country)

		for i := 0; i < len(country.ProductionCountries); i++ {
			entry := pb.CountryEntry{
				Name:   &country.ProductionCountries[i],
				Budget: &country.Budget[i],
			}

			answer2.Countries = append(answer2.Countries, &entry)
		}
	}

	response.Answer2 = &answer2
}

func isTopAndBottomEOF(data []byte) bool {
	var d pb.TopAndBottomRatingAvg
	_ = proto.Unmarshal(data, &d)
	return d.GetEof()
}

func processTopAndBottom(data [][]byte, response *pb.ReportResponse) {
	answer3 := pb.Answer3{}

	for _, d := range data {
		var ratingAvg pb.TopAndBottomRatingAvg
		_ = proto.Unmarshal(d, &ratingAvg)

		minRating := pb.MovieRating{
			Id:     proto.Int32(0),
			Title:  ratingAvg.TitleTop,
			Rating: ratingAvg.RatingAvgTop,
		}

		answer3.Min = &minRating

		maxRating := pb.MovieRating{
			Id:     proto.Int32(1),
			Title:  ratingAvg.TitleBottom,
			Rating: ratingAvg.RatingAvgBottom,
		}

		answer3.Max = &maxRating
	}

	response.Answer3 = &answer3
}

func main() {
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

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		panic(fmt.Errorf("failed to listen: %v", err))
	}

	grpcServer := grpc.NewServer()
	pb.RegisterReportServiceServer(grpcServer, NewReportGenerator(ch))

	logger.Infof("Report generator listening on :50052")
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve gRPC: %v", err)
	}
}
