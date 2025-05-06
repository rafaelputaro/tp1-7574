package main

import (
	"context"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"net"
	"time"
	"tp1/globalconfig"
	pb "tp1/protobuf/protopb"
	"tp1/rabbitmq"
	"tp1/server/report/internal"
)

var logger = logging.MustGetLogger("report")

type ReportGenerator struct {
	pb.UnimplementedReportServiceServer
	ch *amqp.Channel
	rr *internal.ReportRegistry
}

func NewReportGenerator(ch *amqp.Channel, rr *internal.ReportRegistry) *ReportGenerator {
	return &ReportGenerator{ch: ch, rr: rr}
}

func (r *ReportGenerator) StartConsuming() {
	queues := []queueConfig{
		{
			Name:    globalconfig.Report1Queue,
			IsEOF:   isMoviesEOF,
			Process: processMovies,
		},
		{
			Name:    globalconfig.Report2Queue,
			IsEOF:   isTop5EOF,
			Process: processTop5,
		},
		{
			Name:    globalconfig.Report3Queue,
			IsEOF:   isTopAndBottomEOF,
			Process: processTopAndBottom,
		},
		{
			Name:    globalconfig.Report4Queue,
			IsEOF:   isTop10EOF,
			Process: processTop10,
		},
		{
			Name:    globalconfig.Report5Queue,
			IsEOF:   isMetricsEOF,
			Process: processMetrics,
		},
	}

	for _, q := range queues {
		go func(config queueConfig) {
			msgsCh, err := rabbitmq.ConsumeFromQueue(r.ch, config.Name)
			if err != nil {
				logger.Errorf("failed to consume from %s: %v", config.Name, err)
				return
			}

			for d := range msgsCh {
				/*if config.IsEOF(d.Body) {
					logger.Infof("received EOF for %s", config.Name)
					// break
				}*/
				config.Process(d.Body, r.rr)
			}

		}(q)
	}
}

func (r *ReportGenerator) GetReport(_ context.Context, req *pb.ReportRequest) (*pb.ReportResponse, error) {
	report := r.rr.WaitForReport(req.GetClientId(), 21*time.Second)
	if report == nil {
		return nil, status.Error(codes.DeadlineExceeded, "report not ready")
	}
	return report, nil
}

type queueConfig struct {
	Name    string
	IsEOF   func([]byte) bool
	Process func([]byte, *internal.ReportRegistry)
}

func isMoviesEOF(data []byte) bool {
	var movie pb.MovieSanit
	_ = proto.Unmarshal(data, &movie)
	return movie.GetEof()
}

func processMovies(data []byte, rr *internal.ReportRegistry) {

	answer1 := pb.Answer1{}

	var movie pb.MovieSanit
	_ = proto.Unmarshal(data, &movie)

	entry := pb.MovieEntry{
		Id:     movie.Id,
		Title:  movie.Title,
		Genres: movie.Genres,
	}

	answer1.Movies = append(answer1.Movies, &entry)

	logger.Infof("Adding answer1 for client %s: %v", movie.GetClientId(), &answer1)
	rr.AddAnswer1(movie.GetClientId(), &answer1)
}

func isTop5EOF(data []byte) bool {
	var country pb.Top5Country
	_ = proto.Unmarshal(data, &country)
	return country.GetEof()
}

func processTop5(data []byte, rr *internal.ReportRegistry) {
	answer2 := pb.Answer2{}

	var top5 pb.Top5Country
	_ = proto.Unmarshal(data, &top5)

	for i := 0; i < len(top5.ProductionCountries); i++ {
		entry := pb.CountryEntry{
			Name:   &top5.ProductionCountries[i],
			Budget: &top5.Budget[i],
		}

		answer2.Countries = append(answer2.Countries, &entry)
	}

	logger.Infof("Adding answer2 for client %s: %v", top5.GetClientId(), &answer2)
	rr.AddAnswer2(top5.GetClientId(), &answer2)
}

func isTopAndBottomEOF(data []byte) bool {
	var d pb.TopAndBottomRatingAvg
	_ = proto.Unmarshal(data, &d)
	return d.GetEof()
}

func processTopAndBottom(data []byte, rr *internal.ReportRegistry) {
	answer3 := pb.Answer3{}

	var ratingAvg pb.TopAndBottomRatingAvg
	_ = proto.Unmarshal(data, &ratingAvg)

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

	logger.Infof("Adding answer3 for client %s: %v", ratingAvg.GetClientId(), &answer3)
	rr.AddAnswer3(ratingAvg.GetClientId(), &answer3)
}

func isTop10EOF(data []byte) bool {
	var d pb.Top10
	_ = proto.Unmarshal(data, &d)
	return d.GetEof()
}

func processTop10(data []byte, rr *internal.ReportRegistry) {
	answer4 := pb.Answer4{}

	var top10 pb.Top10
	_ = proto.Unmarshal(data, &top10)

	for i := 0; i < len(top10.GetNames()); i++ {
		entry := pb.ActorEntry{
			Id:    proto.Int64(int64(i)),
			Name:  proto.String(top10.Names[i]),
			Count: proto.Int64(top10.CountMovies[i]),
		}

		answer4.Actors = append(answer4.Actors, &entry)
	}

	logger.Infof("Adding answer4 for client %s: %v", top10.GetClientId(), &answer4)
	rr.AddAnswer4(top10.GetClientId(), &answer4)
}

func isMetricsEOF(data []byte) bool {
	var d pb.Metrics
	_ = proto.Unmarshal(data, &d)
	return d.GetEof()
}

func processMetrics(data []byte, rr *internal.ReportRegistry) {
	answer5 := pb.Answer5{}

	var metrics pb.Metrics
	_ = proto.Unmarshal(data, &metrics)

	positive := pb.SentimentScore{
		Type:  proto.String("positive"),
		Score: proto.Float32(float32(metrics.GetAvgRevenueOverBudgetPositive())),
	}
	answer5.Positive = &positive

	negative := pb.SentimentScore{
		Type:  proto.String("negative"),
		Score: proto.Float32(float32(metrics.GetAvgRevenueOverBudgetNegative())),
	}
	answer5.Positive = &negative

	logger.Infof("Adding answer5 for client %s: %v", metrics.GetClientId(), &answer5)
	rr.AddAnswer5(metrics.GetClientId(), &answer5)
}

func main() {
	conn, err := rabbitmq.ConnectRabbitMQ(logger)
	if err != nil {
		logger.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitmq.ShutdownConnection(conn)

	ch, err := conn.Channel()
	if err != nil {
		logger.Fatalf("Failed to open RabbitMQ channel: %v", err)
	}
	defer rabbitmq.ShutdownChannel(ch)

	err = rabbitmq.DeclareDirectQueues(ch, globalconfig.ReportQueues...)
	if err != nil {
		logger.Fatalf("Failed to declare movies: %v", err)
	}

	grpcServer := grpc.NewServer()
	reportGenerator := NewReportGenerator(ch, internal.NewReportRegistry())
	pb.RegisterReportServiceServer(grpcServer, reportGenerator)

	reportGenerator.StartConsuming()

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}

	logger.Infof("Report generator listening on :50052")
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve gRPC: %v", err)
	}
}
