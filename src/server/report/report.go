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
	"tp1/globalconfig"
	"tp1/health"
	pb "tp1/protobuf/protopb"
	"tp1/rabbitmq"
	"tp1/server/report/internal"
)

var logger = logging.MustGetLogger("report")

type ReportGenerator struct {
	pb.UnimplementedReportServiceServer
	ch *amqp.Channel
	rr *internal.ResilientReportRegistry
}

func NewReportGenerator(ch *amqp.Channel, rr *internal.ResilientReportRegistry) *ReportGenerator {
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
				logger.Fatalf("failed to consume from %s: %v", config.Name, err)
				return
			}

			for d := range msgsCh {
				err := rabbitmq.SingleAck(d)
				if err != nil {
					logger.Fatalf("failed to ack message: %v", err)
				}

				if config.IsEOF(d.Body, r.rr) {
					continue
				}
				config.Process(d.Body, r.rr)
			}

		}(q)
	}
}

func (r *ReportGenerator) GetReport(_ context.Context, req *pb.ReportRequest) (*pb.ReportResponse, error) {
	logger.Infof("[client_id:%s] waiting for report", req.GetClientId())
	report := r.rr.WaitForReport(req.GetClientId())
	if report == nil {
		return nil, status.Error(codes.DeadlineExceeded, "report not ready")
	}
	logger.Infof("[client_id:%s] returning response: %v", req.GetClientId(), report)
	return report, nil
}

type queueConfig struct {
	Name    string
	IsEOF   func([]byte, *internal.ResilientReportRegistry) bool
	Process func([]byte, *internal.ResilientReportRegistry)
}

func isMoviesEOF(data []byte, rr *internal.ResilientReportRegistry) bool {
	var movie pb.MovieSanit
	_ = proto.Unmarshal(data, &movie)

	eof := movie.GetEof()
	if eof {
		logger.Infof("[client_id:%s] received answer1 EOF", movie.GetClientId())
		rr.DoneAnswer(movie.GetClientId())
	}

	return eof
}

func processMovies(data []byte, rr *internal.ResilientReportRegistry) {

	var movie pb.MovieSanit
	_ = proto.Unmarshal(data, &movie)

	entry := pb.MovieEntry{
		Id:     movie.Id,
		Title:  movie.Title,
		Genres: movie.Genres,
	}

	logger.Infof("[client_id:%s] adding answer1: %v", movie.GetClientId(), &entry)
	rr.AddToAnswer1(movie.GetClientId(), &entry)
}

func isTop5EOF(data []byte, rr *internal.ResilientReportRegistry) bool {
	var country pb.Top5Country
	_ = proto.Unmarshal(data, &country)

	eof := country.GetEof()
	if eof {
		logger.Infof("[client_id:%s] received answer2 EOF", country.GetClientId())
		rr.DoneAnswer(country.GetClientId())
	}

	return eof
}

func processTop5(data []byte, rr *internal.ResilientReportRegistry) {
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

	logger.Infof("[client_id:%s] Adding answer2: %v", top5.GetClientId(), &answer2)
	rr.AddAnswer2(top5.GetClientId(), &answer2)
}

func isTopAndBottomEOF(data []byte, rr *internal.ResilientReportRegistry) bool {
	var ratingAvg pb.TopAndBottomRatingAvg
	_ = proto.Unmarshal(data, &ratingAvg)

	eof := ratingAvg.GetEof()
	if eof {
		logger.Infof("[client_id:%s] received answer3 EOF", ratingAvg.GetClientId())
		rr.DoneAnswer(ratingAvg.GetClientId())
	}

	return eof
}

func processTopAndBottom(data []byte, rr *internal.ResilientReportRegistry) {
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

	logger.Infof("[client_id:%s] adding answer3: %v", ratingAvg.GetClientId(), &answer3)
	rr.AddAnswer3(ratingAvg.GetClientId(), &answer3)
}

func isTop10EOF(data []byte, rr *internal.ResilientReportRegistry) bool {
	var top10 pb.Top10
	_ = proto.Unmarshal(data, &top10)

	eof := top10.GetEof()
	if eof {
		logger.Infof("[client_id:%s] received answer4 EOF", top10.GetClientId())
		rr.DoneAnswer(top10.GetClientId())
	}

	return eof
}

func processTop10(data []byte, rr *internal.ResilientReportRegistry) {
	answer4 := pb.Answer4{}

	var top10 pb.Top10
	_ = proto.Unmarshal(data, &top10)

	for i := 0; i < len(top10.GetNames()); i++ {
		entry := pb.ActorEntry{
			Name:  proto.String(top10.Names[i]),
			Count: proto.Int64(top10.CountMovies[i]),
		}

		answer4.Actors = append(answer4.Actors, &entry)
	}

	logger.Infof("[client_id:%s] adding answer4: %v", top10.GetClientId(), &answer4)
	rr.AddAnswer4(top10.GetClientId(), &answer4)
}

func isMetricsEOF(data []byte, rr *internal.ResilientReportRegistry) bool {
	var metrics pb.Metrics
	_ = proto.Unmarshal(data, &metrics)

	eof := metrics.GetEof()
	if eof {
		logger.Infof("[client_id:%s] received answer5 EOF", metrics.GetClientId())
		rr.DoneAnswer(metrics.GetClientId())
	}

	return eof
}

func processMetrics(data []byte, rr *internal.ResilientReportRegistry) {
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
	answer5.Negative = &negative

	logger.Infof("[client_id:%s] adding answer5: %v", metrics.GetClientId(), &answer5)
	rr.AddAnswer5(metrics.GetClientId(), &answer5)
}

func main() {
	healthSrv := health.New(logger)
	healthSrv.Start()

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
	rr := internal.NewResilientReportRegistry("report_service")
	defer rr.Dispose()
	reportGenerator := NewReportGenerator(ch, rr)
	pb.RegisterReportServiceServer(grpcServer, reportGenerator)

	reportGenerator.StartConsuming()

	healthSrv.MarkReady()

	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		logger.Fatalf("Failed to listen: %v", err)
	}

	logger.Infof("Report generator listening on :50052")
	if err := grpcServer.Serve(lis); err != nil {
		logger.Fatalf("Failed to serve gRPC: %v", err)
	}
}
