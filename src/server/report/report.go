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
	queues := []string{"movies_report"}

	type message struct {
		name string
		data []byte
	}
	msgs := make(chan message)
	wg := sync.WaitGroup{}

	for _, q := range queues {
		wg.Add(1)
		go func(queueName string) {
			defer wg.Done()

			msgsCh, err := r.ch.Consume(queueName, "", true, false, false, false, nil)
			if err != nil {
				logger.Errorf("failed to consume from %s: %v", queueName, err)
				return
			}

			for d := range msgsCh {
				if isEOF(d.Body) {
					logger.Infof("received EOF for %s", queueName)
					break
				}
				msgs <- message{name: queueName, data: d.Body}
			}
		}(q)
	}

	go func() {
		wg.Wait()
		close(msgs)
	}()

	for msg := range msgs {
		logger.Infof("received from %s: %d bytes", msg.name, len(msg.data))
	}

	return &pb.ReportResponse{
		Answer1: &pb.Answer1{
			Movies: []*pb.MovieEntry{
				{Id: proto.Int32(1), Title: proto.String("Matrix"), Genres: []string{"Sci-Fi", "Action"}},
			},
		},
		Answer2: &pb.Answer2{
			Countries: []*pb.CountryEntry{
				{Name: proto.String("USA"), Budget: proto.Int64(100000000)},
			},
		},
		Answer3: &pb.Answer3{
			Min: &pb.MovieRating{Id: proto.Int32(2), Title: proto.String("Movie X"), Rating: proto.Float64(1.0)},
			Max: &pb.MovieRating{Id: proto.Int32(3), Title: proto.String("Movie Y"), Rating: proto.Float64(9.5)},
		},
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

func isEOF(data []byte) bool {
	var movie pb.MovieSanit
	_ = proto.Unmarshal(data, &movie)
	return movie.GetEof()
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
