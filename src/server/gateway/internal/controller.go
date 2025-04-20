package internal

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"io"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"tp1/protobuf/protopb"
)

type Controller struct {
	protopb.UnimplementedMovieServiceServer
	protopb.UnimplementedRatingServiceServer
	protopb.UnimplementedCreditServiceServer
	ch *amqp.Channel
}

func NewController(ch *amqp.Channel) *Controller {
	return &Controller{ch: ch}
}

func (c *Controller) StreamMovies(stream protopb.MovieService_StreamMoviesServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		data, err := proto.Marshal(msg)
		if err != nil {
			return err
		}

		err = c.ch.Publish(
			"", "movies", false, false,
			amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        data,
			},
		)
		if err != nil {
			return err
		}
	}
}

func (c *Controller) StreamRatings(stream protopb.RatingService_StreamRatingsServer) error {
	for {
		rating, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		data, err := proto.Marshal(rating)
		if err != nil {
			return err
		}

		err = c.ch.Publish(
			"", "ratings", false, false,
			amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        data,
			},
		)
		if err != nil {
			return err
		}
	}
}

func (c *Controller) StreamCredits(stream protopb.CreditService_StreamCreditsServer) error {
	for {
		credit, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		data, err := proto.Marshal(credit)
		if err != nil {
			return err
		}

		err = c.ch.Publish(
			"", "credits", false, false,
			amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        data,
			},
		)
		if err != nil {
			return err
		}
	}
}
