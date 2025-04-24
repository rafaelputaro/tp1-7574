package internal

import (
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"strconv"
	"strings"
	pb "tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var logger = logging.MustGetLogger("controller")

type Controller struct {
	pb.UnimplementedMovieServiceServer
	pb.UnimplementedRatingServiceServer
	pb.UnimplementedCreditServiceServer
	ch *amqp.Channel
}

func NewController(ch *amqp.Channel) *Controller {
	return &Controller{ch: ch}
}

func (c *Controller) StreamMovies(stream pb.MovieService_StreamMoviesServer) error {
	count := 0

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			err := c.publishMovieEof()
			if err != nil {
				logger.Errorf("failed to publish movie EOF message: %v", err)
				return err
			}
			logger.Infof("StreamMovies: published %d movies to 'movies_exchange'", count)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		sanitized, err := sanitizeMovie(msg)
		if err != nil {
			// logger.Infof("skipping invalid movie (id %d): %v", msg.GetId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
		if err != nil {
			return err
		}

		err = c.publish("movies_exchange", data)
		if err != nil {
			return err
		}

		count++
	}
}

func (c *Controller) StreamRatings(stream pb.RatingService_StreamRatingsServer) error {
	count := 0

	for {
		rating, err := stream.Recv()
		if err == io.EOF {
			err := c.publishRatingEof()
			if err != nil {
				logger.Errorf("failed to publish rating EOF message: %v", err)
				return err
			}
			logger.Infof("StreamRatings: published %d ratings to 'ratings_exchange'", count)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		sanitized, err := sanitizeRating(rating)
		if err != nil {
			logger.Infof("skipping invalid rating (movie id %d): %v", rating.GetMovieId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
		if err != nil {
			return err
		}

		err = c.publish("ratings_exchange", data)
		if err != nil {
			return err
		}

		count++
	}
}

func (c *Controller) StreamCredits(stream pb.CreditService_StreamCreditsServer) error {
	count := 0

	for {
		credit, err := stream.Recv()
		if err == io.EOF {
			err := c.publishCreditEof()
			if err != nil {
				logger.Errorf("failed to publish credit EOF message: %v", err)
				return err
			}
			logger.Infof("StreamCredits: published %d credits to 'credits_exchange'", count)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		sanitized, err := sanitizeCredit(credit)
		if err != nil {
			// logger.Infof("skipping invalid credit (movie id %d): %v", credit.GetId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
		if err != nil {
			return err
		}

		err = c.publish("credits_exchange", data)
		if err != nil {
			return err
		}

		count++
	}
}

func (c *Controller) publish(exchange string, data []byte) error {
	return c.ch.Publish(
		exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/protobuf",
			Body:        data,
		},
	)
}

func (c *Controller) publishMovieEof() error {
	data, err := proto.Marshal(&pb.MovieSanit{
		Budget:      proto.Int32(0),
		Id:          proto.Int32(0),
		Overview:    proto.String(""),
		ReleaseYear: proto.Uint32(0),
		Revenue:     proto.Float64(0),
		Title:       proto.String(""),
		Eof:         proto.Bool(true),
	})
	if err != nil {
		return err
	}

	err = c.publish("movies_exchange", data)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) publishCreditEof() error {
	data, err := proto.Marshal(&pb.CreditSanit{
		Id:  proto.Int64(0),
		Eof: proto.Bool(true),
	})
	if err != nil {
		return err
	}

	err = c.publish("credits_exchange", data)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) publishRatingEof() error {
	data, err := proto.Marshal(&pb.RatingSanit{
		MovieId: proto.Int64(0),
		Rating:  proto.Float32(0),
		Eof:     proto.Bool(true),
	})
	if err != nil {
		return err
	}

	err = c.publish("ratings_exchange", data)
	if err != nil {
		return err
	}

	return nil
}

func sanitizeMovie(m *pb.Movie) (*pb.MovieSanit, error) {
	// Release Year
	var releaseYear uint32
	if date := m.GetReleaseDate(); date != "" {
		parts := strings.Split(date, "-")
		if len(parts) < 1 || len(parts[0]) != 4 {
			return nil, fmt.Errorf("invalid release_date format: %s", date)
		}
		yr, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid release year format: %v", err)
		}
		releaseYear = uint32(yr)
	} else {
		return nil, fmt.Errorf("missing release_date")
	}

	// Genres
	genresJson := NormalizeJSON(m.GetGenres())
	var genresParsed []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(genresJson), &genresParsed); err != nil {
		return nil, fmt.Errorf("invalid genres: %v", err)
	}
	var genres []string
	for _, g := range genresParsed {
		genres = append(genres, strings.TrimSpace(g.Name))
	}

	// Production Countries
	countriesJson := NormalizeJSON(m.GetProductionCountries())
	var countriesParsed []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(countriesJson), &countriesParsed); err != nil {
		return nil, fmt.Errorf("invalid production_countries: %v", err)
	}
	var countries []string
	for _, c := range countriesParsed {
		countries = append(countries, strings.TrimSpace(c.Name))
	}

	return &pb.MovieSanit{
		Budget:              m.Budget,
		Genres:              genres,
		Id:                  m.Id,
		Overview:            proto.String(strings.TrimSpace(m.GetOverview())),
		ProductionCountries: countries,
		ReleaseYear:         proto.Uint32(releaseYear),
		Revenue:             m.Revenue,
		Title:               proto.String(strings.TrimSpace(m.GetTitle())),
		Eof:                 m.Eof,
	}, nil
}

func sanitizeRating(r *pb.Rating) (*pb.RatingSanit, error) {
	if r.GetRating() < 0.0 || r.GetRating() > 5.0 {
		return nil, fmt.Errorf("invalid rating value: %v", r.GetRating())
	}

	return &pb.RatingSanit{
		MovieId: r.MovieId,
		Rating:  r.Rating,
		Eof:     r.Eof,
	}, nil
}

func sanitizeCredit(c *pb.Credit) (*pb.CreditSanit, error) {
	castJson := NormalizeJSON(c.GetCast())
	var parsedCast []struct {
		Name        string `json:"name"`
		ProfilePath string `json:"profile_path"`
	}
	if err := json.Unmarshal([]byte(castJson), &parsedCast); err != nil {
		return nil, fmt.Errorf("invalid cast: %v", err)
	}

	var names []string
	var profiles []string
	for _, actor := range parsedCast {
		name := strings.TrimSpace(actor.Name)
		profile := strings.TrimSpace(actor.ProfilePath)
		if name != "" && profile != "" {
			names = append(names, name)
			profiles = append(profiles, profile)
		}
	}

	return &pb.CreditSanit{
		CastNames:    names,
		ProfilePaths: profiles,
		Id:           c.Id,
		Eof:          c.Eof,
	}, nil
}
