package internal

import (
	"encoding/json"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"io"
	"log"
	"strconv"
	"strings"
	pb "tp1/protobuf/protopb"

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

		sanitized, err := sanitizeMovie(msg)
		if err != nil {
			log.Printf("skipping invalid movie (id %d): %v", msg.GetId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
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

		sanitized, err := sanitizeRating(rating)
		if err != nil {
			log.Printf("skipping invalid rating (movie id %d): %v", rating.GetMovieId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
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

		sanitized, err := sanitizeCredit(credit)
		if err != nil {
			log.Printf("skipping invalid credit (id %d): %v", credit.GetId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
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

func sanitizeMovie(m *pb.Movie) (*pb.MovieSanit, error) {
	// Release Year
	var releaseYear uint32
	if date := m.GetReleaseDate(); date != "" {
		parts := strings.Split(date, "-")
		if len(parts) == 0 {
			return nil, fmt.Errorf("invalid release_date: %s", date)
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
	jsonGenres := strings.ReplaceAll(m.GetGenres(), `'`, `"`)
	var genresParsed []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(jsonGenres), &genresParsed); err != nil {
		return nil, fmt.Errorf("invalid genres: %v", err)
	}
	var genres []string
	for _, g := range genresParsed {
		genres = append(genres, strings.TrimSpace(g.Name))
	}

	// Production Countries
	jsonCountries := strings.ReplaceAll(m.GetProductionCountries(), `'`, `"`)
	var countriesParsed []struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal([]byte(jsonCountries), &countriesParsed); err != nil {
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
	jsonCast := strings.ReplaceAll(c.GetCast(), `'`, `"`)
	var parsedCast []struct {
		Name        string `json:"name"`
		ProfilePath string `json:"profile_path"`
	}
	if err := json.Unmarshal([]byte(jsonCast), &parsedCast); err != nil {
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
