package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"strconv"
	"strings"
	"tp1/globalconfig"
	pb "tp1/protobuf/protopb"
	"tp1/rabbitmq"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	logger = logging.MustGetLogger("controller")
)

type Controller struct {
	pb.UnimplementedMovieServiceServer
	pb.UnimplementedRatingServiceServer
	pb.UnimplementedCreditServiceServer
	pb.UnimplementedControllerServer
	ch             *amqp.Channel
	reportClient   pb.ReportServiceClient
	clientRegistry *ClientRegistry
}

func NewController(ch *amqp.Channel, client pb.ReportServiceClient, registry *ClientRegistry) *Controller {
	return &Controller{ch: ch, reportClient: client, clientRegistry: registry}
}

func (c *Controller) StreamMovies(stream pb.MovieService_StreamMoviesServer) error {
	clientID, err := c.clientRegistry.GetOrCreateClientID(stream.Context())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get client id: %v", err)
	}

	count := 0

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			err := c.publishMovieEof(clientID)
			if err != nil {
				logger.Errorf("failed to publish movie EOF message: %v", err)
				return err
			}
			logger.Infof("[client_id:%s] StreamMovies: published %d movies", clientID, count)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		sanitized, err := sanitizeMovie(msg, clientID)
		if err != nil {
			// logger.Infof("skipping invalid movie (id %d): %v", msg.GetId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
		if err != nil {
			return err
		}

		err = c.publishToQueues(data, globalconfig.MoviesQueues...)
		if err != nil {
			return err
		}

		count++
	}
}

func (c *Controller) StreamRatings(stream pb.RatingService_StreamRatingsServer) error {
	clientID, err := c.clientRegistry.GetOrCreateClientID(stream.Context())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get client id: %v", err)
	}

	count := 0

	for {
		rating, err := stream.Recv()
		if err == io.EOF {
			err := c.publishRatingEof(clientID)
			if err != nil {
				logger.Errorf("failed to publish rating EOF message: %v", err)
				return err
			}
			logger.Infof("[client_id:%s] StreamRatings: published %d ratings", clientID, count)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		sanitized, err := sanitizeRating(rating, clientID)
		if err != nil {
			logger.Infof("skipping invalid rating (movie id %d): %v", rating.GetMovieId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
		if err != nil {
			return err
		}

		err = c.publishToExchange(globalconfig.RatingsExchange, data)
		if err != nil {
			return err
		}

		count++
	}
}

func (c *Controller) StreamCredits(stream pb.CreditService_StreamCreditsServer) error {
	clientID, err := c.clientRegistry.GetOrCreateClientID(stream.Context())
	if err != nil {
		return status.Errorf(codes.Internal, "failed to get client id: %v", err)
	}

	count := 0

	for {
		credit, err := stream.Recv()
		if err == io.EOF {
			err := c.publishCreditEof(clientID)
			if err != nil {
				logger.Errorf("failed to publish credit EOF message: %v", err)
				return err
			}
			logger.Infof("[client_id:%s] StreamCredits: published %d credits", clientID, count)
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}

		sanitized, err := sanitizeCredit(credit, clientID)
		if err != nil {
			// logger.Infof("skipping invalid credit (movie id %d): %v", credit.GetId(), err)
			continue
		}

		data, err := proto.Marshal(sanitized)
		if err != nil {
			return err
		}

		err = c.publishToExchange(globalconfig.CreditsExchange, data)
		if err != nil {
			return err
		}

		count++
	}
}

func (c *Controller) GetReport(ctx context.Context, _ *emptypb.Empty) (*pb.ReportResponse, error) {
	clientID, err := c.clientRegistry.GetOrCreateClientID(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get client id: %v", err)
	}

	logger.Infof("[client_id:%s] getting report", clientID)

	report, err := c.reportClient.GetReport(context.Background(), &pb.ReportRequest{ClientId: &clientID})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get report: %v", err)
	}

	err = c.clientRegistry.MarkAsDone(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to mark client as done: %v", err)
	}

	logger.Infof("[client_id:%s] returning report: %v", clientID, report)
	return report, nil
}

func (c *Controller) publishToExchange(exchange string, data []byte) error {
	return rabbitmq.Publish(c.ch, exchange, "", data)
}

func (c *Controller) publishToQueues(data []byte, queues ...string) error {
	for _, queue := range queues {
		err := rabbitmq.Publish(c.ch, "", queue, data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) publishMovieEof(clientID string) error {
	data, err := proto.Marshal(&pb.MovieSanit{
		Budget:      proto.Int64(0),
		Id:          proto.Int32(0),
		Overview:    proto.String(""),
		ReleaseYear: proto.Uint32(0),
		Revenue:     proto.Float64(0),
		Title:       proto.String(""),
		Eof:         proto.Bool(true),
		ClientId:    proto.String(clientID),
	})
	if err != nil {
		return err
	}

	err = c.publishToQueues(data, globalconfig.MoviesQueues...)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) publishCreditEof(clientID string) error {
	data, err := proto.Marshal(&pb.CreditSanit{
		Id:       proto.Int64(0),
		Eof:      proto.Bool(true),
		ClientId: proto.String(clientID),
	})
	if err != nil {
		return err
	}

	err = c.publishToExchange(globalconfig.CreditsExchange, data)
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) publishRatingEof(clientID string) error {
	data, err := proto.Marshal(&pb.RatingSanit{
		MovieId:  proto.Int64(0),
		Rating:   proto.Float32(0),
		Eof:      proto.Bool(true),
		ClientId: proto.String(clientID),
	})
	if err != nil {
		return err
	}

	err = c.publishToExchange(globalconfig.RatingsExchange, data)
	if err != nil {
		return err
	}

	return nil
}

func sanitizeMovie(m *pb.Movie, clientID string) (*pb.MovieSanit, error) {
	// Clean up
	if m.Id == nil || m.GetId() <= 0 {
		return nil, fmt.Errorf("invalid movie id")
	}
	if m.Overview == nil || m.GetOverview() == "" {
		return nil, fmt.Errorf("invalid movie overview")
	}
	/*if m.Title == nil {
		return nil, fmt.Errorf("invalid movie title")
	}
	if m.Genres == nil {
		return nil, fmt.Errorf("invalid movie genres")
	}
	if m.ReleaseDate == nil {
		return nil, fmt.Errorf("invalid movie release date")
	}
	if m.ProductionCountries == nil {
		return nil, fmt.Errorf("invalid movie production countries")
	}
	if m.SpokenLanguages == nil {
		return nil, fmt.Errorf("invalid movie spoken languages")
	}
	if m.Budget == nil || m.GetBudget() <= 0 {
		return nil, fmt.Errorf("invalid movie budget")
	}
	if m.Revenue == nil || m.GetRevenue() <= 0 {
		return nil, fmt.Errorf("invalid movie revenue")
	}*/

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
		ClientId:            proto.String(clientID),
	}, nil
}

func sanitizeRating(r *pb.Rating, clientId string) (*pb.RatingSanit, error) {
	// Clean up
	if r.MovieId == nil || r.GetMovieId() <= 0 {
		return nil, fmt.Errorf("invalid movie id")
	}
	if r.Rating == nil || r.GetRating() < 0.0 || r.GetRating() > 5.0 {
		return nil, fmt.Errorf("invalid rating value")
	}
	if r.Timestamp == nil || r.GetTimestamp() <= 0 {
		return nil, fmt.Errorf("invalid timestamp")
	}

	if r.GetRating() < 0.0 || r.GetRating() > 5.0 {
		return nil, fmt.Errorf("invalid rating value: %v", r.GetRating())
	}

	return &pb.RatingSanit{
		MovieId:  r.MovieId,
		Rating:   r.Rating,
		Eof:      r.Eof,
		ClientId: proto.String(clientId),
	}, nil
}

func sanitizeCredit(c *pb.Credit, clientId string) (*pb.CreditSanit, error) {
	// Clean up
	if c.Id == nil || c.GetId() <= 0 {
		return nil, fmt.Errorf("invalid movie id")
	}

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
		ClientId:     proto.String(clientId),
	}, nil
}
