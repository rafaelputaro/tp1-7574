package main

import (
	"context"
	"io"
	"log"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

var lis *bufconn.Listener // global

func TestParseMovie(t *testing.T) {
	record := []string{
		"True", "collection", "1000000", "Action", "homepage", "123", "tt1234567", "en",
		"Original Title", "Some overview", "6.7", "poster.jpg", "Company", "AR", "2005-05-10",
		"5000000", "120.5", "es", "Released", "A tagline", "Movie Title", "False", "7.8", "1200",
	}

	expected := &pb.Movie{
		Adult:               proto.Bool(true),
		BelongsToCollection: proto.String("collection"),
		Budget:              proto.Int32(1000000),
		Genres:              proto.String("Action"),
		Homepage:            proto.String("homepage"),
		Id:                  proto.Int32(123),
		ImdbId:              proto.String("tt1234567"),
		OriginalLanguage:    proto.String("en"),
		OriginalTitle:       proto.String("Original Title"),
		Overview:            proto.String("Some overview"),
		Popularity:          proto.Float32(6.7),
		PosterPath:          proto.String("poster.jpg"),
		ProductionCompanies: proto.String("Company"),
		ProductionCountries: proto.String("AR"),
		ReleaseDate:         proto.String("2005-05-10"),
		Revenue:             proto.Float64(5000000),
		Runtime:             proto.Float64(120.5),
		SpokenLanguages:     proto.String("es"),
		Status:              proto.String("Released"),
		Tagline:             proto.String("A tagline"),
		Title:               proto.String("Movie Title"),
		Video:               proto.Bool(false),
		VoteAverage:         proto.Float64(7.8),
		VoteCount:           proto.Int32(1200),
	}

	got := parseMovie(record)

	if !proto.Equal(got, expected) {
		t.Errorf("parseMovie() = %v\nExpected = %v", got, expected)
	}
}

func TestSendBatch(t *testing.T) {
	lis = bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	mock := &mockMovieServiceServer{}
	pb.RegisterMovieServiceServer(server, mock)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Errorf("Server exited with error: %v", err)
			return
		}
	}()
	defer server.Stop()

	// Optional: small delay to let server come up
	time.Sleep(1000 * time.Millisecond)

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewMovieServiceClient(conn)

	testBatch := []*pb.Movie{
		{
			Adult:               proto.Bool(true),
			BelongsToCollection: proto.String("collection"),
			Budget:              proto.Int32(1000000),
			Genres:              proto.String("Action"),
			Homepage:            proto.String("homepage"),
			Id:                  proto.Int32(123),
			ImdbId:              proto.String("tt1234567"),
			OriginalLanguage:    proto.String("en"),
			OriginalTitle:       proto.String("Original Title"),
			Overview:            proto.String("Some overview"),
			Popularity:          proto.Float32(6.7),
			PosterPath:          proto.String("poster.jpg"),
			ProductionCompanies: proto.String("Company"),
			ProductionCountries: proto.String("AR"),
			ReleaseDate:         proto.String("2005-05-10"),
			Revenue:             proto.Float64(5000000),
			Runtime:             proto.Float64(120.5),
			SpokenLanguages:     proto.String("es"),
			Status:              proto.String("Released"),
			Tagline:             proto.String("A tagline"),
			Title:               proto.String("Movie Title"),
			Video:               proto.Bool(false),
			VoteAverage:         proto.Float64(7.8),
			VoteCount:           proto.Int32(1200),
		},
	}

	sendBatch(client, testBatch)

	// Wait for server to receive stream (optional delay)
	time.Sleep(1000 * time.Millisecond)

	if len(mock.Received) != 1 {
		t.Fatalf("Expected 1 movie received, got %d", len(mock.Received))
	}
	if mock.Received[0].GetTitle() != "Movie Title" {
		t.Errorf("Expected title 'Movie Title', got '%s'", mock.Received[0].GetTitle())
	}
}

type mockMovieServiceServer struct {
	pb.UnimplementedMovieServiceServer
	Received []*pb.Movie
}

func (s *mockMovieServiceServer) StreamMovies(stream pb.MovieService_StreamMoviesServer) error {
	for {
		movie, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&emptypb.Empty{})
		}
		if err != nil {
			return err
		}
		s.Received = append(s.Received, movie)
	}
}
