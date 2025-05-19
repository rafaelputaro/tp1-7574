package internal

import (
	"context"
	"io"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

var lis *bufconn.Listener

func TestSendMoviesBatch(t *testing.T) {
	lis = bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	mock := &mockMovieServiceServer{}
	pb.RegisterMovieServiceServer(server, mock)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Errorf("Server exited with error: %v", err)
		}
	}()
	defer server.Stop()

	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewMovieServiceClient(conn)

	testBatch := []*pb.Movie{
		{
			Budget:              proto.Int64(1000000),
			Genres:              proto.String("Action"),
			Id:                  proto.Int32(123),
			Overview:            proto.String("Some overview"),
			ProductionCountries: proto.String("AR"),
			ReleaseDate:         proto.String("2005-05-10"),
			Revenue:             proto.Float64(5000000),
			SpokenLanguages:     proto.String("es"),
			Title:               proto.String("Movie Title"),
		},
	}

	parser := &mockParser[pb.Movie]{batches: [][]*pb.Movie{testBatch}}

	SendMovies(ctx, client, parser)

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

type mockParser[T any] struct {
	batches [][]*T
	i       int
}

func (m *mockParser[T]) NextBatch() ([]*T, error) {
	if m.i >= len(m.batches) {
		return nil, io.EOF
	}
	batch := m.batches[m.i]
	m.i++
	return batch, nil
}

func (m *mockParser[T]) Close() error {
	return nil
}
