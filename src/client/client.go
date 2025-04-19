package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
	pb "tp1/protobuf/protopb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	batchSize = 100
	grpcAddr  = "localhost:50051"
)

func main() {
	if len(os.Args) != 4 {
		fmt.Println("Usage: client <movies> <ratings> <credits>")
		os.Exit(1)
	}

	moviesPath := os.Args[1]
	ratingsPath := os.Args[2]
	creditsPath := os.Args[3]

	fmt.Println("Reading files:")
	fmt.Println(" - Movies:", moviesPath)
	fmt.Println(" - Ratings:", ratingsPath)
	fmt.Println(" - Credits:", creditsPath)

	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()

	client := pb.NewMovieServiceClient(conn)

	moviesFile, err := os.Open(moviesPath)
	if err != nil {
		log.Fatalf("Failed to open file %s: %v", moviesPath, err)
	}
	defer moviesFile.Close()

	reader := csv.NewReader(moviesFile)
	_, err = reader.Read() // Skip header
	if err != nil {
		log.Fatalf("Failed to read CSV header: %v", err)
	}

	// Send batches
	var batch []*pb.Movie
	for {
		record, err := reader.Read()
		if err != nil {
			break
		}
		movie := parseMovie(record)
		batch = append(batch, movie)

		if len(batch) >= batchSize {
			sendBatch(client, batch)
			batch = nil
		}
	}
	if len(batch) > 0 {
		sendBatch(client, batch)
	}
}

func sendBatch(client pb.MovieServiceClient, movies []*pb.Movie) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	stream, err := client.StreamMovies(ctx)
	if err != nil {
		log.Fatalf("Failed to open stream: %v", err)
	}

	for _, movie := range movies {
		err := stream.Send(movie)
		if err != nil {
			log.Printf("Failed to send movie ID %d: %v", movie.GetId(), err)
		}
	}

	_, err = stream.CloseAndRecv()
	if err != nil {
		log.Printf("Stream close error: %v", err)
	}
}

func parseMovie(record []string) *pb.Movie {
	budget, _ := strconv.Atoi(record[2])
	id, _ := strconv.Atoi(record[5])
	popularity, _ := strconv.ParseFloat(record[10], 32)
	revenue, _ := strconv.ParseFloat(record[15], 64)
	runtime, _ := strconv.ParseFloat(record[16], 64)
	voteAverage, _ := strconv.ParseFloat(record[22], 64)
	voteCount, _ := strconv.Atoi(record[23])

	return &pb.Movie{
		Adult:               proto.Bool(record[0] == "True"),
		BelongsToCollection: proto.String(record[1]),
		Budget:              proto.Int32(int32(budget)),
		Genres:              proto.String(record[3]),
		Homepage:            proto.String(record[4]),
		Id:                  proto.Int32(int32(id)),
		ImdbId:              proto.String(record[6]),
		OriginalLanguage:    proto.String(record[7]),
		OriginalTitle:       proto.String(record[8]),
		Overview:            proto.String(record[9]),
		Popularity:          proto.Float32(float32(popularity)),
		PosterPath:          proto.String(record[11]),
		ProductionCompanies: proto.String(record[12]),
		ProductionCountries: proto.String(record[13]),
		ReleaseDate:         proto.String(record[14]),
		Revenue:             proto.Float64(revenue),
		Runtime:             proto.Float64(runtime),
		SpokenLanguages:     proto.String(record[17]),
		Status:              proto.String(record[18]),
		Tagline:             proto.String(record[19]),
		Title:               proto.String(record[20]),
		Video:               proto.Bool(record[21] == "True"),
		VoteAverage:         proto.Float64(voteAverage),
		VoteCount:           proto.Int32(int32(voteCount)),
	}
}
