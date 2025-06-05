package internal

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"os"
	"strconv"
	pb "tp1/protobuf/protopb"
)

type Parser[T any] interface {
	NextBatch() ([]*T, error)
	Close() error
}

type parser[T any] struct {
	file   *os.File
	reader *csv.Reader
	fn     func([]string) (*T, error)
	size   int
}

func (p *parser[T]) NextBatch() ([]*T, error) {
	var batch []*T
	for len(batch) < p.size {

		record, err := p.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// logger.Errorf("skipping line due to error: %v", err)
			continue
		}

		item, err := p.fn(record)
		if err != nil {
			// logger.Errorf("skipping line due to parse error: %v", err)
			continue
		}

		batch = append(batch, item)
	}

	if len(batch) == 0 {
		return nil, io.EOF
	}

	return batch, nil
}

func (p *parser[T]) Close() error {
	return p.file.Close()
}

func newBatchCSVParser[T any](path string, fn func([]string) (*T, error), size int) (Parser[T], error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	bufferedReader := bufio.NewReaderSize(file, 1024*1024)
	reader := csv.NewReader(bufferedReader)

	// skip header
	_, err = reader.Read()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	return &parser[T]{
		file:   file,
		reader: reader,
		fn:     fn,
		size:   size,
	}, nil

}

func NewMoviesParser(path string, batchSize int) (Parser[pb.Movie], error) {
	return newBatchCSVParser[pb.Movie](path, parseMovie, batchSize)
}

func NewRatingsParser(path string, batchSize int) (Parser[pb.Rating], error) {
	return newBatchCSVParser[pb.Rating](path, parseRating, batchSize)
}

func NewCreditsParser(path string, batchSize int) (Parser[pb.Credit], error) {
	return newBatchCSVParser[pb.Credit](path, parseCredit, batchSize)
}

func ShutdownParser[T any](p Parser[T]) {
	if p != nil {
		_ = p.Close()
	}
}

func parseMovie(record []string) (*pb.Movie, error) {
	budget, _ := strconv.Atoi(record[2])
	id, _ := strconv.Atoi(record[5])
	revenue, _ := strconv.ParseFloat(record[15], 64)

	return &pb.Movie{
		Budget:              proto.Int64(int64(budget)),
		Genres:              proto.String(record[3]),
		Id:                  proto.Int32(int32(id)),
		Overview:            proto.String(record[9]),
		ProductionCountries: proto.String(record[13]),
		ReleaseDate:         proto.String(record[14]),
		Revenue:             proto.Float64(revenue),
		SpokenLanguages:     proto.String(record[17]),
		Title:               proto.String(record[20]),
	}, nil
}

func parseRating(record []string) (*pb.Rating, error) {
	movieID, _ := strconv.ParseInt(record[1], 10, 64)
	rating, _ := strconv.ParseFloat(record[2], 32)
	timestamp, _ := strconv.ParseInt(record[3], 10, 64)

	r := &pb.Rating{
		MovieId:   proto.Int64(movieID),
		Rating:    proto.Float32(float32(rating)),
		Timestamp: proto.Int64(timestamp),
	}

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

	return r, nil
}

func parseCredit(record []string) (*pb.Credit, error) {
	id, _ := strconv.ParseInt(record[2], 10, 64)

	return &pb.Credit{
		Cast: proto.String(record[0]),
		Id:   proto.Int64(id),
	}, nil
}
