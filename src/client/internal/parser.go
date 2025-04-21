package internal

import (
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
			return nil, err
		}

		item, err := p.fn(record)
		if err != nil {
			logger.Errorf("skipping line due to parse error: %v", err)
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

	reader := csv.NewReader(file)

	// skip header
	_, err = reader.Read()
	if err != nil {
		file.Close()
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

func parseMovie(record []string) (*pb.Movie, error) {
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
	}, nil
}

func parseRating(record []string) (*pb.Rating, error) {
	userID, _ := strconv.ParseInt(record[0], 10, 64)
	movieID, _ := strconv.ParseInt(record[1], 10, 64)
	rating, _ := strconv.ParseFloat(record[2], 32)
	timestamp, _ := strconv.ParseInt(record[3], 10, 64)

	return &pb.Rating{
		UserId:    proto.Int64(userID),
		MovieId:   proto.Int64(movieID),
		Rating:    proto.Float32(float32(rating)),
		Timestamp: proto.Int64(timestamp),
	}, nil
}

func parseCredit(record []string) (*pb.Credit, error) {
	id, _ := strconv.ParseInt(record[2], 10, 64)

	return &pb.Credit{
		Cast: proto.String(record[0]),
		Crew: proto.String(record[1]),
		Id:   proto.Int64(id),
	}, nil
}
