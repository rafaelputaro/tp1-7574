package internal

import (
	"google.golang.org/protobuf/proto"
	"testing"
	pb "tp1/protobuf/protopb"
)

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

	got, err := parseMovie(record)

	if err != nil || !proto.Equal(got, expected) {
		t.Errorf("parseMovie() = %v\nExpected = %v", got, expected)
	}
}

func TestParseRating(t *testing.T) {
	record := []string{
		"1", "31", "2.5", "1260759144",
	}

	expected := &pb.Rating{
		UserId:    proto.Int64(1),
		MovieId:   proto.Int64(31),
		Rating:    proto.Float32(2.5),
		Timestamp: proto.Int64(1260759144),
	}

	got, err := parseRating(record)

	if err != nil || !proto.Equal(got, expected) {
		t.Errorf("parseRating() = %v\nExpected = %v", got, expected)
	}
}

func TestParseCredit(t *testing.T) {
	record := []string{
		"cast", "crew", "123",
	}

	expected := &pb.Credit{
		Cast: proto.String("cast"),
		Crew: proto.String("crew"),
		Id:   proto.Int64(123),
	}

	got, err := parseCredit(record)

	if err != nil || !proto.Equal(got, expected) {
		t.Errorf("parseCredit() = %v\nExpected = %v", got, expected)
	}
}
