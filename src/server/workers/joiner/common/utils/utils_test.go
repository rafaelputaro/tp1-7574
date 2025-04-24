package utils

import (
	"fmt"
	"testing"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

func TestActor(t *testing.T) {
	counter := NewActorCounter()
	movies := createMovieSet()
	for _, movie := range *movies {
		counter.AppendMovie(movie)
	}
	credits := createCredits()
	for _, credit := range *credits {
		counter.Count(credit)
	}
	actors := []*protopb.Actor{}
	for actorPath := range counter.Actors {
		actors = append(actors, counter.GetActor(actorPath))
	}
	fmt.Printf("%v", actors)
	// TODO chequear los resultados sean correctos, al parecer lo son
	/*
	   // Expected
	   actorsCountExpected := []int64{3, 2, 2, 4, 3, 4, 2, 4, 2, 3, 2, 3, 2, 5, 2, 6, 4, 2}

	   	for index, actor := range actors {
	   		if *actor.CountMovies != actorsCountExpected[index] {
	   			fmt.Printf("%v, %v", *actor.CountMovies, actorsCountExpected[index])
	   			t.Fatal("Error on count")
	   		}
	   	}
	*/
}

func TestRatings(t *testing.T) {
	totalizer := NewRatingTotalizer()
	movies := createMovieSet()
	for _, movie := range *movies {
		totalizer.AppendMovie(movie)
	}
	ratings := createRatings(movies)
	for _, rating := range *ratings {
		totalizer.Sum(rating)
	}
	report := totalizer.GetTopAndBottom()
	fmt.Printf("%v", report)
	if *report.TitleTop != "Movie 19" || *report.TitleBottom != "Movie 0" {
		t.Fatal("Error on top and bottom")
	}
}

func createRatings(movies *[]*protopb.MovieSanit) *[]*protopb.RatingSanit {
	toReturn := []*protopb.RatingSanit{}
	for _, movie := range *movies {
		rating := (1 + 2*float32(*movie.Id)) / (1 + float32(*movie.Id))
		toReturn = append(toReturn, &protopb.RatingSanit{
			MovieId: proto.Int64(int64(*movie.Id)),
			Rating:  proto.Float32(rating),
		})
	}
	return &toReturn
}

func createMovieSet() *[]*protopb.MovieSanit {
	toRetun := []*protopb.MovieSanit{}
	for movieId := range 20 {
		toRetun = append(toRetun, &protopb.MovieSanit{
			Budget:              proto.Int32(int32(100 * movieId)),
			Genres:              []string{},
			Id:                  proto.Int32(int32(movieId)),
			Overview:            proto.String(""),
			ProductionCountries: []string{},
			ReleaseYear:         proto.Uint32(uint32(2000 + movieId)),
			Revenue:             proto.Float64(float64(movieId * 150)),
			Title:               proto.String(fmt.Sprintf("Movie %v", movieId)),
		})
	}
	return &toRetun
}

func createCredits() *[]*protopb.CreditSanit {
	cast := [][]string{
		{"Robert Downey Jr", "Mel Gibson"},
		{"Mark Rufallo", "Franchella"},
		{"Franchella", "Julia Roberts"},
		{"Chris Evans", "Chris Pratt"},
		{"Angelina Jolie", "The Rock", "Ryan Reynolds"},

		{"Hugh Jackman", "Ryan Reynolds", "Wesley Snipes"},
		{"Hugh Grant", "Gina Carano", "Morena Baccarin"},
		{"Charlie Cox", "Vincent D'Onofrio", "Rosario Dawson"},
		{"Morena Baccarin", "Charlie Cox", "Vincent D'Onofrio", "Rosario Dawson"},
		{"Mel Gibson", "Mark Rufallo"},

		{"Robert Downey Jr", "Mel Gibson", "Mark Rufallo"},
		{"Hugh Jackman", "Ryan Reynolds", "Wesley Snipes", "Robert Downey Jr", "Mel Gibson"},
		{"Angelina Jolie", "The Rock", "Ryan Reynolds", "Wesley Snipes"},
		{"Robert Downey Jr", "Mel Gibson"},
		{"Mark Rufallo", "Franchella"},

		{"Franchella", "Julia Roberts"},
		{"Chris Evans", "Chris Pratt"},
		{"Angelina Jolie", "The Rock", "Ryan Reynolds"},
		{"Hugh Jackman", "Ryan Reynolds", "Wesley Snipes"},
		{"Hugh Grant", "Gina Carano", "Morena Baccarin"},
	}
	toRetun := []*protopb.CreditSanit{}
	for creditId := range 20 {
		toRetun = append(toRetun, &protopb.CreditSanit{
			CastNames:    cast[creditId],
			Id:           proto.Int64(int64(creditId)),
			ProfilePaths: cast[creditId],
		})
	}
	return &toRetun
}
