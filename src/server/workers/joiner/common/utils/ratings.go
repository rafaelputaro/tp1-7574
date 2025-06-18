package utils

import (
	"fmt"
	"slices"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

type MovieInfo struct {
	Title     string
	RatingSum float32
	Count     int64
	RatingAvg float64
	Ratings   map[int64]float32
}

type RatingTotalizer struct {
	Movies     map[int64]*MovieInfo
	SeenMovies map[int64]struct{}
}

func newMovieInfo(title string) *MovieInfo {
	return &MovieInfo{
		Title:     title,
		RatingSum: 0,
		Count:     0,
		RatingAvg: 0,
		Ratings:   make(map[int64]float32),
	}
}

func NewRatingTotalizer() *RatingTotalizer {
	return &RatingTotalizer{
		Movies:     make(map[int64]*MovieInfo),
		SeenMovies: make(map[int64]struct{}),
	}
}

func (totalizer *RatingTotalizer) AppendMovie(movie *protopb.MovieSanit) {
	movieID := int64(movie.GetId())

	totalizer.SeenMovies[movieID] = struct{}{}

	if _, exists := totalizer.Movies[movieID]; !exists {
		totalizer.Movies[movieID] = newMovieInfo(movie.GetTitle())
	} else {
		totalizer.Movies[movieID].Title = movie.GetTitle()
	}
}

func (totalizer *RatingTotalizer) Sum(rating *protopb.RatingSanit) {
	movieID := rating.GetMovieId()
	ratingValue := rating.GetRating()

	if _, exists := totalizer.Movies[movieID]; !exists {
		totalizer.Movies[movieID] = newMovieInfo("")
	}

	ratingID := int64(len(totalizer.Movies[movieID].Ratings) + 1)
	totalizer.Movies[movieID].Ratings[ratingID] = ratingValue

	totalizer.Movies[movieID].RatingSum += ratingValue
	totalizer.Movies[movieID].Count++
}

func CreateTopAndBottomRatingAvgEof(clientID string, messageId int64, sourceId string) *protopb.TopAndBottomRatingAvg {
	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Dummy1"),
		TitleBottom:     proto.String("Dummy2"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(0.0),
		Eof:             proto.Bool(true),
		ClientId:        proto.String(clientID),
		MessageId:       proto.Int64(messageId),
		SourceId:        proto.String(sourceId),
	}
}

func TopAndBottomToString(topAndBottom *protopb.TopAndBottomRatingAvg) string {
	return fmt.Sprintf("Top: %s(%v) | Bottom: %s(%v)",
		*topAndBottom.TitleTop, *topAndBottom.RatingAvgTop,
		*topAndBottom.TitleBottom, *topAndBottom.RatingAvgBottom)
}

// Returns: 1 rating1 < rating2; 0 rating1 == rating2; -1 rating1 > rating2
func cmpMovieInfo(movie1, movie2 *MovieInfo) int {
	if movie1.RatingAvg == movie2.RatingAvg {
		return 0
	}
	if movie1.RatingAvg > movie2.RatingAvg {
		return -1
	} else {
		return 1
	}
}

func (totalizer *RatingTotalizer) updateRatingAverage() {
	for _, movie := range totalizer.Movies {
		if movie.Count > 0 {
			movie.RatingAvg = float64(movie.RatingSum) / float64(movie.Count)
		}
	}
}

func (totalizer *RatingTotalizer) getSortedMovies() []*MovieInfo {
	totalizer.updateRatingAverage()

	validMovies := make([]*MovieInfo, 0)
	for movieID, movie := range totalizer.Movies {
		_, exists := totalizer.SeenMovies[movieID]
		if exists && movie.Count > 0 {
			validMovies = append(validMovies, movie)
		}
	}

	slices.SortFunc(validMovies, cmpMovieInfo)
	return validMovies
}

func (totalizer *RatingTotalizer) GetTopAndBottom(clientID string, messageId int64, sourceId string) *protopb.TopAndBottomRatingAvg {
	sortedMovies := totalizer.getSortedMovies()

	if len(sortedMovies) < 2 {
		return nil
	}

	topMovie := sortedMovies[0]
	bottomMovie := sortedMovies[len(sortedMovies)-1]

	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String(topMovie.Title),
		TitleBottom:     proto.String(bottomMovie.Title),
		RatingAvgTop:    proto.Float64(topMovie.RatingAvg),
		RatingAvgBottom: proto.Float64(bottomMovie.RatingAvg),
		ClientId:        proto.String(clientID),
		MessageId:       proto.Int64(messageId),
		SourceId:        proto.String(sourceId),
	}
}
