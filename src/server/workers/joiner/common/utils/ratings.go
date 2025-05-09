package utils

import (
	"fmt"
	"slices"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

type MovieData struct {
	Title     string
	RatingSum float32
	Count     int64
	RatingAvg float64
}

type RatingTotalizer struct {
	Movies []MovieData
	Index  map[int64]int64 // key: movie id
}

func newMovieData(title string, ratingSum float32, count int64, ratingAvg float64) *MovieData {
	return &MovieData{
		Title:     title,
		RatingSum: ratingSum,
		Count:     count,
		RatingAvg: ratingAvg,
	}
}

func NewRatingTotalizer() *RatingTotalizer {
	return &RatingTotalizer{
		Movies: []MovieData{},
		Index:  make(map[int64]int64),
	}
}

// append a movie
func (totalizer *RatingTotalizer) AppendMovie(movie *protopb.MovieSanit) {
	totalizer.Index[int64(*movie.Id)] = int64(len(totalizer.Movies))
	totalizer.Movies = append(totalizer.Movies, *newMovieData(*movie.Title, 0.0, 0, 0.0))
}

// update rating for a movie
func (totalizer *RatingTotalizer) Sum(rating *protopb.RatingSanit) {
	index, exists := totalizer.Index[*rating.MovieId]
	if exists {
		founded := totalizer.Movies[index]
		// update
		newData := *newMovieData(
			founded.Title,
			founded.RatingSum+*rating.Rating,
			founded.Count+1,
			0.0,
		)
		totalizer.Movies[index] = newData
	}
}

func CreateTopAndBottomRatingAvgEof(clientID string) *protopb.TopAndBottomRatingAvg {
	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Dummy1"),
		TitleBottom:     proto.String("Dummy2"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(0.0),
		Eof:             proto.Bool(true),
		ClientId:        proto.String(clientID),
	}
}

// Returns string from TopAndBottomRagingAvg
func TopAndBottomToString(topAndBottom *protopb.TopAndBottomRatingAvg) string {
	return fmt.Sprintf("Top: %s(%v) | Bottom: %s(%v)",
		*topAndBottom.TitleTop, *topAndBottom.RatingAvgTop,
		*topAndBottom.TitleBottom, *topAndBottom.RatingAvgBottom)
}

// Returns: 1 rating1 < rating2; 0 rating1 == rating2; -1 rating1 > rating2
func cmpMovieData(data1, data2 MovieData) int {
	if data1.RatingAvg == data2.RatingAvg {
		return 0
	}
	if data1.RatingAvg > data2.RatingAvg {
		return -1
	} else {
		return 1
	}
}

func (totalizer *RatingTotalizer) updateRatingAverage() {
	for index := range totalizer.Movies {
		founded := totalizer.Movies[index]
		if founded.Count != 0 {
			totalizer.Movies[index] = *newMovieData(
				founded.Title,
				founded.RatingSum,
				founded.Count,
				float64(founded.RatingSum)/float64(founded.Count),
			)
		}
	}
}

// After this function is called, the index becomes inconsistent.
func (totalizer *RatingTotalizer) sort() {
	totalizer.updateRatingAverage()
	slices.SortFunc(totalizer.Movies, cmpMovieData)
}

func (totalizer *RatingTotalizer) GetTopAndBottom(clientID string) *protopb.TopAndBottomRatingAvg {
	totalizer.sort()

	lastIndex := -1
	for i := len(totalizer.Movies) - 1; i >= 0; i-- {
		if totalizer.Movies[i].Count > 0 {
			lastIndex = i
			break
		}
	}
	last := totalizer.Movies[lastIndex]

	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        &totalizer.Movies[0].Title,
		TitleBottom:     &last.Title,
		RatingAvgTop:    &totalizer.Movies[0].RatingAvg,
		RatingAvgBottom: &last.RatingAvg,
		ClientId:        proto.String(clientID),
	}
}
