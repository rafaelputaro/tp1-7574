package protoUtils

import (
	"fmt"
	"math"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

const EOF_MESSAGE = "EOF Message"

func CreateDummyActor(clientId string, eof bool) *protopb.Actor {
	return &protopb.Actor{
		Name:        proto.String("Dummys"),
		ProfilePath: proto.String("Dummy.jpg"),
		CountMovies: proto.Int64(0),
		ClientId:    &clientId,
		Eof:         proto.Bool(eof),
	}
}

func CreateEofActor(clientId string) *protopb.Actor {
	return CreateDummyActor(clientId, true)
}

func CreateEofMessageActor(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofActor(clientId))
}

// Returns string from actor count
func ActorToString(actor *protopb.Actor) string {
	return fmt.Sprintf("Name: %s | Path Profile %s (%v) ", actor.GetName(), actor.GetProfilePath(), actor.GetCountMovies())
}

func CreateDummyCreditSanit(clientId string, eof bool) *protopb.CreditSanit {
	return &protopb.CreditSanit{
		CastNames:    []string{"Dummy%"},
		ProfilePaths: []string{"Dummys.jpg"},
		Id:           proto.Int64(0),
		ClientId:     &clientId,
		Eof:          proto.Bool(eof),
	}
}

func CreateEofCreditSanit(clientId string) *protopb.CreditSanit {
	return CreateDummyCreditSanit(clientId, true)
}

func CreateEofMessageCreditSanit(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofCreditSanit(clientId))
}

func CreateDummyCredit(clientId string, eof bool) *protopb.Credit {
	return &protopb.Credit{
		Cast:     proto.String(""),
		Crew:     proto.String(""),
		Id:       proto.Int64(0),
		ClientId: &clientId,
		Eof:      proto.Bool(eof),
	}
}

func CreateEofCredit(clientId string) *protopb.Credit {
	return CreateDummyCredit(clientId, true)
}

func CreateEofMessageCredit(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofCredit(clientId))
}

func CreateDummyMetrics(clientId string, eof bool) *protopb.Metrics {
	return &protopb.Metrics{
		AvgRevenueOverBudgetNegative: proto.Float64(0.0),
		AvgRevenueOverBudgetPositive: proto.Float64(0.0),
		ClientId:                     &clientId,
		Eof:                          proto.Bool(eof),
	}
}

func CreateEofMetrics(clientId string) *protopb.Metrics {
	return CreateDummyMetrics(clientId, true)
}

func CreateEofMessageMetrics(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofMetrics(clientId))
}

func MetricsToString(metrics *protopb.Metrics) string {
	return fmt.Sprintf("Negative: %v | Positive: %v", metrics.GetAvgRevenueOverBudgetNegative(), metrics.GetAvgRevenueOverBudgetPositive())
}

func CreateDummyMovieSanit(clientId string, eof bool) *protopb.MovieSanit {
	return &protopb.MovieSanit{
		Budget:              proto.Int32(0),
		Genres:              []string{"dummy_gen"},
		Id:                  proto.Int32(0),
		Overview:            proto.String("A dummy movie"),
		ProductionCountries: []string{"DummyCountry"},
		ReleaseYear:         proto.Uint32(0),
		Revenue:             proto.Float64(0),
		Title:               proto.String("Dummy"),
		ClientId:            &clientId,
		Eof:                 proto.Bool(eof),
	}
}

func CreateEofMovieSanit(clientId string) *protopb.MovieSanit {
	return CreateDummyMovieSanit(clientId, true)
}

func CreateEofMessageMovieSanit(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofMovieSanit(clientId))
}

func CreateDummyMovie(clientId string, eof bool) *protopb.Movie {
	return &protopb.Movie{
		Adult:               proto.Bool(false),
		BelongsToCollection: proto.String("{}"),
		Budget:              proto.Int32(0),
		Genres:              proto.String("[]"),
		Homepage:            proto.String("Dummy"),
		Id:                  proto.Int32(0),
		ImdbId:              proto.String("dummyImdbId"),
		OriginalLanguage:    proto.String("dummyLang"),
		OriginalTitle:       proto.String("Dummy"),
		Overview:            proto.String("A dummy movie"),
		Popularity:          proto.Float32(0.0),
		PosterPath:          proto.String("Dummy.jpg"),
		ProductionCompanies: proto.String("[]"),
		ProductionCountries: proto.String("[]"),
		ReleaseDate:         proto.String("1-1-1"),
		Revenue:             proto.Float64(0),
		Runtime:             proto.Float64(0.0),
		SpokenLanguages:     proto.String("[]"),
		Status:              proto.String("Released"),
		Tagline:             proto.String(""),
		Title:               proto.String("Dummy"),
		Video:               proto.Bool(false),
		VoteAverage:         proto.Float64(0.0),
		VoteCount:           proto.Int32(0),
		ClientId:            &clientId,
		Eof:                 proto.Bool(eof),
	}
}

func CreateEofMovie(clientId string) *protopb.Movie {
	return CreateDummyMovie(clientId, true)
}

func CreateEofMessageMovie(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofMovie(clientId))
}

func CreateDummyRatingSanit(clientId string, eof bool) *protopb.RatingSanit {
	return &protopb.RatingSanit{
		MovieId:  proto.Int64(0),
		Rating:   proto.Float32(0),
		ClientId: &clientId,
		Eof:      proto.Bool(eof),
	}
}

func CreateEofRatingSanit(clientId string) *protopb.RatingSanit {
	return CreateDummyRatingSanit(clientId, true)
}

func CreateEofMessageRatingSanit(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofRatingSanit(clientId))
}

func CreateDummyRating(clientId string, eof bool) *protopb.Rating {
	return &protopb.Rating{
		UserId:    proto.Int64(0),
		MovieId:   proto.Int64(0),
		Rating:    proto.Float32(0),
		Timestamp: proto.Int64(0),
		ClientId:  &clientId,
		Eof:       proto.Bool(eof),
	}
}

func CreateEofRating(clientId string) *protopb.Rating {
	return CreateDummyRating(clientId, true)
}

func CreateEofMessageRating(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofRating(clientId))
}

func CreateDummyRevenueOverBudget(clientId string, eof bool) *protopb.RevenueOverBudget {
	return &protopb.RevenueOverBudget{
		SumRevenueOverBudget: proto.Float64(0.0),
		AmountReviews:        proto.Int64(0),
		ClientId:             &clientId,
		Eof:                  proto.Bool(eof),
	}
}

func CreateEofRevenueOverBudget(clientId string) *protopb.RevenueOverBudget {
	return CreateDummyRevenueOverBudget(clientId, true)
}

func CreateEofMessageRevenueOverBudget(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofRevenueOverBudget(clientId))
}

func CreateDummyTop5Country(clientId string, eof bool) *protopb.Top5Country {
	return &protopb.Top5Country{
		Budget:              []int32{},
		ProductionCountries: []string{},
		ClientId:            &clientId,
		Eof:                 proto.Bool(eof),
	}
}

func CreateMinimumTop5Country(clientId string) *protopb.Top5Country {
	return &protopb.Top5Country{
		Budget:              []int32{0, 0, 0, 0, 0},
		ProductionCountries: []string{"Empty0", "Empty1", "Empty2", "Empty3", "Empty4"},
		ClientId:            &clientId,
	}
}

func CreateEofTop5Country(clientId string) *protopb.Top5Country {
	return CreateDummyTop5Country(clientId, true)
}

func CreateEofMessageTop5Country(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofTop5Country(clientId))
}

// Returns string from Top5
func Top5ToString(top *protopb.Top5Country) string {
	if top.GetEof() {
		return EOF_MESSAGE
	}
	toReturn := ""
	for index := range len(top.GetProductionCountries()) {
		toReturn += fmt.Sprintf("%v(US$ %v) ", top.GetProductionCountries()[index], top.GetBudget()[index])
	}
	return toReturn
}

func CreateDummyTop10(clientId string, eof bool) *protopb.Top10 {
	return &protopb.Top10{
		Names:        []string{},
		ProfilePaths: []string{},
		CountMovies:  []int64{},
		ClientId:     &clientId,
		Eof:          proto.Bool(eof),
	}
}

func CreateEofTop10(clientId string) *protopb.Top10 {
	return CreateDummyTop10(clientId, true)
}

func CreateEofMessageTop10(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofTop10(clientId))
}

// Return names an count as string
func Top10ToString(top *protopb.Top10) string {
	if top.GetEof() {
		return EOF_MESSAGE
	}
	toReturn := ""
	for index := range len(top.GetNames()) {
		toReturn += fmt.Sprintf("%v(%v) ", top.GetNames()[index], top.GetCountMovies()[index])
	}
	return toReturn
}

func CreateDummyTopAndBottomRatingAvg(clientId string, eof bool) *protopb.TopAndBottomRatingAvg {
	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Dummy"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(0.0),
		ClientId:        &clientId,
		Eof:             proto.Bool(eof),
	}
}

func CreateSeedTopAndBottom(clientId string) protopb.TopAndBottomRatingAvg {
	return protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Empty1"),
		TitleBottom:     proto.String("Empty2"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(math.MaxFloat64),
		ClientId:        &clientId,
	}
}

func CreateEofTopAndBottomRatingAvg(clientId string) *protopb.TopAndBottomRatingAvg {
	return CreateDummyTopAndBottomRatingAvg(clientId, true)
}

func CreateEofMessageTopAndBottomRatingAvg(clientId string) ([]byte, error) {
	return proto.Marshal(CreateEofTopAndBottomRatingAvg(clientId))
}

// Returns string from TopAndBottomRagingAvg
func TopAndBottomToString(topAndBottom *protopb.TopAndBottomRatingAvg) string {
	return fmt.Sprintf("Top: %s(%v) | Bottom: %s(%v)",
		topAndBottom.GetTitleTop(), topAndBottom.GetRatingAvgTop(),
		topAndBottom.GetTitleBottom(), topAndBottom.GetRatingAvgBottom())
}
