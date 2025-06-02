package protoUtils

import (
	"fmt"
	"math"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

const EOF_MESSAGE = "EOF Message"

func CreateDummyActor(clientId string, messageId int64, eof bool) *protopb.Actor {
	return &protopb.Actor{
		Name:        proto.String("Dummys"),
		ProfilePath: proto.String("Dummy.jpg"),
		CountMovies: proto.Int64(0),
		ClientId:    &clientId,
		MessageId:   proto.Int64(messageId),
		Eof:         proto.Bool(eof),
	}
}

func CreateEofActor(clientId string, messageId int64) *protopb.Actor {
	return CreateDummyActor(clientId, messageId, true)
}

func CreateEofMessageActor(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofActor(clientId, messageId))
}

// Returns string from actor count
func ActorToString(actor *protopb.Actor) string {
	return fmt.Sprintf("Name: %s | Path Profile %s (%v) ", actor.GetName(), actor.GetProfilePath(), actor.GetCountMovies())
}

func CreateDummyCreditSanit(clientId string, messageId int64, eof bool) *protopb.CreditSanit {
	return &protopb.CreditSanit{
		CastNames:    []string{"Dummy%"},
		ProfilePaths: []string{"Dummys.jpg"},
		Id:           proto.Int64(0),
		ClientId:     &clientId,
		MessageId:    proto.Int64(messageId),
		Eof:          proto.Bool(eof),
	}
}

func CreateEofCreditSanit(clientId string, messageId int64) *protopb.CreditSanit {
	return CreateDummyCreditSanit(clientId, messageId, true)
}

func CreateEofMessageCreditSanit(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofCreditSanit(clientId, messageId))
}

func CreateDummyCredit(clientId string, messageId int64, eof bool) *protopb.Credit {
	return &protopb.Credit{
		Cast:      proto.String(""),
		Id:        proto.Int64(0),
		ClientId:  &clientId,
		MessageId: proto.Int64(messageId),
		Eof:       proto.Bool(eof),
	}
}

func SetMessageIdCredit(credit *protopb.Credit, messageId int64) {
	credit.MessageId = proto.Int64(messageId)
}

func CreateEofCredit(clientId string, messageId int64) *protopb.Credit {
	return CreateDummyCredit(clientId, messageId, true)
}

func CreateEofMessageCredit(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofCredit(clientId, messageId))
}

func CreateDummyMetrics(clientId string, messageId int64, eof bool) *protopb.Metrics {
	return &protopb.Metrics{
		AvgRevenueOverBudgetNegative: proto.Float64(0.0),
		AvgRevenueOverBudgetPositive: proto.Float64(0.0),
		ClientId:                     &clientId,
		MessageId:                    proto.Int64(messageId),
		Eof:                          proto.Bool(eof),
	}
}

func CreateEofMetrics(clientId string, messageId int64) *protopb.Metrics {
	return CreateDummyMetrics(clientId, messageId, true)
}

func CreateEofMessageMetrics(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofMetrics(clientId, messageId))
}

func MetricsToString(metrics *protopb.Metrics) string {
	return fmt.Sprintf("Negative: %v | Positive: %v", metrics.GetAvgRevenueOverBudgetNegative(), metrics.GetAvgRevenueOverBudgetPositive())
}

func CreateDummyMovieSanit(clientId string, messageId int64, eof bool) *protopb.MovieSanit {
	return &protopb.MovieSanit{
		Budget:              proto.Int64(0),
		Genres:              []string{"dummy_gen"},
		Id:                  proto.Int32(0),
		Overview:            proto.String("A dummy movie"),
		ProductionCountries: []string{"DummyCountry"},
		ReleaseYear:         proto.Uint32(0),
		Revenue:             proto.Float64(0),
		Title:               proto.String("Dummy"),
		ClientId:            &clientId,
		MessageId:           proto.Int64(messageId),
		Eof:                 proto.Bool(eof),
	}
}

func CreateEofMovieSanit(clientId string, messageId int64) *protopb.MovieSanit {
	return CreateDummyMovieSanit(clientId, messageId, true)
}

func CreateEofMessageMovieSanit(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofMovieSanit(clientId, messageId))
}

func CreateDummyMovie(clientId string, messageId int64, eof bool) *protopb.Movie {
	return &protopb.Movie{
		Budget:              proto.Int64(0),
		Genres:              proto.String("[]"),
		Id:                  proto.Int32(0),
		Overview:            proto.String("A dummy movie"),
		ProductionCountries: proto.String("[]"),
		ReleaseDate:         proto.String("1-1-1"),
		Revenue:             proto.Float64(0),
		SpokenLanguages:     proto.String("[]"),
		Title:               proto.String("Dummy"),
		ClientId:            &clientId,
		MessageId:           proto.Int64(messageId),
		Eof:                 proto.Bool(eof),
	}
}

func SetMessageIdMovie(movie *protopb.Movie, messageId int64) {
	movie.MessageId = proto.Int64(messageId)
}

func CreateEofMovie(clientId string, messageId int64) *protopb.Movie {
	return CreateDummyMovie(clientId, messageId, true)
}

func CreateEofMessageMovie(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofMovie(clientId, messageId))
}

func CreateDummyRatingSanit(clientId string, messageId int64, eof bool) *protopb.RatingSanit {
	return &protopb.RatingSanit{
		MovieId:   proto.Int64(0),
		Rating:    proto.Float32(0),
		ClientId:  &clientId,
		MessageId: proto.Int64(messageId),
		Eof:       proto.Bool(eof),
	}
}

func CreateEofRatingSanit(clientId string, messageId int64) *protopb.RatingSanit {
	return CreateDummyRatingSanit(clientId, messageId, true)
}

func CreateEofMessageRatingSanit(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofRatingSanit(clientId, messageId))
}

func CreateDummyRating(clientId string, messageId int64, eof bool) *protopb.Rating {
	return &protopb.Rating{
		MovieId:   proto.Int64(0),
		Rating:    proto.Float32(0),
		Timestamp: proto.Int64(0),
		ClientId:  &clientId,
		MessageId: proto.Int64(messageId),
		Eof:       proto.Bool(eof),
	}
}

func SetMessageIdRating(rating *protopb.Rating, messageId int64) {
	rating.MessageId = proto.Int64(messageId)
}

func CreateEofRating(clientId string, messageId int64) *protopb.Rating {
	return CreateDummyRating(clientId, messageId, true)
}

func CreateEofMessageRating(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofRating(clientId, messageId))
}

func CreateDummyRevenueOverBudget(clientId string, messageId int64, eof bool) *protopb.RevenueOverBudget {
	return &protopb.RevenueOverBudget{
		SumRevenueOverBudget: proto.Float64(0.0),
		AmountReviews:        proto.Int64(0),
		ClientId:             &clientId,
		MessageId:            proto.Int64(messageId),
		Eof:                  proto.Bool(eof),
	}
}

func CreateEofRevenueOverBudget(clientId string, messageId int64) *protopb.RevenueOverBudget {
	return CreateDummyRevenueOverBudget(clientId, messageId, true)
}

func CreateEofMessageRevenueOverBudget(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofRevenueOverBudget(clientId, messageId))
}

func CreateDummyTop5Country(clientId string, messageId int64, eof bool) *protopb.Top5Country {
	return &protopb.Top5Country{
		Budget:              []int64{},
		ProductionCountries: []string{},
		ClientId:            &clientId,
		MessageId:           proto.Int64(messageId),
		Eof:                 proto.Bool(eof),
	}
}

func CreateMinimumTop5Country(clientId string, messageId int64) *protopb.Top5Country {
	return &protopb.Top5Country{
		Budget:              []int64{0, 0, 0, 0, 0},
		ProductionCountries: []string{"Empty0", "Empty1", "Empty2", "Empty3", "Empty4"},
		ClientId:            &clientId,
		MessageId:           proto.Int64(messageId),
	}
}

func CreateEofTop5Country(clientId string, messageId int64) *protopb.Top5Country {
	return CreateDummyTop5Country(clientId, messageId, true)
}

func CreateEofMessageTop5Country(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofTop5Country(clientId, messageId))
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

func CreateDummyTop10(clientId string, messageId int64, eof bool) *protopb.Top10 {
	return &protopb.Top10{
		Names:        []string{},
		ProfilePaths: []string{},
		CountMovies:  []int64{},
		ClientId:     &clientId,
		MessageId:    proto.Int64(messageId),
		Eof:          proto.Bool(eof),
	}
}

func CreateEofTop10(clientId string, messageId int64) *protopb.Top10 {
	return CreateDummyTop10(clientId, messageId, true)
}

func CreateEofMessageTop10(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofTop10(clientId, messageId))
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

func CreateDummyTopAndBottomRatingAvg(clientId string, messageId int64, eof bool) *protopb.TopAndBottomRatingAvg {
	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Dummy"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(0.0),
		ClientId:        &clientId,
		MessageId:       proto.Int64(messageId),
		Eof:             proto.Bool(eof),
	}
}

func CreateSeedTopAndBottom(clientId string, messageId int64) *protopb.TopAndBottomRatingAvg {
	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Empty1"),
		TitleBottom:     proto.String("Empty2"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(math.MaxFloat64),
		MessageId:       proto.Int64(messageId),
		ClientId:        &clientId,
	}
}

func CreateEofTopAndBottomRatingAvg(clientId string, messageId int64) *protopb.TopAndBottomRatingAvg {
	return CreateDummyTopAndBottomRatingAvg(clientId, messageId, true)
}

func CreateEofMessageTopAndBottomRatingAvg(clientId string, messageId int64) ([]byte, error) {
	return proto.Marshal(CreateEofTopAndBottomRatingAvg(clientId, messageId))
}

// Returns string from TopAndBottomRagingAvg
func TopAndBottomToString(topAndBottom *protopb.TopAndBottomRatingAvg) string {
	return fmt.Sprintf("Top: %s(%v) | Bottom: %s(%v)",
		topAndBottom.GetTitleTop(), topAndBottom.GetRatingAvgTop(),
		topAndBottom.GetTitleBottom(), topAndBottom.GetRatingAvgBottom())
}
