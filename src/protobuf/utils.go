package proto

import (
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

func createDummyActor(clientId string, eof bool) *protopb.Actor {
	return &protopb.Actor{
		Name:        proto.String("Dummys"),
		ProfilePath: proto.String("Dummy.jpg"),
		CountMovies: proto.Int64(0),
		ClientId:    &clientId,
		Eof:         proto.Bool(eof),
	}
}

func createEofActor(clientId string) *protopb.Actor {
	return createDummyActor(clientId, true)
}

func createDummyCreditSanit(clientId string, eof bool) *protopb.CreditSanit {
	return &protopb.CreditSanit{
		CastNames:    []string{"Dummy%"},
		ProfilePaths: []string{"Dummys.jpg"},
		Id:           proto.Int64(0),
		ClientId:     &clientId,
		Eof:          proto.Bool(eof),
	}
}

func createEofCreditSanit(clientId string) *protopb.CreditSanit {
	return createDummyCreditSanit(clientId, true)
}

func createDummyCredit(clientId string, eof bool) *protopb.Credit {
	return &protopb.Credit{
		Cast:     proto.String(""),
		Crew:     proto.String(""),
		Id:       proto.Int64(0),
		ClientId: &clientId,
		Eof:      proto.Bool(eof),
	}
}

func createEofCredit(clientId string) *protopb.Credit {
	return createDummyCredit(clientId, true)
}

func createDummyMetrics(clientId string, eof bool) *protopb.Metrics {
	return &protopb.Metrics{
		AvgRevenueOverBudgetNegative: proto.Float64(0.0),
		AvgRevenueOverBudgetPositive: proto.Float64(0.0),
		ClientId:                     &clientId,
		Eof:                          proto.Bool(eof),
	}
}

func createEofMetrics(clientId string) *protopb.Metrics {
	return createDummyMetrics(clientId, true)
}

func createDummyMovieSanit(clientId string, eof bool) *protopb.MovieSanit {
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

func createEofMovieSanit(clientId string) *protopb.MovieSanit {
	return createDummyMovieSanit(clientId, true)
}

func createDummyMovie(clientId string, eof bool) *protopb.Movie {
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

func createEofMovie(clientId string) *protopb.Movie {
	return createDummyMovie(clientId, true)
}

func createDummyRatingSanit(clientId string, eof bool) *protopb.RatingSanit {
	return &protopb.RatingSanit{
		MovieId:  proto.Int64(0),
		Rating:   proto.Float32(0),
		ClientId: &clientId,
		Eof:      proto.Bool(eof),
	}
}

func createEofRatingSanit(clientId string) *protopb.RatingSanit {
	return createDummyRatingSanit(clientId, true)
}

func createDummyRating(clientId string, eof bool) *protopb.Rating {
	return &protopb.Rating{
		UserId:    proto.Int64(0),
		MovieId:   proto.Int64(0),
		Rating:    proto.Float32(0),
		Timestamp: proto.Int64(0),
		ClientId:  &clientId,
		Eof:       proto.Bool(eof),
	}
}

func createEofRating(clientId string) *protopb.Rating {
	return createDummyRating(clientId, true)
}

func createDummyRevenueOverBudget(clientId string, eof bool) *protopb.RevenueOverBudget {
	return &protopb.RevenueOverBudget{
		SumRevenueOverBudget: proto.Float64(0.0),
		AmountReviews:        proto.Int64(0),
		ClientId:             &clientId,
		Eof:                  proto.Bool(eof),
	}
}

func createEofRevenueOverBudget(clientId string) *protopb.RevenueOverBudget {
	return createDummyRevenueOverBudget(clientId, true)
}

func createDummyTop5Country(clientId string, eof bool) *protopb.Top5Country {
	return &protopb.Top5Country{
		Budget:              []int32{},
		ProductionCountries: []string{},
		ClientId:            &clientId,
		Eof:                 proto.Bool(eof),
	}
}

func createEofTop5Country(clientId string) *protopb.Top5Country {
	return createDummyTop5Country(clientId, true)
}

func createDummyTop10(clientId string, eof bool) *protopb.Top10 {
	return &protopb.Top10{
		Names:        []string{},
		ProfilePaths: []string{},
		CountMovies:  []int64{},
		ClientId:     &clientId,
		Eof:          proto.Bool(eof),
	}
}

func createEofTop10(clientId string) *protopb.Top10 {
	return createDummyTop10(clientId, true)
}

func createDummyTopAndBottomRatingAvg(clientId string, eof bool) *protopb.TopAndBottomRatingAvg {
	return &protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Dummy"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(0.0),
		ClientId:        &clientId,
		Eof:             proto.Bool(eof),
	}
}

func createEofTopAndBottomRatingAvg(clientId string) *protopb.TopAndBottomRatingAvg {
	return createDummyTopAndBottomRatingAvg(clientId, true)
}
