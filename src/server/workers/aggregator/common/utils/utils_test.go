package utils

import (
	"fmt"
	"strings"
	"testing"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

func TestTop5(t *testing.T) {
	top1 := CreateEmptyTop5()
	top1.Budget[2] = 500
	top2 := protopb.Top5Country{
		Budget:              []int32{50, 2000, 10000, 500},
		ProductionCountries: []string{"Argentine", "England", "USA", "France"},
	}
	sorted := protopb.Top5Country{
		Budget:              []int32{10000, 2000, 500, 500, 50},
		ProductionCountries: []string{"USA", "England", "Empty2", "France", "Argentine"},
	}
	globalTop := ReduceTop5(&top1, &top2)
	if Top5ToString(globalTop) != Top5ToString(&sorted) {
		t.Fatal("Error on reduce top 5")
	}
}

func TestTopAndBottom(t *testing.T) {
	topAndBottom1 := CreateEmptyTopAndBottom()
	topAndBottom2 := protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Rocky"),
		RatingAvgTop:    proto.Float64(8.3),
		TitleBottom:     proto.String("Attack of the killer tomatoes"),
		RatingAvgBottom: proto.Float64(3.2),
	}
	newTop := ReduceTopAndBottom(&topAndBottom1, &topAndBottom2)
	if *newTop.TitleTop != *topAndBottom2.TitleTop || *newTop.RatingAvgTop != *topAndBottom2.RatingAvgTop ||
		*newTop.TitleBottom != *topAndBottom2.TitleBottom || *newTop.RatingAvgBottom != *topAndBottom2.RatingAvgBottom {
		t.Fatal("Error on reduce bottom and top")
	}
	newTop = ReduceTopAndBottom(&topAndBottom2, &topAndBottom1)
	if *newTop.TitleTop != *topAndBottom2.TitleTop || *newTop.RatingAvgTop != *topAndBottom2.RatingAvgTop ||
		*newTop.TitleBottom != *topAndBottom2.TitleBottom || *newTop.RatingAvgBottom != *topAndBottom2.RatingAvgBottom {
		t.Fatal("Error on reduce bottom and top")
	}
	topAndBottom1 = protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Iron Man"),
		RatingAvgTop:    proto.Float64(7.2),
		TitleBottom:     proto.String("Mars Attack"),
		RatingAvgBottom: proto.Float64(1.2),
	}
	newTop = ReduceTopAndBottom(&topAndBottom2, &topAndBottom1)
	if *newTop.TitleTop != *topAndBottom2.TitleTop || *newTop.RatingAvgTop != *topAndBottom2.RatingAvgTop ||
		*newTop.TitleBottom != *topAndBottom1.TitleBottom || *newTop.RatingAvgBottom != *topAndBottom1.RatingAvgBottom {
		t.Fatal("Error on reduce bottom and top")
	}
	newTop = ReduceTopAndBottom(&topAndBottom1, &topAndBottom2)
	if *newTop.TitleTop != *topAndBottom2.TitleTop || *newTop.RatingAvgTop != *topAndBottom2.RatingAvgTop ||
		*newTop.TitleBottom != *topAndBottom1.TitleBottom || *newTop.RatingAvgBottom != *topAndBottom1.RatingAvgBottom {
		t.Fatal("Error on reduce bottom and top")
	}
}

func TestTop10(t *testing.T) {
	actorsData := NewActorsData()
	actorsName := []string{
		"Robert Downey Jr",
		"Mel Gibson",
		"Mark Rufallo",
		"Franchella",
		"Julia Roberts",
		"Chris Evans",
		"Chris Pratt",
		"Angelina Jolie",
		"The Rock",
		"Ryan Reynolds",
		"Hugh Jackman",
		"Hugh Grant",
		"Gina Carano",
		"Morena Baccarin",
		"Charlie Cox",
		"Vincent D'Onofrio",
		"Rosario Dawson",
		"Morena Baccarin",
		"Charlie Cox",
		"Vincent D'Onofrio",
		"Rosario Dawson",
		"Mel Gibson",
		"Mark Rufallo",
	}
	actorsCount := []int64{1, 3, 1, 5, 4, 3, 1, 2, 1, 3, 1, 5, 4, 3, 1, 2, 3, 3, 1, 2, 3, 2, 13}
	for index := range len(actorsName) {
		actorsData.UpdateCount(&protopb.Actor{
			Name:        proto.String(actorsName[index]),
			ProfilePath: proto.String(fmt.Sprintf("%v.jpeg", actorsName[index])),
			CountMovies: proto.Int64(actorsCount[index]),
		})
	}
	toCheck := "Mark Rufallo(14) Rosario Dawson(6) Morena Baccarin(6) Hugh Grant(5) Mel Gibson(5) Franchella(5) Gina Carano(4) Vincent D'Onofrio(4) Julia Roberts(4) Ryan Reynolds(3)"
	if !strings.Contains(Top10ToString(actorsData.GetTop10()), toCheck) {
		t.Fatal("Error on reduce top10")
	}
}
