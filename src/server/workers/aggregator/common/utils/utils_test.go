package utils

import (
	"fmt"
	"strings"
	"testing"
	"tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"

	"google.golang.org/protobuf/proto"
)

func TestTopAndBottom(t *testing.T) {
	topAndBottom1 := protoUtils.CreateSeedTopAndBottom("", 0)
	topAndBottom2 := protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Rocky"),
		RatingAvgTop:    proto.Float64(8.3),
		TitleBottom:     proto.String("Attack of the killer tomatoes"),
		RatingAvgBottom: proto.Float64(3.2),
	}
	newTop := ReduceTopAndBottom(topAndBottom1, &topAndBottom2, 0)
	if *newTop.TitleTop != *topAndBottom2.TitleTop || *newTop.RatingAvgTop != *topAndBottom2.RatingAvgTop ||
		*newTop.TitleBottom != *topAndBottom2.TitleBottom || *newTop.RatingAvgBottom != *topAndBottom2.RatingAvgBottom {
		t.Fatal("Error on reduce bottom and top")
	}
	newTop = ReduceTopAndBottom(&topAndBottom2, topAndBottom1, 0)
	if *newTop.TitleTop != *topAndBottom2.TitleTop || *newTop.RatingAvgTop != *topAndBottom2.RatingAvgTop ||
		*newTop.TitleBottom != *topAndBottom2.TitleBottom || *newTop.RatingAvgBottom != *topAndBottom2.RatingAvgBottom {
		t.Fatal("Error on reduce bottom and top")
	}
	*topAndBottom1 = protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Iron Man"),
		RatingAvgTop:    proto.Float64(7.2),
		TitleBottom:     proto.String("Mars Attack"),
		RatingAvgBottom: proto.Float64(1.2),
	}
	newTop = ReduceTopAndBottom(&topAndBottom2, topAndBottom1, 0)
	if *newTop.TitleTop != *topAndBottom2.TitleTop || *newTop.RatingAvgTop != *topAndBottom2.RatingAvgTop ||
		*newTop.TitleBottom != *topAndBottom1.TitleBottom || *newTop.RatingAvgBottom != *topAndBottom1.RatingAvgBottom {
		t.Fatal("Error on reduce bottom and top")
	}
	newTop = ReduceTopAndBottom(topAndBottom1, &topAndBottom2, 0)
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
			MessageId:   proto.Int64(0),
		})
	}
	toCheck := "Mark Rufallo(14) Morena Baccarin(6) Rosario Dawson(6) Franchella(5) Hugh Grant(5) Mel Gibson(5) Gina Carano(4) Julia Roberts(4) Vincent D'Onofrio(4) Chris Evans(3)"
	if !strings.Contains(protoUtils.Top10ToString(actorsData.GetTop10("", 0)), toCheck) {
		t.Fatal("Error on reduce top10")
	}
}

func TestGetOrInitKeyMap(t *testing.T) {
	init := func() int {
		return 1
	}
	aMap := make(map[string]int)
	aMap["0"] = GetOrInitKeyMap(&aMap, "0", init) + 1
	if aMap["0"] != 2 {
		t.Fatal("Error on map")
	}
	aMap["0"] = GetOrInitKeyMap(&aMap, "0", init) + 1
	if aMap["0"] != 3 {
		t.Fatal("Error on map")
	}
}

func TestMetricsReport(t *testing.T) {
	resultNegative := NewMetricResultClient("", 1, true)
	resultPositive := NewMetricResultClient("", 2, false)
	if resultNegative == nil && resultPositive == nil {
		t.Fatal("Error on new metric")
	}
	avgRevenueOverBudgetNegative := make(map[string]float64)
	avgRevenueOverBudgetPositive := make(map[string]float64)
	var report *protopb.Metrics
	_, err := CreateMetricsReport("", &avgRevenueOverBudgetNegative, &avgRevenueOverBudgetPositive, 0)
	if err == nil {
		t.Fatal("Error on report not null")
	}
	UpdateMetrics(&avgRevenueOverBudgetNegative, &avgRevenueOverBudgetPositive, resultNegative)
	_, err = CreateMetricsReport("", &avgRevenueOverBudgetNegative, &avgRevenueOverBudgetPositive, 0)
	if err == nil {
		t.Fatal("Error on report not null")
	}
	UpdateMetrics(&avgRevenueOverBudgetNegative, &avgRevenueOverBudgetPositive, resultPositive)
	if len(avgRevenueOverBudgetNegative) != 1 && len(avgRevenueOverBudgetPositive) != 1 {
		t.Fatal("Error on len")
	}
	valueNeg, okNeg := avgRevenueOverBudgetNegative[""]
	valuePos, okPos := avgRevenueOverBudgetPositive[""]
	if !okPos || !okNeg {
		t.Fatal("Error on map")
	}
	if valueNeg != resultNegative.AvgRevenueOverBudget || valuePos != resultPositive.AvgRevenueOverBudget {
		fmt.Printf("%v", valueNeg)
		t.Fatal("Error on value")
	}
	report, err = CreateMetricsReport("", &avgRevenueOverBudgetNegative, &avgRevenueOverBudgetPositive, 0)
	if report == nil {
		t.Fatal("Error on report")
	}
	if err != nil {
		t.Fatal("Error on report null")
	}
	value := protoUtils.MetricsToString(report)
	if !strings.Contains(value, "Negative") || !strings.Contains(value, "Positive") ||
		!strings.Contains(value, "2") || !strings.Contains(value, "1") {
		t.Fatal("Error on report values")
	}
}

func TestMetricsResult(t *testing.T) {
	count := make(map[string]int64)
	sumAvg := make(map[string]float64)
	//var result *MetricResultClient
	var err error
	_, err = CreateMetricResult("", false, &count, &sumAvg)
	if err == nil {
		t.Fatal("Result not null")
	}
	revenueMovie := 1000.0
	budgetMovie := 100
	clientID := ""
	// update sum and count
	count[""] = GetOrInitKeyMap(&count, clientID, InitCount) + 1
	avg := (revenueMovie) / float64(budgetMovie)
	sumAvg[clientID] = GetOrInitKeyMap(&sumAvg, clientID, InitSumAvg) + avg
	// try result
	_, err = CreateMetricResult("", false, &count, &sumAvg)
	if err != nil {
		t.Fatal("Result null")
	}
	if count[""] != 1 {
		t.Fatal("Error count")
	}
	if sumAvg[""] != 10 {
		t.Fatal("Error sumAvg")
	}

}

func TestGetOrInitKeyMapWithKey(t *testing.T) {
	globalTop5 := make(map[string]*protopb.Top5Country)
	found := GetOrInitKeyMapWithKeyAndMsgId(&globalTop5, "", 0, protoUtils.CreateMinimumTop5Country)
	if *globalTop5[""].ClientId != "" {
		t.Fatal("Error on map")
	}
	if *found.ClientId != "" {
		t.Fatal("Error on map")
	}
	globalTop5[""].Budget[0] = 1000
	found = GetOrInitKeyMapWithKeyAndMsgId(&globalTop5, "", 0, protoUtils.CreateMinimumTop5Country)
	if found.Budget[0] != 1000 {
		t.Fatal("Error on map")
	}
	if globalTop5[""].Budget[0] != 1000 {
		t.Fatal("Error on map")
	}
	found = GetOrInitKeyMapWithKeyAndMsgId(&globalTop5, "", 0, protoUtils.CreateMinimumTop5Country)
	found.Budget[0] = 2000
	found = GetOrInitKeyMapWithKeyAndMsgId(&globalTop5, "", 0, protoUtils.CreateMinimumTop5Country)
	if found.Budget[0] != 2000 {
		t.Fatal("Error on map")
	}
	if globalTop5[""].Budget[0] != 2000 {
		t.Fatal("Error on map")
	}
	actorsData := make(map[string]*ActorsData)
	actorsDataClient := GetOrInitKeyMapWithKey(&actorsData, "", InitActorsData)
	actorsDataClient.UpdateCount(&protopb.Actor{
		Name:        proto.String("Pepe"),
		ProfilePath: proto.String("Pepe.jpg"),
		CountMovies: proto.Int64(1000),
		ClientId:    proto.String(""),
	})
	actorsDataClient = GetOrInitKeyMapWithKey(&actorsData, "", InitActorsData)
	actorsDataClient.UpdateCount(&protopb.Actor{
		Name:        proto.String("Pepe"),
		ProfilePath: proto.String("Pepe.jpg"),
		CountMovies: proto.Int64(1000),
		ClientId:    proto.String(""),
	})
	top10 := actorsDataClient.GetTop10("", 1)
	if top10.CountMovies[0] != 2000 {
		t.Fatal("Error on map")
	}
}
