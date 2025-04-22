package utils

import (
	"fmt"
	"slices"
	"tp1/protobuf/protopb"
)

type CountryBudget struct {
	Budget             int32
	ProductionCountrie string
}

func CreateEmptyTop5() protopb.Top5Country {
	return protopb.Top5Country{
		Budget:              []int32{0, 0, 0, 0, 0},
		ProductionCountries: []string{"Empty0", "Empty1", "Empty2", "Empty3", "Empty4"},
	}
}

// Returns: 1 country1 < country2; 0 country1 == country2; -1 country1 > country2
func cmpCountry(country1, country2 CountryBudget) int {
	if country1.Budget == country2.Budget {
		return 0
	}
	if country1.Budget > country2.Budget {
		return -1
	} else {
		return 1
	}
}

// Order from highest to lowest budget
func sortCountries(top1, top2 *protopb.Top5Country) []CountryBudget {
	countries := []CountryBudget{}
	for index := range len(top1.ProductionCountries) {
		countries = append(
			countries,
			CountryBudget{
				Budget:             top1.Budget[index],
				ProductionCountrie: top1.ProductionCountries[index],
			})
	}
	for index := range len(top2.ProductionCountries) {
		countries = append(
			countries,
			CountryBudget{
				Budget:             top2.Budget[index],
				ProductionCountrie: top2.ProductionCountries[index],
			})
	}
	slices.SortFunc(countries, cmpCountry)
	return countries
}

// Create a new top from two partials top's. len(top1)+len(top2) >= 5
func ReduceTop5(top1, top2 *protopb.Top5Country) *protopb.Top5Country {
	sorted := sortCountries(top1, top2)
	toReturn := CreateEmptyTop5()
	for index := range 5 {
		toReturn.Budget[index] = sorted[index].Budget
		toReturn.ProductionCountries[index] = sorted[index].ProductionCountrie
	}
	return &toReturn
}

// Returns string from Top5
func Top5ToString(top *protopb.Top5Country) string {
	toReturn := ""
	for index := range len(top.ProductionCountries) {
		toReturn += fmt.Sprintf("%v(US$ %v) ", top.ProductionCountries[index], top.Budget[index])
	}
	return toReturn
}
