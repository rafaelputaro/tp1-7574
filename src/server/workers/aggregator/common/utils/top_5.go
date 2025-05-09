package utils

import (
	"slices"
	"tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"
)

type CountryBudget struct {
	Budget             int64
	ProductionCountrie string
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
	for index := range len(top1.GetProductionCountries()) {
		countries = append(
			countries,
			CountryBudget{
				Budget:             top1.GetBudget()[index],
				ProductionCountrie: top1.GetProductionCountries()[index],
			})
	}
	for index := range len(top2.GetProductionCountries()) {
		countries = append(
			countries,
			CountryBudget{
				Budget:             top2.GetBudget()[index],
				ProductionCountrie: top2.GetProductionCountries()[index],
			})
	}
	slices.SortFunc(countries, cmpCountry)
	return countries
}

// Create a new top from two partials top's. len(top1)+len(top2) >= 5
func ReduceTop5(top1, top2 *protopb.Top5Country) *protopb.Top5Country {
	sorted := sortCountries(top1, top2)
	toReturn := protoUtils.CreateMinimumTop5Country(top1.GetClientId())
	for index := range 5 {
		toReturn.Budget[index] = sorted[index].Budget
		toReturn.ProductionCountries[index] = sorted[index].ProductionCountrie
	}
	return toReturn
}
