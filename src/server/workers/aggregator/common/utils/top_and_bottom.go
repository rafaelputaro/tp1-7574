package utils

import (
	"tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"
)

// Create a new top from two partials top's.
func ReduceTopAndBottom(topAndBottom1, topAndBottom2 *protopb.TopAndBottomRatingAvg) *protopb.TopAndBottomRatingAvg {
	toReturn := protoUtils.CreateSeedTopAndBottom(topAndBottom1.GetClientId())
	// Compare top
	if topAndBottom1.GetRatingAvgTop() > topAndBottom2.GetRatingAvgTop() {
		*toReturn.RatingAvgTop = topAndBottom1.GetRatingAvgTop()
		*toReturn.TitleTop = topAndBottom1.GetTitleTop()
	} else {
		*toReturn.RatingAvgTop = topAndBottom2.GetRatingAvgTop()
		*toReturn.TitleTop = topAndBottom2.GetTitleTop()
	}
	// Compare Bottom
	if topAndBottom1.GetRatingAvgBottom() < topAndBottom2.GetRatingAvgBottom() {
		*toReturn.RatingAvgBottom = topAndBottom1.GetRatingAvgBottom()
		*toReturn.TitleBottom = topAndBottom1.GetTitleBottom()
	} else {
		*toReturn.RatingAvgBottom = topAndBottom2.GetRatingAvgBottom()
		*toReturn.TitleBottom = topAndBottom2.GetTitleBottom()
	}
	return toReturn
}
