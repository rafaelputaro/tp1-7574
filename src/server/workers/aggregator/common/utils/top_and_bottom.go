package utils

import (
	"fmt"
	"math"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

func CreateEmptyTopAndBottom() protopb.TopAndBottomRatingAvg {
	return protopb.TopAndBottomRatingAvg{
		TitleTop:        proto.String("Empty1"),
		TitleBottom:     proto.String("Empty2"),
		RatingAvgTop:    proto.Float64(0.0),
		RatingAvgBottom: proto.Float64(math.MaxFloat64),
	}
}

// Create a new top from two partials top's.
func ReduceTopAndBottom(topAndBottom1, topAndBottom2 *protopb.TopAndBottomRatingAvg) *protopb.TopAndBottomRatingAvg {
	toReturn := CreateEmptyTopAndBottom()
	// Compare top
	if *topAndBottom1.RatingAvgTop > *topAndBottom2.RatingAvgTop {
		toReturn.RatingAvgTop = topAndBottom1.RatingAvgTop
		toReturn.TitleTop = topAndBottom1.TitleTop
	} else {
		toReturn.RatingAvgTop = topAndBottom2.RatingAvgTop
		toReturn.TitleTop = topAndBottom2.TitleTop
	}
	// Compare Bottom
	if *topAndBottom1.RatingAvgBottom < *topAndBottom2.RatingAvgBottom {
		toReturn.RatingAvgBottom = topAndBottom1.RatingAvgBottom
		toReturn.TitleBottom = topAndBottom1.TitleBottom
	} else {
		toReturn.RatingAvgBottom = topAndBottom2.RatingAvgBottom
		toReturn.TitleBottom = topAndBottom2.TitleBottom
	}
	return &toReturn
}

// Returns string from TopAndBottomRagingAvg
func TopAndBottomToString(topAndBottom *protopb.TopAndBottomRatingAvg) string {
	return fmt.Sprintf("Top: %s(%v) | Bottom: %s(%v)",
		*topAndBottom.TitleTop, *topAndBottom.RatingAvgTop,
		*topAndBottom.TitleBottom, *topAndBottom.RatingAvgBottom)
}
