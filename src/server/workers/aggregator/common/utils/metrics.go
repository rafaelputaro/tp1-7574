package utils

import (
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

func CreateMetricsReport(avgRevenueOverBudgetNegative float64, avgRevenueOverBudgetPositive float64) protopb.Metrics {
	return protopb.Metrics{
		AvgRevenueOverBudgetNegative: proto.Float64(avgRevenueOverBudgetNegative),
		AvgRevenueOverBudgetPositive: proto.Float64(avgRevenueOverBudgetPositive),
	}
}
