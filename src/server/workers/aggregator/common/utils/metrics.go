package utils

import (
	"fmt"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

func CreateMetricsReport(avgRevenueOverBudgetNegative float64, avgRevenueOverBudgetPositive float64) protopb.Metrics {
	return protopb.Metrics{
		AvgRevenueOverBudgetNegative: proto.Float64(avgRevenueOverBudgetNegative),
		AvgRevenueOverBudgetPositive: proto.Float64(avgRevenueOverBudgetPositive),
	}
}

func CreateMetricsEof() protopb.Metrics {
	return protopb.Metrics{
		AvgRevenueOverBudgetNegative: proto.Float64(0.0),
		AvgRevenueOverBudgetPositive: proto.Float64(0.0),
		Eof:                          proto.Bool(true),
	}
}

func MetricsToString(metrics *protopb.Metrics) string {
	return fmt.Sprintf("Negative: %v | Positive: %v", metrics.AvgRevenueOverBudgetNegative, metrics.AvgRevenueOverBudgetPositive)
}
