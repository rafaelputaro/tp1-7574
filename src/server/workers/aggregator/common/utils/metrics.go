package utils

import (
	"errors"
	"tp1/protobuf/protopb"

	"google.golang.org/protobuf/proto"
)

const MSG_REPORT_NOT_READY = "report is not ready"
const MSG_ERROR_ON_CREATE_RESULT = "error calculating a metric"

type MetricResultClient struct {
	ClientID             string
	AvgRevenueOverBudget float64
	IsNegative           bool
}

func NewMetricResultClient(clientID string, avgRevenueOverBudget float64, isNegative bool) *MetricResultClient {
	return &MetricResultClient{
		ClientID:             clientID,
		AvgRevenueOverBudget: avgRevenueOverBudget,
		IsNegative:           isNegative,
	}
}

func UpdateMetrics(avgRevenueOverBudgetNegative *map[string]float64, avgRevenueOverBudgetPositive *map[string]float64, metric *MetricResultClient) {
	clientID := metric.ClientID
	if metric.IsNegative {
		(*avgRevenueOverBudgetNegative)[clientID] = metric.AvgRevenueOverBudget
	} else {
		(*avgRevenueOverBudgetPositive)[clientID] = metric.AvgRevenueOverBudget
	}
}

func CreateMetricsReport(clientID string, avgRevenueOverBudgetNegative *map[string]float64, avgRevenueOverBudgetPositive *map[string]float64, messageId int64) (*protopb.Metrics, error) {
	avgROBNeg, okNeg := (*avgRevenueOverBudgetNegative)[clientID]
	avgROBPos, okPos := (*avgRevenueOverBudgetPositive)[clientID]
	if okNeg && okPos {
		return &protopb.Metrics{
			AvgRevenueOverBudgetNegative: proto.Float64(avgROBNeg),
			AvgRevenueOverBudgetPositive: proto.Float64(avgROBPos),
			ClientId:                     proto.String(clientID),
			MessageId:                    proto.Int64(messageId),
		}, nil
	}
	return nil, errors.New(MSG_REPORT_NOT_READY)
}

func CreateMetricResult(clientID string, isNegative bool, countMap *map[string]int64, sumAvgMap *map[string]float64) (*MetricResultClient, error) {
	count, okCount := (*countMap)[clientID]
	sumAvg, okSumAvg := (*sumAvgMap)[clientID]
	if okCount && okSumAvg {
		var avgRevenueOverBudget float64
		if count != 0 {
			avgRevenueOverBudget = sumAvg / float64(count)
		} else {
			avgRevenueOverBudget = 0
		}
		return NewMetricResultClient(clientID, avgRevenueOverBudget, isNegative), nil
	}
	return nil, errors.New(MSG_ERROR_ON_CREATE_RESULT)
}

func InitCount() int64 {
	return 0
}

func InitSumAvg() float64 {
	return 0.0
}
