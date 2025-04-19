package main

import (
	"tp1/server/workers/aggregator/common"
)

// AGGREGATOR_ID: "1"
// AGGREGATOR_TYPE: "movies" "top_5" "top_10" "top_and_bottom" "metrics"
// AGGREGATOR_AMQP_URL: "amqp://admin:admin@rabbitmq:5672/"
// AGGREGATOR_AMOUNT_SOURCES:
// AGGREGATOR_INPUT_QUEUE_NAME:
// AGGREGATOR_OUTPUT_QUEUE_NAME:
func main() {
	common.Log.Info("Starting aggregator...")
	common.InitLogger()
}
