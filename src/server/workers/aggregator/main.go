package main

import (
	"sync"
	"tp1/health"
	"tp1/server/workers/aggregator/common"
)

/*
Environment variables:
AGGREGATOR_ID;
AGGREGATOR_TYPE: "movies" "top_5" "top_10" "top_and_bottom" "metrics";
AGGREGATOR_AMOUNT_SOURCES: 2 filters, 2 sources. 2 shards 2 sources (Even in the case of metrics);
AGGREGATOR_INPUT_QUEUE_NAME: "negative_movies" in the case of metrics;
AGGREGATOR_INPUT_QUEUE_SEC_NAME: "positive_movies" in the case of metrics;
AGGREGATOR_OUTPUT_QUEUE_NAME;
*/
func main() {
	common.Log.Info("Starting aggregator...")
	common.InitLogger()

	healthSrv := health.New(common.Log)
	healthSrv.Start()

	var aggregator, err = common.NewAggregator(common.Log)
	if err != nil {
		common.Log.Fatalf("error creating aggregator: %v", err)
	}

	common.Log.Infof("Aggregator type: %s | Aggregator ID: %s\n",
		aggregator.Config.AggregatorType,
		aggregator.Config.ID,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		aggregator.Start()
	}()

	healthSrv.MarkReady()

	// Wait for go routine to finish TODO: or SIGKILL signals
	wg.Wait()
	aggregator.Dispose()
}
