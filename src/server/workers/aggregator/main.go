package main

import (
	"os"
	"sync"
	"tp1/server/workers/aggregator/common"
)

const MSG_ERROR_CREATE_AGGREGATOR = "Error on creating aggregator"

/*
Environment variables:
AGGREGATOR_ID;
AGGREGATOR_TYPE: "movies" "top_5" "top_10" "top_and_bottom" "metrics";
AGGREGATOR_AMOUNT_SOURCES: 2 filters, 2 sources. 2 shards 2 sources (Even in the case of metrics);
AGGREGATOR_INPUT_QUEUE_NAME: Negative in the case of metrics;
AGGREGATOR_INPUT_QUEUE_SEC_NAME: Possitive in the case of metrics;
AGGREGATOR_OUTPUT_QUEUE_NAME;
*/
func main() {
	common.Log.Info("Starting aggregator...")
	common.InitLogger()
	var aggregator, err = common.NewAggregator(common.Log)
	if err != nil {
		common.Log.Infof("%v: %v", MSG_ERROR_CREATE_AGGREGATOR, err)
		os.Exit(1)
	} else {
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
		// Wait for go routine to finish TODO: or SIGKILL signals
		wg.Wait()
		aggregator.Dispose()
	}
}
