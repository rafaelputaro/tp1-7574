package main

import (
	"sync"
	"tp1/health"
	"tp1/server/workers/aggregator/common"
)

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
