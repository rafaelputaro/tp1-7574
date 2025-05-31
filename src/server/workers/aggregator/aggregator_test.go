package main

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"tp1/server/workers/aggregator/common"
)

func TestAggregatorConfig(t *testing.T) {
	common.Log.Infof("Testing aggregator")
	common.Log.Info("Starting aggregator...")
	fmt.Println("Testing Aggregator")
	common.InitLogger()
	var aggregator, err = common.NewAggregator(common.Log)
	if err != nil {
		common.Log.Infof("%v: %v", "error creating aggregator", err)
		os.Exit(1)
	} else {
		common.Log.Infof("Aggregator type: %s | Aggregator ID: %s\n",
			aggregator.Config.AggregatorType,
			aggregator.Config.ID,
		)
		common.Log.Infof("%v", aggregator.Config)
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
