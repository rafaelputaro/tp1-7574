package main

import (
	"os"
	"sync"
	"tp1/health"
	"tp1/server/workers/joiner/common"
)

func main() {
	common.Log.Info("Starting joiner...")
	common.InitLogger()

	healthSrv := health.New(common.Log)
	healthSrv.Start()

	var joiner, err = common.NewJoiner(common.Log) // todo: error always nil
	if err != nil {
		common.Log.Infof("Error creating joiner: %v", err)
		os.Exit(1)
	}

	common.Log.Infof("Joiner type: %s | Joiner ID: %s\n",
		joiner.Config.JoinerType,
		joiner.Config.ID,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		joiner.Start()
	}()

	healthSrv.MarkReady()

	// Wait for go routine to finish TODO: or SIGKILL signals
	wg.Wait()
	joiner.Dispose()
}
