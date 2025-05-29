package main

import (
	"os"
	"sync"
	"tp1/health"

	"tp1/server/workers/filter/lib"
	"tp1/server/workers/filter/lib/filter"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("filter")

func initLogger() {
	format := logging.MustStringFormatter(
		`%{level:.5s} | %{shortfunc} | %{message}`,
	)
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}

func main() {
	log.Info("Starting filter...")
	initLogger()

	healthSrv := health.New(log)
	healthSrv.Start()

	config, err := lib.LoadConfig()
	if err != nil {
		log.Errorf("Error loading config: %+v", err)
		os.Exit(1)
	}
	log.Infof("Filter type: %s | Filter number: %d\n", config.Type, config.ID)

	var wg sync.WaitGroup
	wg.Add(1)
	f := filter.NewFilter(config, log)

	go func() {
		defer wg.Done()
		f.StartFilterLoop()
	}()

	healthSrv.MarkReady()

	// Wait for go routine to finish TODO: or SIGKILL signals
	wg.Wait()
	f.Close()
}
