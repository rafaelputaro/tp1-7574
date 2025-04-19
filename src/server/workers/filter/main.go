package main

import (
	"fmt"
	"os"
	"sync"

	"tp1/rabbitmq"
	"tp1/server/workers/filter/lib"

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
	initLogger()

	log.Info("Starting filter...")

	config, err := lib.LoadConfig()
	if err != nil {
		log.Errorf("Error loading config: %+v", err)
		os.Exit(1)
	}

	fmt.Printf("Filter type: %s | Filter number: %d\n", config.Type, config.Num)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		conn, err := rabbitmq.ConnectRabbitMQ(log)
		if err != nil {
			log.Fatalf("Could not connect to RabbitMQ: %v", err)
		}
		defer conn.Close()
		log.Info("Successful connection with RabbitMQ")
	}()

	// Wait for go routine to finish
	wg.Wait()
}
