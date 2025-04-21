package main

// https://www.rabbitmq.com/tutorials/tutorial-one-go

import (
	//"github-app/protobuf/protopb"

	//"time"

	//"os"

	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	//"time"
	"tp1/server/workers/aggregator/common"
	//amqp "github.com/rabbitmq/amqp091-go"
	//"google.golang.org/protobuf/proto"
)

func TestAggregatorConfig(t *testing.T) {
	common.Log.Infof("Testing aggregator")
	common.Log.Info("Starting aggregator...")
	fmt.Println("Testing Aggregator")
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

	/*
		err := aggregator.Channel.Publish("", aggregator.Config.InputQueue.Name, false, false, amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Hola"),
		})
		if err != nil {
			common.Log.Infof("Error subir mensaje")
		}*/
	//time.Sleep(20 * time.Second)
	//	common.Log.Infof("Subio mensaje")

	//	common.Consume(aggregator)

	time.Sleep(600 * time.Second)
}
