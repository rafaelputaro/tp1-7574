package main

// https://www.rabbitmq.com/tutorials/tutorial-one-go

import (
	//"github-app/protobuf/protopb"
	"fmt"
	"time"

	//"os"
	"testing"

	"tp1/server/workers/aggregator/common"
	//"github.com/NeowayLabs/wabbit/amqptest"
	//"github.com/NeowayLabs/wabbit/amqptest/server"
	//"github.com/NeowayLabs/wabbit/amqp"
	//"google.golang.org/protobuf/proto"
)

func TestAggregatorConfig(t *testing.T) {
	fmt.Printf("TEsting on docker")
	var aggregator, _ = common.NewAggregator()
	if aggregator != nil {
		fmt.Printf("Config Aggregator: %v", aggregator.Config)
		//Dispose(aggregator)
	} else {
		fmt.Printf("Aggregator no se ha podido crear")

	}
	time.Sleep(30 * time.Second)
}
