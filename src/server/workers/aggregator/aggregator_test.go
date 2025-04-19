package main

// https://www.rabbitmq.com/tutorials/tutorial-one-go

import (
	//"github-app/protobuf/protopb"

	//"time"

	//"os"
	"context"
	"testing"
	"time"
	"tp1/server/workers/aggregator/common"

	amqp "github.com/rabbitmq/amqp091-go"
	//"google.golang.org/protobuf/proto"
)

func TestAggregatorConfig(t *testing.T) {
	common.Log.Infof("Testing aggregator")
	common.InitLogger()
	var aggregator, _ = common.NewAggregator(common.Log)
	if aggregator != nil {
		common.Log.Infof("Config Aggregator: %v", aggregator.Config)

	} else {
		common.Log.Infof("Aggregator no se ha podido crear")
	}
	// Publish
	conn, _ := amqp.Dial(aggregator.Config.AmqUrl)
	ch, _ := conn.Channel()

	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	err := ch.PublishWithContext(
		ctx,
		"",                         // exchange
		aggregator.InputQueue.Name, // routing key
		false,                      // mandatory
		false,                      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte("Hola"),
		})
	if err != nil {
		common.Log.Infof("Error subir mensaje")
	}
	common.Log.Infof("Subio mensaje")
	//time.Sleep(120 * time.Second)
	common.Consume(aggregator)
	defer conn.Close()
	common.Dispose(aggregator)

}
