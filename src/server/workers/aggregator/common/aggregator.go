package common

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Aggregator struct {
	Channel     *amqp.Channel
	Connection  *amqp.Connection
	InputQueue  amqp.Queue
	OutputQueue amqp.Queue
	Config      AggregatorConfig
}

// Returns new aggregator ready to work with rabbit
func NewAggregator() (*Aggregator, error) {
	var config, err = LoadAggregatorConfig()
	if err != nil {
		return nil, err
	}

	maxRetries := 10
	var connection *amqp.Connection
	for i := 1; i <= maxRetries; i++ {
		connection, err = amqp.Dial(config.AmqUrl)
		if err == nil {
			break
		}
		fmt.Printf("Attempt %d: Could not connect to RabbitMQ: %v", i, err)
		if i < maxRetries {
			fmt.Printf("Retrying in 3 seconds...")
			time.Sleep(3 * time.Second)
		}
	}
	//connection, err := amqp.Dial(config.AmqUrl)
	if err != nil {
		fmt.Printf("Error dial: %v", err)
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		fmt.Printf("Error channel: %v", err)
		connection.Close()
		return nil, err
	}
	inputQueue, err := channel.QueueDeclare(
		config.InputQueue.Name,
		config.InputQueue.Durable,
		config.InputQueue.DeleteWhenUnused,
		config.InputQueue.Exclusive,
		config.InputQueue.NoWait,
		nil,
	)
	if err != nil {
		connection.Close()
		channel.Close()
		return nil, err
	}
	outputQueue, err := channel.QueueDeclare(
		config.OutputQueue.Name,
		config.OutputQueue.Durable,
		config.OutputQueue.DeleteWhenUnused,
		config.OutputQueue.Exclusive,
		config.OutputQueue.NoWait,
		nil,
	)
	if err != nil {
		connection.Close()
		channel.Close()
		return nil, err
	}
	return &Aggregator{
		Channel:     channel,
		Connection:  connection,
		InputQueue:  inputQueue,
		OutputQueue: outputQueue,
		Config:      *config,
	}, nil
}

func Dispose(aggregator *Aggregator) {
	if aggregator.Channel != nil {
		aggregator.Channel.Close()
	}
	if aggregator.Connection != nil {
		aggregator.Connection.Close()
	}
}
