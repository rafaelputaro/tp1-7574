package common

import (
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Aggregator struct {
	Channel     *amqp.Channel
	Connection  *amqp.Connection
	InputQueue  amqp.Queue
	OutputQueue amqp.Queue
	Config      AggregatorConfig
	Log         *logging.Logger
}

// Returns new aggregator ready to work with rabbit
func NewAggregator(log *logging.Logger) (*Aggregator, error) {
	var config, err = LoadAggregatorConfig()
	if err != nil {
		log.Errorf("Configuration could not be read from config file. Using env variables instead")
		return nil, err
	}
	maxRetries := 10
	var connection *amqp.Connection
	for i := 1; i <= maxRetries; i++ {
		connection, err = amqp.Dial(config.AmqUrl)
		if err == nil {
			break
		}
		log.Errorf("Attempt %d: Could not connect to RabbitMQ: %v", i, err)
		if i < maxRetries {
			log.Infof("Retrying in 3 seconds...")
			time.Sleep(3 * time.Second)
		}
	}
	if err != nil {
		log.Errorf("Error dial: %v", err)
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		log.Errorf("Error channel: %v", err)
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
		Log:         log,
	}, nil
}

func Consume(aggregator *Aggregator) {
	//msgs, _ :=
	msgs, err := aggregator.Channel.Consume(
		aggregator.InputQueue.Name,
		"",
		true,
		aggregator.Config.InputQueue.Exclusive,
		false,
		aggregator.Config.InputQueue.NoWait,
		nil,
	)
	if err != nil {
		aggregator.Log.Infof("error al consumir")
	} else {
		aggregator.Log.Infof("Longitud consumido: %v", len(msgs))
		/*
			for d := range msgs {
				aggregator.Log.Infof("Received a message: %s", d.Body)
			}*/
	}

}

func Dispose(aggregator *Aggregator) {
	aggregator.Log.Infof("Close aggregator")
	if aggregator.Channel != nil {
		aggregator.Channel.Close()
	}
	if aggregator.Connection != nil {
		aggregator.Connection.Close()
	}
}
