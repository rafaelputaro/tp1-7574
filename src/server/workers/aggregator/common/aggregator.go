package common

import (
	"tp1/protobuf/protopb"
	rabbitUtils "tp1/rabbitmq"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// Aggregator types:
const MOVIES string = "movies"
const TOP5 string = "top_5"
const TOP10 string = "top_10"
const TOP_AND_BOTTOM string = "top_and_bottom"
const METRICS string = "metrics"

// Messages to log:
const MSG_ERROR_CONFIG = "Configuration could not be read from config file. Using env variables instead"
const MSG_ERROR_DIAL = "Error on dial rabbitmq"
const MSG_ERROR_ON_CREATE_CHANNEL = "Error on create rabbitmq channel"
const MSG_ERROR_ON_DECLARE_QUEUE = "Error on declare queue"
const MSG_START = "Starting job for ID"
const MSG_FAILED_CONSUME = "Failed to consume messages from"
const MSG_JOB_FINISHED = "Job finished"
const MSG_FAILED_TO_UNMARSHAL = "Failed to unmarshal message"
const MSG_RECEIVED_EOF_MARKER = "Received EOF marker"
const MSG_AGGREGATED = "Accepted"
const MSG_FAILED_TO_MARSHAL = "Failed to marshal message"
const MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE = "Failed to publish on outputqueue"

// Aggregator which can be of types "movies", "top_5", "top_10", "top_and_bottom" and "metrices".
// In the case of type "metrics" works with two queues:
// InputQueue: Negative
// InputQueueSec: Possitive
type Aggregator struct {
	Channel       *amqp.Channel
	Connection    *amqp.Connection
	InputQueue    amqp.Queue
	InputQueueSec amqp.Queue
	OutputQueue   amqp.Queue
	Config        AggregatorConfig
	Log           *logging.Logger
}

// Returns new aggregator ready to work with rabbit
func NewAggregator(log *logging.Logger) (*Aggregator, error) {
	var config, err = LoadAggregatorConfig()
	if err != nil {
		log.Fatalf(MSG_ERROR_CONFIG)
		return nil, err
	}
	connection, err := rabbitUtils.ConnectRabbitMQ(log)
	if err != nil {
		log.Fatalf("%v: %v", MSG_ERROR_DIAL, err)
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("%v: %v", MSG_ERROR_ON_CREATE_CHANNEL, err)
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
		log.Fatalf("%v: %v", MSG_ERROR_ON_DECLARE_QUEUE, err)
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
		log.Fatalf("%v: %v", MSG_ERROR_ON_DECLARE_QUEUE, err)
		return nil, err
	}
	if config.AggregatorType == METRICS {
		inputQueueSec, err := channel.QueueDeclare(
			config.InputQueueSec.Name,
			config.InputQueueSec.Durable,
			config.InputQueueSec.DeleteWhenUnused,
			config.InputQueueSec.Exclusive,
			config.InputQueueSec.NoWait,
			nil,
		)
		if err != nil {
			connection.Close()
			channel.Close()
			log.Fatalf("%v: %v", MSG_ERROR_ON_DECLARE_QUEUE, err)
			return nil, err
		}
		return &Aggregator{
			Channel:       channel,
			Connection:    connection,
			InputQueue:    inputQueue,
			InputQueueSec: inputQueueSec,
			OutputQueue:   outputQueue,
			Config:        *config,
			Log:           log,
		}, nil
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

func (aggregator *Aggregator) Start() {
	aggregator.Log.Infof("[%s] %s: %d", aggregator.Config.AggregatorType, MSG_START, aggregator.Config.ID)
	switch aggregator.Config.AggregatorType {
	case MOVIES:
		aggregator.aggregateMovies()
	case TOP5:
		aggregator.aggregateTop5()
	case TOP10:
		aggregator.aggregateTop10()
	case TOP_AND_BOTTOM:
		aggregator.aggregateTopAndBottom()
	case METRICS:
		aggregator.aggregateMetrics()
	}
	aggregator.Log.Infof("[%s] %s", aggregator.Config.AggregatorType, MSG_JOB_FINISHED)
}

func (aggregator *Aggregator) checkEofSingleQueue(amountEOF int) bool {
	if amountEOF == int(aggregator.Config.AmountSources) {
		aggregator.Log.Infof("[%s] %s", aggregator.Config.AggregatorType, MSG_RECEIVED_EOF_MARKER)
		return true
	}
	return false
}

func (aggregator *Aggregator) checkEofQueues(amountEOF int, amountQueesPerSource int) bool {
	if amountEOF == amountQueesPerSource*int(aggregator.Config.AmountSources) {
		aggregator.Log.Infof("[%s] %s", aggregator.Config.AggregatorType, MSG_RECEIVED_EOF_MARKER)
		return true
	}
	return false
}

func (aggregator *Aggregator) aggregateMovies() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err == nil {
		amountEOF := 0
		for msg := range msgs {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				aggregator.Log.Errorf("[%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
				continue
			}
			// EOF
			if movie.Eof != nil && *movie.Eof {
				amountEOF += 1
				if aggregator.checkEofSingleQueue(amountEOF) {
					break
				}
			}
			aggregator.Log.Debugf("[%s] %s: %s (%d)", aggregator.Config.AggregatorType, MSG_AGGREGATED, movie.Title, movie.ReleaseYear)
			data, err := proto.Marshal(&movie)
			if err != nil {
				aggregator.Log.Errorf("[%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_MARSHAL, err)
				continue
			}
			// send to report
			err = aggregator.Channel.Publish("", aggregator.Config.OutputQueue.Name, false, false, amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        data,
			})
			if err != nil {
				aggregator.Log.Errorf("[%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE, err)
			}
		}
	}
}

func (aggregator *Aggregator) aggregateTop5() {

}

func (aggregator *Aggregator) aggregateTop10() {

}

func (aggregator *Aggregator) aggregateTopAndBottom() {

}

func (aggregator *Aggregator) aggregateMetrics() {

}

func (aggregator *Aggregator) consumeQueue(
	config QueueConfig,
) (<-chan amqp.Delivery, error) {
	msgs, err := aggregator.Channel.Consume(
		config.Name,      // name
		"",               // consumerTag: "" lets rabbitmq generate a tag for this consumer
		true,             // autoAck: when a msg arrives, the consumers acks the msg
		config.Exclusive, // exclusive: allow others to consume from the queue
		false,            // no-local: ignored field
		config.NoWait,    // no-wait: wait for confirmation of the consumers correct registration
		nil,              // args
	)
	if err != nil {
		aggregator.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, config.Name, err)
	}
	aggregator.Log.Infof("[%s] Waiting for messages...", aggregator.Config.AggregatorType)
	return msgs, err
}

func (aggregator *Aggregator) Dispose() {
	aggregator.Log.Infof("Close aggregator")
	if aggregator.Channel != nil {
		aggregator.Channel.Close()
	}
	if aggregator.Connection != nil {
		aggregator.Connection.Close()
	}
}
