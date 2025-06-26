package rabbitmq

import (
	"fmt"
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

func ConnectRabbitMQ(log *logging.Logger) (*amqp.Connection, error) {
	var conn *amqp.Connection
	var err error
	maxRetries := 20

	for i := 1; i <= maxRetries; i++ {
		conn, err = amqp.Dial("amqp://admin:admin@rabbitmq:5672/")
		if err == nil {
			return conn, nil
		}

		log.Infof("Attempt %d: Could not connect to RabbitMQ: %v", i, err)
		if i < maxRetries {
			log.Info("Retrying in 3 seconds...")
			time.Sleep(3 * time.Second)
		}
	}

	return nil, err
}

func DeclareDirectQueues(channel *amqp.Channel, queues ...string) error {
	for _, queue := range queues {
		_, err := channel.QueueDeclare(
			queue,
			true,
			false,
			false,
			false,
			map[string]interface{}{"x-queue-type": "stream"},
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func DeclareDirectQueuesWithFreshChannel(conn *amqp.Connection, queues ...string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	return DeclareDirectQueues(ch, queues...)
}

func DeclareTemporaryQueue(channel *amqp.Channel) (amqp.Queue, error) {
	queue, err := channel.QueueDeclare(
		"",
		false,
		true,
		true,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, err
	}

	return queue, nil
}

func DeclareTopicExchanges(channel *amqp.Channel, exchanges ...string) error {
	for _, name := range exchanges {
		err := channel.ExchangeDeclare(
			name,
			"topic",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func DeclareDirectExchanges(channel *amqp.Channel, exchanges ...string) error {
	for _, name := range exchanges {
		err := channel.ExchangeDeclare(
			name,
			"direct",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func BindQueueToExchange(channel *amqp.Channel, queue, exchange, routingKey string) error {
	return channel.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	)
}

func BindQueueWithFreshChannel(conn *amqp.Connection, queue, exchange, routingKey string) error {
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func(ch *amqp.Channel) {
		_ = ch.Close()
	}(ch)

	return ch.QueueBind(queue, routingKey, exchange, false, nil)
}

func ConsumeFromQueue(channel *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	// Set prefetch count to 1 for streams
	err := channel.Qos(
		1,     // prefetch count - set to 1 as specified
		0,     // prefetch size (0 means no specific size limit)
		false, // global - false means apply to this consumer only
	)
	if err != nil {
		return nil, fmt.Errorf("failed to set QoS for stream queue '%s': %v", queue, err)
	}

	return channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func ConsumeFromQueue2(channel *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
}

func SingleAck(msg amqp.Delivery) error {
	return msg.Ack(false)
}

func Publish(channel *amqp.Channel, exchange, routingKey string, data []byte) error {
	return channel.Publish(
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/protobuf",
			Body:        data,
		},
	)
}

func ShutdownConnection(conn *amqp.Connection) {
	if conn != nil {
		_ = conn.Close()
	}
}

func ShutdownChannel(ch *amqp.Channel) {
	if ch != nil {
		_ = ch.Close()
	}
}
