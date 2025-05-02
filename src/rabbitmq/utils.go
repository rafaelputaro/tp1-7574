package rabbitmq

import (
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
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func ConsumeFromQueue(channel *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queue,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
}

func DeclareFanoutExchanges(channel *amqp.Channel, exchanges ...string) error {
	for _, name := range exchanges {
		err := channel.ExchangeDeclare(
			name,
			"fanout",
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
