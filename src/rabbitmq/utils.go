package rabbitmq

import (
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Attempts to establish a connection with RabbitMQ, retrying at most 5 times with 5 seconds intervals
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

// Attempts to declare a direct queue in rabbitmq
func DeclareDirectQueue(channel *amqp.Channel, queue string) error {
	_, err := channel.QueueDeclare(
		queue, // name
		true,  // durable: the queue is maintained event after the broker is resetted
		false, // autoDelete: the queue is not automatically deleted when there are no more consumers
		false, // exclusive: the queue can be accessed by multiple connections
		false, // no-wait: wait for the broker's confirmation to know if the queue was succesfully created
		nil,   // args
	)
	return err
}

func ConsumeFromQueue(channel *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	return channel.Consume(
		queue, // name
		"",    // consumerTag: "" lets rabbitmq generate a tag for this consumer
		true,  // autoAck: when a msg arrives, the consumers acks the msg
		false, // exclusive: allow others to consume from the queue
		false, // no-local: ignored field
		false, // no-wait: wait for confirmation of the consumers correct registration
		nil,   // args
	)
}
