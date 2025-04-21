package rabbitmq

import (
	"time"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Attempts to establish a connection with RabbitMQ, retrying at most 3 times with 3 seconds intervals
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
