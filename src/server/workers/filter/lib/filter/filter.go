package filter

import (
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"

	"google.golang.org/protobuf/proto"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FilterConfig struct {
	Type string
	ID   int
}

type Filter struct {
	config  FilterConfig
	log     *logging.Logger
	conn    *amqp.Connection
	channel *amqp.Channel
}

// Creates a new filter with an established connection to rabbitmq (if succesful).
// If it fails, it exits the program.
func NewFilter(config *FilterConfig, log *logging.Logger) *Filter {
	log.Info("Connecting to RabbitMQ...")

	conn, err := rabbitmq.ConnectRabbitMQ(log)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}

	log.Info("Successful connection with RabbitMQ")

	return &Filter{
		config:  *config,
		log:     log,
		conn:    conn,
		channel: ch,
	}
}

func (f *Filter) StartFilterLoop() {
	switch f.config.Type {
	case "2000s_filter":
		f.process2000sFilter()
	case "ar_es_filter":
		f.processArEsFilter()
	case "ar_filter":
		f.processArFilter()
	case "top-5-investors-filter":
		f.processTop5InvestorsFilter()
	default:
		f.log.Errorf("Unknown filter type: %s", f.config.Type)
	}
}

func (f *Filter) Close() {
	if f.channel != nil {
		f.channel.Close()
	}

	if f.conn != nil {
		f.conn.Close()
	}
}

// Jobs --------------------------------------------------------------------------------------------

func (f *Filter) process2000sFilter() {
	inputQueue := "movies_sanit_queue"
	outputQueue := "movies_2000s_queue"

	f.log.Infof("[2000s_filter] Starting job for ID: %d", f.config.ID)

	_, err := f.channel.QueueDeclare(
		inputQueue, // Name
		true,       // Durable
		false,      // Delete when unused
		false,      // Exclusive
		false,      // No-Wait
		nil,        // Arguments
	)
	if err != nil {
		f.log.Fatalf("Failed to declare input queue: %v", err)
	}

	_, err = f.channel.QueueDeclare(
		outputQueue, // Name
		true,        // Durable
		false,       // Delete when unused
		false,       // Exclusive
		false,       // No-Wait
		nil,         // Arguments
	)
	if err != nil {
		f.log.Fatalf("Failed to declare output queue: %v", err)
	}

	// Subscribe to input queue
	msgs, err := f.channel.Consume(
		inputQueue,
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		f.log.Fatalf("Failed to consume messages: %v", err)
	}

	f.log.Infof("[2000s_filter] Waiting for messages...")

	for msg := range msgs {
		var movie protopb.MovieSanit
		err := proto.Unmarshal(msg.Body, &movie)
		if err != nil {
			f.log.Errorf("Failed to unmarshal message: %v", err)
			continue
		}

		if movie.Eof != nil && *movie.Eof {
			f.log.Infof("[2000s_filter] Received EOF marker")
			break
		}

		if movie.ReleaseYear != nil && *movie.ReleaseYear >= 2000 && *movie.ReleaseYear <= 2009 {
			f.log.Infof("[2000s_filter] Accepted: %s (%d)", movie.Title, movie.ReleaseYear)

			data, err := proto.Marshal(&movie)
			if err != nil {
				f.log.Errorf("Failed to marshal message: %v", err)
				continue
			}

			err = f.channel.Publish(
				"",          // exchange
				outputQueue, // routing key (cola de destino)
				false,       // mandatory
				false,       // immediate
				amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        data,
				})

			// TODO: ver como handlear el error
			if err != nil {
				f.log.Errorf("Failed to publish filtered message: %v", err)
			}
		}
	}
	f.log.Infof("[2000s_filter] Job finished")
}

func (f *Filter) processArEsFilter() {
	f.log.Infof("[ar_es_filter] Starting job for ID: %d", f.config.ID)
}

func (f *Filter) processArFilter() {
	f.log.Infof("[ar_filter] Starting job for ID: %d", f.config.ID)
}

func (f *Filter) processTop5InvestorsFilter() {
	f.log.Infof("[top_5_investors_filter] Starting job for ID: %d", f.config.ID)
}
