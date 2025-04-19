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

type MovieFilterFunc func(movie *protopb.MovieSanit) bool

func (f *Filter) process2000sFilter() {
	inputQueue := "movies_sanit_queue"
	outputQueue := "movies_2000s_queue"
	filterName := "2000s_filter"

	filterFunc := func(movie *protopb.MovieSanit) bool {
		return movie.ReleaseYear != nil && *movie.ReleaseYear >= 2000 && *movie.ReleaseYear <= 2009
	}

	f.runFilterJob(inputQueue, outputQueue, filterName, filterFunc)
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

func (f *Filter) runFilterJob(
	inputQueue string,
	outputQueue string,
	filterName string,
	filterFunc MovieFilterFunc,
) {
	f.log.Infof("[%s] Starting job for ID: %d", filterName, f.config.ID)

	// Declare queues (if rabbitmq doesn't have them, it creates them)
	for _, queue := range []string{inputQueue, outputQueue} {
		_, err := f.channel.QueueDeclare(
			queue, true, false, false, false, nil,
		)
		if err != nil {
			f.log.Fatalf("Failed to declare queue '%s': %v", queue, err)
		}
	}

	msgs, err := f.channel.Consume(inputQueue, "", true, false, false, false, nil)
	if err != nil {
		f.log.Fatalf("Failed to consume messages from '%s': %v", inputQueue, err)
	}

	f.log.Infof("[%s] Waiting for messages...", filterName)

	for msg := range msgs {
		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			f.log.Errorf("[%s] Failed to unmarshal message: %v", filterName, err)
			continue
		}

		// EOF
		if movie.Eof != nil && *movie.Eof {
			f.log.Infof("[%s] Received EOF marker", filterName)
			break
		}

		if filterFunc(&movie) {
			f.log.Infof("[%s] Accepted: %s (%d)", filterName, movie.Title, movie.ReleaseYear)

			data, err := proto.Marshal(&movie)
			if err != nil {
				f.log.Errorf("[%s] Failed to marshal message: %v", filterName, err)
				continue
			}

			err = f.channel.Publish("", outputQueue, false, false, amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        data,
			})
			if err != nil {
				f.log.Errorf("[%s] Failed to publish filtered message: %v", filterName, err)
			}
		}
	}
	f.log.Infof("[%s] Job finished", filterName)
}
