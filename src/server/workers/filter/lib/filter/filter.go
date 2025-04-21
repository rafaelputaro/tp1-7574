package filter

import (
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"

	"google.golang.org/protobuf/proto"

	"slices"

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
		f.processYearFilters()
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

// Reads result from 2000s filter and returns the movies produced by both Spain and Argentina
func (f *Filter) processArEsFilter() {
	inputQueue := "movies_2000s"
	outputQueue := "movies_ar_es_2000s"
	filterName := "ar_es_filter"

	filterFunc := func(movie *protopb.MovieSanit) bool {
		productionCountries := movie.GetProductionCountries()

		return slices.Contains(productionCountries, "Argentina") && slices.Contains(productionCountries, "Spain")
	}

	f.runFilterJob(inputQueue, outputQueue, filterName, filterFunc)
}

func (f *Filter) processArFilter() {
	inputQueue := "movies_2000_and_later"
	outputQueue := "movies_ar"
	filterName := "ar_filter"

	filterFunc := func(movie *protopb.MovieSanit) bool {
		productionCountries := movie.GetProductionCountries()
		return slices.Contains(productionCountries, "Argentina")
	}

	f.runFilterJob(inputQueue, outputQueue, filterName, filterFunc)
}

func (f *Filter) processTop5InvestorsFilter() {
	// inputQueue := "movies"
	// outputQueue := "movies_top_5_investors"
	// filterName := "top_5_investors_filter"

	// TODO: hay que guardar un estado interno del top 5 actual y si la nueva entrada no entra en el
	// top 5 entonces se descarta. Al recibir el eof se envía el top 5 al aggregator
}

// Reads messages from input queue until an EOF is received. Filters based on the filterFunc and
// sends the resultas through the output queue
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
			queue, // name
			true,  // durable: the queue is maintained event after the broker is resetted
			false, // autoDelete: the queue is not automatically deleted when there are no more consumers
			false, // exclusive: the queue can be accessed by multiple connections
			false, // no-wait: wait for the broker's confirmation to know if the queue was succesfully created
			nil,   // args
		)
		if err != nil {
			f.log.Fatalf("Failed to declare queue '%s': %v", queue, err)
		}
	}

	msgs, err := f.channel.Consume(
		inputQueue, // name
		"",         // consumerTag: "" lets rabbitmq generate a tag for this consumer
		true,       // autoAck: when a msg arrives, the consumers acks the msg
		false,      // exclusive: allow others to consume from the queue
		false,      // no-local: ignored field
		false,      // no-wait: wait for confirmation of the consumers correct registration
		nil,        // args
	)
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
			f.log.Debugf("[%s] Accepted: %s (%d)", filterName, movie.GetProductionCountries(), movie.GetReleaseYear())

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

// Filter that reads from `movies` queue and writes to 3 different queues with the following conditions:
// * Year between 2000 and 2009
// * Year greater than 2000
// * Year greater or equal to 2000
func (f *Filter) processYearFilters() {
	inputQueue := "movies"
	outputQueues := map[string]func(uint32) bool{
		"movies_2000_to_2009":   func(year uint32) bool { return year >= 2000 && year <= 2009 },
		"movies_after_2000":     func(year uint32) bool { return year > 2000 },
		"movies_2000_and_later": func(year uint32) bool { return year >= 2000 },
	}
	filterName := "year_branch_filter"

	f.log.Infof("[%s] Starting job for ID: %d", filterName, f.config.ID)

	// Declare queues (input and output)
	queues := []string{"movies", "movies_2000_to_2009", "movies_after_2000", "movies_2000_and_later"}
	for _, queue := range queues {
		// TODO: func declareDirectQueue
		_, err := f.channel.QueueDeclare(
			queue, true, false, false, false, nil,
		)
		if err != nil {
			f.log.Fatalf("[%s] Failed to declare queue '%s': %v", filterName, queue, err)
		}
	}

	msgs, err := f.channel.Consume(inputQueue, "", true, false, false, false, nil)

	if err != nil {
		f.log.Fatalf("[%s] Failed to consume messages from '%s': %v", filterName, inputQueue, err)
	}

	f.log.Infof("[%s] Waiting for messages...", filterName)

	for msg := range msgs {
		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			f.log.Errorf("[%s] Failed to unmarshal message: %v", filterName, err)
			continue
		}

		if movie.Eof != nil && *movie.Eof {
			f.log.Infof("[%s] Received EOF marker", filterName)
			break
		}

		releaseYear := movie.GetReleaseYear()

		data, err := proto.Marshal(&movie)
		if err != nil {
			f.log.Errorf("[%s] Failed to marshal message: %v", filterName, err)
			continue
		}

		for queueName, condition := range outputQueues {
			if condition(releaseYear) {
				f.log.Debugf("[%s] -> [%s] %s (%d)", filterName, queueName, movie.GetTitle(), releaseYear)
				err = f.channel.Publish("", queueName, false, false, amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        data,
				})
				if err != nil {
					f.log.Errorf("[%s] Failed to publish to '%s': %v", filterName, queueName, err)
				}
			}
		}
	}

	f.log.Infof("[%s] Job finished", filterName)
}
