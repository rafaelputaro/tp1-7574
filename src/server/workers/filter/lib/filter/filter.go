package filter

import (
	"fmt"
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"

	"google.golang.org/protobuf/proto"

	"slices"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FilterConfig struct {
	Type   string
	Shards int
	ID     int
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
// until an EOF is received.
// Used for query 1: "Peliculas y sus géneros de los años 00' con producción Argentina y Española"
func (f *Filter) processArEsFilter() {
	inputQueue := "movies_2000s"
	outputQueue := "movies_ar_es_2000s"
	filterName := "ar_es_filter"

	filterFunc := func(movie *protopb.MovieSanit) bool {
		productionCountries := movie.GetProductionCountries()

		return slices.Contains(productionCountries, "Argentina") && slices.Contains(productionCountries, "Spain")
	}

	f.log.Infof("[%s] Starting job for ID: %d", filterName, f.config.ID)

	// Declare queues (if rabbitmq doesn't have them, it creates them)
	for _, queue := range []string{inputQueue, outputQueue} {
		err := rabbitmq.DeclareDirectQueue(f.channel, queue)
		if err != nil {
			f.log.Fatalf("Failed to declare queue '%s': %v", queue, err)
		}
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, inputQueue)
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

func (f *Filter) processTop5InvestorsFilter() {
	// inputQueue := "movies"
	// outputQueue := "movies_top_5_investors"
	// filterName := "top_5_investors_filter"

	// TODO: hay que guardar un estado interno del top 5 actual y si la nueva entrada no entra en el
	// top 5 entonces se descarta. Al recibir el eof se envía el top 5 al aggregator
}

// Filter that reads from `movies` queue and writes to 3 different queues with the following conditions:
// * Year between 2000 and 2009
// * Year greater than 2000
// * Year greater or equal to 2000
// Used in the following queries:
// * 1: "Peliculas y sus géneros de los años 00' con producción Argentina y Española"
// * 3: "Películas de Producción Argentina estrenadas a partir del 2000, con mayor y menor promedio de rating"
// * 4: "Top 10 de actores con mayor participación en películas de producción Argentina posterior al 2000"
func (f *Filter) processYearFilters() {
	exchangeName := "movies_exchange"
	exchangeType := "fanout"

	outputQueues := map[string]func(uint32) bool{
		"movies_2000_to_2009":   func(year uint32) bool { return year >= 2000 && year <= 2009 },
		"movies_after_2000":     func(year uint32) bool { return year > 2000 },
		"movies_2000_and_later": func(year uint32) bool { return year >= 2000 },
	}
	filterName := "year_branch_filter"

	f.log.Infof("[%s] Starting job for ID: %d", filterName, f.config.ID)

	err := f.channel.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to declare exchange: %v", filterName, err)
	}

	// Declare temporal queue used to read from the exchange
	inputQueue, err := f.channel.QueueDeclare(
		"",    // empty = temporal queue with generated name
		false, // durable
		true,  // auto-delete when unused
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to declare temporary queue: %v", filterName, err)
	}

	// Bind the queue to the exchange
	err = f.channel.QueueBind(
		inputQueue.Name, // queue name
		"",              // routing key (empty on a fanout)
		exchangeName,    // exchange
		false,
		nil,
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to bind queue to exchange: %v", filterName, err)
	}

	for outputQueue := range outputQueues {
		err := rabbitmq.DeclareDirectQueue(f.channel, outputQueue)
		if err != nil {
			f.log.Fatalf("[%s] Failed to declare queue '%s': %v", filterName, outputQueue, err)
		}
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, inputQueue.Name)
	if err != nil {
		f.log.Fatalf("[%s] Failed to consume messages from '%s': %v", filterName, inputQueue.Name, err)
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

func (f *Filter) processArFilter() {
	inputQueues := [2]string{"movies_2000_and_later", "movies_after_2000"}
	outputQueuePrefixes := [2]string{"ar_movies_2000_and_later_shard", "ar_movies_after_2000_shard"}

	for _, inputQueue := range inputQueues {
		err := rabbitmq.DeclareDirectQueue(f.channel, inputQueue)
		if err != nil {
			f.log.Fatalf("Failed to declare queue '%s': %v", inputQueue, err)
		}
	}

	// Sharded output queues
	for _, prefix := range outputQueuePrefixes {
		// TODO: usar una sola queue pero con routing_key
		for i := 0; i < f.config.Shards; i++ {
			outputQueue := fmt.Sprintf("%s_%d", prefix, i)

			err := rabbitmq.DeclareDirectQueue(f.channel, outputQueue)
			if err != nil {
				f.log.Fatalf("Failed to declare queue '%s': %v", outputQueue, err)
			}
		}
	}
}
