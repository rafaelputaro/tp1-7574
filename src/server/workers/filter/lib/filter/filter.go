package filter

import (
	"fmt"
	"hash/fnv"
	"sync"
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
	case "single_country_origin_filter":
		f.processSingleCountryOriginFilter()
	case "top_5_investors_filter":
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
	inputQueue := "movies_2000_to_2009"
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
	outputExchanges := [2]string{"ar_movies_2000_and_later_exchange", "ar_movies_after_2000_exchange"}
	filterName := "ar_filter"

	filterFunc := func(movie *protopb.MovieSanit) bool {
		productionCountries := movie.GetProductionCountries()

		return slices.Contains(productionCountries, "Argentina")
	}

	shardingFunc := func(movie *protopb.MovieSanit) int {
		return (int(movie.GetId()) % f.config.Shards) + 1
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		f.runShardedFilter(inputQueues[0], true, outputExchanges[0], filterName, filterFunc, shardingFunc)
	}()

	go func() {
		defer wg.Done()
		f.runShardedFilter(inputQueues[1], true, outputExchanges[1], filterName, filterFunc, shardingFunc)
	}()

	wg.Wait()
}

func (f *Filter) processSingleCountryOriginFilter() {
	inputExchange := "movies_exchange"
	exchangeType := "fanout"
	outputExchange := "single_country_origin_exchange"

	filterName := "single_country_origin_filter"

	err := f.channel.ExchangeDeclare(
		inputExchange,
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
		"movies_for_single_country_origin", // empty = temporal queue with generated name
		false,                              // durable
		true,                               // auto-delete when unused
		true,                               // exclusive
		false,                              // no-wait
		nil,
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to declare temporary queue: %v", filterName, err)
	}

	// Bind the queue to the exchange
	err = f.channel.QueueBind(
		inputQueue.Name, // queue name
		"",              // routing key (empty on a fanout)
		inputExchange,   // exchange
		false,
		nil,
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to bind queue to exchange: %v", filterName, err)
	}

	filterFunc := func(movie *protopb.MovieSanit) bool {
		productionCountries := movie.GetProductionCountries()
		return len(productionCountries) == 1
	}

	shardingFunc := func(movie *protopb.MovieSanit) int {
		countries := movie.GetProductionCountries()
		if len(countries) != 1 {
			return -1 // Shouldn't happen
		}

		hasher := fnv.New32a()
		_, err := hasher.Write([]byte(countries[0]))
		if err != nil {
			return -1
		}

		return (int(hasher.Sum32()) % f.config.Shards) + 1
	}

	declareInput := false
	f.runShardedFilter(inputQueue.Name, declareInput, outputExchange, filterName, filterFunc, shardingFunc)
}

// Creates a direct exchange using sharding with routing keys
func (f *Filter) runShardedFilter(
	inputQueue string,
	declareInput bool,
	outputExchange string,
	filterName string,
	filterFunc func(movie *protopb.MovieSanit) bool,
	shardingFunc func(movie *protopb.MovieSanit) int,
) {
	// TODO: sacar esta lógica de acá
	if declareInput {
		err := rabbitmq.DeclareDirectQueue(f.channel, inputQueue)
		if err != nil {
			f.log.Fatalf("Failed to declare queue '%s': %v", inputQueue, err)
		}
	}

	// Create a direct exchange (that uses sharding)
	err := f.channel.ExchangeDeclare(
		outputExchange, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // no-wait
		nil,            // arguments
	)
	if err != nil {
		f.log.Fatalf("Failed to declare exchange: %v", err)
	}

	// Declare and bind sharded queues to the exchange
	for i := range f.config.Shards {
		queueName := fmt.Sprintf("%s_shard_%d", outputExchange, i)
		routingKey := fmt.Sprintf("%d", i)

		err := rabbitmq.DeclareDirectQueue(f.channel, queueName)
		if err != nil {
			f.log.Fatalf("Failed to declare queue '%s': %v", queueName, err)
		}

		err = f.channel.QueueBind(
			queueName,      // queue name
			routingKey,     // routing key
			outputExchange, // exchange name
			false,          // no-wait
			nil,            // args
		)
		if err != nil {
			f.log.Fatalf("Failed to bind queue '%s' to exchange '%s': %v", queueName, outputExchange, err)
		}
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, inputQueue)
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

		if filterFunc(&movie) {
			f.log.Debugf("[%s] Accepted: %s - %s (%d)", filterName, movie.GetTitle(), movie.GetProductionCountries(), movie.GetReleaseYear())

			data, err := proto.Marshal(&movie)
			if err != nil {
				f.log.Errorf("[%s] Failed to marshal message: %v", filterName, err)
				continue
			}

			shard := shardingFunc(&movie)
			routingKey := fmt.Sprintf("%d", shard)

			err = f.channel.Publish(
				outputExchange, // exchange
				routingKey,     // routing key
				false,          // mandatory
				false,          // inmediate
				amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        data,
				})
			if err != nil {
				f.log.Errorf("[%s] Failed to publish filtered message: %v", filterName, err)
			}

			queueName := fmt.Sprintf("%s_shard_%d", outputExchange, shard)
			f.log.Debugf("[%s] Message published to queue: %s (routing key: %s)", filterName, queueName, routingKey)
		}
	}
}
