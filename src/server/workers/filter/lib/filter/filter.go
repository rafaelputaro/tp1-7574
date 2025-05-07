package filter

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"tp1/globalconfig"
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
	f.log.Infof("Starting filter loop with type: %s", f.config.Type)

	switch f.config.Type {
	case "2000s_filter":
		f.log.Infof("Selected filter: 2000s_filter")
		f.processYearFilters()
	case "ar_es_filter":
		f.log.Infof("Selected filter: ar_es_filter")
		f.processArEsFilter()
	case "ar_filter":
		f.log.Infof("Selected filter: ar_filter")
		f.processArFilter()
	case "single_country_origin_filter":
		f.log.Infof("Selected filter: single_country_origin_filter")
		f.processSingleCountryOriginFilter()
	case "top_5_investors_filter":
		f.log.Infof("Selected filter: top_5_investors_filter")
		f.processTop5InvestorsFilter()
	default:
		f.log.Errorf("Unknown filter type: %s", f.config.Type)
	}
}

func (f *Filter) Close() {
	if f.channel != nil {
		_ = f.channel.Close()
	}

	if f.conn != nil {
		_ = f.conn.Close()
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

	err := rabbitmq.DeclareDirectQueues(f.channel, inputQueue, outputQueue)
	if err != nil {
		f.log.Fatalf("Failed to declare queues: %v", err)
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

			eofBytes, err := proto.Marshal(&movie)
			if err != nil {
				f.log.Errorf("[%s] Failed to marshal EOF marker: %v", filterName, err)
				break
			}

			// Propagate EOF to output queue
			err = f.channel.Publish("", outputQueue, false, false, amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        eofBytes,
			})
			if err != nil {
				f.log.Errorf("[%s] Failed to publish EOF marker: %v", filterName, err)
			} else {
				f.log.Infof("[%s] Propagated EOF marker to %s", filterName, outputQueue)
			}
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
	inputExchange := "single_country_origin_exchange"
	shardID := f.config.ID

	filterName := fmt.Sprintf("top_5_investors_filter_shard_%d", shardID)

	inputQueue := strings.Replace(inputExchange, "exchange", fmt.Sprintf("shard_%d", shardID), 1)

	err := f.channel.ExchangeDeclare(
		inputExchange,
		"direct",
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
	_, err = f.channel.QueueDeclare(
		inputQueue, // empty = temporal queue with generated name
		true,       // durable
		false,      // auto-delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to declare temporary queue: %v", filterName, err)
	}

	// Bind the queue to the exchange
	err = f.channel.QueueBind(
		inputQueue,                     // queue name
		fmt.Sprintf("%d", f.config.ID), // routing key (empty on a fanout)
		inputExchange,                  // exchange
		false,
		nil,
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to bind queue to exchange: %v", filterName, err)
	}

	outputQueue := "movies_top_5_investors"
	_, err = f.channel.QueueDeclare(
		outputQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		f.log.Fatalf("[%s] Failed to declare queue 'movies_top_5_investors': %v", filterName, err)
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, inputQueue)
	if err != nil {
		f.log.Fatalf("[%s] Failed to consume messages from '%s': %v", filterName, inputQueue, err)
	}

	f.log.Infof("[%s] Waiting for messages...", filterName)

	countryBudget := make(map[string]int32)

	for msg := range msgs {
		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			f.log.Errorf("[%s] Failed to unmarshal message: %v", filterName, err)
			continue
		}

		if movie.Eof != nil && *movie.Eof {
			f.log.Infof("[%s] Received EOF marker", filterName)

			type entry struct {
				Country string
				Budget  int32
			}

			var sorted []entry
			for country, total := range countryBudget {
				sorted = append(sorted, entry{country, total})
			}

			sort.Slice(sorted, func(i, j int) bool {
				return sorted[i].Budget > sorted[j].Budget
			})

			// Crear mensaje de respuesta
			top5 := &protopb.Top5Country{
				Budget:              []int32{},
				ProductionCountries: []string{},
				ClientId:            movie.ClientId,
			}
			// eof := true
			// top5.Eof = &eof

			for i := 0; i < len(sorted) && i < 5; i++ {
				top5.ProductionCountries = append(top5.ProductionCountries, sorted[i].Country)
				top5.Budget = append(top5.Budget, sorted[i].Budget)
			}

			data, err := proto.Marshal(top5)
			if err != nil {
				f.log.Errorf("[%s] Failed to marshal Top5Country: %v", filterName, err)
				continue
			}

			f.log.Debugf("[%s] Sending Top5Country: %+v", filterName, top5)

			err = f.channel.Publish(
				"",
				outputQueue,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        data,
				},
			)
			if err != nil {
				f.log.Errorf("[%s] Failed to publish Top5Country: %v", filterName, err)
			}

			eofData := &protopb.Top5Country{
				Budget:              []int32{},
				ProductionCountries: []string{},
				Eof:                 proto.Bool(true),
				ClientId:            movie.ClientId,
			}

			eofDataBytes, err := proto.Marshal(eofData)
			if err != nil {
				f.log.Errorf("[%s] Failed to marshal Top5Country EOF: %v", filterName, err)
				continue
			}

			err = f.channel.Publish(
				"",
				outputQueue,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        eofDataBytes,
				},
			)
			if err != nil {
				f.log.Errorf("[%s] Failed to publish Top5Country: %v", filterName, err)
			}

			break
		}

		budget := movie.GetBudget()
		for _, country := range movie.GetProductionCountries() {
			countryBudget[country] += budget
		}

		f.log.Infof("[%s] Received: %v - %v", filterName, movie.GetProductionCountries(), budget)
		f.log.Infof("[%s] Budget per country: %+v", filterName, countryBudget)
	}
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
	outputQueues := map[string]func(uint32) bool{
		"movies_2000_to_2009":   func(year uint32) bool { return year >= 2000 && year <= 2009 },
		"movies_after_2000":     func(year uint32) bool { return year > 2000 },
		"movies_2000_and_later": func(year uint32) bool { return year >= 2000 },
	}
	filterName := "year_branch_filter"

	f.log.Infof("[%s] Starting job for ID: %d", filterName, f.config.ID)

	err := rabbitmq.DeclareDirectQueues(f.channel, globalconfig.Movies2Queue)
	if err != nil {
		f.log.Fatalf("Failed to declare queue '%s': %v", globalconfig.Movies2Queue, err)
	}

	for outputQueue := range outputQueues {
		err := rabbitmq.DeclareDirectQueues(f.channel, outputQueue)
		if err != nil {
			f.log.Fatalf("[%s] Failed to declare queue '%s': %v", filterName, outputQueue, err)
		}
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, globalconfig.Movies2Queue)
	if err != nil {
		f.log.Fatalf("[%s] Failed to consume messages from '%s': %v", filterName, globalconfig.Movies2Queue, err)
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

			eofBytes, err := proto.Marshal(&movie)
			if err != nil {
				f.log.Errorf("[%s] Failed to marshal EOF marker: %v", filterName, err)
				break
			}

			for queueName := range outputQueues {
				err = f.channel.Publish("", queueName, false, false, amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        eofBytes,
				})
				if err != nil {
					f.log.Errorf("[%s] Failed to publish EOF to '%s': %v", filterName, queueName, err)
				} else {
					f.log.Infof("[%s] Propagated EOF to '%s'", filterName, queueName)
				}
			}

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
		f.runShardedFilter(inputQueues[0], true, outputExchanges[0], filterFunc, shardingFunc)
	}()

	go func() {
		defer wg.Done()
		f.runShardedFilter(inputQueues[1], true, outputExchanges[1], filterFunc, shardingFunc)
	}()

	wg.Wait()
}

func (f *Filter) processSingleCountryOriginFilter() {
	f.log.Infof("Runing filter...")
	outputExchange := "single_country_origin_exchange"

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

	f.log.Infof("Runing filter...")
	f.runShardedFilter(globalconfig.Movies1Queue, true, outputExchange, filterFunc, shardingFunc)
}

// Creates a direct exchange using sharding with routing keys
func (f *Filter) runShardedFilter(inputQueue string, declareInput bool, outputExchange string, filterFunc func(movie *protopb.MovieSanit) bool, shardingFunc func(movie *protopb.MovieSanit) int) {
	// TODO: sacar esta lógica de acá
	if declareInput {
		err := rabbitmq.DeclareDirectQueues(f.channel, inputQueue)
		if err != nil {
			f.log.Fatalf("failed to declare queue '%s': %v", inputQueue, err)
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
		f.log.Fatalf("failed to declare exchange %s: %v", outputExchange, err)
	}

	// Declare and bind sharded queues to the exchange
	for i := 1; i <= f.config.Shards; i++ {
		queueName := strings.Replace(outputExchange, "exchange", fmt.Sprintf("shard_%d", i), 1)

		routingKey := fmt.Sprintf("%d", i)

		err := rabbitmq.DeclareDirectQueues(f.channel, queueName)
		f.log.Debugf("Declaring %v", queueName)
		if err != nil {
			f.log.Fatalf("failed to declare queue '%s': %v", queueName, err)
		}

		err = f.channel.QueueBind(
			queueName,      // queue name
			routingKey,     // routing key
			outputExchange, // exchange name
			false,          // no-wait
			nil,            // args
		)
		if err != nil {
			f.log.Fatalf("failed to bind queue '%s' to exchange '%s': %v", queueName, outputExchange, err)
		}
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, inputQueue)
	if err != nil {
		f.log.Fatalf("failed to consume messages from '%s': %v", inputQueue, err)
	}

	f.log.Infof("Waiting for messages...")

	for msg := range msgs {
		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			f.log.Errorf("failed to unmarshal message: %v", err)
			continue
		}

		if movie.GetEof() {
			clientID := movie.GetClientId()

			f.log.Infof("[client_id:%s] received EOF marker", clientID)
			eofBytes, err := proto.Marshal(&movie)
			if err != nil {
				f.log.Errorf("failed to marshal EOF marker: %v", err)
				break
			}

			// Propagate EOF
			for i := 1; i <= f.config.Shards; i++ {
				routingKey := fmt.Sprintf("%d", i)
				err := f.channel.Publish(
					outputExchange, // exchange
					routingKey,     // routing key
					false,          // mandatory
					false,          // immediate
					amqp.Publishing{
						ContentType: "application/protobuf",
						Body:        eofBytes,
					},
				)
				if err != nil {
					f.log.Errorf("failed to publish EOF to %s shard %d: %v", outputExchange, i, err)
				} else {
					f.log.Infof("[client_id:%s] propagated EOF to %s shard %d", clientID, outputExchange, i)
				}
			}
			break
		}

		if filterFunc(&movie) {
			clientID := movie.GetClientId()

			f.log.Debugf("[client_id:%s] accepted: %s - %s", clientID, movie.GetTitle(), movie.GetProductionCountries())

			data, err := proto.Marshal(&movie)
			if err != nil {
				f.log.Errorf("failed to marshal message: %v", err)
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
				f.log.Errorf("failed to publish filtered message: %v", err)
			}

			queueName := fmt.Sprintf("%s_shard_%d", outputExchange, shard)
			f.log.Debugf("[client_id:%s] message published to queue: %s (routing key: %s)", clientID, queueName, routingKey)
		}
	}
}
