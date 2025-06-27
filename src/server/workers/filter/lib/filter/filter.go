package filter

import (
	"sync"
	"tp1/coordinator"
	"tp1/globalconfig"

	"tp1/helpers/state"
	"tp1/helpers/window"
	"tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"
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
	config             FilterConfig
	log                *logging.Logger
	conn               *amqp.Connection
	channel            *amqp.Channel
	stateHelperDefault *state.StateHelper[FilterDefaultState, FilterDefaultUpdateArgs]
	messageWindow      *window.MessageWindow
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
		// Creates state helpers
		f.InitStateHelperDefault()
		f.log.Infof("Selected filter: 2000s_filter")
		f.processYearFilters()
	case "ar_es_filter":
		// Creates state helpers
		f.InitStateHelperDefault()
		f.log.Infof("Selected filter: ar_es_filter")
		f.processArEsFilter()
	case "ar_filter":
		// Creates state helpers
		f.InitStateHelperDefault()
		f.log.Infof("Selected filter: ar_filter")
		f.processArFilter()
	case "single_country_origin_filter":
		// Creates state helpers
		f.InitStateHelperDefault()
		f.log.Infof("Selected filter: single_country_origin_filter")
		f.processSingleCountryOriginFilter()
	default:
		f.log.Fatalf("Unknown filter type: %s", f.config.Type)
	}
}

func (f *Filter) Close() {
	if f.channel != nil {
		_ = f.channel.Close()
	}

	if f.conn != nil {
		_ = f.conn.Close()
	}

	f.DisposeStateHelpers()

}

// Jobs --------------------------------------------------------------------------------------------

type MovieFilterFunc func(movie *protopb.MovieSanit) bool

// Reads result from 2000s filter and returns the movies produced by both Spain and Argentina
// until an EOF is received.
// Used for query 1: "Peliculas y sus géneros de los años 00' con producción Argentina y Española"
func (f *Filter) processArEsFilter() {
	inputQueue := "movies_2000_to_2009"
	outputQueue := "movies_ar_es_2000s"

	filterFunc := func(movie *protopb.MovieSanit) bool {
		productionCountries := movie.GetProductionCountries()

		return slices.Contains(productionCountries, "Argentina") && slices.Contains(productionCountries, "Spain")
	}

	err := rabbitmq.DeclareDirectQueues(f.channel, inputQueue, outputQueue)
	if err != nil {
		f.log.Fatalf("failed to declare queues: %v", err)
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, inputQueue)
	if err != nil {
		f.log.Fatalf("failed to consume messages from '%s': %v", inputQueue, err)
	}

	coord := coordinator.NewEOFLeader(f.log, f.channel, "ar_es_filter")

	f.log.Infof("waiting for messages...")

	for msg := range msgs {
		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			f.log.Errorf("failed to unmarshal message: %v", err)
			continue
		}

		clientID := movie.GetClientId()

		if movie.GetEof() {
			f.log.Infof("[client_id:%s] received EOF marker", clientID)

			coord.TakeLeadership(clientID)
			coord.WaitForACKs(clientID)

			err = f.channel.Publish("", outputQueue, false, false, amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        msg.Body,
			})
			if err != nil {
				f.log.Fatalf("[client_id:%s] failed to publish EOF marker: %v", movie.GetClientId(), err)
			}

			f.log.Infof("[client_id:%s] propagated EOF marker to %s", movie.GetClientId(), outputQueue)

			f.sendAck(msg)
			continue
		}

		if filterFunc(&movie) {
			f.log.Debugf("[client_id:%s] accepted: %s (%d)", movie.GetClientId(), movie.GetProductionCountries(), movie.GetReleaseYear())

			err = f.channel.Publish("", outputQueue, false, false, amqp.Publishing{
				ContentType: "application/protobuf",
				Body:        msg.Body,
			})
			if err != nil {
				f.log.Errorf("[client_id:%s]failed to publish filtered message: %v", movie.GetClientId(), err)
			}

			f.sendAck(msg)
			coord.SendACKs()
		}
	}

	f.log.Infof("job finished")
}

func (f *Filter) processYearFilters() {
	outputQueues := map[string]func(uint32) bool{
		"movies_2000_to_2009":   func(year uint32) bool { return year >= 2000 && year <= 2009 },
		"movies_after_2000":     func(year uint32) bool { return year > 2000 },
		"movies_2000_and_later": func(year uint32) bool { return year >= 2000 },
	}

	f.log.Infof("starting job for ID: %d", f.config.ID)

	err := rabbitmq.DeclareDirectQueues(f.channel, globalconfig.Movies1Queue)
	if err != nil {
		f.log.Fatalf("Failed to declare queue '%s': %v", globalconfig.Movies1Queue, err)
	}

	for outputQueue := range outputQueues {
		err := rabbitmq.DeclareDirectQueues(f.channel, outputQueue)
		if err != nil {
			f.log.Fatalf("failed to declare queue '%s': %v", outputQueue, err)
		}
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, globalconfig.Movies1Queue)
	if err != nil {
		f.log.Fatalf("failed to consume messages from '%s': %v", globalconfig.Movies1Queue, err)
	}

	coord := coordinator.NewEOFLeader(f.log, f.channel, "2000s_filter")

	f.log.Infof("waiting for messages...")

	for msg := range msgs {
		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			f.log.Errorf("failed to unmarshal message: %v", err)
			continue
		}

		if f.messageWindow.IsDuplicate(*movie.ClientId, *movie.MessageId) {
			f.log.Debugf("duplicate message: %v", *movie.MessageId)
			f.sendAck(msg)
			continue
		}

		clientID := movie.GetClientId()

		if movie.GetEof() {
			f.log.Infof("[client_id:%s] received EOF marker", clientID)

			coord.TakeLeadership(clientID)
			coord.WaitForACKs(clientID)

			for queueName := range outputQueues {
				err = f.channel.Publish("", queueName, false, false, amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        msg.Body,
				})
				if err != nil {
					f.log.Fatalf("failed to publish EOF to '%s': %v", queueName, err)
				}

				f.log.Infof("[client_id:%s] propagated EOF to %s", movie.GetClientId(), queueName)
			}
			f.SaveDefaultStateAndSendAck(msg, *movie.ClientId, *movie.MessageId)
			state.Synch(f.stateHelperDefault)
			continue
		}

		releaseYear := movie.GetReleaseYear()
		for queueName, condition := range outputQueues {
			if condition(releaseYear) {
				f.log.Debugf("[%s] %s (%d)", queueName, movie.GetTitle(), releaseYear)
				err = f.channel.Publish("", queueName, false, false, amqp.Publishing{
					ContentType: "application/protobuf",
					Body:        msg.Body,
				})
				if err != nil {
					f.log.Errorf("failed to publish to '%s': %v", queueName, err)
				}
			}
		}
		f.SaveDefaultStateAndSendAckCoordinator(coord, msg, *movie.ClientId, *movie.MessageId)
	}

	f.log.Infof("job finished")
}

func (f *Filter) processArFilter() {
	inputQueues := [2]string{"movies_2000_and_later", "movies_after_2000"}
	outputQueues := [2]string{"ar_movies_2000_and_later", "ar_movies_after_2000"}

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
		f.runShardedFilter(inputQueues[0], outputQueues[0], filterFunc, shardingFunc)
	}()

	go func() {
		defer wg.Done()
		f.runShardedFilter(inputQueues[1], outputQueues[1], filterFunc, shardingFunc)
	}()

	wg.Wait()
}

func (f *Filter) processSingleCountryOriginFilter() {
	f.log.Infof("Runing filter...")
	inputQueue := globalconfig.Movies2Queue
	outputQueue := "single_country_origin_movies" // TODO: to config

	coord := coordinator.NewEOFLeader(f.log, f.channel, "single_country_origin_filter")

	err := rabbitmq.DeclareDirectQueuesWithFreshChannel(f.conn, inputQueue, outputQueue)
	if err != nil {
		f.log.Fatalf("Failed to declare queue: %v", err)
	}

	msgs, err := rabbitmq.ConsumeFromQueue(f.channel, inputQueue)
	if err != nil {
		f.log.Fatalf("failed to consume messages from: %v", err)
	}

	filterFunc := func(movie *protopb.MovieSanit) bool {
		productionCountries := movie.GetProductionCountries()
		return len(productionCountries) == 1
	}

	for msg := range msgs {
		var movie protopb.MovieSanit
		err = proto.Unmarshal(msg.Body, &movie)
		if err != nil {
			f.log.Errorf("failed to unmarshal message: %v", err)
			continue
		}

		clientID := movie.GetClientId()

		// check duplicate
		if f.messageWindow.IsDuplicate(clientID, *movie.MessageId) {
			f.log.Debugf("duplicate message: %v", *movie.MessageId)
			f.sendAck(msg)
			continue
		}

		if movie.GetEof() {
			f.log.Infof("[client_id:%s] received EOF marker", clientID)

			coord.TakeLeadership(clientID)
			coord.WaitForACKs(clientID)

			dataEof, err := protoUtils.CreateEofMessageMovieSanit(clientID, movie.GetMessageId())
			if err != nil {
				f.log.Fatalf("[client_id:%s] failed to marshal eof: %v", clientID, err)
			}

			err = rabbitmq.Publish(f.channel, "", outputQueue, dataEof)
			if err != nil {
				f.log.Fatalf("[client_id:%s] failed to publish eof: %v", clientID, err)
			}

			f.log.Infof("[client_id:%s] published eof", clientID)
			f.SaveDefaultStateAndSendAck(msg, *movie.ClientId, *movie.MessageId)
			state.Synch(f.stateHelperDefault)
			continue
		}

		if filterFunc(&movie) {
			err = rabbitmq.Publish(f.channel, "", outputQueue, msg.Body)
			if err != nil {
				f.log.Errorf("[client_id:%s] failed to publish movie: %v", clientID, err)
			}
			// f.log.Debugf("[client_id:%s] published movie: %s", clientID, movie.GetTitle())
			f.SaveDefaultStateAndSendAckCoordinator(coord, msg, *movie.ClientId, *movie.MessageId)
		}
	}
}

func (f *Filter) runShardedFilter(inputQueue string, baseOutputQueue string, filterFunc func(movie *protopb.MovieSanit) bool, shardingFunc func(movie *protopb.MovieSanit) int) {
	err := rabbitmq.DeclareDirectQueuesWithFreshChannel(f.conn, inputQueue)
	if err != nil {
		f.log.Fatalf("failed to declare queue '%s': %v", inputQueue, err)
	}

	queueNames := globalconfig.GetAllShardedQueueNames(baseOutputQueue, int64(f.config.Shards))
	for _, queueName := range queueNames {
		f.log.Infof("Declaring %v", queueName)
		err := rabbitmq.DeclareDirectQueuesWithFreshChannel(f.conn, queueName)
		if err != nil {
			f.log.Fatalf("failed to declare queue '%s': %v", queueName, err)
		}
	}

	coord := coordinator.NewEOFLeader(f.log, f.channel, "ar_filter-"+inputQueue)

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

		clientID := movie.GetClientId()

		if movie.GetEof() {
			f.log.Infof("[client_id:%s] received EOF marker", clientID)

			coord.TakeLeadership(clientID)
			coord.WaitForACKs(clientID)

			queueNames := globalconfig.GetAllShardedQueueNames(baseOutputQueue, int64(f.config.Shards))
			for _, queueName := range queueNames {
				err = rabbitmq.Publish(f.channel, "", queueName, msg.Body)
				if err != nil {
					f.log.Fatalf("[client_id:%s] failed to publish EOF to %s: %v", clientID, queueName, err)
				}
				f.log.Infof("[client_id:%s] propagated EOF to %s", clientID, queueName)
			}
		}

		if filterFunc(&movie) {
			queueName := globalconfig.GetShardedQueueName(baseOutputQueue, int64(f.config.Shards), int64(movie.GetId()))
			err = rabbitmq.Publish(f.channel, "", queueName, msg.Body)
			if err != nil {
				f.log.Fatalf("[client_id:%s] failed send movie to %s: %v", clientID, queueName, err)
			}
			coord.SendACKs()
		}

		f.sendAck(msg)
	}
}

func (f *Filter) sendAck(msg amqp.Delivery) error {
	err := rabbitmq.SingleAck(msg)
	if err != nil {
		f.log.Fatalf("failed to ack message: %v", err)
		return err
	}
	return nil
}
