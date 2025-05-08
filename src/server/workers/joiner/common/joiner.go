package common

import (
	"tp1/globalconfig"
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"

	"tp1/server/workers/joiner/common/utils"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// Joiner types:
const G_B_M_ID_RATINGS string = "group_by_movie_id_ratings"
const G_B_M_ID_CREDITS string = "group_by_movie_id_credits"

// Messages to log:
const MSG_START = "Starting job for ID"
const MSG_FAILED_CONSUME = "Failed to consume messages from"
const MSG_JOB_FINISHED = "Job finished"
const MSG_RECEIVED_EOF_MARKER = "Received EOF marker"
const MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE = "Failed to publish on outputqueue"

// Joiner which can be of types "group_by_movie_id_ratings" or "group_by_movie_id_credits".
type Joiner struct {
	Channel    *amqp.Channel
	Connection *amqp.Connection
	Config     JoinerConfig
	Log        *logging.Logger
}

func NewJoiner(log *logging.Logger) (*Joiner, error) {
	var c = LoadJoinerConfig()
	c.LogConfig(log)

	connection, err := rabbitmq.ConnectRabbitMQ(log)
	if err != nil {
		Shutdown(log, connection, nil, "Error on dial rabbitmq", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		Shutdown(log, connection, channel, "Error on create rabbitmq channel", err)
	}

	err = rabbitmq.DeclareDirectExchanges(channel, c.InputQueuesExchange)
	if err != nil {
		Shutdown(log, connection, channel, "Failed to declare exchange", err)
	}

	err = rabbitmq.DeclareDirectQueues(channel, c.InputQueueName, c.OutputQueueName, c.InputQueueSecName)
	if err != nil {
		Shutdown(log, connection, channel, "Error on declare queue", err)
	}

	err = rabbitmq.BindQueueToExchange(channel, c.InputQueueName, c.InputQueuesExchange, "")
	if err != nil {
		Shutdown(log, connection, channel, "Failed to bin queue", err)
	}

	return &Joiner{
		Channel:    channel,
		Connection: connection,
		Config:     *c,
		Log:        log,
	}, nil
}

// Init joiner loop
func (joiner *Joiner) Start() {
	joiner.Log.Infof("[%s] %s: %d", joiner.Config.JoinerType, MSG_START, joiner.Config.ID)
	switch joiner.Config.JoinerType {
	case G_B_M_ID_CREDITS:
		joiner.joiner_g_b_m_id_credits()
	case G_B_M_ID_RATINGS:
		joiner.joiner_g_b_m_id_ratings()
	}
	joiner.Log.Infof("[%s] %s", joiner.Config.JoinerType, MSG_JOB_FINISHED)
}

// Check EOF condition
func (joiner *Joiner) logEofQueue(queueName string) {
	joiner.Log.Infof("[%s,%s] %s", joiner.Config.JoinerType, queueName, MSG_RECEIVED_EOF_MARKER)
}

// Send to report
func (joiner *Joiner) publishData(data []byte) {
	err := joiner.Channel.Publish("", joiner.Config.OutputQueueName, false, false, amqp.Publishing{
		ContentType: "application/protobuf",
		Body:        data,
	})
	if err != nil {
		joiner.Log.Fatalf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE, err)
	}
}

func (joiner *Joiner) joiner_g_b_m_id_credits() {
	inputExchange := "ar_movies_after_2000_exchange"

	err := rabbitmq.DeclareDirectExchanges(joiner.Channel, inputExchange)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare exchange", err)
	}

	err = rabbitmq.DeclareDirectQueues(joiner.Channel, joiner.Config.InputQueueName)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare queue", err)
	}

	err = rabbitmq.BindQueueToExchange(joiner.Channel, joiner.Config.InputQueueName, inputExchange, joiner.Config.ID)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to bind queue to exchange", err)
	}

	err = rabbitmq.DeclareFanoutExchanges(joiner.Channel, globalconfig.CreditsExchange)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare fanout exchange", err)
	}

	inputQueue, err := rabbitmq.DeclareTemporaryQueue(joiner.Channel)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare temporary queue", err)
	}

	err = rabbitmq.BindQueueToExchange(joiner.Channel, inputQueue.Name, globalconfig.CreditsExchange, "")
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to bind temporary queue to exchange", err)
	}

	// Store client-specific data
	type clientState struct {
		counter   *utils.ActorsCounter
		movieEOF  bool
		creditEOF bool
	}
	clientStates := make(map[string]*clientState)

	// Function to send the report when both EOFs are received
	sendReportIfReady := func(clientID string, state *clientState) {
		if state.movieEOF && state.creditEOF {
			// Send actor counts for the client
			for actorPath := range state.counter.Actors {
				actor := state.counter.GetActor(actorPath, clientID)
				data, err := proto.Marshal(actor)
				if err != nil {
					joiner.Log.Fatalf("[client_id:%s] failed to marshal actor data: %v", clientID, err)
				}
				joiner.publishData(data)
				joiner.Log.Debugf("[client_id:%s] sent actor count: %v", clientID, actor)
			}

			// Send EOF for the client
			eof := utils.CreateActorEof(clientID)
			data, err := proto.Marshal(eof)
			if err != nil {
				joiner.Log.Fatalf("[client_id:%s] failed to marshal actor data eof: %v", clientID, err)
			}
			joiner.publishData(data)
			joiner.Log.Debugf("[client_id:%s] sent eof marker", clientID)

			// Remove processed client to free resources
			// delete(clientStates, clientID)
		}
	}

	// Generic message processing function
	processMessage := func(msg amqp.Delivery, isCredit bool) {
		var clientID string
		if isCredit {
			var credit protopb.CreditSanit
			if err := proto.Unmarshal(msg.Body, &credit); err != nil {
				joiner.Log.Errorf("failed to unmarshal credit: %v", err)
				return
			}

			clientID = credit.GetClientId()

			// Initialize client state if not exists
			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{counter: utils.NewActorCounter()}
			}
			state := clientStates[clientID]

			// Process EOF or count actors
			if credit.GetEof() {
				state.creditEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueSecName)
			} else {
				state.counter.Count(&credit)
			}
			sendReportIfReady(clientID, state)
		} else {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("failed to unmarshal movie: %v", err)
				return
			}
			clientID = movie.GetClientId()

			// Initialize client state if not exists
			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{counter: utils.NewActorCounter()}
			}
			state := clientStates[clientID]

			// Process EOF or append movie
			if movie.GetEof() {
				state.movieEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueName)
			} else {
				state.counter.AppendMovie(&movie)
			}
			sendReportIfReady(clientID, state)
		}
	}

	// Start message consumers in separate goroutines
	go func() {
		msgs, err := joiner.consumeQueue(joiner.Config.InputQueueName)
		if err == nil {
			for msg := range msgs {
				processMessage(msg, false)
			}
		} else {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueName, err)
		}
	}()

	go func() {
		msgs, err := joiner.consumeQueue(inputQueue.Name)
		if err == nil {
			for msg := range msgs {
				processMessage(msg, true)
			}
		} else {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueName, err)
		}
	}()

	// Keep the joiner running indefinitely
	select {}
}

func (joiner *Joiner) joiner_g_b_m_id_ratings() {
	inputExchange := "ar_movies_2000_and_later_exchange"

	err := rabbitmq.DeclareDirectExchanges(joiner.Channel, inputExchange)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare exchange", err)
	}

	err = rabbitmq.DeclareDirectQueues(joiner.Channel, joiner.Config.InputQueueName)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare queue", err)
	}

	err = rabbitmq.BindQueueToExchange(joiner.Channel, joiner.Config.InputQueueName, inputExchange, joiner.Config.ID)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to bind queue to exchange", err)
	}

	err = rabbitmq.DeclareFanoutExchanges(joiner.Channel, globalconfig.RatingsExchange)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare fanout exchange", err)
	}

	inputQueue, err := rabbitmq.DeclareTemporaryQueue(joiner.Channel)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare temporary queue", err)
	}

	err = rabbitmq.BindQueueToExchange(joiner.Channel, inputQueue.Name, globalconfig.RatingsExchange, "")
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to bind temporary queue to exchange", err)
	}

	// Store client-specific data
	type clientState struct {
		totalizer *utils.RatingTotalizer
		movieEOF  bool
		ratingEOF bool
	}
	clientStates := make(map[string]*clientState)

	// Send report when both EOFs are received for a client
	sendReportIfReady := func(clientID string, state *clientState) {
		if state.movieEOF && state.ratingEOF {
			// Get top and bottom ratings
			topAndBottom := state.totalizer.GetTopAndBottom(clientID)

			// Prepare report
			joiner.Log.Debugf("[client_id:%s] send top and bottom: %s", clientID, utils.TopAndBottomToString(topAndBottom))
			data, err := proto.Marshal(topAndBottom)
			if err != nil {
				joiner.Log.Fatalf("[client_id:%s] failed to marshall top and bottom: %v", clientID, err)
				return
			}

			// Send report
			joiner.publishData(data)

			// Send EOF for the client
			eof := utils.CreateTopAndBottomRatingAvgEof(clientID)
			data, err = proto.Marshal(eof)
			if err != nil {
				joiner.Log.Fatalf("[client_id:%s] failed to marshall top and bottom EOF: %v", clientID, err)
			}
			joiner.publishData(data)

			// Remove client state to free resources
			// delete(clientStates, clientID)
		}
	}

	// Message processing function
	processMessage := func(msg amqp.Delivery, isRating bool) {
		var clientID string
		if isRating {
			var rating protopb.RatingSanit
			if err := proto.Unmarshal(msg.Body, &rating); err != nil {
				joiner.Log.Errorf("failed to unmarshal rating: %v", err)
				return
			}
			clientID = rating.GetClientId()

			// Initialize client state if not exists
			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{totalizer: utils.NewRatingTotalizer()}
			}
			state := clientStates[clientID]

			if rating.GetEof() {
				state.ratingEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueSecName)
			} else {
				state.totalizer.Sum(&rating)
			}
			sendReportIfReady(clientID, state)

		} else {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("failed to unmarshal movie: %v", err)
				return
			}
			clientID = movie.GetClientId()

			// Initialize client state if not exists
			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{totalizer: utils.NewRatingTotalizer()}
			}
			state := clientStates[clientID]

			if movie.GetEof() {
				state.movieEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueName)
			} else {
				state.totalizer.AppendMovie(&movie)
			}
			sendReportIfReady(clientID, state)
		}
	}

	// Start message consumers
	go func() {
		msgs, err := joiner.consumeQueue(joiner.Config.InputQueueName)
		if err != nil {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueName, err)
		}

		for msg := range msgs {
			processMessage(msg, false) // Movie messages
		}
	}()

	go func() {
		msgs, err := joiner.consumeQueue(inputQueue.Name)
		if err != nil {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", inputQueue.Name, err)
		}

		for msg := range msgs {
			processMessage(msg, true) // Rating messages
		}
	}()

	// Keep the joiner running indefinitely
	select {}
}

func (joiner *Joiner) consumeQueue(name string) (<-chan amqp.Delivery, error) {
	msgs, err := joiner.Channel.Consume(
		name,  // name
		"",    // consumerTag: "" lets rabbitmq generate a tag for this consumer
		true,  // autoAck: when a msg arrives, the consumers acks the msg
		false, // exclusive: allow others to consume from the queue
		false, // no-local: ignored field
		false, // no-wait: wait for confirmation of the consumers correct registration
		nil,   // args
	)
	if err != nil {
		joiner.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, name, err)
	}
	joiner.Log.Infof("[%s] Waiting for messages from %s...", joiner.Config.JoinerType, name)
	return msgs, err
}

func (joiner *Joiner) Dispose() {
	joiner.Log.Infof("Close joiner")
	Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "", nil)
}

func Shutdown(log *logging.Logger, connection *amqp.Connection, channel *amqp.Channel, message string, err error) {
	rabbitmq.ShutdownConnection(connection)
	rabbitmq.ShutdownChannel(channel)

	if err != nil {
		log.Fatalf("%v: %v", message, err)
	}
}
