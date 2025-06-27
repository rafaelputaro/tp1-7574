package common

import (
	"strconv"
	"sync"
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

// Ouput message IDs:
const DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT int64 = 0
const DEFAULT_MESSAGE_ID_EOF_UNIQUE_OUTPUT int64 = 1

// Messages to log:
const MSG_START = "Starting job for ID"
const MSG_JOB_FINISHED = "Job finished"
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

	err = rabbitmq.DeclareDirectQueuesWithFreshChannel(connection, c.InputQueueName, c.InputQueueSecName, c.OutputQueueName)
	if err != nil {
		Shutdown(log, connection, nil, "Error on declare queue", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		Shutdown(log, connection, channel, "Error on create rabbitmq channel", err)
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
	type clientState struct {
		counter   *utils.ActorsCounter
		movieEOF  bool
		creditEOF bool
	}
	clientStates := make(map[string]*clientState)
	var clientStatesMutex sync.RWMutex

	// Function to send the report when both EOFs are received
	sendReportIfReady := func(clientID string, state *clientState) {
		if state.movieEOF && state.creditEOF {
			numMsg, _ := strconv.ParseInt(clientID, 10, 64) // todo fix this
			numMsg *= int64(100000000)
			// Send actor counts for the client
			for actorPath := range state.counter.Actors {
				actor := state.counter.GetActor(actorPath, clientID, numMsg, joiner.Config.InputQueueName)
				data, err := proto.Marshal(actor)
				if err != nil {
					joiner.Log.Fatalf("[client_id:%s] failed to marshal actor data: %v", clientID, err)
				}
				joiner.publishData(data)
				numMsg++
				joiner.Log.Debugf("[client_id:%s][message_id:%d] sent actor count: %v", clientID, numMsg, actor)
			}

			// Send EOF for the client
			eof := utils.CreateActorEof(clientID, numMsg, joiner.Config.InputQueueName)
			data, err := proto.Marshal(eof)
			if err != nil {
				joiner.Log.Fatalf("[client_id:%s] failed to marshal actor data eof: %v", clientID, err)
			}
			joiner.publishData(data)
			joiner.Log.Debugf("[client_id:%s] sent eof marker", clientID)

			// TODO: Remove processed client to free resources
			// delete(clientStates, clientID)
		}
	}

	processMessage := func(msg amqp.Delivery, isCredit bool) {
		var clientID string
		var state *clientState

		if isCredit {
			var credit protopb.CreditSanit
			if err := proto.Unmarshal(msg.Body, &credit); err != nil {
				joiner.Log.Errorf("failed to unmarshal credit: %v", err)
				return
			}

			clientID = credit.GetClientId()

			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{counter: utils.NewActorCounter()}
			}
			state = clientStates[clientID]

			if credit.GetEof() {
				state.creditEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueSecName)
			} else {
				state.counter.Count(&credit)
				joiner.Log.Infof("[client_id:%s][queue:%s] processed credit: %v", clientID, joiner.Config.InputQueueSecName, &credit)
			}

		} else {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("failed to unmarshal movie: %v", err)
				return
			}

			clientID = movie.GetClientId()

			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{counter: utils.NewActorCounter()}
			}
			state = clientStates[clientID]

			if movie.GetEof() {
				state.movieEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueName)
			} else {
				state.counter.AppendMovie(&movie)
				joiner.Log.Infof("[client_id:%s][queue:%s] processed movie: %v", clientID, joiner.Config.InputQueueName, &movie)
			}
		}

		sendReportIfReady(clientID, state)
	}

	go func() {
		msgs, err := rabbitmq.ConsumeFromQueue(joiner.Channel, joiner.Config.InputQueueName)
		if err == nil {
			for msg := range msgs {
				err := rabbitmq.SingleAck(msg)
				if err != nil {
					joiner.Log.Fatalf("failed to ack message: %v", err)
				}
				clientStatesMutex.Lock()
				processMessage(msg, false)
				clientStatesMutex.Unlock()
			}
		} else {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueName, err)
		}
	}()

	go func() {
		msgs, err := rabbitmq.ConsumeFromQueue(joiner.Channel, joiner.Config.InputQueueSecName)
		if err != nil {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueSecName, err)
		}

		for msg := range msgs {
			err := rabbitmq.SingleAck(msg)
			if err != nil {
				joiner.Log.Fatalf("failed to ack message: %v", err)
			}
			clientStatesMutex.Lock()
			processMessage(msg, true)
			clientStatesMutex.Unlock()
		}

	}()

	select {}
}

func (joiner *Joiner) joiner_g_b_m_id_ratings() {
	type clientState struct {
		totalizer *utils.RatingTotalizer
		movieEOF  bool
		ratingEOF bool
	}
	clientStates := make(map[string]*clientState)
	var clientStatesMutex sync.RWMutex

	// Send report when both EOFs are received for a client
	sendReportIfReady := func(clientID string, state *clientState) {
		if state.movieEOF && state.ratingEOF {
			// Get top and bottom ratings
			topAndBottom := state.totalizer.GetTopAndBottom(clientID, DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT, joiner.Config.InputQueueName)

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
			eof := utils.CreateTopAndBottomRatingAvgEof(clientID, DEFAULT_MESSAGE_ID_EOF_UNIQUE_OUTPUT, joiner.Config.InputQueueName)
			data, err = proto.Marshal(eof)
			if err != nil {
				joiner.Log.Fatalf("[client_id:%s] failed to marshall top and bottom EOF: %v", clientID, err)
			}
			joiner.publishData(data)

			// TODO: Remove client state to free resources
			// delete(clientStates, clientID)
		}
	}

	processMessage := func(msg amqp.Delivery, isRating bool) {
		var clientID string
		var state *clientState

		if isRating {
			var rating protopb.RatingSanit
			if err := proto.Unmarshal(msg.Body, &rating); err != nil {
				joiner.Log.Errorf("failed to unmarshal rating: %v", err)
				return
			}
			clientID = rating.GetClientId()

			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{totalizer: utils.NewRatingTotalizer()}
			}
			state = clientStates[clientID]

			if rating.GetEof() {
				state.ratingEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueSecName)
			} else {
				state.totalizer.Sum(&rating)
			}

		} else {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("failed to unmarshal movie: %v", err)
				return
			}
			clientID = movie.GetClientId()

			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &clientState{totalizer: utils.NewRatingTotalizer()}
			}
			state = clientStates[clientID]

			if movie.GetEof() {
				state.movieEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueName)
			} else {
				state.totalizer.AppendMovie(&movie)
			}
		}

		sendReportIfReady(clientID, state)
	}

	// Start message consumers
	go func() {
		msgs, err := rabbitmq.ConsumeFromQueue(joiner.Channel, joiner.Config.InputQueueName)
		if err != nil {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueName, err)
		}

		for msg := range msgs {
			err := rabbitmq.SingleAck(msg)
			if err != nil {
				joiner.Log.Fatalf("failed to ack message: %v", err)
			}

			clientStatesMutex.Lock()
			processMessage(msg, false)
			clientStatesMutex.Unlock()
		}
	}()

	go func() {
		msgs, err := rabbitmq.ConsumeFromQueue(joiner.Channel, joiner.Config.InputQueueSecName)
		if err != nil {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueSecName, err)
		}

		for msg := range msgs {
			err := rabbitmq.SingleAck(msg)
			if err != nil {
				joiner.Log.Fatalf("failed to ack message: %v", err)
			}

			clientStatesMutex.Lock()
			processMessage(msg, true)
			clientStatesMutex.Unlock()
		}
	}()

	// Keep the joiner running indefinitely
	select {}
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
