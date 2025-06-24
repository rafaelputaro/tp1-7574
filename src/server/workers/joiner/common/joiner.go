package common

import (
	"strconv"
	"sync"
	"tp1/helpers/state"
	"tp1/helpers/window"
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
const MSG_RECEIVED_EOF_MARKER = "Received EOF marker"
const MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE = "Failed to publish on outputqueue"

// Joiner which can be of types "group_by_movie_id_ratings" or "group_by_movie_id_credits".
type Joiner struct {
	Channel            *amqp.Channel
	Connection         *amqp.Connection
	Config             JoinerConfig
	Log                *logging.Logger
	WindowMovies       *window.MessageWindow
	WindowSec          *window.MessageWindow // Ratings or Credits
	StateHelperMovies  *state.StateHelper[JoinerMoviesState, JoinerMoviesUpdateArgs, AckArgs]
	StateHelperRatings *state.StateHelper[JoinerRatingsState, JoinerRatingsUpdateArgs, AckArgs]
	StateHelperCredits *state.StateHelper[JoinerCreditsState, JoinerCreditsUpdateArgs, AckArgs]
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
	// Init state movies
	joiner.InitStateHelperMovies(inputExchange)

	err := rabbitmq.DeclareDirectExchanges(joiner.Channel, inputExchange)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare exchange", err)
	}

	err = rabbitmq.DeclareDirectQueues(joiner.Channel, joiner.Config.InputQueueName, joiner.Config.InputQueueSecName)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare queue", err)
	}

	err = rabbitmq.BindQueueToExchange(joiner.Channel, joiner.Config.InputQueueName, inputExchange, joiner.Config.ID)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to bind queue to exchange", err)
	}
	/*
		// Store client-specific data
		type ClientStateCredits struct {
			counter   *utils.ActorsCounter
			movieEOF  bool
			creditEOF bool
		}*/
	clientStates := make(map[string]*utils.ClientStateCredits)
	//clientStates := joiner.CreateJoinerCreditsState()
	var clientStatesMutex sync.RWMutex

	// Function to send the report when both EOFs are received
	sendReportIfReady := func(clientID string, state *utils.ClientStateCredits) {
		if state.MovieEOF && state.CreditEOF {
			numMsg, _ := strconv.ParseInt(clientID, 10, 64) // todo fix this
			numMsg *= int64(100000000)
			// Send actor counts for the client
			for actorPath := range state.Counter.Actors {
				actor := state.Counter.GetActor(actorPath, clientID, numMsg, joiner.Config.InputQueueName)
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
		var state *utils.ClientStateCredits

		if isCredit {
			var credit protopb.CreditSanit
			if err := proto.Unmarshal(msg.Body, &credit); err != nil {
				joiner.Log.Errorf("failed to unmarshal credit: %v", err)
				return
			}

			clientID = credit.GetClientId()

			if _, exists := clientStates[clientID]; !exists {
				clientStates[clientID] = &utils.ClientStateCredits{Counter: utils.NewActorCounter()}
			}
			state = clientStates[clientID]

			if credit.GetEof() {
				state.CreditEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueSecName)
			} else {
				state.Counter.Count(&credit)
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
				clientStates[clientID] = &utils.ClientStateCredits{Counter: utils.NewActorCounter()}
			}
			state = clientStates[clientID]

			if movie.GetEof() {
				state.MovieEOF = true
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueName)
			} else {
				state.Counter.AppendMovie(&movie)
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
	inputExchange := "ar_movies_2000_and_later_exchange"
	// Init state movies
	joiner.InitStateHelperMovies(inputExchange)

	err := rabbitmq.DeclareDirectExchanges(joiner.Channel, inputExchange)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare exchange", err)
	}

	err = rabbitmq.DeclareDirectQueues(joiner.Channel, joiner.Config.InputQueueName, joiner.Config.InputQueueSecName)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to declare queue", err)
	}

	err = rabbitmq.BindQueueToExchange(joiner.Channel, joiner.Config.InputQueueName, inputExchange, joiner.Config.ID)
	if err != nil {
		Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "failed to bind queue to exchange", err)
	}

	// Store client-specific data
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
