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
const MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE = "Failed to publish on outputqueue"

// Joiner which can be of types "group_by_movie_id_ratings" or "group_by_movie_id_credits".
type Joiner struct {
	Channel    *amqp.Channel
	Connection *amqp.Connection
	Config     JoinerConfig
	Log        *logging.Logger
	//WindowMovies       *window.MessageWindow
	MessagesWindow *window.MessageWindow // Ratings or Credits
	//StateHelperMovies  *state.StateHelper[JoinerMoviesState, JoinerMoviesUpdateArgs, AckArgs]
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

/*
// Check EOF condition
func (joiner *Joiner) logEofQueue(queueName string) {
	joiner.Log.Infof("[%s,%s] %s", joiner.Config.JoinerType, queueName, MSG_RECEIVED_EOF_MARKER)
}
*/
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

	// Init state credits
	joiner.InitStateHelperCredits(inputExchange) // todo: ya no se usan exchanges en los joiners. Se declaran las queues directamente

	clientStates := joiner.CreateJoinerCreditsState()
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
		}
	}

	processMessage := func(msg amqp.Delivery, isCredit bool) {
		var clientID string
		var stateCred *utils.ClientStateCredits

		if isCredit {
			var credit protopb.CreditSanit
			if err := proto.Unmarshal(msg.Body, &credit); err != nil {
				joiner.Log.Errorf("failed to unmarshal credit: %v", err)
				return
			}

			clientID = credit.GetClientId()

			if joiner.MessagesWindow.IsDuplicate(*credit.ClientId, "credits", *credit.MessageId) {
				joiner.Log.Debugf("duplicate message: %v", *credit.MessageId)
				joiner.sendAck(msg)
				return
			}

			if _, exists := clientStates.ClientStates[clientID]; !exists {
				clientStates.ClientStates[clientID] = &utils.ClientStateCredits{Counter: utils.NewActorCounter()}
			}
			stateCred = clientStates.ClientStates[clientID]

			if credit.GetEof() {
				stateCred.CreditEOF = true
				joiner.SaveCreditsState(
					clientStates,
					msg,
					clientID,
					"credits",
					0,
					[]string{},
					[]string{},
					0,
					true,
					true,
					*credit.MessageId)
				state.Synch(joiner.StateHelperCredits, SendAck)
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueSecName)
			} else {
				stateCred.Counter.Count(&credit)
				joiner.SaveCreditsState(
					clientStates,
					msg,
					clientID,
					"credits",
					*credit.Id,
					credit.GetCastNames(),
					credit.GetProfilePaths(),
					0,
					true,
					false,
					*credit.MessageId)
				joiner.Log.Infof("[client_id:%s][queue:%s] processed credit: %v", clientID, joiner.Config.InputQueueSecName, &credit)
			}

		} else {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("failed to unmarshal movie: %v", err)
				return
			}

			clientID = movie.GetClientId()

			if joiner.MessagesWindow.IsDuplicate(*movie.ClientId, GenerateSourceIdMovies(movie.GetSourceId()), *movie.MessageId) {
				joiner.Log.Debugf("duplicate message: %v", *movie.MessageId)
				joiner.sendAck(msg)
				return
			}

			if _, exists := clientStates.ClientStates[clientID]; !exists {
				clientStates.ClientStates[clientID] = &utils.ClientStateCredits{Counter: utils.NewActorCounter()}
			}
			stateCred = clientStates.ClientStates[clientID]

			if movie.GetEof() {
				stateCred.MovieEOF = true
				joiner.SaveCreditsState(
					clientStates,
					msg,
					clientID,
					GenerateSourceIdMovies(movie.GetSourceId()),
					0,
					[]string{},
					[]string{},
					0,
					false,
					true,
					*movie.MessageId)
				state.Synch(joiner.StateHelperCredits, SendAck)
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueName)
			} else {
				stateCred.Counter.AppendMovie(&movie)
				joiner.SaveCreditsState(
					clientStates,
					msg,
					clientID,
					GenerateSourceIdMovies(movie.GetSourceId()),
					0,
					[]string{},
					[]string{},
					movie.GetId(),
					false,
					false,
					*movie.MessageId)
				joiner.Log.Infof("[client_id:%s][queue:%s] processed movie: %v", clientID, joiner.Config.InputQueueName, &movie)
			}
		}

		sendReportIfReady(clientID, stateCred)
	}

	go func() {
		msgs, err := rabbitmq.ConsumeFromQueue(joiner.Channel, joiner.Config.InputQueueName)
		if err == nil {
			for msg := range msgs {
				/*
					err := rabbitmq.SingleAck(msg)

					if err != nil {
						joiner.Log.Fatalf("failed to ack message: %v", err)
					}
				*/

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
			/*
				err := rabbitmq.SingleAck(msg)
				if err != nil {
					joiner.Log.Fatalf("failed to ack message: %v", err)
				}*/
			clientStatesMutex.Lock()
			processMessage(msg, true)
			clientStatesMutex.Unlock()
		}

	}()

	select {}
}

func (joiner *Joiner) joiner_g_b_m_id_ratings() {
	// Init state ratings
	joiner.InitStateHelperRatings("ar_movies_2000_and_later_exchange")

	// Store client-specific data
	clientStates := joiner.CreateJoinerRatingsState()
	var clientStatesMutex sync.RWMutex
	joiner.StateHelperRatings.SetMaxStates(state.MAX_STATES / 100)
	// Send report when both EOFs are received for a client
	sendReportIfReady := func(clientID string, state *utils.ClientStateRatings) {
		if state.MovieEOF && state.RatingEOF {
			// Get top and bottom ratings
			topAndBottom := state.Totalizer.GetTopAndBottom(clientID, DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT, joiner.Config.InputQueueName)

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

		}
	}

	processMessage := func(msg amqp.Delivery, isRating bool) {
		var clientID string
		var stateRatings *utils.ClientStateRatings

		if isRating {
			var rating protopb.RatingSanit
			if err := proto.Unmarshal(msg.Body, &rating); err != nil {
				joiner.Log.Errorf("failed to unmarshal rating: %v", err)
				return
			}
			clientID = rating.GetClientId()

			// check duplicate
			if joiner.MessagesWindow.IsDuplicate(*rating.ClientId, "ratings", *rating.MessageId) {
				joiner.Log.Debugf("rating duplicate message: %v", *rating.MessageId)
				//joiner.sendAck(msg)
				//return
			}

			if _, exists := clientStates.ClientStates[clientID]; !exists {
				clientStates.ClientStates[clientID] = &utils.ClientStateRatings{Totalizer: utils.NewRatingTotalizer()}
			}
			stateRatings = clientStates.ClientStates[clientID]

			if rating.GetEof() {
				stateRatings.RatingEOF = true
				/*
					joiner.SaveRatingsState(
						clientStates,
						msg,
						clientID,
						"ratings",
						0,
						0,
						0,
						"",
						true,
						true,
						*rating.MessageId)
					state.Synch(joiner.StateHelperRatings, SendAck)*/
				/*
					joiner.SaveRatingsStatePerformant(
						clientStates,
						msg,
						clientID,
						"ratings",
						0,
						0,
						0,
						"",
						true,
						true,
						*rating.MessageId)
					joiner.SynchRatingsStatePerformant(clientStates)*/
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueSecName)
			} else {
				stateRatings.Totalizer.Sum(&rating)
				/*
					joiner.SaveRatingsState(
						clientStates,
						msg,
						clientID,
						"ratings",
						rating.GetMovieId(),
						rating.GetRating(),
						0,
						"",
						true,
						false,
						rating.GetMessageId())*/
				/*
					joiner.SaveRatingsStatePerformant(
						clientStates,
						msg,
						clientID,
						"ratings",
						rating.GetMovieId(),
						rating.GetRating(),
						0,
						"",
						true,
						false,
						rating.GetMessageId())*/
			}

		} else {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("failed to unmarshal movie: %v", err)
				return
			}
			clientID = movie.GetClientId()

			if joiner.MessagesWindow.IsDuplicate(*movie.ClientId, GenerateSourceIdMovies(movie.GetSourceId()), *movie.MessageId) {
				joiner.Log.Debugf("movie duplicate message: %v %v", *movie.MessageId, movie.GetTitle())
				//joiner.sendAck(msg)
				//return
			}

			if _, exists := clientStates.ClientStates[clientID]; !exists {
				clientStates.ClientStates[clientID] = &utils.ClientStateRatings{Totalizer: utils.NewRatingTotalizer()}
			}
			stateRatings = clientStates.ClientStates[clientID]

			if movie.GetEof() {
				stateRatings.MovieEOF = true
				/*
					joiner.SaveRatingsState(
						clientStates,
						msg,
						clientID,
						GenerateSourceIdMovies(movie.GetSourceId()),
						0,
						0,
						0,
						"",
						false,
						true,
						*movie.MessageId)
					state.Synch(joiner.StateHelperRatings, SendAck)
				*/
				/*
					joiner.SaveRatingsStatePerformant(
						clientStates,
						msg,
						clientID,
						GenerateSourceIdMovies(movie.GetSourceId()),
						0,
						0,
						0,
						"",
						false,
						true,
						*movie.MessageId)
					joiner.SynchRatingsStatePerformant(clientStates)
				*/
				joiner.Log.Infof("[client_id:%s][queue:%s] recevied EOF", clientID, joiner.Config.InputQueueName)
			} else {
				stateRatings.Totalizer.AppendMovie(&movie)
				/*
					joiner.SaveRatingsState(
						clientStates,
						msg,
						clientID,
						GenerateSourceIdMovies(movie.GetSourceId()),
						0,
						0,
						movie.GetId(),
						movie.GetTitle(),
						false,
						false,
						*movie.MessageId)
				*/
				/*
					joiner.SaveRatingsStatePerformant(
						clientStates,
						msg,
						clientID,
						GenerateSourceIdMovies(movie.GetSourceId()),
						0,
						0,
						movie.GetId(),
						movie.GetTitle(),
						false,
						false,
						*movie.MessageId)
				*/
			}
		}

		sendReportIfReady(clientID, stateRatings)
	}

	// Start message consumers
	go func() {
		msgs, err := rabbitmq.ConsumeFromQueue(joiner.Channel, joiner.Config.InputQueueName)
		if err != nil {
			joiner.Log.Fatalf("[queue:%s] failed to consume: %v", joiner.Config.InputQueueName, err)
		}

		for msg := range msgs {
			//Coment to test
			err := rabbitmq.SingleAck(msg)
			if err != nil {
				joiner.Log.Fatalf("failed to ack message: %v", err)
			}
			//Coment to test
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

			//Coment to test

			err := rabbitmq.SingleAck(msg)
			if err != nil {
				joiner.Log.Fatalf("failed to ack message: %v", err)
			}
			//Coment to test
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
	joiner.DisposeStateHelpers()
	Shutdown(joiner.Log, joiner.Connection, joiner.Channel, "", nil)
}

func Shutdown(log *logging.Logger, connection *amqp.Connection, channel *amqp.Channel, message string, err error) {
	rabbitmq.ShutdownConnection(connection)
	rabbitmq.ShutdownChannel(channel)

	if err != nil {
		log.Fatalf("%v: %v", message, err)
	}
}

func (joiner *Joiner) sendAck(msg amqp.Delivery) error {
	err := rabbitmq.SingleAck(msg)
	if err != nil {
		joiner.Log.Fatalf("failed to ack message: %v", err)
		return err
	}
	return nil
}
