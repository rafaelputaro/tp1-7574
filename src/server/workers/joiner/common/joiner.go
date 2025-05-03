package common

import (
	"tp1/config"
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
const MSG_FAILED_TO_UNMARSHAL = "Failed to unmarshal message"
const MSG_RECEIVED_EOF_MARKER = "Received EOF marker"
const MSG_JOINED = "Joined"
const MSG_FAILED_TO_MARSHAL = "Failed to marshal message"
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
		joiner.Log.Errorf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE, err)
	}
}

func (joiner *Joiner) joiner_g_b_m_id_credits() {
	inputExchange := "ar_movies_after_2000_exchange"

	err := joiner.Channel.ExchangeDeclare(
		inputExchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare exchange %s: %v", joiner.Config.JoinerType, inputExchange, err)
	}

	// Declare temporal queue used to read from the exchange
	_, err = joiner.Channel.QueueDeclare(
		joiner.Config.InputQueueName, // empty = temporal queue with generated name
		true,                         // durable
		false,                        // auto-delete when unused
		false,                        // exclusive
		false,                        // no-wait
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare temporary queue: %v", joiner.Config.JoinerType, err)
	}

	// Bind the queue to the exchange
	err = joiner.Channel.QueueBind(
		joiner.Config.InputQueueName, // queue name
		joiner.Config.ID,             // routing key (empty on a fanout)
		inputExchange,                // exchange
		false,
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to bind queue to exchange: %v", joiner.Config.JoinerType, err)
	}

	err = rabbitmq.DeclareFanoutExchanges(joiner.Channel, config.CreditsExchange)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare exchange: %v", joiner.Config.JoinerType, err)
	}

	// Declare temporal queue used to read from the exchange
	inputQueue, err := joiner.Channel.QueueDeclare(
		"",    // empty = temporal queue with generated name
		false, // durable
		true,  // auto-delete when unused
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare temporary queue: %v", joiner.Config.JoinerType, err)
	}

	// Bind the queue to the exchange
	err = joiner.Channel.QueueBind(
		inputQueue.Name,        // queue name
		"",                     // routing key (empty on a fanout)
		config.CreditsExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to bind queue to exchange: %v", joiner.Config.JoinerType, err)
	}

	msgs, err := joiner.consumeQueue(joiner.Config.InputQueueName)
	if err == nil {
		counter := utils.NewActorCounter()
		// read all movies
		for msg := range msgs {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_UNMARSHAL, err)
				continue
			}
			// EOF
			if movie.Eof != nil && *movie.Eof {
				joiner.logEofQueue(joiner.Config.InputQueueName)
				break
			}
			// append movie
			counter.AppendMovie(&movie)
		}

		msgs, err = joiner.consumeQueue(inputQueue.Name)
		if err == nil {
			// read all credits
			for msg := range msgs {
				var credit protopb.CreditSanit
				if err := proto.Unmarshal(msg.Body, &credit); err != nil {
					joiner.Log.Errorf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_UNMARSHAL, err)
					continue
				}
				// EOF
				if credit.Eof != nil && *credit.Eof {
					joiner.logEofQueue(joiner.Config.InputQueueSecName)
					break
				}
				// count credit
				counter.Count(&credit)
			}
		}
		// send actors count
		for actorPath := range counter.Actors {
			actor := *counter.GetActor(actorPath)
			joiner.Log.Debugf("[%s] %s: %s", joiner.Config.JoinerType, MSG_JOINED, utils.ActorToString(&actor))
			data, err := proto.Marshal(&actor)
			if err != nil {
				joiner.Log.Fatalf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_MARSHAL, err)
				break
			}
			// send actor
			joiner.publishData(data)
		}
		// send eof
		eof := *utils.CreateActorEof()
		data, err := proto.Marshal(&eof)
		if err != nil {
			joiner.Log.Fatalf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_MARSHAL, err)
		}
		// send eof
		joiner.publishData(data)
	}
}

func (joiner *Joiner) joiner_g_b_m_id_ratings() {
	inputExchange := "ar_movies_2000_and_later_exchange"

	err := joiner.Channel.ExchangeDeclare(
		inputExchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare exchange %s: %v", joiner.Config.JoinerType, inputExchange, err)
	}

	// Declare temporal queue used to read from the exchange
	_, err = joiner.Channel.QueueDeclare(
		joiner.Config.InputQueueName, // empty = temporal queue with generated name
		true,                         // durable
		false,                        // auto-delete when unused
		false,                        // exclusive
		false,                        // no-wait
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare temporary queue: %v", joiner.Config.JoinerType, err)
	}

	// Bind the queue to the exchange
	err = joiner.Channel.QueueBind(
		joiner.Config.InputQueueName, // queue name
		joiner.Config.ID,             // routing key (empty on a fanout)
		inputExchange,                // exchange
		false,
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to bind queue to exchange: %v", joiner.Config.JoinerType, err)
	}

	err = rabbitmq.DeclareFanoutExchanges(joiner.Channel, config.RatingsExchange)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare exchange: %v", joiner.Config.JoinerType, err)
	}

	// Declare temporal queue used to read from the exchange
	inputQueue, err := joiner.Channel.QueueDeclare(
		"",    // empty = temporal queue with generated name
		false, // durable
		true,  // auto-delete when unused
		true,  // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to declare temporary queue: %v", joiner.Config.JoinerType, err)
	}

	// Bind the queue to the exchange
	err = joiner.Channel.QueueBind(
		inputQueue.Name,        // queue name
		"",                     // routing key (empty on a fanout)
		config.RatingsExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		joiner.Log.Fatalf("[%s] Failed to bind queue to exchange: %v", joiner.Config.JoinerType, err)
	}

	msgs, err := joiner.consumeQueue(joiner.Config.InputQueueName)
	if err == nil {
		totalizer := utils.NewRatingTotalizer()
		// read all movies
		for msg := range msgs {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				joiner.Log.Errorf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_UNMARSHAL, err)
				continue
			}
			// EOF
			if movie.Eof != nil && *movie.Eof {
				joiner.logEofQueue(joiner.Config.InputQueueName)
				break
			}
			// append movie
			totalizer.AppendMovie(&movie)
		}

		msgs, err = joiner.consumeQueue(inputQueue.Name)
		if err == nil {
			// read all ratings
			for msg := range msgs {
				var rating protopb.RatingSanit
				if err := proto.Unmarshal(msg.Body, &rating); err != nil {
					joiner.Log.Errorf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_UNMARSHAL, err)
					continue
				}
				// EOF
				if rating.Eof != nil && *rating.Eof {
					joiner.logEofQueue(joiner.Config.InputQueueSecName)
					break
				}
				// sum rating
				totalizer.Sum(&rating)
			}
		}
		// get top and bottom
		topAndBottom := *totalizer.GetTopAndBottom()
		// prepare report
		joiner.Log.Debugf("[%s] %s: %s", joiner.Config.JoinerType, MSG_JOINED, utils.TopAndBottomToString(&topAndBottom))
		data, err := proto.Marshal(&topAndBottom)
		if err != nil {
			joiner.Log.Fatalf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_MARSHAL, err)
			return
		}
		// send report
		joiner.publishData(data)
		// send eof
		eof := *utils.CreateTopAndBottomRatingAvgEof()
		data, err = proto.Marshal(&eof)
		if err != nil {
			joiner.Log.Fatalf("[%s] %s: %v", joiner.Config.JoinerType, MSG_FAILED_TO_MARSHAL, err)
		}
		// send eof
		joiner.publishData(data)
	}
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
