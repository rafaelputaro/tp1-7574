package common

import (
	"sync"
	"tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"
	"tp1/rabbitmq"
	rabbitUtils "tp1/rabbitmq"

	"tp1/server/workers/aggregator/common/utils"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

// Aggregator types:
const MOVIES string = "movies"
const TOP5 string = "top_5"
const TOP10 string = "top_10"
const TOP_AND_BOTTOM string = "top_and_bottom"
const METRICS string = "metrics"

// Messages to log:
const MSG_ERROR_CONFIG = "Configuration could not be read from config file. Using env variables instead"
const MSG_ERROR_DIAL = "Error on dial rabbitmq"
const MSG_ERROR_ON_CREATE_CHANNEL = "Error on create rabbitmq channel"
const MSG_ERROR_ON_DECLARE_QUEUE = "Error on declare queue"
const MSG_START = "Starting job for ID"
const MSG_FAILED_CONSUME = "Failed to consume messages from"
const MSG_JOB_FINISHED = "Job finished"
const MSG_FAILED_TO_UNMARSHAL = "Failed to unmarshal message"
const MSG_RECEIVED_EOF_MARKER = "Received EOF marker"
const MSG_AGGREGATED = "Accepted"
const MSG_RECEIVED = "Received"
const MSG_SENT_TO_REPORT = "Sent to report"
const MSG_FAILED_TO_MARSHAL = "Failed to marshal message"
const MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE = "Failed to publish on outputqueue"
const MSG_FAILED_TO_GENERATE_A_METRIC = "Failure to generate a metric"

// Aggregator which can be of types "movies", "top_5", "top_10", "top_and_bottom" and "metrices".
// In the case of type "metrics" works with two queues:
// InputQueue: negative_movies
// InputQueueSec: positive_movies
type Aggregator struct {
	Channel    *amqp.Channel
	Connection *amqp.Connection
	Config     AggregatorConfig
	Log        *logging.Logger
}

// Returns new aggregator ready to work with rabbit
func NewAggregator(log *logging.Logger) (*Aggregator, error) {
	var config, err = LoadAggregatorConfig()
	if err != nil {
		log.Fatalf(MSG_ERROR_CONFIG)
		return nil, err
	}
	config.LogConfig(log)
	connection, err := rabbitUtils.ConnectRabbitMQ(log)
	if err != nil {
		log.Fatalf("%v: %v", MSG_ERROR_DIAL, err)
		return nil, err
	}
	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("%v: %v", MSG_ERROR_ON_CREATE_CHANNEL, err)
		connection.Close()
		return nil, err
	}
	// Exchange config and bind for metrics case
	if config.AggregatorType == METRICS {
		// declare exchange
		err = rabbitmq.DeclareDirectExchanges(channel, "sentiment_exchange")
		if err != nil {
			log.Fatalf("[aggregator_%s] Failed to declare exchange %s: %v", config.AggregatorType, "", err)
		}
	}
	err = rabbitmq.DeclareDirectQueues(channel, config.InputQueue, config.OutputQueue)
	if err != nil {
		connection.Close()
		channel.Close()
		log.Fatalf("%v: %v", MSG_ERROR_ON_DECLARE_QUEUE, err)
		return nil, err
	}
	if config.AggregatorType == METRICS {
		// Bind the queue to the exchange
		err = rabbitmq.BindQueueToExchange(channel, config.InputQueue, "sentiment_exchange", "")
		if err != nil {
			log.Fatalf("[aggregator_%s] Failed on bind %s: %v", config.AggregatorType, "", err)
		}
		// Declare secundary input queue
		err = rabbitmq.DeclareDirectQueues(channel, config.InputQueueSec)
		if err != nil {
			connection.Close()
			channel.Close()
			log.Fatalf("%v: %v", MSG_ERROR_ON_DECLARE_QUEUE, err)
			return nil, err
		}
		// Bind the queue to the exchange
		err = rabbitmq.BindQueueToExchange(channel, config.InputQueueSec, "sentiment_exchange", "")
		if err != nil {
			log.Fatalf("[aggregator_%s] Failed on bind %s: %v", config.AggregatorType, "", err)
		}
		return &Aggregator{
			Channel:    channel,
			Connection: connection,
			Config:     *config,
			Log:        log,
		}, nil
	}
	return &Aggregator{
		Channel:    channel,
		Connection: connection,
		Config:     *config,
		Log:        log,
	}, nil
}

// Init aggregator loop
func (aggregator *Aggregator) Start() {
	aggregator.Log.Infof("[aggregator_%s] %s: %s", aggregator.Config.AggregatorType, MSG_START, aggregator.Config.ID)
	switch aggregator.Config.AggregatorType {
	case MOVIES:
		aggregator.aggregateMovies()
	case TOP5:
		aggregator.aggregateTop5()
	case TOP10:
		aggregator.aggregateTop10()
	case TOP_AND_BOTTOM:
		aggregator.aggregateTopAndBottom()
	case METRICS:
		aggregator.aggregateMetrics()
	}
	aggregator.Log.Infof("[aggregator_%s] %s", aggregator.Config.AggregatorType, MSG_JOB_FINISHED)
}

// Check EOF condition
func (aggregator *Aggregator) checkEofSingleQueue(amountEOF int) bool {
	if amountEOF == int(aggregator.Config.AmountSources) {
		aggregator.Log.Infof("[aggregator_%s] %s", aggregator.Config.AggregatorType, MSG_RECEIVED_EOF_MARKER)
		return true
	}
	return false
}

// Send to report
func (aggregator *Aggregator) publishData(data []byte) {
	err := rabbitmq.Publish(aggregator.Channel, "", aggregator.Config.OutputQueue, data)
	if err != nil {
		aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE, err)
	}
}

// Check error and publish
func (aggregator *Aggregator) checkErrorAndPublish(clientID string, data []byte, err error) {
	if err != nil {
		aggregator.Log.Fatalf("[aggregator_%s client_%s] %s: %v", aggregator.Config.AggregatorType, clientID, MSG_FAILED_TO_MARSHAL, err)
	} else {
		aggregator.publishData(data)
	}
}

func (aggregator *Aggregator) aggregateMovies() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err == nil {
		amountEOF := make(map[string]int)
		for msg := range msgs {
			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
				continue
			}
			// EOF
			if movie.Eof != nil && *movie.Eof {
				clientID := movie.GetClientId()
				amountEOF[clientID] = utils.GetOrInitKeyMap(&amountEOF, clientID, utils.InitEOFCount) + 1
				// If all sources sent EOF, submit the EOF to report
				if aggregator.checkEofSingleQueue(amountEOF[movie.GetClientId()]) {
					dataEof, errEof := protoUtils.CreateEofMessageMovieSanit(movie.GetClientId())
					aggregator.checkErrorAndPublish(clientID, dataEof, errEof)
				}
			} else {
				aggregator.Log.Debugf("[aggregator_%s client_%s] %s: %s (%d)", aggregator.Config.AggregatorType, movie.GetClientId(), MSG_AGGREGATED, *movie.Title, *movie.ReleaseYear)
				aggregator.publishData(msg.Body)
			}
		}
	}
}

func (aggregator *Aggregator) aggregateTop5() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err != nil {
		aggregator.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, aggregator.Config.InputQueue, err)
	}
	// Count EOF and globalTop5 for all clients
	amountEOF := make(map[string]int)
	globalTop5 := make(map[string]*protopb.Top5Country)
	// Message loop
	for msg := range msgs {
		var top5 protopb.Top5Country
		if err := proto.Unmarshal(msg.Body, &top5); err != nil {
			aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
			continue
		}
		clientID := top5.GetClientId()
		aggregator.Log.Debugf("[aggregator_%s client_%s] %s: %s", aggregator.Config.AggregatorType, clientID, MSG_RECEIVED, protoUtils.Top5ToString(&top5))
		// get the last global top 5 or init for a client
		lastGlobalTop5Cli := utils.GetOrInitKeyMapWithKey(&globalTop5, clientID, protoUtils.CreateMinimumTop5Country)
		// EOF
		if top5.GetEof() {
			amountEOF[clientID] = utils.GetOrInitKeyMap(&amountEOF, clientID, utils.InitEOFCount) + 1
			// If all sources sent EOF, send top 5 and submit the EOF to report
			if aggregator.checkEofSingleQueue(amountEOF[clientID]) {
				// Prepare to send report
				data, err := proto.Marshal(lastGlobalTop5Cli)
				if err != nil {
					aggregator.Log.Fatalf("[aggregator_%s client_%s] %s: %v", aggregator.Config.AggregatorType, clientID, MSG_FAILED_TO_MARSHAL, err)
					continue
				}
				// send top5 to report
				aggregator.publishData(data)
				aggregator.Log.Debugf("[aggregator_%s client_%s] %s: %s", aggregator.Config.AggregatorType, clientID, MSG_SENT_TO_REPORT, protoUtils.Top5ToString(lastGlobalTop5Cli))
				// submit the EOF to report
				dataEof, errEof := protoUtils.CreateEofMessageTop5Country(clientID)
				aggregator.checkErrorAndPublish(clientID, dataEof, errEof)
			}
			continue
		}
		// Update global top 5
		globalTop5[clientID] = utils.ReduceTop5(lastGlobalTop5Cli, &top5)
		// To log
		newGlobalTop5Cli := utils.GetOrInitKeyMapWithKey(&globalTop5, clientID, protoUtils.CreateMinimumTop5Country)
		aggregator.Log.Debugf("[aggregator_%s client_%s] Top After Reduce: %s", aggregator.Config.AggregatorType, clientID, protoUtils.Top5ToString(newGlobalTop5Cli))
	}
}

func (aggregator *Aggregator) aggregateTop10() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err != nil {
		aggregator.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, aggregator.Config.InputQueue, err)
	}
	// Count EOF and actors data for all clients
	amountEOF := make(map[string]int)
	actorsData := make(map[string](*utils.ActorsData))
	for msg := range msgs {
		var actorCount protopb.Actor
		if err := proto.Unmarshal(msg.Body, &actorCount); err != nil {
			aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
			continue
		}
		clientID := actorCount.GetClientId()
		aggregator.Log.Debugf("[aggregator_%s client_%s] %s : %s", aggregator.Config.AggregatorType, clientID, MSG_RECEIVED, protoUtils.ActorToString(&actorCount))
		// Actual data for a client
		actorsDataClient := utils.GetOrInitKeyMapWithKey(&actorsData, clientID, utils.InitActorsData)
		// EOF
		if actorCount.GetEof() {
			amountEOF[clientID] = utils.GetOrInitKeyMap(&amountEOF, clientID, utils.InitEOFCount) + 1
			// If all sources sent EOF, send top 10 and submit the EOF to report
			if aggregator.checkEofSingleQueue(amountEOF[clientID]) {
				top10 := actorsDataClient.GetTop10()
				data, err := proto.Marshal(top10)
				if err != nil {
					aggregator.Log.Fatalf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_MARSHAL, err)
					continue
				}
				// send top10 to report
				aggregator.publishData(data)
				aggregator.Log.Debugf("[aggregator_%s client_%s] %s: %s", aggregator.Config.AggregatorType, clientID, MSG_SENT_TO_REPORT, protoUtils.Top10ToString(top10))
				// submit the EOF to report
				dataEof, errEof := protoUtils.CreateEofMessageTop10("")
				aggregator.checkErrorAndPublish("", dataEof, errEof)
			}
			continue
		}
		// Update top for a client
		actorsDataClient.UpdateCount(&actorCount)
	}
}

func (aggregator *Aggregator) aggregateTopAndBottom() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err == nil {
		// Count EOF and top_and_bottom for all clients
		amountEOF := 0
		globalTopAndBottom := protoUtils.CreateSeedTopAndBottom("")
		for msg := range msgs {
			var topAndBottom protopb.TopAndBottomRatingAvg
			if err := proto.Unmarshal(msg.Body, &topAndBottom); err != nil {
				aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
				continue
			}
			// EOF
			if topAndBottom.Eof != nil && *topAndBottom.Eof {
				amountEOF += 1
				// If all sources sent EOF, send top and Bottom and submit the EOF to report
				if aggregator.checkEofSingleQueue(amountEOF) {
					aggregator.Log.Debugf("[aggregator_%s] %s: %s", aggregator.Config.AggregatorType, MSG_AGGREGATED, protoUtils.TopAndBottomToString(&globalTopAndBottom))
					data, err := proto.Marshal(&globalTopAndBottom)
					if err != nil {
						aggregator.Log.Fatalf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_MARSHAL, err)
						break
					}
					// send topAndBottom to report
					aggregator.publishData(data)
					// submit the EOF to report
					aggregator.publishData(msg.Body)
					break
				}
			}
			globalTopAndBottom = *utils.ReduceTopAndBottom(&globalTopAndBottom, &topAndBottom)
		}
	}
}

func (aggregator *Aggregator) aggregateMetrics() {
	// declare results
	var avgRevenueOverBudgetNegative, avgRevenueOverBudgetPositive float64
	// wait group
	var wg sync.WaitGroup
	wg.Add(1)
	// negative queue
	var errNeg, errPos error
	go func() {
		defer wg.Done()
		avgRevenueOverBudgetNegative, errNeg = aggregator.aggregateMetric(aggregator.Config.InputQueue)
	}()
	// prositive queue
	avgRevenueOverBudgetPositive, errPos = aggregator.aggregateMetric(aggregator.Config.InputQueueSec)
	// wait for negative result
	wg.Wait()
	if errNeg != nil || errPos != nil {
		aggregator.Log.Fatalf("[aggregator_%s] %s: Neg error: %v | Pos error: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_GENERATE_A_METRIC, errNeg, errPos)
		return
	}
	// prepare report
	report := utils.CreateMetricsReport(avgRevenueOverBudgetNegative, avgRevenueOverBudgetPositive)
	aggregator.Log.Debugf("[aggregator_%s] %s: %s", aggregator.Config.AggregatorType, MSG_AGGREGATED, protoUtils.MetricsToString(&report))
	data, err := proto.Marshal(&report)
	if err != nil {
		aggregator.Log.Fatalf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_MARSHAL, err)
		return
	}
	// send report
	aggregator.publishData(data)
	// prepare eof
	eof := protoUtils.CreateEofMetrics("")
	data, err = proto.Marshal(eof)
	if err != nil {
		aggregator.Log.Fatalf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_MARSHAL, err)
		return
	}
	// submit the EOF to report
	aggregator.publishData(data)
}

func (aggregator *Aggregator) aggregateMetric(queueName string) (float64, error) {
	msgs, err := aggregator.consumeQueue(queueName)
	if err != nil {
		return 0.0, err
	}
	amountEOF := 0
	var avgRevenueOverBudget float64 = 0.0
	var count int64 = 0
	var sumAvg float64 = 0.0
	for msg := range msgs {
		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
			continue
		}
		aggregator.Log.Debugf("aggregateMetric - queue %s - got message %s - eof %t", queueName, movie.GetTitle(), movie.GetEof())
		// EOF
		if movie.GetEof() {
			amountEOF += 1
			// If all sources sent EOF calculate avg and break loop
			if aggregator.checkEofSingleQueue(amountEOF) {
				if count != 0 {
					avgRevenueOverBudget = sumAvg / float64(count)
				}
				aggregator.Log.Debugf("[aggregator_%s] %s: AvgRevenueOverBudget: %v", aggregator.Config.AggregatorType, MSG_AGGREGATED, avgRevenueOverBudget)
				break
			}
			continue
		}
		// update sum and count
		count++
		sumAvg += (*movie.Revenue) / float64(*movie.Budget)
	}
	return avgRevenueOverBudget, nil
}

func (aggregator *Aggregator) consumeQueue(queueName string) (<-chan amqp.Delivery, error) {
	msgs, err := rabbitmq.ConsumeFromQueue(aggregator.Channel, queueName)
	if err != nil {
		aggregator.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, queueName, err)
	}
	aggregator.Log.Infof("[aggregator_%s] Waiting for messages in queue %s ...", aggregator.Config.AggregatorType, queueName)
	return msgs, err
}

func (aggregator *Aggregator) Dispose() {
	aggregator.Log.Infof("Close aggregator")
	if aggregator.Channel != nil {
		aggregator.Channel.Close()
	}
	if aggregator.Connection != nil {
		aggregator.Connection.Close()
	}
}
