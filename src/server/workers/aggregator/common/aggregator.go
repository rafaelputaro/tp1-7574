package common

import (
	"sort"
	"sync"

	"tp1/helpers/state"
	"tp1/rabbitmq"

	"tp1/helpers/window"
	"tp1/protobuf/protopb"
	protoUtils "tp1/protobuf/utils"

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

// Ouput message IDs:
const DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT int64 = 0
const DEFAULT_MESSAGE_ID_EOF_UNIQUE_OUTPUT int64 = 1
const DEFAULT_MESSAGE_SOURCE_ID string = ""

// Messages to log:
const MSG_ERROR_CONFIG = "Configuration could not be read from config file. Using env variables instead"
const MSG_ERROR_DIAL = "Error on dial rabbitmq"
const MSG_ERROR_ON_CREATE_CHANNEL = "Error on create rabbitmq channel"
const MSG_ERROR_ON_DECLARE_QUEUE = "Error on declare queue"
const MSG_FAILED_CONSUME = "Failed to consume messages from"
const MSG_FAILED_TO_UNMARSHAL = "Failed to unmarshal message"
const MSG_RECEIVED_EOF_MARKER = "Received EOF marker"
const MSG_RECEIVED = "Received"
const MSG_SENT_TO_REPORT = "Sent to report"
const MSG_FAILED_TO_MARSHAL = "Failed to marshal message"
const MSG_FAILED_TO_PUBLISH_ON_OUTPUT_QUEUE = "Failed to publish on outputqueue"
const MSG_FAILED_TO_GENERATE_A_METRIC = "Failure to generate a metric"
const MSG_NO_BUDGET_MOVIE = "No budget movie"

// Aggregator which can be of types "movies", "top_5", "top_10", "top_and_bottom" and "metrices".
// In the case of type "metrics" works with two queues:
// InputQueue: negative_movies
// InputQueueSec: positive_movies
type Aggregator struct {
	Channel                 *amqp.Channel
	Connection              *amqp.Connection
	Config                  AggregatorConfig
	Log                     *logging.Logger
	StateHelperMovies       *state.StateHelper[AggregatorMoviesState, AggregatorMoviesUpdateArgs, AckArgs]
	StateHelperTop5         *state.StateHelper[AggregatorTop5State, AggregatorTop5UpdateArgs, AckArgs]
	StateHelperTop10        *state.StateHelper[AggregatorTop10State, AggregatorTop10UpdateArgs, AckArgs]
	StateHelperTopAndBottom *state.StateHelper[AggregatorTopAndBottomState, AggregatorTopAndBottomUpdateArgs, AckArgs]
	StateHelperMetricsNeg   *state.StateHelper[AggregatorMetricsState, AggregatorMetricsUpdateArgs, AckArgs]
	StateHelperMetricsPos   *state.StateHelper[AggregatorMetricsState, AggregatorMetricsUpdateArgs, AckArgs]
	Window                  *window.MessageWindow // All state helper and possitive metrics
	WindowSec               *window.MessageWindow // Negative metrics
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
	aggregator.Log.Infof("starting job for ID: %s", aggregator.Config.ID)
	switch aggregator.Config.AggregatorType {
	case MOVIES:
		aggregator.InitStateHelperMovie()
		aggregator.aggregateMovies()
	case TOP5:
		aggregator.InitStateHelperTop5()
		aggregator.aggregateTop5()
	case TOP10:
		aggregator.InitiStateHelperTop10()
		aggregator.aggregateTop10()
	case TOP_AND_BOTTOM:
		aggregator.InitiStateHelperTopAndBottom()
		aggregator.aggregateTopAndBottom()
	case METRICS:
		aggregator.InitiStateHelperMetrics()
		aggregator.aggregateMetrics()
	}
	aggregator.Log.Infof("job finished")
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
		aggregator.Log.Fatalf("[client_id:%s] %s: %v", clientID, MSG_FAILED_TO_MARSHAL, err)
	} else {
		aggregator.publishData(data)
	}
}

func (aggregator *Aggregator) aggregateMovies() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err == nil {
		aggregatorState := aggregator.CreateAggregatorMoviesState()

		for msg := range msgs {

			var movie protopb.MovieSanit
			if err := proto.Unmarshal(msg.Body, &movie); err != nil {
				aggregator.Log.Errorf("failed to unmarshal message %v", err)
				continue
			}

			if aggregator.Window.IsDuplicate(*movie.ClientId, movie.GetSourceId(), *movie.MessageId) {
				aggregator.Log.Debugf("duplicate message: %v", *movie.MessageId)
				aggregator.sendAck(msg)
				continue
			}

			// EOF
			if movie.GetEof() {
				clientID := movie.GetClientId()

				aggregator.Log.Infof("[client_id:%s] received eof marker", clientID)

				aggregatorState.AmountEOF[clientID] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, clientID, utils.InitEOFCount) + 1

				// If all sources sent EOF, submit the EOF to report
				if aggregator.checkEofSingleQueue(aggregatorState.AmountEOF[movie.GetClientId()]) {
					dataEof, errEof := protoUtils.CreateEofMessageMovieSanit(movie.GetClientId(), movie.GetMessageId())
					aggregator.checkErrorAndPublish(clientID, dataEof, errEof)
					aggregator.Log.Infof("[client_id:%s] sent eof marker", clientID)
				}
				aggregator.SaveMoviesState(*aggregatorState, msg, clientID, movie.GetSourceId(), true, *movie.MessageId)
				state.Synch(aggregator.StateHelperMovies, SendAck)
			} else {
				aggregator.Log.Debugf("[client_id:%s] accepted: %s (%d)", movie.GetClientId(), movie.GetTitle(), movie.GetReleaseYear())
				aggregator.publishData(msg.Body)
				aggregator.SaveMoviesState(*aggregatorState, msg, *movie.ClientId, movie.GetSourceId(), false, *movie.MessageId)
			}
		}
	}
}

func (a *Aggregator) aggregateTop5() {
	// TODO: kill queue movies_top_5_investors and related
	msgs, err := a.consumeQueue(a.Config.InputQueue)
	if err != nil {
		a.Log.Fatalf("failed to consume messages: %v", err)
	}
	aggregatorState := a.CreateAggregatorTop5State()
	for msg := range msgs {
		var movie protopb.MovieSanit
		err = proto.Unmarshal(msg.Body, &movie)
		if err != nil {
			a.Log.Errorf("failed to unmarshal message: %v", err)
			continue
		}
		if a.Window.IsDuplicate(*movie.ClientId, movie.GetSourceId(), *movie.MessageId) {
			a.Log.Debugf("duplicate message: %v", *movie.MessageId)
			a.sendAck(msg)
			continue
		}
		clientID := movie.GetClientId()
		_, found := aggregatorState.CountriesByClient[clientID]
		if !found {
			aggregatorState.CountriesByClient[clientID] = make(map[string]int64)
		}
		countryForClient := aggregatorState.CountriesByClient[clientID]

		if movie.GetEof() {
			var top5 protopb.Top5Country
			top5.ClientId = proto.String(clientID)
			top5.ProductionCountries = []string{}
			top5.Budget = []int64{}
			top5.MessageId = proto.Int64(DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT)
			type kv struct {
				Key   string
				Value int64
			}

			var pairs []kv
			for k, v := range countryForClient {
				pairs = append(pairs, kv{k, v})
			}

			sort.Slice(pairs, func(i, j int) bool {
				return pairs[i].Value > pairs[j].Value
			})

			top := 5
			for _, p := range pairs {
				top5.ProductionCountries = append(top5.ProductionCountries, p.Key)
				top5.Budget = append(top5.Budget, p.Value)

				top--
				if top == 0 {
					break
				}
			}

			data, err := proto.Marshal(&top5)
			if err != nil {
				a.Log.Fatalf("[client_id:%s] failed to marshal top5: %v", clientID, err)
			}

			err = rabbitmq.Publish(a.Channel, "", a.Config.OutputQueue, data)
			if err != nil {
				a.Log.Fatalf("[client_id:%s] failed to publish top5: %v", clientID, err)
			}

			dataEof, err := protoUtils.CreateEofMessageTop5Country(clientID, DEFAULT_MESSAGE_ID_EOF_UNIQUE_OUTPUT)
			if err != nil {
				a.Log.Fatalf("[client_id:%s] failed to marshal eof: %v", clientID, err)
			}

			err = rabbitmq.Publish(a.Channel, "", a.Config.OutputQueue, dataEof)
			if err != nil {
				a.Log.Fatalf("[client_id:%s] failed to publish eof: %v", clientID, err)
			}

			a.Log.Infof("[client_id:%s] published top5: %v", clientID, &top5)

			a.SaveTop5State(*aggregatorState, msg, clientID, movie.GetSourceId(), 0, "", true, *movie.MessageId)
			state.Synch(a.StateHelperTop5, SendAck)
			continue
		} else if movie.GetBudget() > 0 {
			a.Log.Debugf("[client_id:%s] received: %v", clientID, &movie)

			_, found = countryForClient[movie.GetProductionCountries()[0]]
			if !found {
				countryForClient[movie.GetProductionCountries()[0]] = 0
			}
			countryForClient[movie.GetProductionCountries()[0]] += movie.GetBudget()
			a.SaveTop5State(*aggregatorState, msg, *movie.ClientId, movie.GetSourceId(), movie.GetBudget(), movie.GetProductionCountries()[0], false, *movie.MessageId)
		} else {
			a.SaveTop5State(*aggregatorState, msg, clientID, movie.GetSourceId(), 0, "", false, *movie.MessageId)
		}
	}
}

func (aggregator *Aggregator) aggregateTop10() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err != nil {
		aggregator.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, aggregator.Config.InputQueue, err)
	}

	// Count EOF and actors data for all clients
	aggregatorState := aggregator.CreateAggregatorTop10State()

	for msg := range msgs {

		var actorCount protopb.Actor
		if err := proto.Unmarshal(msg.Body, &actorCount); err != nil {
			aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
			continue
		}
		clientID := actorCount.GetClientId()

		if aggregator.Window.IsDuplicate(clientID, actorCount.GetSourceId(), *actorCount.MessageId) {
			aggregator.Log.Debugf("duplicate message: %v", *actorCount.MessageId)
			aggregator.sendAck(msg)
			continue
		} else {
			aggregator.Window.AddMessage(clientID, actorCount.GetSourceId(), *actorCount.MessageId)
		}

		aggregator.Log.Debugf("[aggregator_%s client_%s] %s : %s", aggregator.Config.AggregatorType, clientID, MSG_RECEIVED, protoUtils.ActorToString(&actorCount))
		// Actual data for a client
		actorsDataClient := utils.GetOrInitKeyMapWithKey(&aggregatorState.ActorsData, clientID, utils.InitActorsData)
		// EOF
		if actorCount.GetEof() {
			aggregatorState.AmountEOF[clientID] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, clientID, utils.InitEOFCount) + 1
			// If all sources sent EOF, send top 10 and submit the EOF to report
			if aggregator.checkEofSingleQueue(aggregatorState.AmountEOF[clientID]) {
				top10 := actorsDataClient.GetTop10(clientID, DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT)
				data, err := proto.Marshal(top10)
				if err != nil {
					aggregator.Log.Fatalf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_MARSHAL, err)
					continue
				}
				// send top10 to report
				aggregator.publishData(data)
				aggregator.Log.Debugf("[aggregator_%s client_%s] %s: %s", aggregator.Config.AggregatorType, clientID, MSG_SENT_TO_REPORT, protoUtils.Top10ToString(top10))
				// submit the EOF to report
				dataEof, errEof := protoUtils.CreateEofMessageTop10(clientID, DEFAULT_MESSAGE_ID_EOF_UNIQUE_OUTPUT)
				aggregator.checkErrorAndPublish(clientID, dataEof, errEof)
			}
			aggregator.SaveTop10State(*aggregatorState, msg, clientID, actorCount.GetSourceId(), "", "", 0, true, actorCount.GetMessageId())
			state.Synch(aggregator.StateHelperTop10, SendAck)
			continue
		}
		// Update top for a client
		actorsDataClient.UpdateCount(&actorCount)
		aggregator.SaveTop10State(*aggregatorState, msg, clientID, actorCount.GetSourceId(), actorCount.GetProfilePath(), actorCount.GetName(), actorCount.GetCountMovies(), false, *actorCount.MessageId)
	}
}

func (aggregator *Aggregator) aggregateTopAndBottom() {
	msgs, err := aggregator.consumeQueue(aggregator.Config.InputQueue)
	if err != nil {
		aggregator.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, aggregator.Config.InputQueue, err)
	}
	// Count EOF and top_and_bottom for all clients
	aggregatorState := aggregator.CreateAggregatorTopAndBottom()
	for msg := range msgs {

		var topAndBottom protopb.TopAndBottomRatingAvg
		if err := proto.Unmarshal(msg.Body, &topAndBottom); err != nil {
			aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
			continue
		}
		clientID := topAndBottom.GetClientId()
		if aggregator.Window.IsDuplicate(clientID, topAndBottom.GetSourceId(), *topAndBottom.MessageId) {
			aggregator.Log.Debugf("duplicate message: %v", *topAndBottom.MessageId)
			aggregator.sendAck(msg)
			continue
		} else {
			aggregator.Window.AddMessage(clientID, topAndBottom.GetSourceId(), *topAndBottom.MessageId)
		}
		aggregator.Log.Debugf("[aggregator_%s client_%s] %s : %s", aggregator.Config.AggregatorType, clientID, MSG_RECEIVED, protoUtils.TopAndBottomToString(&topAndBottom))
		// Actual top and bottom for a client
		globalTopAndBottomClient := utils.GetOrInitKeyMapWithKeyAndMsgIdAndSrcId(&aggregatorState.GlobalTopAndBottom, clientID, DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT, DEFAULT_MESSAGE_SOURCE_ID, protoUtils.CreateSeedTopAndBottom)
		// EOF
		if topAndBottom.GetEof() {
			aggregatorState.AmountEOF[clientID] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, clientID, utils.InitEOFCount) + 1
			// If all sources sent EOF, send top and Bottom and submit the EOF to report
			if aggregator.checkEofSingleQueue(aggregatorState.AmountEOF[clientID]) {
				data, err := proto.Marshal(globalTopAndBottomClient)
				if err != nil {
					aggregator.Log.Fatalf("[aggregator_%s cliente_%s] %s: %v", aggregator.Config.AggregatorType, clientID, MSG_FAILED_TO_MARSHAL, err)
					continue
				}
				// send topAndBottom to report
				aggregator.publishData(data)
				aggregator.Log.Debugf("[aggregator_%s client_%s] %s: %s", aggregator.Config.AggregatorType, clientID, MSG_SENT_TO_REPORT, protoUtils.TopAndBottomToString(globalTopAndBottomClient))
				// submit the EOF to report
				dataEof, errEof := protoUtils.CreateEofMessageTopAndBottomRatingAvg(clientID, DEFAULT_MESSAGE_ID_EOF_UNIQUE_OUTPUT, DEFAULT_MESSAGE_SOURCE_ID)
				aggregator.checkErrorAndPublish(clientID, dataEof, errEof)
			}
			aggregator.SaveTopAndBottomState(*aggregatorState, msg, clientID, topAndBottom.GetSourceId(), protoUtils.CreateDummyTopAndBottomRatingAvg(clientID, topAndBottom.GetMessageId(), topAndBottom.GetSourceId(), true), true, topAndBottom.GetMessageId())
			state.Synch(aggregator.StateHelperTopAndBottom, SendAck)
			continue
		}
		reduced := utils.ReduceTopAndBottom(globalTopAndBottomClient, &topAndBottom, DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT, DEFAULT_MESSAGE_SOURCE_ID)
		aggregatorState.GlobalTopAndBottom[clientID] = reduced
		aggregator.SaveTopAndBottomState(*aggregatorState, msg, clientID, topAndBottom.GetSourceId(), reduced, false, *topAndBottom.MessageId)
	}
}

func (aggregator *Aggregator) aggregateMetrics() {
	// declare results
	avgRevenueOverBudgetNegative := make(map[string]float64)
	avgRevenueOverBudgetPositive := make(map[string]float64)
	// channel to receive results from aggregate gorutines
	channelResults := make(chan utils.MetricResultClient)
	// wait group
	var wg sync.WaitGroup
	wg.Add(2)
	// negative queue
	var errNeg, errPos error
	go func() {
		defer wg.Done()
		errNeg = aggregator.aggregateMetric(aggregator.Config.InputQueue, channelResults)
	}()
	// prositive queue
	go func() {
		defer wg.Done()
		errPos = aggregator.aggregateMetric(aggregator.Config.InputQueueSec, channelResults)
	}()
	// read resuls from channel
	for result := range channelResults {
		// update average for a client
		utils.UpdateMetrics(&avgRevenueOverBudgetNegative, &avgRevenueOverBudgetPositive, &result)
		// get client id
		clientID := result.ClientID
		// try to report
		report, errReport := utils.CreateMetricsReport(clientID, &avgRevenueOverBudgetNegative, &avgRevenueOverBudgetPositive, DEFAULT_MESSAGE_ID_UNIQUE_OUTPUT)
		if errReport != nil {
			continue
		}
		// prepare report
		data, err := proto.Marshal(report)
		if err != nil {
			aggregator.Log.Fatalf("[client_id:%s] %s: %v", clientID, MSG_FAILED_TO_MARSHAL, err)
			continue
		}

		// send report
		aggregator.publishData(data)
		aggregator.Log.Debugf("[client_id:%s] %s: %v", clientID, MSG_SENT_TO_REPORT, report)

		// submit the EOF to report
		dataEof, errEof := protoUtils.CreateEofMessageMetrics(clientID, DEFAULT_MESSAGE_ID_EOF_UNIQUE_OUTPUT)
		aggregator.checkErrorAndPublish(clientID, dataEof, errEof)
	}
	// wait go func
	wg.Wait()
	// check errors
	if errNeg != nil || errPos != nil {
		aggregator.Log.Fatalf("[aggregator_%s] %s: Neg error: %v | Pos error: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_GENERATE_A_METRIC, errNeg, errPos)
		return
	}
}

func (aggregator *Aggregator) aggregateMetric(queueName string, channelResults chan utils.MetricResultClient) error {
	msgs, err := aggregator.consumeQueue(queueName)
	if err != nil {
		return err
	}

	// check negative
	var isNegative bool
	var messagesWindow *window.MessageWindow
	if aggregator.Config.InputQueue == queueName {
		isNegative = true
		messagesWindow = aggregator.WindowSec
	} else {
		isNegative = false
		messagesWindow = aggregator.Window
	}

	// count EOF and count and sumAvg for each clients
	aggregatorState := aggregator.CreateAggregatorMetricsState(isNegative)

	// read all message from queue for each clients
	for msg := range msgs {

		var movie protopb.MovieSanit
		if err := proto.Unmarshal(msg.Body, &movie); err != nil {
			aggregator.Log.Errorf("[aggregator_%s] %s: %v", aggregator.Config.AggregatorType, MSG_FAILED_TO_UNMARSHAL, err)
			continue
		}
		clientID := movie.GetClientId()

		if messagesWindow.IsDuplicate(clientID, movie.GetSourceId(), *movie.MessageId) {
			aggregator.Log.Debugf("duplicate message: %v", *movie.MessageId)
			aggregator.sendAck(msg)
			continue
		} else {
			messagesWindow.AddMessage(clientID, movie.GetSourceId(), *movie.MessageId)
		}

		aggregator.Log.Debugf("[aggregator_%s client_%s] %s : queue(%s) - title(%s) - eof(%t)", aggregator.Config.AggregatorType, clientID, MSG_RECEIVED, queueName, movie.GetTitle(), movie.GetEof())
		// EOF
		if movie.GetEof() {
			aggregatorState.AmountEOF[clientID] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, clientID, utils.InitEOFCount) + 1
			// If all sources sent EOF calculate avg and send result to channel
			if aggregator.checkEofSingleQueue(aggregatorState.AmountEOF[clientID]) {
				result, errResult := utils.CreateMetricResult(clientID, isNegative, &aggregatorState.Count, &aggregatorState.SumAvg)
				if errResult != nil {
					aggregator.Log.Errorf("[aggregator_%s client_%s] error: %s", aggregator.Config.AggregatorType, clientID, errResult)
					continue
				}
				channelResults <- *result
			}
			aggregator.SaveMetricsState(*aggregatorState, msg, clientID, movie.GetSourceId(), 0, 0, true, movie.GetMessageId(), isNegative)
			if isNegative {
				state.Synch(aggregator.StateHelperMetricsNeg, SendAck)
			} else {
				state.Synch(aggregator.StateHelperMetricsPos, SendAck)
			}
			continue
		}
		if *movie.Budget == 0 {
			aggregator.Log.Errorf("[aggregator_%s client_%s] %s: %s", aggregator.Config.AggregatorType, clientID, MSG_NO_BUDGET_MOVIE, movie.GetTitle())
			err := rabbitmq.SingleAck(msg)
			if err != nil {
				aggregator.Log.Fatalf("failed to ack message: %v", err)
			}
			continue
		}
		// update sum and count
		UpdateSumAndCount(aggregatorState, clientID, *movie.Revenue, *movie.Budget)
		aggregator.SaveMetricsState(*aggregatorState, msg, clientID, movie.GetSourceId(), *movie.Revenue, *movie.Budget, false, *movie.MessageId, isNegative)
	}
	return nil
}

func UpdateSumAndCount(aggregatorState *AggregatorMetricsState, clientID string, movieRevenue float64, movieBudget int64) {
	aggregatorState.Count[clientID] = utils.GetOrInitKeyMap(&aggregatorState.Count, clientID, utils.InitCount) + 1
	avg := (movieRevenue) / float64(movieBudget)
	aggregatorState.SumAvg[clientID] = utils.GetOrInitKeyMap(&aggregatorState.SumAvg, clientID, utils.InitSumAvg) + avg
}

func (aggregator *Aggregator) consumeQueue(queueName string) (<-chan amqp.Delivery, error) {
	msgs, err := rabbitmq.ConsumeFromQueue(aggregator.Channel, queueName)
	if err != nil {
		aggregator.Log.Fatalf("%s '%s': %v", MSG_FAILED_CONSUME, queueName, err)
	}
	aggregator.Log.Infof("waiting for messages in queue %s ...", queueName)
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
	aggregator.DisposeStateHelpers()
}

func (aggregator *Aggregator) sendAck(msg amqp.Delivery) error {
	err := rabbitmq.SingleAck(msg)
	if err != nil {
		aggregator.Log.Fatalf("failed to ack message: %v", err)
		return err
	}
	return nil
}
