package common

import (
	"tp1/helpers/state"
	"tp1/helpers/window"
	"tp1/protobuf/protopb"
	"tp1/rabbitmq"
	"tp1/server/workers/aggregator/common/utils"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

const DEFAULT_UNIQUE_SHARD string = ""
const MESSAGE_FAILED_TO_CREATE_STATE_HELPER string = "Failed to create state helper"
const MESSAGE_UNABLE_TO_SAVE_STATE string = "Unable to save state"

type AckArgs struct {
	msg amqp.Delivery
}

type AggregatorMoviesState struct {
	AmountEOF map[string]int
}

type AggregatorMoviesUpdateArgs struct {
	MessageId int64
	ClientId  string
	SourceId  string
	EOF       bool
}

type AggregatorTop5State struct {
	CountriesByClient map[string]map[string]int64
}

type AggregatorTop5UpdateArgs struct {
	MessageId         int64
	ClientId          string
	SourceId          string
	ProductionCountry string
	Budget            int64
	EOF               bool
}

type AggregatorTop10StateInternal struct {
	AmountEOF  map[string]int
	ActorsData map[string](*utils.ActorsData)
}

type AggregatorTop10State struct {
	AmountEOF  map[string]int
	ActorsData map[string](utils.ActorsData)
}

type AggregatorTop10UpdateArgs struct {
	MessageId   int64
	ClientId    string
	SourceId    string
	ProfilePath string
	Name        string
	CountMovies int64
	EOF         bool
}

type TopAndBottomDB struct {
	TitleTop        string
	TitleBottom     string
	RatingAvgTop    float64
	RatingAvgBottom float64
	ClientId        string
	MessageId       int64
	SourceId        string
	Eof             bool
}

type AggregatorTopAndBottomStateInternal struct {
	AmountEOF          map[string]int
	GlobalTopAndBottom map[string](*protopb.TopAndBottomRatingAvg)
}

type AggregatorTopAndBottomState struct {
	AmountEOF          map[string]int
	GlobalTopAndBottom map[string](TopAndBottomDB)
}

type AggregatorTopAndBottomUpdateArgs struct {
	MessageId    int64
	ClientId     string
	SourceId     string
	TopAndBottom TopAndBottomDB
	Eof          bool
}

type AggregatorMetricsState struct {
	AmountEOF map[string]int
	Count     map[string]int64
	SumAvg    map[string]float64
}

type AggregatorMetricsUpdateArgs struct {
	MessageId    int64
	ClientId     string
	SourceId     string
	MovieRevenue float64
	MovieBudget  int64
	Eof          bool
}

func SendAck(args AckArgs) error {
	err := rabbitmq.SingleAck(args.msg)
	if err != nil {
		Log.Fatalf("failed to ack message: %v", err)
		return err
	}
	return nil
}

// Create a state from file or from scratch.
func (aggregator *Aggregator) CreateAggregatorMoviesState() *AggregatorMoviesState {
	aggregatorState, _ := state.GetLastValidState(aggregator.StateHelperMovies)
	if aggregatorState == nil {
		aggregatorState = &AggregatorMoviesState{
			AmountEOF: make(map[string]int),
		}
	}
	return aggregatorState
}

// Create a state from file or from scratch.
func (aggregator *Aggregator) CreateAggregatorTop5State() *AggregatorTop5State {
	aggregatorState, _ := state.GetLastValidState(aggregator.StateHelperTop5)
	if aggregatorState == nil {
		aggregatorState = &AggregatorTop5State{
			CountriesByClient: make(map[string]map[string]int64),
		}
	}
	return aggregatorState
}

// Create a state from file or from scratch.
func (aggregator *Aggregator) CreateAggregatorTop10State() *AggregatorTop10StateInternal {
	aggregatorStateDB, _ := state.GetLastValidState(aggregator.StateHelperTop10)
	if aggregatorStateDB == nil {
		return &AggregatorTop10StateInternal{
			AmountEOF:  make(map[string]int),
			ActorsData: make(map[string](*utils.ActorsData)),
		}
	} else {
		toReturn := AggregatorTop10StateInternal{
			AmountEOF:  aggregatorStateDB.AmountEOF,
			ActorsData: make(map[string](*utils.ActorsData)),
		}
		for keyDB, stateDB := range aggregatorStateDB.ActorsData {
			toReturn.ActorsData[keyDB] = &stateDB
		}
		return &toReturn
	}
}

// Create a state from file or from scratch.
func (aggregator *Aggregator) CreateAggregatorTopAndBottom() *AggregatorTopAndBottomStateInternal {
	aggregatorStateDB, _ := state.GetLastValidState(aggregator.StateHelperTopAndBottom)
	if aggregatorStateDB == nil {
		return &AggregatorTopAndBottomStateInternal{
			AmountEOF:          make(map[string]int),
			GlobalTopAndBottom: make(map[string](*protopb.TopAndBottomRatingAvg)),
		}
	} else {
		toReturn := AggregatorTopAndBottomStateInternal{
			AmountEOF:          aggregatorStateDB.AmountEOF,
			GlobalTopAndBottom: make(map[string](*protopb.TopAndBottomRatingAvg)),
		}
		for keyDB, stateDB := range aggregatorStateDB.GlobalTopAndBottom {
			toReturn.GlobalTopAndBottom[keyDB] = &protopb.TopAndBottomRatingAvg{
				TitleTop:        proto.String(stateDB.TitleTop),
				TitleBottom:     proto.String(stateDB.TitleBottom),
				RatingAvgTop:    proto.Float64(stateDB.RatingAvgTop),
				RatingAvgBottom: proto.Float64(stateDB.RatingAvgBottom),
				ClientId:        proto.String(stateDB.ClientId),
				MessageId:       proto.Int64(stateDB.MessageId),
				SourceId:        proto.String(stateDB.SourceId),
				Eof:             proto.Bool(stateDB.Eof),
			}
		}
		return &toReturn
	}
}

// Create a state from file or from scratch.
func (aggregator *Aggregator) CreateAggregatorMetricsState(negative bool) *AggregatorMetricsState {
	var aggregatorState *AggregatorMetricsState
	if negative {
		aggregatorState, _ = state.GetLastValidState(aggregator.StateHelperMetricsNeg)
	} else {
		aggregatorState, _ = state.GetLastValidState(aggregator.StateHelperMetricsPos)
	}
	if aggregatorState == nil {
		return &AggregatorMetricsState{
			AmountEOF: make(map[string]int),
			Count:     make(map[string]int64),
			SumAvg:    make(map[string]float64),
		}
	}
	return &AggregatorMetricsState{
		AmountEOF: aggregatorState.AmountEOF,
		Count:     aggregatorState.Count,
		SumAvg:    aggregatorState.SumAvg,
	}
}

// Return the state helpers and the window
func (aggregator *Aggregator) CreateStateHelperMetrics(possitive bool) (*state.StateHelper[AggregatorMetricsState, AggregatorMetricsUpdateArgs, AckArgs], *window.MessageWindow) {
	var postfix string
	if possitive {
		postfix = "possitive"
	} else {
		postfix = "negative"
	}
	stateHelper := state.NewStateHelper[AggregatorMetricsState, AggregatorMetricsUpdateArgs, AckArgs](
		aggregator.Config.ID+"_"+postfix,
		aggregator.Config.AggregatorType,
		DEFAULT_UNIQUE_SHARD,
		UpdateMetrics)
	if stateHelper == nil {
		aggregator.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return nil, nil
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	return stateHelper, &messageWindow
}

// Return the state helpers and the window
func (aggregator *Aggregator) InitStateHelperMovie() {
	stateHelper := state.NewStateHelper[AggregatorMoviesState, AggregatorMoviesUpdateArgs, AckArgs](
		aggregator.Config.ID,
		aggregator.Config.AggregatorType,
		DEFAULT_UNIQUE_SHARD,
		UpdateMovie)
	if stateHelper == nil {
		aggregator.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	aggregator.StateHelperMovies = stateHelper
	aggregator.Window = &messageWindow
}

// Return the state helpers and the window
func (aggregator *Aggregator) InitStateHelperTop5() {
	stateHelper := state.NewStateHelper[AggregatorTop5State, AggregatorTop5UpdateArgs, AckArgs](
		aggregator.Config.ID,
		aggregator.Config.AggregatorType,
		DEFAULT_UNIQUE_SHARD,
		UpdateTop5)
	if stateHelper == nil {
		aggregator.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	aggregator.StateHelperTop5 = stateHelper
	aggregator.Window = &messageWindow
}

// Return the state helpers and the window
func (aggregator *Aggregator) InitiStateHelperTop10() {
	stateHelper := state.NewStateHelper[AggregatorTop10State, AggregatorTop10UpdateArgs, AckArgs](
		aggregator.Config.ID,
		aggregator.Config.AggregatorType,
		DEFAULT_UNIQUE_SHARD,
		UpdateTop10)
	if stateHelper == nil {
		aggregator.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	aggregator.StateHelperTop10 = stateHelper
	aggregator.Window = &messageWindow
}

// Return the state helpers and the window
func (aggregator *Aggregator) InitiStateHelperTopAndBottom() {
	stateHelper := state.NewStateHelper[AggregatorTopAndBottomState, AggregatorTopAndBottomUpdateArgs, AckArgs](
		aggregator.Config.ID,
		aggregator.Config.AggregatorType,
		DEFAULT_UNIQUE_SHARD,
		UpdateTopAndBottom)
	if stateHelper == nil {
		aggregator.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	aggregator.StateHelperTopAndBottom = stateHelper
	aggregator.Window = &messageWindow
}

// Return the state helpers and the window
func (aggregator *Aggregator) InitiStateHelperMetrics() {
	// Possitive
	stateHelperPos, messageWindowPos := aggregator.CreateStateHelperMetrics(true)
	aggregator.StateHelperMetricsPos = stateHelperPos
	aggregator.Window = messageWindowPos
	// Negative
	stateHelperNeg, messageWindowNeg := aggregator.CreateStateHelperMetrics(false)
	aggregator.StateHelperMetricsNeg = stateHelperNeg
	aggregator.WindowSec = messageWindowNeg
}

// Updates the aggregator status and refresh the window
func UpdateMovie(aggregatorState *AggregatorMoviesState, messageWindow *window.MessageWindow, updateArgs *AggregatorMoviesUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
	if updateArgs.EOF {
		aggregatorState.AmountEOF[updateArgs.ClientId] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, updateArgs.ClientId, utils.InitEOFCount) + 1
	}
}

// Updates the aggregator status and refresh the window
func UpdateTop5(aggregatorState *AggregatorTop5State, messageWindow *window.MessageWindow, updateArgs *AggregatorTop5UpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
	if !updateArgs.EOF && updateArgs.Budget > 0 {
		clientID := updateArgs.ClientId
		_, found := aggregatorState.CountriesByClient[clientID]
		if !found {
			aggregatorState.CountriesByClient[clientID] = make(map[string]int64)
		}
		_, found = aggregatorState.CountriesByClient[clientID][updateArgs.ProductionCountry]
		if !found {
			aggregatorState.CountriesByClient[clientID][updateArgs.ProductionCountry] = 0
		}
		aggregatorState.CountriesByClient[clientID][updateArgs.ProductionCountry] += updateArgs.Budget
	}
}

// Updates the aggregator status and refresh the window
func UpdateTop10(aggregatorState *AggregatorTop10State, messageWindow *window.MessageWindow, updateArgs *AggregatorTop10UpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
	if updateArgs.EOF {
		aggregatorState.AmountEOF[updateArgs.ClientId] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, updateArgs.ClientId, utils.InitEOFCount) + 1
		return
	}
	if updateArgs.CountMovies > 0 {
		found, ok := (aggregatorState.ActorsData)[updateArgs.ClientId]
		if ok {
			found.DoUpdateCount(updateArgs.ProfilePath, updateArgs.Name, updateArgs.CountMovies)
		} else {
			aggregatorState.ActorsData[updateArgs.ClientId] = *utils.InitActorsData(updateArgs.ClientId)
			actorsCount := aggregatorState.ActorsData[updateArgs.ClientId]
			actorsCount.DoUpdateCount(updateArgs.ProfilePath, updateArgs.Name, updateArgs.CountMovies)
		}
	}
}

// Updates the aggregator status and refresh the window
func UpdateTopAndBottom(aggregatorState *AggregatorTopAndBottomState, messageWindow *window.MessageWindow, updateArgs *AggregatorTopAndBottomUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
	if updateArgs.Eof {
		aggregatorState.AmountEOF[updateArgs.ClientId] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, updateArgs.ClientId, utils.InitEOFCount) + 1
		return
	}
	aggregatorState.GlobalTopAndBottom[updateArgs.ClientId] = updateArgs.TopAndBottom
}

// Updates the aggregator status and refresh the window
func UpdateMetrics(aggregatorState *AggregatorMetricsState, messageWindow *window.MessageWindow, updateArgs *AggregatorMetricsUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
	if updateArgs.Eof {
		aggregatorState.AmountEOF[updateArgs.ClientId] = utils.GetOrInitKeyMap(&aggregatorState.AmountEOF, updateArgs.ClientId, utils.InitEOFCount) + 1
		return
	}
	UpdateSumAndCount(aggregatorState, updateArgs.ClientId, updateArgs.MovieRevenue, updateArgs.MovieBudget)
}

// Refresh the window, save the state and send the ack
func (aggregator *Aggregator) SaveMoviesState(aggregatorState AggregatorMoviesState, msg amqp.Delivery, clientId string, sourceId string, eof bool, messageId int64) error {
	// update window
	aggregator.Window.AddMessage(clientId, sourceId, messageId)
	// save state
	err := state.SaveState(
		aggregator.StateHelperMovies,
		aggregatorState,
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*aggregator.Window,
		AggregatorMoviesUpdateArgs{
			ClientId:  clientId,
			MessageId: messageId,
			EOF:       eof,
		},
	)
	if err != nil {
		aggregator.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}

// Refresh the window, save the state and send the ack
func (aggregator *Aggregator) SaveTop5State(aggregatorState AggregatorTop5State, msg amqp.Delivery, clientId string, sourceId string, budget int64, productionCountry string, eof bool, messageId int64) error {
	// update window
	aggregator.Window.AddMessage(clientId, sourceId, messageId)
	// save state
	err := state.SaveState(
		aggregator.StateHelperTop5,
		aggregatorState,
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*aggregator.Window,
		AggregatorTop5UpdateArgs{
			ClientId:          clientId,
			MessageId:         messageId,
			ProductionCountry: productionCountry,
			Budget:            budget,
			EOF:               eof,
		},
	)
	if err != nil {
		aggregator.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}

// Refresh the window, save the state and send the ack
func (aggregator *Aggregator) SaveTop10State(aggregatorState AggregatorTop10StateInternal, msg amqp.Delivery, clientId string, sourceId string, profilePath string, name string, countMovies int64, eof bool, messageId int64) error {
	// update window
	aggregator.Window.AddMessage(clientId, sourceId, messageId)
	// create state to save
	toSave := AggregatorTop10State{
		AmountEOF:  aggregatorState.AmountEOF,
		ActorsData: make(map[string](utils.ActorsData)),
	}
	for key, state := range aggregatorState.ActorsData {
		toSave.ActorsData[key] = *state
	}
	updateArgs := AggregatorTop10UpdateArgs{
		MessageId:   messageId,
		ClientId:    clientId,
		SourceId:    sourceId,
		ProfilePath: profilePath,
		Name:        name,
		CountMovies: countMovies,
		EOF:         eof,
	}
	// save state
	err := state.SaveState(
		aggregator.StateHelperTop10,
		toSave,
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*aggregator.Window,
		updateArgs,
	)
	if err != nil {
		aggregator.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}

// Refresh the window, save the state and send the ack
func (aggregator *Aggregator) SaveTopAndBottomState(aggregatorState AggregatorTopAndBottomStateInternal, msg amqp.Delivery, clientId string, sourceId string, reduced *protopb.TopAndBottomRatingAvg, eof bool, messageId int64) error {
	// update window
	aggregator.Window.AddMessage(clientId, sourceId, messageId)
	// parse
	topAndBottom := TopAndBottomDB{
		TitleTop:        reduced.GetTitleTop(),
		TitleBottom:     reduced.GetTitleBottom(),
		RatingAvgTop:    reduced.GetRatingAvgTop(),
		RatingAvgBottom: reduced.GetRatingAvgBottom(),
		ClientId:        reduced.GetClientId(),
		MessageId:       reduced.GetMessageId(),
		SourceId:        reduced.GetSourceId(),
		Eof:             reduced.GetEof(),
	}
	// create state to save
	toSave := AggregatorTopAndBottomState{
		AmountEOF:          aggregatorState.AmountEOF,
		GlobalTopAndBottom: make(map[string](TopAndBottomDB)),
	}
	for key, state := range aggregatorState.GlobalTopAndBottom {
		toSave.GlobalTopAndBottom[key] = TopAndBottomDB{
			TitleTop:        state.GetTitleTop(),
			TitleBottom:     state.GetTitleBottom(),
			RatingAvgTop:    state.GetRatingAvgTop(),
			RatingAvgBottom: state.GetRatingAvgBottom(),
			ClientId:        state.GetClientId(),
			MessageId:       state.GetMessageId(),
			SourceId:        state.GetSourceId(),
			Eof:             state.GetEof(),
		}
	}
	updateArgs := AggregatorTopAndBottomUpdateArgs{
		MessageId:    messageId,
		ClientId:     clientId,
		SourceId:     sourceId,
		TopAndBottom: topAndBottom,
		Eof:          eof,
	}
	// save state
	err := state.SaveState(
		aggregator.StateHelperTopAndBottom,
		toSave,
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*aggregator.Window,
		updateArgs,
	)
	if err != nil {
		aggregator.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}

// Refresh the window, save the state and send the ack
func (aggregator *Aggregator) SaveMetricsState(aggregatorState AggregatorMetricsState, msg amqp.Delivery, clientId string, sourceId string, movieRevenue float64, movieBudget int64, eof bool, messageId int64, negative bool) error {
	var aggregatorStateHelper *state.StateHelper[AggregatorMetricsState, AggregatorMetricsUpdateArgs, AckArgs]
	var messagesWindow *window.MessageWindow
	if negative {
		aggregatorStateHelper = aggregator.StateHelperMetricsNeg
		messagesWindow = aggregator.WindowSec
	} else {
		aggregatorStateHelper = aggregator.StateHelperMetricsPos
		messagesWindow = aggregator.Window
	}
	// update window
	messagesWindow.AddMessage(clientId, sourceId, messageId)
	// args
	updateArgs := AggregatorMetricsUpdateArgs{
		MessageId:    messageId,
		ClientId:     clientId,
		SourceId:     sourceId,
		MovieRevenue: movieRevenue,
		MovieBudget:  movieBudget,
		Eof:          eof,
	}
	// save state
	err := state.SaveState(
		aggregatorStateHelper,
		aggregatorState,
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*messagesWindow,
		updateArgs,
	)
	if err != nil {
		aggregator.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}

func (aggregator *Aggregator) DisposeStateHelpers() {
	if aggregator.StateHelperMovies != nil {
		aggregator.StateHelperMovies.Dispose(SendAck)
	}
	if aggregator.StateHelperTop5 != nil {
		aggregator.StateHelperTop5.Dispose(SendAck)
	}
	if aggregator.StateHelperTop10 != nil {
		aggregator.StateHelperTop10.Dispose(SendAck)
	}
	if aggregator.StateHelperTopAndBottom != nil {
		aggregator.StateHelperTopAndBottom.Dispose(SendAck)
	}
	if aggregator.StateHelperMetricsNeg != nil {
		aggregator.StateHelperMetricsNeg.Dispose(SendAck)
	}
	if aggregator.StateHelperMetricsPos != nil {
		aggregator.StateHelperMetricsPos.Dispose(SendAck)
	}
}
