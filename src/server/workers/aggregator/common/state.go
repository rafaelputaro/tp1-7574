package common

import (
	"tp1/helpers/state"
	"tp1/helpers/window"

	amqp "github.com/rabbitmq/amqp091-go"
)

const DEFAULT_UNIQUE_SHARD string = ""
const MESSAGE_FAILED_TO_CREATE_STATE_HELPER string = "Failed to create state helper"
const MESSAGE_UNABLE_TO_SAVE_STATE string = "Unable to save state"

type AggregatorMoviesState struct {
	AmountEOF map[string]int
}

type AggregatorMoviesUpdateArgs struct {
	MessageId int64
	ClientId  string
	EOF       bool
}

type AggregatorTop5State struct {
	CountriesByClient map[string]map[string]int64
}

type AggregatorTop5UpdateArgs struct {
	MessageId         int64
	ClientId          string
	ProductionCountry string
	Budget            int64
	EOF               bool
}

type AggregatorTop10State string

type AggregatorTop10UpdateArgs struct {
	MessageId int64
	ClientId  string
}

type AggregatorTopAndBottomState string

type AggregatorTopAndBottomUpdateArgs struct {
	MessageId int64
	ClientId  string
}

type AggregatorMetricsState string

type AggregatorMetricsUpdateArgs struct {
	MessageId int64
	ClientId  string
}

// Create a state from file or from scratch. Also return the window
func (aggregator *Aggregator) CreateAggregatorMoviesState() *AggregatorMoviesState {
	aggregatorState, _ := state.GetLastValidState(aggregator.StateHelperMovies)
	if aggregatorState == nil {
		aggregatorState = &AggregatorMoviesState{
			AmountEOF: make(map[string]int),
		}
	}
	return aggregatorState
}

// Create a state from file or from scratch. Also return the window
func (aggregator *Aggregator) CreateAggregatorTop5State() *AggregatorTop5State {
	aggregatorState, _ := state.GetLastValidState(aggregator.StateHelperTop5)
	if aggregatorState == nil {
		aggregatorState = &AggregatorTop5State{
			CountriesByClient: make(map[string]map[string]int64),
		}
	}
	return aggregatorState
}

// Return the state helpers and the window
func (aggregator *Aggregator) InitStateHelperMovie() {
	stateHelper := state.NewStateHelper(
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
	stateHelper := state.NewStateHelper(
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
	stateHelper := state.NewStateHelper(
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
	stateHelper := state.NewStateHelper(
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
	stateHelper := state.NewStateHelper(
		aggregator.Config.ID,
		aggregator.Config.AggregatorType,
		DEFAULT_UNIQUE_SHARD,
		UpdateMetrics)
	if stateHelper == nil {
		aggregator.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	aggregator.StateHelperMetrics = stateHelper
	aggregator.Window = &messageWindow
}

// Updates the aggregator status and refresh the window
func UpdateMovie(aggregatorState *AggregatorMoviesState, messageWindow *window.MessageWindow, updateArgs *AggregatorMoviesUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
	if updateArgs.EOF {
		aggregatorState.AmountEOF[updateArgs.ClientId]++
	}
}

// Updates the aggregator status and refresh the window
func UpdateTop5(aggregatorState *AggregatorTop5State, messageWindow *window.MessageWindow, updateArgs *AggregatorTop5UpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
	if !updateArgs.EOF && updateArgs.Budget > 0 {
		clientID := updateArgs.ClientId
		_, found := aggregatorState.CountriesByClient[clientID]
		if !found {
			aggregatorState.CountriesByClient[clientID] = make(map[string]int64)
		}
		countryForClient := aggregatorState.CountriesByClient[clientID]
		_, found = countryForClient[updateArgs.ProductionCountry]
		if !found {
			countryForClient[updateArgs.ProductionCountry] = 0
		}
		countryForClient[updateArgs.ProductionCountry] += updateArgs.Budget
	}
}

// Updates the aggregator status and refresh the window
func UpdateTop10(aggregatorState *AggregatorTop10State, messageWindow *window.MessageWindow, updateArgs *AggregatorTop10UpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

// Updates the aggregator status and refresh the window
func UpdateTopAndBottom(aggregatorState *AggregatorTopAndBottomState, messageWindow *window.MessageWindow, updateArgs *AggregatorTopAndBottomUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

// Updates the aggregator status and refresh the window
func UpdateMetrics(aggregatorState *AggregatorMetricsState, messageWindow *window.MessageWindow, updateArgs *AggregatorMetricsUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

// Refresh the window, save the state and send the ack
func (aggregator *Aggregator) SaveMoviesStateAndSendAck(aggregatorState AggregatorMoviesState, msg amqp.Delivery, clientId string, eof bool, messageId int64) error {
	// update window
	aggregator.Window.AddMessage(clientId, messageId)
	// save state
	err := state.SaveState(
		aggregator.StateHelperMovies,
		aggregatorState,
		msg,
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
	// send ack
	//return aggregator.sendAck(msg)
	return nil
}

// Refresh the window, save the state and send the ack
func (aggregator *Aggregator) SaveTop5StateAndSendAck(aggregatorState AggregatorTop5State, msg amqp.Delivery, clientId string, budget int64, productionCountry string, eof bool, messageId int64) error {
	// update window
	aggregator.Window.AddMessage(clientId, messageId)
	// save state
	err := state.SaveState(
		aggregator.StateHelperTop5,
		aggregatorState,
		msg,
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
	// send ack
	//return aggregator.sendAck(msg)
	return nil
}

func (aggregator *Aggregator) DisposeStateHelpers() {
	if aggregator.StateHelperMovies != nil {
		aggregator.StateHelperMovies.Dispose()
	}
	if aggregator.StateHelperTop5 != nil {
		aggregator.StateHelperTop5.Dispose()
	}
	if aggregator.StateHelperTop10 != nil {
		aggregator.StateHelperTop10.Dispose()
	}
	if aggregator.StateHelperTopAndBottom != nil {
		aggregator.StateHelperTopAndBottom.Dispose()
	}
	if aggregator.StateHelperMetrics != nil {
		aggregator.StateHelperMetrics.Dispose()
	}
}
