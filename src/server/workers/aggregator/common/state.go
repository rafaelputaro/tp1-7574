package common

import (
	"tp1/helpers/state"
	"tp1/helpers/window"
)

const DEFAULT_UNIQUE_SHARD string = ""
const MESSAGE_FAILED_TO_CREATE_STATE_HELPER string = "Failed to create state helper"

type AggregatorMovieState string

type AggregatorMovieUpdateArgs struct {
	MessageId int64
	ClientId  string
}

type AggregatorTop5State string

type AggregatorTop5UpdateArgs struct {
	MessageId int64
	ClientId  string
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
func UpdateMovie(aggregatorState *AggregatorMovieState, messageWindow *window.MessageWindow, updateArgs *AggregatorMovieUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

// Updates the aggregator status and refresh the window
func UpdateTop5(aggregatorState *AggregatorTop5State, messageWindow *window.MessageWindow, updateArgs *AggregatorTop5UpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
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
