package filter

import (
	"strconv"
	"tp1/coordinator"
	"tp1/helpers/state"
	"tp1/helpers/window"

	amqp "github.com/rabbitmq/amqp091-go"
)

const MESSAGE_FAILED_TO_CREATE_STATE_HELPER string = "Failed to create state helper"

type FilterDefaultState string

type FilterDefaultUpdateArgs struct {
	MessageId int64
	ClientId  string
}

type FilterTop5InvestorsState struct {
	CountryBudget map[string]int64
}

type FilterTop5InvestorsUpdateArgs struct {
	MessageId           int64
	ClientId            string
	ProductionCountries []string
	Budget              int64
}

// Return the state helpers and the window
func (f *Filter) InitStateHelperDefault() {
	stateHelper := state.NewStateHelper(strconv.Itoa(f.config.ID), f.config.Type, strconv.Itoa(f.config.Shards), UpdateFilterDefault)
	if stateHelper == nil {
		f.log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	f.stateHelperDefault = stateHelper
	f.messageWindow = &messageWindow
}

// Return the state helpers and the window
func (f *Filter) InitStateHelpersTop5Investors() {
	stateHelper := state.NewStateHelper(strconv.Itoa(f.config.ID), f.config.Type, strconv.Itoa(f.config.Shards), UpdateFilterTop5Investors)
	if stateHelper == nil {
		f.log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	f.stateHelperTop5Inv = stateHelper
	f.messageWindow = &messageWindow
}

// Refresh the window
func UpdateFilterDefault(filterState *FilterDefaultState, messageWindow *window.MessageWindow, updateArgs *FilterDefaultUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

// Refresh the window, save the state and send the ack
func (f *Filter) SaveDefaultStateAndSendAck(msg amqp.Delivery, clientId string, messageId int64) error {
	// update window
	f.messageWindow.AddMessage(clientId, messageId)
	// save state
	err := state.SaveState(f.stateHelperDefault, "", *f.messageWindow, FilterDefaultUpdateArgs{
		ClientId:  clientId,
		MessageId: messageId,
	})
	if err != nil {
		f.log.Fatalf("Unable to save state")
		return err
	}
	// send ack
	return f.sendAck(msg)
}

// Refresh the window, save the state and send the ack
func (f *Filter) SaveDefaultStateAndSendAckCoordinator(coordinator *coordinator.EOFLeader, msg amqp.Delivery, clientId string, messageId int64) error {
	// update window
	f.messageWindow.AddMessage(clientId, messageId)
	// save state
	err := state.SaveState(f.stateHelperDefault, "", *f.messageWindow, FilterDefaultUpdateArgs{
		ClientId:  clientId,
		MessageId: messageId,
	})
	if err != nil {
		f.log.Fatalf("Unable to save state")
		return err
	}
	// send ack
	coordinator.SendACKs()
	return nil
}

// Return new filter state
func NewFilterTop5InvestorsState() *FilterTop5InvestorsState {
	return &FilterTop5InvestorsState{
		CountryBudget: make(map[string]int64),
	}
}

// Updates the filter status and refresh the window
func UpdateFilterTop5Investors(filterState *FilterTop5InvestorsState, messageWindow *window.MessageWindow, updateArgs *FilterTop5InvestorsUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
	updateCountryBudget(&filterState.CountryBudget, updateArgs.ProductionCountries, updateArgs.Budget)
}

// Refresh the window, save the state and send the ack
func (f *Filter) SaveTop5StateAndSendAck(stateTop5 FilterTop5InvestorsState, msg amqp.Delivery, clientId string, messageId int64, productionCountries []string, budget int64) error {
	// update window
	f.messageWindow.AddMessage(clientId, messageId)
	// save state
	err := state.SaveState(f.stateHelperTop5Inv, stateTop5, *f.messageWindow, FilterTop5InvestorsUpdateArgs{
		ClientId:            clientId,
		MessageId:           messageId,
		ProductionCountries: productionCountries,
		Budget:              budget,
	})
	if err != nil {
		f.log.Fatalf("Unable to save state")
		return err
	}
	// send ack
	return f.sendAck(msg)
}

// Refresh the window, save the state and send the ack
func (f *Filter) SaveTop5DummyStateAndSendAck(stateTop5 FilterTop5InvestorsState, msg amqp.Delivery, clientId string, messageId int64) error {
	dummyCountrys := make([]string, 0)
	return f.SaveTop5StateAndSendAck(stateTop5, msg, clientId, messageId, dummyCountrys, 0)
}

func (f *Filter) DisposeStateHelpers() {
	if f.stateHelperDefault != nil {
		f.stateHelperDefault.Dispose()
	}
	if f.stateHelperTop5Inv != nil {
		f.stateHelperTop5Inv.Dispose()
	}
}
