package filter

import (
	"strconv"
	"tp1/coordinator"
	"tp1/helpers/state"
	"tp1/helpers/window"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

type FilterDefaultState string

type FilterDefaultUpdateArgs struct {
	MessageId int64
	ClientId  string
}

func UpdateFilterDefault(state *FilterDefaultState, messageWindow *window.MessageWindow, updateArgs *FilterDefaultUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

type FilterTop5InvestorsState string

type FilterTop5InvestorsUpdateArgs struct {
	MessageId int64
	ClientId  string
}

func UpdateFilterTop5Investors(state *FilterTop5InvestorsState, messageWindow *window.MessageWindow, updateArgs *FilterTop5InvestorsUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

// Return the state helpers and the window
func CreateStateHelpers(config *FilterConfig, log *logging.Logger) (*state.StateHelper[FilterDefaultState, FilterDefaultUpdateArgs], *state.StateHelper[FilterTop5InvestorsState, FilterTop5InvestorsUpdateArgs], window.MessageWindow) {
	switch config.Type {
	case "top_5_investors_filter":
		stateHelper := state.NewStateHelper(strconv.Itoa(config.ID), config.Type, strconv.Itoa(config.Shards), UpdateFilterTop5Investors)
		if stateHelper == nil {
			log.Fatalf("Failed to create state helper")
		}
		_, window := state.GetLastValidState(stateHelper)
		return nil, stateHelper, window
	default:
		stateHelper := state.NewStateHelper(strconv.Itoa(config.ID), config.Type, strconv.Itoa(config.Shards), UpdateFilterDefault)
		if stateHelper == nil {
			log.Fatalf("Failed to create state helper")
		}
		_, window := state.GetLastValidState(stateHelper)
		return stateHelper, nil, window
	}
}

// Refresh the window, save the state and send the ack
func (f *Filter) SaveDefaultStateAndSendAck(msg amqp.Delivery, clientId string, messageId int64) error {
	// update window
	f.messageWindow.AddMessage(clientId, messageId)
	// save state
	err := state.SaveState(f.stateHelperDefault, "", f.messageWindow, FilterDefaultUpdateArgs{
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
func (f *Filter) SaveTop5StateAndSendAck(msg amqp.Delivery, clientId string, messageId int64) error {
	// update window
	f.messageWindow.AddMessage(clientId, messageId)
	// save state
	err := state.SaveState(f.stateHelperTop5Inv, "", f.messageWindow, FilterTop5InvestorsUpdateArgs{
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
	err := state.SaveState(f.stateHelperDefault, "", f.messageWindow, FilterDefaultUpdateArgs{
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
