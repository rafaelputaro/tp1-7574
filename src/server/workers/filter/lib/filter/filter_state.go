package filter

import (
	"strconv"
	"tp1/coordinator"
	"tp1/helpers/state"
	"tp1/helpers/window"
	"tp1/rabbitmq"

	amqp "github.com/rabbitmq/amqp091-go"
)

const MESSAGE_FAILED_TO_CREATE_STATE_HELPER string = "Failed to create state helper"

type AckArgs struct {
	msg amqp.Delivery
}

type FilterDefaultState string

type FilterDefaultUpdateArgs struct {
	MessageId int64
	ClientId  string
}

func SendAck(args AckArgs) error {
	err := rabbitmq.SingleAck(args.msg)
	if err != nil {
		//Log.Fatalf("failed to ack message: %v", err)
		return err
	}
	return nil
}

// Return the state helpers and the window
func (f *Filter) InitStateHelperDefault() {
	stateHelper := state.NewStateHelper[FilterDefaultState, FilterDefaultUpdateArgs, AckArgs](strconv.Itoa(f.config.ID), f.config.Type, strconv.Itoa(f.config.Shards), UpdateFilterDefault)
	if stateHelper == nil {
		f.log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	f.stateHelperDefault = stateHelper
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
	err := state.SaveState(
		f.stateHelperDefault,
		"",
		AckArgs{msg: msg},
		SendAck,
		*f.messageWindow, FilterDefaultUpdateArgs{
			ClientId:  clientId,
			MessageId: messageId,
		})
	if err != nil {
		f.log.Fatalf("Unable to save state")
		return err
	}
	// send ack
	return nil
	//return f.sendAck(msg)
}

// Refresh the window, save the state and send the ack
func (f *Filter) SaveDefaultStateAndSendAckCoordinator(coordinator *coordinator.EOFLeader, msg amqp.Delivery, clientId string, messageId int64) error {
	// update window
	f.messageWindow.AddMessage(clientId, messageId)
	// save state
	err := state.SaveStateNoMsg(f.stateHelperDefault, "", SendAck, *f.messageWindow, FilterDefaultUpdateArgs{
		ClientId:  clientId,
		MessageId: messageId,
	})
	if err != nil {
		f.log.Fatalf("Unable to save state")
		return err
	}
	// send ack
	msg.Ack(false)
	coordinator.SendACKs()
	return nil
}

func (f *Filter) DisposeStateHelpers() {
	if f.stateHelperDefault != nil {
		f.stateHelperDefault.Dispose(SendAck)
	}
}
