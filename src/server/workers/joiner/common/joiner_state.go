package common

import (
	"tp1/helpers/state"
	"tp1/helpers/window"
	"tp1/rabbitmq"
	"tp1/server/workers/joiner/common/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

const MESSAGE_FAILED_TO_CREATE_STATE_HELPER string = "Failed to create state helper"
const MESSAGE_UNABLE_TO_SAVE_STATE string = "Unable to save state"

type AckArgs struct {
	msg amqp.Delivery
}

type JoinerMoviesState string

type JoinerMoviesUpdateArgs struct {
	MessageId int64
	ClientId  string
	SourceId  string
	EOF       bool
}

type ClientStatesCredits struct {
	ClientStates map[string]*utils.ClientStateCredits
}

type JoinerCreditsState struct {
	ClientStates map[string]utils.ClientStateCredits
}

type JoinerCreditsUpdateArgs struct {
}

/*
	type ClientStatesRatings struct {
		ClientStates map[string]*utils.ClientStateRatings
	}
*/
type JoinerRatingsState struct {
	ClientStates map[string]utils.ClientStateCredits
}

type JoinerRatingsUpdateArgs struct {
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
func (joiner *Joiner) CreateJoinerCreditsState() *ClientStatesCredits {
	joinerStateDB, _ := state.GetLastValidState(joiner.StateHelperCredits)
	var joinerState = &ClientStatesCredits{
		ClientStates: make(map[string]*utils.ClientStateCredits),
	}
	if joinerStateDB != nil {
		for keyDB, state := range joinerStateDB.ClientStates {
			joinerState.ClientStates[keyDB] = &state
		}
	}
	return joinerState
}

// Return the state helpers and the window
func (joiner *Joiner) InitStateHelperMovies(exchange string) {
	stateHelper := state.NewStateHelper[JoinerMoviesState, JoinerMoviesUpdateArgs, AckArgs](
		joiner.Config.ID,
		joiner.Config.JoinerType+"_movies",
		exchange,
		UpdateMovie)
	if stateHelper == nil {
		joiner.Log.Fatalf(MESSAGE_FAILED_TO_CREATE_STATE_HELPER)
		return
	}
	_, messageWindow := state.GetLastValidState(stateHelper)
	joiner.StateHelperMovies = stateHelper
	joiner.WindowMovies = &messageWindow
}

// Updates the joiner status and refresh the window
func UpdateMovie(joinerState *JoinerMoviesState, messageWindow *window.MessageWindow, updateArgs *JoinerMoviesUpdateArgs) {
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.SourceId, updateArgs.MessageId)
}

// Refresh the window, save the state and send the ack
func (joiner *Joiner) SaveMoviesState(msg amqp.Delivery, clientId string, sourceId string, eof bool, messageId int64) error {
	// update window
	joiner.WindowMovies.AddMessage(clientId, sourceId, messageId)
	// save state
	err := state.SaveState(
		joiner.StateHelperMovies,
		"",
		&AckArgs{
			msg: msg,
		},
		SendAck,
		*joiner.WindowMovies,
		JoinerMoviesUpdateArgs{
			ClientId:  clientId,
			MessageId: messageId,
			EOF:       eof,
		},
	)
	if err != nil {
		joiner.Log.Fatalf(MESSAGE_UNABLE_TO_SAVE_STATE)
		return err
	}
	return nil
}
