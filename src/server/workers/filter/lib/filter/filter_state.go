package filter

/*
import (
	"strconv"
	"tp1/helpers/state"
	"tp1/helpers/window"

	"github.com/op/go-logging"
)

type FilterFakeState string

type FilterFakeUpdateArgs struct {
	MessageId int64
}

func FilterFakeUpdate(state *FilterFakeState, messageWindow *window.MessageWindow, updateArgs *FilterFakeUpdateArgs) {
	messageWindow.AddMessage(updateArgs.MessageId)
}

type FilterTop5InvestorsState string

type FilterTop5InvestorsUpdateArgs struct {
	MessageId int64
}

func FilterTop5InvestorsUpdate(state *FilterTop5InvestorsState, messageWindow *window.MessageWindow, updateArgs *FilterTop5InvestorsUpdateArgs) {
	messageWindow.AddMessage(updateArgs.MessageId)
}

func CreateStateHelpers(config *FilterConfig, log *logging.Logger) (*state.StateHelper[FilterFakeState, FilterFakeUpdateArgs], *state.StateHelper[FilterTop5InvestorsState, FilterTop5InvestorsUpdateArgs]) {
	switch config.Type {
	case "top_5_investors_filter":
		stateHelper := state.NewStateHelper(strconv.Itoa(config.ID), config.Type, strconv.Itoa(config.Shards), FilterTop5InvestorsUpdate)
		if stateHelper == nil {
			log.Fatalf("Failed to create state helper")
		}
		return nil, stateHelper
	default:
		stateHelper := state.NewStateHelper(strconv.Itoa(config.ID), config.Type, strconv.Itoa(config.Shards), FilterFakeUpdate)
		if stateHelper == nil {
			log.Fatalf("Failed to create state helper")
		}
		return stateHelper, nil
	}
}
*/
