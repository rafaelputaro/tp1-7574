package state

import (
	"testing"
	"tp1/helpers/window"
)

// go test -timeout 30m
func TestState(t *testing.T) {
	type Data struct {
		Data0 string
		Data1 int
	}
	stateHelper := NewStateHelper("cli-1", "testing", "0")
	data := Data{
		Data0: "test",
		Data1: 42,
	}
	window1 := window.NewMessageWindow()
	window1.AddMessage(1001)
	window1.AddMessage(100)
	SaveState(stateHelper, data, window1)
	var state *Data
	var win *window.MessageWindow
	var err error
	state, win, err = LoadLastValidState[Data](stateHelper)
	if err != nil {
		t.Errorf("Failed to load last valid state: %v", err)
		return
	} else {
		println("Loaded state:", state.Data1, win.IsDuplicate(1001))

	}

}
