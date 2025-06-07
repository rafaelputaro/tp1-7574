package state

import (
	"os"
	"strconv"
	"testing"
	"tp1/helpers/window"
)

// go test -timeout 30m
func TestState(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	messageIds := []int{100, 102, 99}
	// Clean files
	err := os.RemoveAll(StatesDir)
	if err != nil {
		t.Errorf("Error on clean folder: %v", err)
	}
	type Data struct {
		Name string
		Id   int
	}
	stateHelper := NewStateHelper[Data](clientId, moduleName, shard)
	state, windowP := GetLastValidState(stateHelper)
	if state != nil || windowP != nil {
		t.Errorf("Error new state helper: %v", err)
	}
	// window to test
	windowData := window.NewMessageWindow()
	for message := range messageIds {
		windowData.AddMessage(int64(message))
	}
	// Insert states
	for id := range MAX_VALIDS_STATES {
		data := Data{
			Name: "Subject " + strconv.Itoa(id),
			Id:   id,
		}
		SaveState(stateHelper, data, windowData)
	}
	// Check window and state
	state, windowP = GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error insert windows or window")
	} else {
		for message := range messageIds {
			if !windowP.IsDuplicate(int64(message)) {
				t.Errorf("Error insert window")
			}
		}
		if state.Id != MAX_VALIDS_STATES-1 {
			t.Errorf("Error insert states %v", state.Id)
		}
	}
	// Exceed the maximum number of valid states
	{
		id := MAX_VALIDS_STATES
		data := Data{
			Name: "Subject " + strconv.Itoa(id),
			Id:   id,
		}
		SaveState(stateHelper, data, windowData)
	}
	// Check window and state
	state, windowP = GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error clean files: %v", err)
	} else {
		for message := range messageIds {
			if !windowP.IsDuplicate(int64(message)) {
				t.Errorf("Error insert window")
			}
		}
		if state.Id != MAX_VALIDS_STATES {
			t.Errorf("Error insert states %v", state.Id)
		}
	}
	// TODO más test con archivos en estado inválidos etcétera
}
