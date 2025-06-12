package window

import (
	"testing"
	"time"
)

// go test -timeout 2*MAX_AGE
func TestGetWindow(t *testing.T) {
	extend := 5
	messageWindow := NewMessageWindow()
	for i := range MAX_LENGTH_TO_CLEAN + 1 {
		messageWindow.AddMessage(int64(i))
	}
	// Expect messages presents in the window
	for i := range MAX_LENGTH_TO_CLEAN + 1 {
		if !messageWindow.IsDuplicate(int64(i)) {
			t.Errorf("Expected message %d to be a duplicate, but it was not found in the window", i)
		}
	}
	time.Sleep(MAX_AGE) // Ensure that the messages are older than MAX_AGE
	for i := MAX_LENGTH_TO_CLEAN + 1; i < MAX_LENGTH_TO_CLEAN+extend; i++ {
		messageWindow.AddMessage(int64(i))
	}
	// Expect messages to be in the window
	for i := MAX_LENGTH_TO_CLEAN + 1; i < MAX_LENGTH_TO_CLEAN+extend; i++ {
		if !messageWindow.IsDuplicate(int64(i)) {
			t.Errorf("Expected message %d to be a duplicate, but it was not found in the window", i)
		}
	}
	// Expect messages to be cleaned up
	for i := range MAX_LENGTH_TO_CLEAN {
		if messageWindow.IsDuplicate(int64(i)) {
			t.Errorf("Expected message %d to not be a duplicate, but it was found in the window", i)
		}
	}
}
