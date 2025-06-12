package window

import (
	"time"
)

const LAYOUT_TIMESTAMP = "2006-01-02 15:04:05.000000000"
const MAX_LENGTH_TO_CLEAN = 1000
const MAX_AGE = 2 * time.Second

// MessageWindow is holds message IDs and their corresponding valid timestamps and implements a fifo queue.
type MessageWindow struct {
	Messages map[int64]string // map that holds message IDs and their corresponding valid timestamps.
	Queue    []int64          // fifo queue
}

// NewMessageWindow creates a new MessageWindow with a maximum length of MAX_LENGTH.
func NewMessageWindow() *MessageWindow {
	return &MessageWindow{
		Messages: make(map[int64]string),
	}
}

// Return true if the messageId is already present in the messageWindow slice, indicating a duplicate message.
func (messageWindow *MessageWindow) IsDuplicate(messageId int64) bool {
	_, exists := messageWindow.Messages[messageId]
	return exists
}

// Return true if the window is empty
func (messageWindow *MessageWindow) IsEmpty() bool {
	return len(messageWindow.Messages) == 0
}

// Adds new message to the window
func (messageWindow *MessageWindow) AddMessage(messageId int64) {
	messageWindow.tryCleanOld()
	messageWindow.Messages[messageId] = time.Now().Add(MAX_AGE).UTC().Format(LAYOUT_TIMESTAMP)
	messageWindow.Queue = append(messageWindow.Queue, messageId)
}

// RemoveMessage removes a message from the messageWindow by its ID.
func (messageWindow *MessageWindow) removeMessage(messageId int64) {
	delete(messageWindow.Messages, messageId)
}

// tryCleanOld removes messages from the messageWindow that are older than MAX_AGE.
func (messageWindow MessageWindow) tryCleanOld() {
	if len(messageWindow.Messages) < MAX_LENGTH_TO_CLEAN {
		return
	}
	messageWindow.CleanOldByTime()
}

// cleanOld removes messages from the messageWindow that are older than MAX_AGE.
func (messageWindow *MessageWindow) CleanOldByTime() {
	for len(messageWindow.Queue) > 0 {
		id := messageWindow.Queue[0]
		timestamp := messageWindow.Messages[id]
		t, err := time.Parse(LAYOUT_TIMESTAMP, timestamp)
		if err != nil {
			break
		}
		now := time.Now().UTC()
		if t.Before(now) {
			messageWindow.removeMessage(id)
			messageWindow.Queue = messageWindow.Queue[1:]
			continue
		}
		break
	}
}
