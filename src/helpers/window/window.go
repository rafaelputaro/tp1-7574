package window

import "time"

const LAYOUT_TIMESTAMP = "2006-01-02 15:04:05.000000000"
const MAX_LENGTH_TO_CLEAN = 1000
const MAX_AGE = 2 * time.Second

// MessageWindow is a map that holds message IDs and their corresponding valid timestamps.
type MessageWindow map[int64]string

// NewMessageWindow creates a new MessageWindow with a maximum length of MAX_LENGTH.
func NewMessageWindow() MessageWindow {
	return make(map[int64]string)
}

// Return true if the messageId is already present in the messageWindow slice, indicating a duplicate message.
func (messageWindow MessageWindow) IsDuplicate(messageId int64) bool {
	_, exists := messageWindow[messageId]
	return exists
}

func (messageWindow MessageWindow) IsEmpty() bool {
	return len(messageWindow) == 0
}

// Adds new message to the window
func (messageWindow MessageWindow) AddMessage(messageId int64) {
	messageWindow.tryCleanOld()
	messageWindow[messageId] = time.Now().Add(MAX_AGE).UTC().Format(LAYOUT_TIMESTAMP)
}

// RemoveMessage removes a message from the messageWindow by its ID.
func (messageWindow MessageWindow) RemoveMessage(messageId int64) {
	delete(messageWindow, messageId)
}

// tryCleanOld removes messages from the messageWindow that are older than MAX_AGE.
func (messageWindow MessageWindow) tryCleanOld() {
	if len(messageWindow) < MAX_LENGTH_TO_CLEAN {
		return
	}
	messageWindow.CleanOldByTime()
}

// cleanOld removes messages from the messageWindow that are older than MAX_AGE.
func (messageWindow MessageWindow) CleanOldByTime() {
	for id, timestamp := range messageWindow {
		t, err := time.Parse(LAYOUT_TIMESTAMP, timestamp)
		if err != nil {
			continue
		}
		now := time.Now().UTC()
		if t.Before(now) {
			messageWindow.RemoveMessage(id)
		}
	}
}
