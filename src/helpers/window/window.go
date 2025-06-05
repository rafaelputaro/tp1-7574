package window

import "time"

const LAYOUT_TIMESTAMP = "2006-01-02 15:04:05.000000000"
const MAX_LENGTH_TO_CLEAN = 10
const MAX_AGE = 2 * time.Minute

// MessageWindow is a map that holds message IDs and their corresponding timestamps.
type MessageWindow map[int64]string

func NewMessageWindow() MessageWindow {
	return make(map[int64]string)
}

// Return true if the messageId is already present in the messageWindow slice, indicating a duplicate message.
func (messageWindow MessageWindow) IsDuplicate(messageId int64) bool {
	_, exists := messageWindow[messageId]
	return exists
}

// NewMessageWindow creates a new MessageWindow with a maximum length of MAX_LENGTH.
func (messageWindow MessageWindow) AddMessage(messageId int64) {
	messageWindow.cleanOld()
	messageWindow[messageId] = time.Now().UTC().Format(LAYOUT_TIMESTAMP)
}

// RemoveMessage removes a message from the messageWindow by its ID.
func (messageWindow MessageWindow) RemoveMessage(messageId int64) {
	delete(messageWindow, messageId)
}

// cleanOld removes messages from the messageWindow that are older than MAX_AGE.
func (messageWindow MessageWindow) cleanOld() {
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
		elapsed := now.Sub(t)
		if elapsed > MAX_AGE {
			messageWindow.RemoveMessage(id)
		}
	}
}
