package window

import (
	"strconv"
	"time"
)

const LAYOUT_TIMESTAMP = "2006-01-02 15:04:05.000000000"
const MAX_LENGTH_TO_CLEAN = 1000000
const MAX_AGE = 30 * time.Minute

// MessageWindow is holds message IDs and their corresponding valid timestamps and implements a fifo queue.
type MessageWindow struct {
	Messages map[string]string // map that holds <clientId>_<message IDs> and their corresponding valid timestamps.
	Queue    []string          // fifo queue
}

// NewMessageWindow creates a new MessageWindow with a maximum length of MAX_LENGTH.
func NewMessageWindow() *MessageWindow {
	return &MessageWindow{
		Messages: make(map[string]string),
	}
}

func generateKey(clientId string, messageId int64) string {
	return clientId + "_" + strconv.FormatInt(messageId, 10)
}

// Return true if the clientId + messageId is already present in the messageWindow slice, indicating a duplicate message.
func (messageWindow *MessageWindow) IsDuplicate(clientId string, messageId int64) bool {
	_, exists := messageWindow.Messages[generateKey(clientId, messageId)]
	return exists
}

// Return true if the window is empty
func (messageWindow *MessageWindow) IsEmpty() bool {
	return len(messageWindow.Messages) == 0
}

// Adds new message to the window
func (messageWindow *MessageWindow) AddMessage(clientId string, messageId int64) {
	messageWindow.tryCleanOld()
	key := generateKey(clientId, messageId)
	messageWindow.Messages[key] = time.Now().Add(MAX_AGE).UTC().Format(LAYOUT_TIMESTAMP)
	messageWindow.Queue = append(messageWindow.Queue, key)
}

// RemoveMessage removes a message from the messageWindow by its ID.
func (messageWindow *MessageWindow) removeMessage(clientId string, messageId int64) {
	messageWindow.removeMessageByKey(generateKey(clientId, messageId))
}

// RemoveMessage removes a message from the messageWindow by its ID.
func (messageWindow *MessageWindow) removeMessageByKey(key string) {
	delete(messageWindow.Messages, key)
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
		key := messageWindow.Queue[0]
		timestamp := messageWindow.Messages[key]
		t, err := time.Parse(LAYOUT_TIMESTAMP, timestamp)
		if err != nil {
			break
		}
		now := time.Now().UTC()
		if t.Before(now) {
			messageWindow.removeMessageByKey(key)
			messageWindow.Queue = messageWindow.Queue[1:]
			continue
		}
		break
	}
}
