package state

import (
	"time"
	"tp1/rabbitmq"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const MAX_AGE_WAIT_FOR_NOT_EOF = 1 * time.Minute
const INF_WAIT_FOR_NOT_EOF = 2 * MAX_AGE_WAIT_FOR_NOT_EOF

func InitClientsTimeStampsRcvMsg() map[string]string {
	return make(map[string]string)
}

func CheckInitTimeStampRcvMsg(lastTimestamp map[string]string, clientId string) string {
	value, found := lastTimestamp[clientId]
	if !found {
		return time.Now().Add(INF_WAIT_FOR_NOT_EOF).UTC().Format(LAYOUT_TIMESTAMP)
	} else {
		return value
	}
}

// If the eof arrives before the timeStamp, it is discarded and returns true. If not eof returns new timestamp. When not discard eof return lastTimeStamp
func CheckAndDiscardEof(msg amqp.Delivery, lastTimestamp string, isEof bool) (string, bool) {
	if isEof {
		maxTimeRcvMsg, err := time.Parse(LAYOUT_TIMESTAMP, lastTimestamp)
		logger := logging.MustGetLogger(MODULE_NAME)
		if err == nil {
			timeEof := time.Now().UTC()
			// Eof arrives before max time rcv msg
			if timeEof.Before(maxTimeRcvMsg) {
				logger.Debugf("Discard eof %v", msg.Expiration)
				rabbitmq.SingleNack(msg)
				return lastTimestamp, true
			}
			return lastTimestamp, false
		}
		logger.Debugf("Discard eof")
	}
	// update timestamp
	return time.Now().Add(MAX_AGE_WAIT_FOR_NOT_EOF).UTC().Format(LAYOUT_TIMESTAMP), false
}
