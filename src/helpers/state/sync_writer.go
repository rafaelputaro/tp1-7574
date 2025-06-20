package state

import (
	"encoding/json"
	"os"
	"time"
	"tp1/rabbitmq"

	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

const TIMEOUT = 1 * time.Second
const MESSAGE_FAILED_TO_SEND_ACK string = "failed to ack message: %v"
const MESSAGE_FAILED_TO_SYNCH string = "Failed to sync with disk: %v"
const MESSAGE_SUCCESS_SYNCH string = "Successful synchronization"

type SynchWriter struct {
	fileDesc    *os.File        // File descriptor for writing to the state file
	timeToSynch string          // Expiration date
	messages    []amqp.Delivery // Messages waiting for ack
	buffer      string
	filePath    string
}

func NewSynchWriter(fileDesc *os.File, filePath string) *SynchWriter {
	return &SynchWriter{
		fileDesc:    fileDesc,
		timeToSynch: generateExpirationDate(),
		messages:    make([]amqp.Delivery, 0),
		buffer:      "",
		filePath:    filePath,
	}
}

// Write on buffer and try to synchronization with disk and try to send ack
func (writer *SynchWriter) Write(msg amqp.Delivery, data string) {
	writer.appendToBuffer(msg, data)
	writer.trySync()
}

// Write on buffer and force synchronization with disk and the ack sending
func (writer *SynchWriter) WriteSync(msg amqp.Delivery, data string) {
	writer.appendToBuffer(msg, data)
	writer.Sync()
}

// Force synchronization with disk
func (writer *SynchWriter) Sync() error {
	return writer.writeAndSendAcks()
}

// Append the data encoded to the buffer and the message to the list
func (writer *SynchWriter) appendToBuffer(msg amqp.Delivery, data string) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	encoded, err := json.Marshal(data)
	if err != nil {
		logger.Errorf(MSG_ERROR_ENCODING_STATE, err)
		return err
	}
	writer.messages = append(writer.messages, msg)
	writer.buffer = writer.buffer + string(encoded) + "\n"
	return nil
}

func generateExpirationDate() string {
	return time.Now().Add(TIMEOUT).UTC().Format(LAYOUT_TIMESTAMP)
}

// If it's time to sync do it
func (writer *SynchWriter) trySync() {
	if len(writer.messages) == 0 {
		return
	}
	t, err := time.Parse(LAYOUT_TIMESTAMP, writer.timeToSynch)
	if err != nil {
		return
	}
	now := time.Now().UTC()
	if t.Before(now) {
		writer.writeAndSendAcks()
	}
}

// Write buffer on file, synch and send acks
func (writer *SynchWriter) writeAndSendAcks() error {
	logger := logging.MustGetLogger(MODULE_NAME)
	if _, err := writer.fileDesc.WriteString(string(writer.buffer)); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, writer.filePath, err)
		return err
	}
	if err := writer.fileDesc.Sync(); err != nil {
		logger.Fatalf(MESSAGE_FAILED_TO_SYNCH, err)
		return err
	}
	for _, msg := range writer.messages {
		err := rabbitmq.SingleAck(msg)
		if err != nil {
			logger.Fatalf(MESSAGE_FAILED_TO_SEND_ACK, err)
			return err
		}
	}
	writer.messages = make([]amqp.Delivery, 0)
	writer.buffer = ""
	writer.timeToSynch = generateExpirationDate()
	logger.Debugf(MESSAGE_SUCCESS_SYNCH)
	return nil
}
