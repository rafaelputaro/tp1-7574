package state

import (
	"os"
	"time"

	"github.com/op/go-logging"
)

const TIMEOUT = 3 * time.Second //1
const MAX_MESSAGES = 10000      //1000
const MESSAGE_FAILED_TO_SEND_ACK string = "failed to ack message: %v"
const MESSAGE_FAILED_TO_SYNCH string = "Failed to sync with disk: %v"
const MESSAGE_SUCCESS_SYNCH string = "Successful synchronization: %s"

type SynchWriter[TAckArgs any] struct {
	fileDesc    *os.File   // File descriptor for writing to the state file
	timeToSynch string     // Expiration date
	messages    []TAckArgs // Messages waiting for ack
	buffer      string
	filePath    string
}

func NewSynchWriter[TAckArgs any](filePath string) *SynchWriter[TAckArgs] {
	logger := logging.MustGetLogger(MODULE_NAME)
	// Open the state file for appending and writing
	fileDesc, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		return nil
	}
	return &SynchWriter[TAckArgs]{
		fileDesc:    fileDesc,
		timeToSynch: generateExpirationDate(),
		messages:    make([]TAckArgs, 0),
		buffer:      "",
		filePath:    filePath,
	}
}

func (writer *SynchWriter[TAckArgs]) Dispose(sendAck func(TAckArgs) error) {
	logger := logging.MustGetLogger(MODULE_NAME)
	writer.Synch(sendAck)
	if err := writer.fileDesc.Close(); err != nil {
		logger.Errorf(MSG_FAILED_ON_CLOSE_FILE, writer.filePath, err)
	} else {
		logger.Debugf(MSG_FILE_CLOSED, writer.filePath)
		writer.fileDesc = nil
	}
}

// Write on buffer and try to synchronization with disk and try to send ack
func (writer *SynchWriter[TAckArgs]) Write(msg *TAckArgs, sendAck func(TAckArgs) error, encodedData []byte) error {
	writer.appendToBuffer(msg, encodedData)
	return writer.trySync(sendAck)
}

// Write on buffer and force synchronization with disk and the ack sending
func (writer *SynchWriter[TAckArgs]) WriteSync(msg *TAckArgs, sendAck func(TAckArgs) error, encodedData []byte) error {
	writer.appendToBuffer(msg, encodedData)
	return writer.Synch(sendAck)
}

// Force synchronization with disk
func (writer *SynchWriter[TAckArgs]) Synch(sendAck func(TAckArgs) error) error {
	return writer.writeAndSendAcks(sendAck)
}

// Append the encoded data (<json>) to the buffer and the message to the list
func (writer *SynchWriter[TAckArgs]) appendToBuffer(msg *TAckArgs, encodedData []byte) {
	if msg != nil {
		writer.messages = append(writer.messages, *msg)
	}
	writer.buffer = writer.buffer + string(encodedData) + "\n"
}

func generateExpirationDate() string {
	return time.Now().Add(TIMEOUT).UTC().Format(LAYOUT_TIMESTAMP)
}

// If it's time to sync do it
func (writer *SynchWriter[TAckArgs]) trySync(sendAck func(TAckArgs) error) error {
	lenMessages := len(writer.messages)
	if lenMessages == 0 {
		return nil
	}
	// synch by length
	if lenMessages >= MAX_MESSAGES {
		return writer.writeAndSendAcks(sendAck)
	}
	// sych by time
	t, err := time.Parse(LAYOUT_TIMESTAMP, writer.timeToSynch)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if t.Before(now) {
		return writer.writeAndSendAcks(sendAck)
	}
	return nil
}

// Write buffer on file, synch and send acks
func (writer *SynchWriter[TAckArgs]) writeAndSendAcks(sendAck func(TAckArgs) error) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	if _, err := writer.fileDesc.WriteString(string(writer.buffer)); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, writer.filePath, err)
		return err
	}
	if err := writer.fileDesc.Sync(); err != nil {
		logger.Fatalf(MESSAGE_FAILED_TO_SYNCH, err)
		return err
	}
	writer.buffer = ""
	if err := writer.sendAcks(sendAck); err != nil {
		return err
	}
	writer.timeToSynch = generateExpirationDate()
	logger.Debugf(MESSAGE_SUCCESS_SYNCH, writer.filePath)
	return nil
}

func (writer *SynchWriter[TAckArgs]) sendAcks(sendAck func(TAckArgs) error) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	for _, msg := range writer.messages {
		err := sendAck(msg)
		if err != nil {
			logger.Fatalf(MESSAGE_FAILED_TO_SEND_ACK, err)
			return err
		}
	}
	writer.messages = make([]TAckArgs, 0)
	return nil
}

// Clean a file and synch
func (writer *SynchWriter[TAckArgs]) CleanFileSynch(sendAck func(TAckArgs) error) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	// clean buffer
	writer.buffer = ""
	// send acks
	if err := writer.sendAcks(sendAck); err != nil {
		return err
	}
	// Truncate the file
	err := writer.fileDesc.Truncate(0)
	if err != nil {
		logger.Errorf(MSG_FAILED_ON_CLEAN_FILE, err)
	}
	// Sync
	if err := writer.fileDesc.Sync(); err != nil {
		logger.Fatalf(MESSAGE_FAILED_TO_SYNCH, err)
		return err
	}
	logger.Debugf(MSG_CLEAN_A_FILE, writer.filePath)
	return nil
}
