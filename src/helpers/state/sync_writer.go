package state

import (
	"os"
	"time"

	"github.com/op/go-logging"
)

const TIMEOUT = 1 * time.Second
const MESSAGE_FAILED_TO_SEND_ACK string = "failed to ack message: %v"
const MESSAGE_FAILED_TO_SYNCH string = "Failed to sync with disk: %v"
const MESSAGE_SUCCESS_SYNCH string = "Successful synchronization"

type SynchWriter[TAckArgs any] struct {
	fileDesc    *os.File   // File descriptor for writing to the state file
	timeToSynch string     // Expiration date
	messages    []TAckArgs // Messages waiting for ack
	buffer      string
	filePath    string
}

func NewSynchWriter[TAckArgs any](fileDesc *os.File, filePath string) *SynchWriter[TAckArgs] {
	return &SynchWriter[TAckArgs]{
		fileDesc:    fileDesc,
		timeToSynch: generateExpirationDate(),
		messages:    make([]TAckArgs, 0),
		buffer:      "",
		filePath:    filePath,
	}
}

func (writer *SynchWriter[TAckArgs]) Dispose(sendAck func(TAckArgs) error) {
	writer.Synch(sendAck)
}

// Write on buffer and try to synchronization with disk and try to send ack
func (writer *SynchWriter[TAckArgs]) Write(msg TAckArgs, sendAck func(TAckArgs) error, encodedData []byte) error {
	writer.appendToBuffer(msg, encodedData)
	return writer.trySync(sendAck)
}

// Write on buffer and try to synchronization with disk and try to send ack
func (writer *SynchWriter[TAckArgs]) WriteNoMsg(sendAck func(TAckArgs) error, encodedData []byte) error {
	writer.appendToBufferNoMsg(encodedData)
	return writer.trySync(sendAck)
}

// Write on buffer and force synchronization with disk and the ack sending
func (writer *SynchWriter[TAckArgs]) WriteSync(msg TAckArgs, sendAck func(TAckArgs) error, encodedData []byte) error {
	writer.appendToBuffer(msg, encodedData)
	return writer.Synch(sendAck)
}

// Write on buffer and force synchronization with disk and the ack sending
func (writer *SynchWriter[TAckArgs]) WriteSyncNoMsg(sendAck func(TAckArgs) error, encodedData []byte) error {
	writer.appendToBufferNoMsg(encodedData)
	return writer.Synch(sendAck)
}

// Force synchronization with disk
func (writer *SynchWriter[TAckArgs]) Synch(sendAck func(TAckArgs) error) error {
	return writer.writeAndSendAcks(sendAck)
}

// Append the encoded data (<json>) to the buffer and the message to the list
func (writer *SynchWriter[TAckArgs]) appendToBuffer(msg TAckArgs, encodedData []byte) {
	writer.messages = append(writer.messages, msg)
	writer.buffer = writer.buffer + string(encodedData) + "\n"
}

// Append the encoded data (<json>) to the buffer
func (writer *SynchWriter[TAckArgs]) appendToBufferNoMsg(encodedData []byte) {
	writer.buffer = writer.buffer + string(encodedData) + "\n"
}

func generateExpirationDate() string {
	return time.Now().Add(TIMEOUT).UTC().Format(LAYOUT_TIMESTAMP)
}

// If it's time to sync do it
func (writer *SynchWriter[TAckArgs]) trySync(sendAck func(TAckArgs) error) error {
	if len(writer.messages) == 0 {
		return nil
	}
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
	logger.Debugf(MESSAGE_SUCCESS_SYNCH)
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
