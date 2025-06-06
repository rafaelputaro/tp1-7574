package state

import (
	"errors"
	"os"

	//"bufio"

	"github.com/op/go-logging"
)

const STATES_DIR = "states/"
const MODULE_NAME = "state"

const MSG_FAILED_TO_OPEN_STATE_FILE = "Failed to open state file: %v"
const MSG_FAILED_TO_OPEN_STATE_FILE_FOR_READING = "Failed to open state file for reading: %v"
const MSG_FAILED_TO_WRITE_STATE = "Failed to write state to file %s: %v"
const MSG_FILE_OPENED = "State file %s opened successfully for writing and reading"
const MSG_FILE_CLOSED = "State file %s closed successfully"
const MSG_NO_FILEDESC_AVAILABLE = "No file descriptor available for writing state to %s"

// StateHelper is a struct that helps manage state files for different clients and modules.
type StateHelper struct {
	filePath       string
	filedescWriter *os.File
	filedescReader *os.File
}

// generateFilePath constructs the file path for the state file based on client ID, module name, and shard.
func generateFilePath(clientId string, moduleName string, shard string) string {
	return STATES_DIR + clientId + "_" + moduleName + "_" + shard + ".ndjson"
}

// NewStateHelper creates a new StateHelper instance with the specified client ID, module name, and shard.
func NewStateHelper(clientId string, moduleName string, shard string) *StateHelper {
	logger := logging.MustGetLogger(MODULE_NAME)
	filePath := generateFilePath(clientId, moduleName, shard)
	fileWr, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		return nil
	}
	fileRd, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE_FOR_READING, err)
		return nil
	}
	logger.Debugf(MSG_FILE_OPENED, filePath)
	return &StateHelper{
		filePath:       filePath,
		filedescWriter: fileWr,
		filedescReader: fileRd,
	}
}

// Dispose closes the file descriptors used by the StateHelper.
func (stateHelper *StateHelper) Dispose() {
	logger := logging.MustGetLogger(MODULE_NAME)
	if err := stateHelper.filedescWriter.Close(); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
	}
	stateHelper.filedescWriter = nil
	if err := stateHelper.filedescReader.Close(); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
	}
	stateHelper.filedescReader = nil
	logger.Debugf(MSG_FILE_CLOSED, stateHelper.filePath)
}

func (stateHelper *StateHelper) LoadLastValidState(callback func()) error {
	//logger := logging.MustGetLogger(MODULE_NAME)
	// TODO que solamente se abra el archivo al leer el estado

	/*
	   scanner := bufio.NewScanner(file)
	   	var lines []string

	   	for scanner.Scan() {
	   		lines = append(lines, scanner.Text())
	   	}
	*/

	return nil
}

// SaveState saves the provided state to the state file in NDJSON format.
func (stateHelper *StateHelper) SaveState(state []byte) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	if stateHelper.filedescWriter == nil {
		logger.Warningf(MSG_NO_FILEDESC_AVAILABLE, stateHelper.filePath)
		return errors.New(MSG_NO_FILEDESC_AVAILABLE)
	}
	if _, err := stateHelper.filedescWriter.WriteString(string(state) + "\n"); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
		return err
	}
	return nil
}
