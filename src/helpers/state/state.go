package state

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"time"
	"tp1/helpers/window"

	"github.com/op/go-logging"
)

var StatesDir = initStatesDir()

const MODULE_NAME = "state"
const DEFAULT_STATES_DIR = "/tmp/states"
const STATES_DIR_ENV_VAR = "STATES_DIR"
const MAX_STATES = 100 // Maximum number of states per state file
const LAYOUT_TIMESTAMP = "2006-01-02 15:04:05.000000000"

const MSG_FAILED_TO_OPEN_STATE_FILE = "Failed to open state file: %v"
const MSG_FAILED_TO_OPEN_AUX_FILE = "Failed to open auxiliary file: %v"
const MSG_FAILED_TO_OPEN_STATE_FILE_FOR_READING = "Failed to open state file for reading: %v"
const MSG_FAILED_TO_WRITE_STATE = "Failed to write state to file %s: %v"
const MSG_FAILED_TO_READ_STATE = "Failed to read state from file %s: %v"
const MSG_FAILED_TO_CREATE_STATES_DIR = "Failed to create states directory %s: %v"
const MSG_FILE_OPENED = "State file %s opened successfully for writing and reading"
const MSG_FILE_CLOSED = "State file %s closed successfully"
const MSG_NO_FILEDESC_AVAILABLE = "No file descriptor available for writing state to file"
const MSG_ERROR_DECODING_STATE = "Error decoding state: %s"
const MSG_NO_VALID_STATE_FOUND = "No valid state found in state file"
const MSG_ERROR_ENCODING_STATE = "Error encoding state: %s"
const MSG_CLEAN_FILE = "Clean File"

// StateHelper is a struct that helps manage state files for different clients and modules.
type StateHelper[T any] struct {
	filePath       string   // Path to the state file
	auxFilePath    string   // Path to the auxiliary state file
	filedescWriter *os.File // File descriptor for writing to the state file
	countStates    int      // Counter to keep track of the number of states written to the state file
	lastValidState *CompleteState[T]
}

// CompleteState is a generic struct that holds the state of type T and a message window.
type CompleteState[T any] struct {
	State     T
	Window    window.MessageWindow
	TimeStamp string
}

// initStatesDir initializes the states directory from the environment variable or uses a default value.
func initStatesDir() string {
	statesDir := os.Getenv(STATES_DIR_ENV_VAR)
	if statesDir == "" {
		statesDir = DEFAULT_STATES_DIR // Default directory if not set
	}
	return statesDir
}

// GenerateFilePath constructs the file path for the state file based on client ID, module name, and shard.
func GenerateFilePath(clientId string, moduleName string, shard string) string {
	return StatesDir + "/" + clientId + "_" + moduleName + "_" + shard + ".ndjson"
}

// GenerateAuxFilePath constructs the file path for the auxiliary state file based on client ID, module name, and shard.
func GenerateAuxFilePath(clientId string, moduleName string, shard string) string {
	return StatesDir + "/" + clientId + "_" + moduleName + "_" + shard + "_aux.ndjson"
}

// NewStateHelper creates a new StateHelper instance with the specified client ID, module name, and shard.
func NewStateHelper[T any](clientId string, moduleName string, shard string) *StateHelper[T] {
	logger := logging.MustGetLogger(MODULE_NAME)
	filePath := GenerateFilePath(clientId, moduleName, shard)
	auxFilePath := GenerateAuxFilePath(clientId, moduleName, shard)
	// Ensure the states directory exists
	err := os.MkdirAll(StatesDir, 0755)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_CREATE_STATES_DIR, StatesDir, err)
		return nil
	}
	// Open the state file for appending and writing
	fileWr, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		return nil
	}
	state, countStates, _ := loadLastValidState[T](filePath, auxFilePath)
	return &StateHelper[T]{
		filePath:       filePath,
		auxFilePath:    auxFilePath,
		filedescWriter: fileWr,
		lastValidState: state,
		countStates:    countStates,
	}
}

// Dispose closes the file descriptors used by the StateHelper.
func (stateHelper *StateHelper[T]) Dispose() {
	logger := logging.MustGetLogger(MODULE_NAME)
	if err := stateHelper.filedescWriter.Close(); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
	}
	stateHelper.filedescWriter = nil
	logger.Debugf(MSG_FILE_CLOSED, stateHelper.filePath)
}

// GetLastValidState returns the last valid state.
func GetLastValidState[T any](stateHelper *StateHelper[T]) (*T, *window.MessageWindow) {
	if stateHelper.lastValidState != nil {
		return &stateHelper.lastValidState.State, &stateHelper.lastValidState.Window
	}
	return nil, nil
}

// Load a valid state from state file and the aux files, parallel the timestamps
func loadLastValidState[T any](filePath string, auxFilePath string) (*CompleteState[T], int, error) {
	stateFile, amountStatesFile, errFile := loadLastValidStateFromPath[T](filePath)
	stateAux, _, errAux := loadLastValidStateFromPath[T](auxFilePath)
	if errAux != nil {
		return stateFile, amountStatesFile, errFile
	}
	if errFile != nil {
		return stateAux, amountStatesFile, errAux
	}
	// Parallel both files
	timeStampFileParsed, errParseFile := time.Parse(LAYOUT_TIMESTAMP, stateFile.TimeStamp)
	timeStampAuxParsed, errParseAux := time.Parse(LAYOUT_TIMESTAMP, stateAux.TimeStamp)
	if errParseAux != nil {
		return stateFile, amountStatesFile, errParseFile
	}
	if errParseFile != nil {
		return stateAux, amountStatesFile, errParseAux
	}
	if timeStampAuxParsed.After(timeStampFileParsed) {
		return stateAux, amountStatesFile, errAux
	}
	return stateFile, amountStatesFile, errFile
}

// loadLastValidState reads the last valid state from a state file and returns it as a pointer to type T.
// It also returns the total number of lines including invalid ones.
func loadLastValidStateFromPath[T any](filePath string) (*CompleteState[T], int, error) {
	logger := logging.MustGetLogger(MODULE_NAME)
	lines, count, err := readLines[T](filePath)
	if err != nil {
		return nil, count, err
	}
	if len(lines) == 0 {
		logger.Debugf("%v: %v", MSG_NO_VALID_STATE_FOUND, filePath)
		return nil, count, errors.New(strings.ToLower(MSG_NO_VALID_STATE_FOUND))
	}
	return &lines[len(lines)-1], count, nil
}

// ReadLines reads the state file line by line and decodes each line into a slice of type T.
// It also returns the total number of lines including invalid ones.
func readLines[T any](filePath string) ([]CompleteState[T], int, error) {
	logger := logging.MustGetLogger(MODULE_NAME)
	fileRd, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE_FOR_READING, err)
		return nil, 0, err
	}
	logger.Debugf(MSG_FILE_OPENED, filePath)
	// Decode the file line by line
	var lines []CompleteState[T]
	scanner := bufio.NewScanner(fileRd)
	count := 0
	for scanner.Scan() {
		var decoded CompleteState[T]
		err := json.Unmarshal([]byte(scanner.Text()), &decoded)
		count++
		if err != nil {
			logger.Errorf(MSG_ERROR_DECODING_STATE, err)
			continue
		}
		lines = append(lines, decoded)
	}
	// Close the file after decoding
	if err := fileRd.Close(); err != nil {
		logger.Errorf(MSG_FAILED_TO_READ_STATE, filePath, err)
		return nil, count, err
	}
	return lines, count, nil
}

// SaveState encodes the provided state and message window into JSON format and writes it to the state file.
func SaveState[T any](stateHelper *StateHelper[T], state T, messageWindow window.MessageWindow) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	if stateHelper.filedescWriter == nil {
		logger.Warningf("%s: %v", MSG_NO_FILEDESC_AVAILABLE, stateHelper.filePath)
		return errors.New(strings.ToLower(MSG_NO_FILEDESC_AVAILABLE))
	}
	completeState := CompleteState[T]{
		State:     state,
		Window:    messageWindow,
		TimeStamp: time.Now().UTC().Format(LAYOUT_TIMESTAMP),
	}
	encodedState, err := json.Marshal(completeState)
	if err != nil {
		logger.Errorf(MSG_ERROR_ENCODING_STATE, err)
		return err
	}
	if _, err := stateHelper.filedescWriter.WriteString(string(encodedState) + "\n"); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
		return err
	}
	// Update state helper
	stateHelper.countStates++
	stateHelper.lastValidState = &completeState
	stateHelper.tryCleanFile()
	return nil
}

// check clean conditiones
func (stateHelper *StateHelper[T]) shouldClean() bool {
	// check count valids states
	if stateHelper.countStates > MAX_STATES {
		logger := logging.MustGetLogger(MODULE_NAME)
		logger.Debugf(MSG_CLEAN_FILE)
		return true
	}
	return false
}

// tries to clean up files in a fail-safe manner
func (stateHelper *StateHelper[T]) tryCleanFile() {
	logger := logging.MustGetLogger(MODULE_NAME)
	// check clean
	if !stateHelper.shouldClean() {
		return
	}
	// clean aux file and save las valid state
	os.Remove(stateHelper.auxFilePath)
	// Open the aux file
	fileAux, err := os.OpenFile(stateHelper.auxFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_AUX_FILE, err)
		return
	}
	defer fileAux.Close()
	// save last state on aux
	encodedState, err := json.Marshal(stateHelper.lastValidState)
	if err != nil {
		logger.Errorf(MSG_ERROR_ENCODING_STATE, err)
		return
	}
	if _, err := fileAux.WriteString(string(encodedState) + "\n"); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.auxFilePath, err)
		return
	}
	// clean original file and save las valid state
	os.Remove(stateHelper.filePath)
	// open the state file for writing from the beginning
	fileWr, err := os.Create(stateHelper.filePath)
	if err != nil {
		logger.Fatalf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		return
	}
	stateHelper.filedescWriter = fileWr
	// write the last valid state on state file
	if _, err := stateHelper.filedescWriter.WriteString(string(encodedState) + "\n"); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
		return
	}
	stateHelper.countStates = 1
}
