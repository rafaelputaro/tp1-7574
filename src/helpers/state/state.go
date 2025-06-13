package state

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
	"tp1/helpers/window"

	"github.com/op/go-logging"
)

var StatesDir = initStatesDir()
var CleanOnStart = initCleanOnStart()

const MODULE_NAME = "state"
const DEFAULT_STATES_DIR = "/tmp/states"
const STATES_DIR_ENV_VAR = "STATES_DIR"
const CLEAN_ON_START_ENV_VAR = "CLEAN_ON_START"
const MAX_STATES = 10           //2000         // Maximum number of states per state file
const MAX_AGE = 5 * time.Minute // Discard files from previous runs
const LAYOUT_TIMESTAMP = "2006-01-02 15:04:05.000000000"

const MSG_FAILED_TO_OPEN_STATE_FILE = "Failed to open state file: %v"
const MSG_FAILED_TO_OPEN_AUX_FILE = "Failed to open auxiliary file: %v"
const MSG_FAILED_TO_OPEN_STATE_FILE_FOR_READING = "Failed to open state file for reading: %v"
const MSG_FAILED_TO_WRITE_STATE = "Failed to write state to file %s: %v"
const MSG_FAILED_TO_WRITE_UPDATE_ARGS = "Failed to write update arguments to file %s: %v"
const MSG_FAILED_TO_READ_STATE = "Failed to read state from file %s: %v"
const MSG_FAILED_TO_CREATE_STATES_DIR = "Failed to create states directory %s: %v"
const MSG_FILE_OPENED = "State file %s opened successfully for writing and reading"
const MSG_FILE_OPENED_FOR_READING = "State file %s opened successfully for reading"
const MSG_FILE_CLOSED = "State file %s closed successfully"
const MSG_FAILED_ON_CLOSE_FILE = "Failed on close file %s: %v"
const MSG_FAILED_ON_REMOVE_FILE = "Fail on remove file %v"
const MSG_FAILED_ON_CLEAN_FILE = "Fail on clean file %v"
const MSG_DISPOSE = "[Dispose] State Helper successfully for files: %s and %s "
const MSG_NO_FILEDESC_AVAILABLE = "No file descriptor available for writing state to file"
const MSG_ERROR_DECODING_STATE = "Error decoding state: %s"
const MSG_NO_VALID_STATE_FOUND = "No valid state found in state file"
const MSG_NO_FIRST_VALID_STATE_FOUND = "No first valid state found in state file"
const MSG_ERROR_ENCODING_STATE = "Error encoding state: %s"
const MSG_ERROR_ENCODING_UPDATE_ARGS = "Error encoding update arguments: %s"
const MSG_CLEAN_A_FILE = "Clean file: %v"
const MSG_CLEAN_FILES_ON_SAVE = "Clean files on save state"
const MSG_CLEAN_FILES_ON_START = "Clean files on start"

// TODO buscar una alternativa para scanner porque no lee más de cierto tamaño

// StateHelper is a struct that helps manage state files for different clients and modules.
type StateHelper[TState any, TUpdateArgs any] struct {
	filePath               string   // Path to the state file
	auxFilePath            string   // Path to the auxiliary state file
	fileDescStateWriter    *os.File // File descriptor for writing to the state file
	auxFileDescStateWriter *os.File // File descriptor for writing to the auxiliary file
	countStates            int      // Counter to keep track of the number of states written to the state file
	lastValidState         *DataToSave[TState, TUpdateArgs]
}

// DataToSave is a generic struct that holds the state of type T and a message window or holds a sequece of update args
type DataToSave[TState any, TUpdateArgs any] struct {
	State           TState
	Window          window.MessageWindow
	TimeStamp       string
	UpdateArgs      TUpdateArgs
	IsCompleteState bool
}

// initStatesDir initializes the states directory from the environment variable or uses a default value.
func initStatesDir() string {
	statesDir := os.Getenv(STATES_DIR_ENV_VAR)
	if statesDir == "" {
		statesDir = DEFAULT_STATES_DIR // Default directory if not set
	}
	return statesDir
}

// initializes the variable CleanOnStart from the environment variable or uses a default value.
func initCleanOnStart() bool {
	env := os.Getenv(CLEAN_ON_START_ENV_VAR)
	value, err := strconv.ParseBool(env)
	if err == nil {
		return value
	}
	return true
}

// if CleanOnStart true delete all files
func tryCleanFilesOnStart(filePath string, auxFilePath string) {
	if CleanOnStart {
		logger := logging.MustGetLogger(MODULE_NAME)
		logger.Debugf(MSG_CLEAN_FILES_ON_START)
		// open file state
		fileState, err := os.OpenFile(filePath, os.O_RDWR, 0666)
		if err != nil {
			logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		}
		defer fileState.Close()
		// Truncate the file
		cleanFile(fileState, filePath)

		// open file aux
		fileAux, err := os.OpenFile(auxFilePath, os.O_RDWR, 0666)
		if err != nil {
			logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		}
		defer fileAux.Close()
		// Truncate the file
		cleanFile(fileAux, auxFilePath)
	}
}

// Clean a file
func cleanFile(fileDesc *os.File, filePath string) {
	logger := logging.MustGetLogger(MODULE_NAME)
	// Truncate the file
	err := fileDesc.Truncate(0)
	if err != nil {
		logger.Errorf(MSG_FAILED_ON_CLEAN_FILE, err)
	}
	logger.Debugf(MSG_CLEAN_A_FILE, filePath)
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
func NewStateHelper[TState any, TUpdateArgs any](clientId string, moduleName string, shard string, updateState func(state *TState, messageWindow *window.MessageWindow, updateArgs *TUpdateArgs)) *StateHelper[TState, TUpdateArgs] {
	logger := logging.MustGetLogger(MODULE_NAME)
	filePath := GenerateFilePath(clientId, moduleName, shard)
	auxFilePath := GenerateAuxFilePath(clientId, moduleName, shard)
	// Ensure the states directory exists
	err := os.MkdirAll(StatesDir, 0755)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_CREATE_STATES_DIR, StatesDir, err)
		return nil
	}
	// ¿Clean on Start?
	tryCleanFilesOnStart(filePath, auxFilePath)
	// Reload from files
	state, countStates, _ := loadLastValidState(filePath, auxFilePath, updateState)
	// Open the state file for appending and writing
	fileStateWr, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		return nil
	}
	// Open the auxiliary state file for appending and writing
	auxFileWr, err := os.OpenFile(auxFilePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		return nil
	}
	return &StateHelper[TState, TUpdateArgs]{
		filePath:               filePath,
		auxFilePath:            auxFilePath,
		fileDescStateWriter:    fileStateWr,
		auxFileDescStateWriter: auxFileWr,
		lastValidState:         state,
		countStates:            countStates,
	}
}

// Dispose closes the file descriptors used by the StateHelper.
func (stateHelper *StateHelper[TState, TUpdateArgs]) Dispose() {
	logger := logging.MustGetLogger(MODULE_NAME)
	if err := stateHelper.fileDescStateWriter.Close(); err != nil {
		logger.Errorf(MSG_FAILED_ON_CLOSE_FILE, stateHelper.filePath, err)
	} else {
		logger.Debugf(MSG_FILE_CLOSED, stateHelper.filePath)
		stateHelper.fileDescStateWriter = nil
	}
	if err := stateHelper.auxFileDescStateWriter.Close(); err != nil {
		logger.Errorf(MSG_FAILED_ON_CLOSE_FILE, stateHelper.auxFilePath, err)
	} else {
		logger.Debugf(MSG_FILE_CLOSED, stateHelper.auxFilePath)
		stateHelper.auxFileDescStateWriter = nil
	}
	logger.Debugf(MSG_DISPOSE, stateHelper.filePath, stateHelper.auxFilePath)
	stateHelper.countStates = 0
	stateHelper.filePath = ""
	stateHelper.auxFilePath = ""
	stateHelper.lastValidState = nil
}

// GetLastValidState returns the last valid state. If there is no valid state, an empty window is returned.
func GetLastValidState[TState any, TUpdateArgs any](stateHelper *StateHelper[TState, TUpdateArgs]) (*TState, window.MessageWindow) {
	if stateHelper.lastValidState != nil {
		return &stateHelper.lastValidState.State, stateHelper.lastValidState.Window
	}
	return nil, *window.NewMessageWindow()
}

// Load a valid state from state file and the aux files, parallel the timestamps
func loadLastValidState[TState any, TUpdateArgs any](filePath string, auxFilePath string, updateState func(state *TState, messageWindow *window.MessageWindow, updateArgs *TUpdateArgs)) (*DataToSave[TState, TUpdateArgs], int, error) {
	stateFile, amountStatesFile, errFile := loadLastValidStateFromPath(filePath, updateState)
	stateAux, _, errAux := loadLastValidStateFromPath(auxFilePath, updateState)
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
func loadLastValidStateFromPath[TState any, TUpdateArgs any](filePath string, updateState func(state *TState, messageWindow *window.MessageWindow, updateArgs *TUpdateArgs)) (*DataToSave[TState, TUpdateArgs], int, error) {
	logger := logging.MustGetLogger(MODULE_NAME)
	lines, count, err := readLines[TState, TUpdateArgs](filePath)
	if err != nil {
		return nil, count, err
	}
	if len(lines) == 0 {
		logger.Debugf("%v: %v", MSG_NO_VALID_STATE_FOUND, filePath)
		return nil, count, errors.New(strings.ToLower(MSG_NO_VALID_STATE_FOUND))
	}
	lastValidStateData, err := processLines(lines, updateState)
	return lastValidStateData, count, err
}

// reconstructs the last valid state from the file
func processLines[TState any, TUpdateArgs any](lines []DataToSave[TState, TUpdateArgs], updateState func(state *TState, messageWindow *window.MessageWindow, updateArgs *TUpdateArgs)) (*DataToSave[TState, TUpdateArgs], error) {
	// first valid state
	firstValidStateIndex := -1
	for index, line := range lines {
		if line.IsCompleteState {
			firstValidStateIndex = index
			break
		}
	}
	if firstValidStateIndex < 0 {
		return nil, errors.New(strings.ToLower(MSG_NO_FIRST_VALID_STATE_FOUND))
	}
	// state reconstruction
	validState := lines[firstValidStateIndex]
	for index := firstValidStateIndex + 1; index < len(lines); index++ {
		// if new state take this
		if lines[index].IsCompleteState {
			validState = lines[index]
			continue
		}
		updateState(&validState.State, &validState.Window, &lines[index].UpdateArgs)
		validState.TimeStamp = lines[index].TimeStamp
	}
	return &validState, nil
}

// ReadLines reads the state file line by line and decodes each line into a slice of type T.
// It also returns the total number of lines including invalid ones.
func readLines[TState any, TUpdateArgs any](filePath string) ([]DataToSave[TState, TUpdateArgs], int, error) {
	logger := logging.MustGetLogger(MODULE_NAME)
	fileRd, err := os.Open(filePath) //os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE_FOR_READING, err)
		return nil, 0, err
	}
	defer fileRd.Close()
	logger.Debugf(MSG_FILE_OPENED_FOR_READING, filePath)
	// Decode the file line by line
	var lines []DataToSave[TState, TUpdateArgs]
	scanner := bufio.NewScanner(fileRd)
	if err := scanner.Err(); err != nil {
		logger.Fatalf("Error while reading file: %s", err)
	}
	count := 0
	for scanner.Scan() {
		//println("Escaneo algo")
		var decoded DataToSave[TState, TUpdateArgs]
		err := json.Unmarshal([]byte(scanner.Text()), &decoded)
		count++
		if err != nil {
			logger.Errorf(MSG_ERROR_DECODING_STATE, err)
			continue
		}
		lines = append(lines, decoded)
	}

	return lines, count, nil
}

// SaveState encodes the provided state and message window into JSON format and writes it to the state file.
func SaveState[TState any, TUpdateArgs any](stateHelper *StateHelper[TState, TUpdateArgs], state TState, messageWindow window.MessageWindow, updateArgs TUpdateArgs) error {
	// Save Complete state
	tryToSaveCompleteStateOnStateNull(stateHelper, state, messageWindow)
	// Save operation
	err := tryToSaveUpdateArgs(stateHelper, state, messageWindow, updateArgs)
	if err != nil {
		return err
	}
	// Try to clean
	stateHelper.tryCleanFile()
	return nil
}

// when the last valid state is null
func tryToSaveCompleteStateOnStateNull[TState any, TUpdateArgs any](stateHelper *StateHelper[TState, TUpdateArgs], state TState, messageWindow window.MessageWindow) error {
	if stateHelper.lastValidState == nil {
		logger := logging.MustGetLogger(MODULE_NAME)
		// Update state helper
		completeState := DataToSave[TState, TUpdateArgs]{
			State:           state,
			Window:          messageWindow,
			TimeStamp:       time.Now().UTC().Format(LAYOUT_TIMESTAMP),
			IsCompleteState: true,
		}
		stateHelper.countStates++
		stateHelper.lastValidState = &completeState
		// Save on file
		encodedState, err := json.Marshal(completeState)
		if err != nil {
			logger.Errorf(MSG_ERROR_ENCODING_STATE, err)
			return err
		}
		if _, err := stateHelper.fileDescStateWriter.WriteString(string(encodedState) + "\n"); err != nil {
			logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
			return err
		}
	}
	return nil
}

// encodes the provided updateArgs into JSON format and writes it to the state file.
func tryToSaveUpdateArgs[TState any, TUpdateArgs any](stateHelper *StateHelper[TState, TUpdateArgs], state TState, messageWindow window.MessageWindow, updateArgs TUpdateArgs) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	if stateHelper.fileDescStateWriter == nil {
		logger.Warningf("%s: %v", MSG_NO_FILEDESC_AVAILABLE, stateHelper.filePath)
		return errors.New(strings.ToLower(MSG_NO_FILEDESC_AVAILABLE))
	}
	operationState := DataToSave[TState, TUpdateArgs]{
		UpdateArgs:      updateArgs,
		TimeStamp:       time.Now().UTC().Format(LAYOUT_TIMESTAMP),
		IsCompleteState: false,
	}
	encodedState, err := json.Marshal(operationState)
	if err != nil {
		logger.Errorf(MSG_ERROR_ENCODING_UPDATE_ARGS, err)
		return err
	}
	if _, err := stateHelper.fileDescStateWriter.WriteString(string(encodedState) + "\n"); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_UPDATE_ARGS, stateHelper.filePath, err)
		return err
	}
	// Update state helper
	completeState := DataToSave[TState, TUpdateArgs]{
		State:           state,
		Window:          messageWindow,
		TimeStamp:       time.Now().UTC().Format(LAYOUT_TIMESTAMP),
		IsCompleteState: true,
	}
	stateHelper.countStates++
	stateHelper.lastValidState = &completeState
	return nil
}

// check clean conditiones
func (stateHelper *StateHelper[TState, TOperation]) shouldClean() bool {
	// check count valids states
	if stateHelper.countStates > MAX_STATES {
		logger := logging.MustGetLogger(MODULE_NAME)
		logger.Debugf(MSG_CLEAN_FILES_ON_SAVE)
		return true
	}
	return false
}

// tries to clean up files in a fail-safe manner
func (stateHelper *StateHelper[TState, TOperation]) tryCleanFile() {
	logger := logging.MustGetLogger(MODULE_NAME)
	// check clean
	if !stateHelper.shouldClean() {
		return
	}
	// clean auxiliary file
	cleanFile(stateHelper.auxFileDescStateWriter, stateHelper.auxFilePath)
	// save last state on auxiliary
	encodedState, err := json.Marshal(stateHelper.lastValidState)
	if err != nil {
		logger.Errorf(MSG_ERROR_ENCODING_STATE, err)
		return
	}
	if _, err := stateHelper.auxFileDescStateWriter.WriteString(string(encodedState) + "\n"); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.auxFilePath, err)
		return
	}
	// clean state file
	cleanFile(stateHelper.fileDescStateWriter, stateHelper.filePath)
	// write the last valid state on state file
	if _, err := stateHelper.fileDescStateWriter.WriteString(string(encodedState) + "\n"); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
		return
	}
	stateHelper.countStates = 1
}
