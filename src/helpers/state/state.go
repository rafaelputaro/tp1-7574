package state

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"tp1/helpers/window"

	"github.com/op/go-logging"
)

// TODO: Recortar el archivo de manera segura cuando se alcance un tamaño máximo
// TODO: Al principio de la corrida de todo el sistema eliminar el contenido de la carpeta states

var statesDir = initStatesDir()

const MODULE_NAME = "state"
const DEFAULT_STATES_DIR = "/tmp/states"
const STATES_DIR_ENV_VAR = "STATES_DIR"
const MSG_FAILED_TO_OPEN_STATE_FILE = "Failed to open state file: %v"
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

// StateHelper is a struct that helps manage state files for different clients and modules.
type StateHelper struct {
	filePath       string
	auxFilePath    string
	filedescWriter *os.File
}

// CompleteState is a generic struct that holds the state of type T and a message window.
type CompleteState[T any] struct {
	State  T
	Window window.MessageWindow
}

func initStatesDir() string {
	statesDir := os.Getenv(STATES_DIR_ENV_VAR)
	if statesDir == "" {
		statesDir = DEFAULT_STATES_DIR // Default directory if not set
	}
	return statesDir
}

// GenerateFilePath constructs the file path for the state file based on client ID, module name, and shard.
func generateFilePath(clientId string, moduleName string, shard string) string {
	return statesDir + "/" + clientId + "_" + moduleName + "_" + shard + ".ndjson"
}

// GenerateAuxFilePath constructs the file path for the auxiliary state file based on client ID, module name, and shard.
func generateAuxFilePath(clientId string, moduleName string, shard string) string {
	return statesDir + "/" + clientId + "_" + moduleName + "_" + shard + "_aux.ndjson"
}

// NewStateHelper creates a new StateHelper instance with the specified client ID, module name, and shard.
func NewStateHelper(clientId string, moduleName string, shard string) *StateHelper {
	logger := logging.MustGetLogger(MODULE_NAME)
	filePath := generateFilePath(clientId, moduleName, shard)
	auxFilePath := generateAuxFilePath(clientId, moduleName, shard)
	// Ensure the states directory exists
	err := os.MkdirAll(statesDir, 0755)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_CREATE_STATES_DIR, statesDir, err)
		return nil
	}
	// Open the state file for appending and writing
	fileWr, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE, err)
		return nil
	}
	// TODO: Chequear si se interrumpió el recortado del archivo completando el mismo y destruyendo el archivo auxiliar

	return &StateHelper{
		filePath:       filePath,
		auxFilePath:    auxFilePath,
		filedescWriter: fileWr,
	}
}

// Dispose closes the file descriptors used by the StateHelper.
func (stateHelper *StateHelper) Dispose() {
	logger := logging.MustGetLogger(MODULE_NAME)
	if err := stateHelper.filedescWriter.Close(); err != nil {
		logger.Errorf(MSG_FAILED_TO_WRITE_STATE, stateHelper.filePath, err)
	}
	stateHelper.filedescWriter = nil
	logger.Debugf(MSG_FILE_CLOSED, stateHelper.filePath)
}

// LoadLastValidState reads the last valid state from the state file and returns it as a pointer to type T.
func LoadLastValidState[T any](stateHelper *StateHelper) (*T, *window.MessageWindow, error) {
	logger := logging.MustGetLogger(MODULE_NAME)
	lines, err := readLines[T](stateHelper)
	if err != nil {
		return nil, nil, err
	}
	if len(lines) == 0 {
		logger.Debugf("%v: %v", MSG_NO_VALID_STATE_FOUND, stateHelper.filePath)
		return nil, nil, errors.New(strings.ToLower(MSG_NO_VALID_STATE_FOUND))
	}
	return &lines[len(lines)-1].State, &lines[len(lines)-1].Window, nil
}

// ReadLines reads the state file line by line and decodes each line into a slice of type T.
func readLines[T any](stateHelper *StateHelper) ([]CompleteState[T], error) {
	logger := logging.MustGetLogger(MODULE_NAME)
	fileRd, err := os.OpenFile(stateHelper.filePath, os.O_RDONLY, 0644)
	if err != nil {
		logger.Errorf(MSG_FAILED_TO_OPEN_STATE_FILE_FOR_READING, err)
		return nil, err
	}
	logger.Debugf(MSG_FILE_OPENED, stateHelper.filePath)
	// Decode the file line by line
	var lines []CompleteState[T]
	scanner := bufio.NewScanner(fileRd)
	for scanner.Scan() {
		var decoded CompleteState[T]
		err := json.Unmarshal([]byte(scanner.Text()), &decoded)
		if err != nil {
			logger.Errorf(MSG_ERROR_DECODING_STATE, err)
			continue
		}
		lines = append(lines, decoded)
	}
	// Close the file after decoding
	if err := fileRd.Close(); err != nil {
		logger.Errorf(MSG_FAILED_TO_READ_STATE, stateHelper.filePath, err)
		return nil, err
	}
	return lines, nil
}

// SaveState encodes the provided state and message window into JSON format and writes it to the state file.
func SaveState[T any](stateHelper *StateHelper, state T, messageWindow window.MessageWindow) error {
	logger := logging.MustGetLogger(MODULE_NAME)
	if stateHelper.filedescWriter == nil {
		logger.Warningf("%s: %v", MSG_NO_FILEDESC_AVAILABLE, stateHelper.filePath)
		return errors.New(strings.ToLower(MSG_NO_FILEDESC_AVAILABLE))
	}
	completeState := CompleteState[T]{
		State:  state,
		Window: messageWindow,
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
	return nil
}
