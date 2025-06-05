package state

const STATES_DIR = "states/"

// StateHelper is a struct that helps manage state files for different clients and modules.
type StateHelper struct {
	filepath string
}

// generateFilePath constructs the file path for the state file based on client ID, module name, and shard.
func generateFilePath(clientId string, moduleName string, shard string) string {
	return STATES_DIR + clientId + "_" + moduleName + "_" + shard + ".json"
}

// NewStateHelper creates a new StateHelper instance with the specified client ID, module name, and shard.
func NewStateHelper(clientId string, moduleName string, shard string) *StateHelper {
	return &StateHelper{
		filepath: generateFilePath(clientId, moduleName, shard),
	}
}

func (stateHelper *StateHelper) LoadState(callback func()) error {
	return nil
}

func (stateHelper *StateHelper) SaveState(callback func()) error {
	return nil
}
