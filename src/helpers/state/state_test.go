package state

import (
	"bufio"
	"os"
	"strconv"
	"testing"
	"tp1/helpers/window"
)

// go test -timeout 30m

type Data struct {
	Name string
	Id   int
}

func TestStateCorrectFiles(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	messageIds := []int{100, 102, 99}
	// Clean files
	err := os.RemoveAll(StatesDir)
	if err != nil {
		t.Errorf("Error on clean folder: %v", err)
	}

	stateHelper := NewStateHelper[Data](clientId, moduleName, shard)
	state, windowP := GetLastValidState(stateHelper)

	if state != nil || windowP != nil {
		t.Errorf("Error new state helper: %v", err)
	}
	// window to test
	windowData := window.NewMessageWindow()
	for message := range messageIds {
		windowData.AddMessage(int64(message))
	}

	// Insert states
	for id := range MAX_STATES {
		data := Data{
			Name: "Subject " + strconv.Itoa(id),
			Id:   id,
		}
		SaveState(stateHelper, data, windowData)
	}
	// Check window and state
	state, windowP = GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error insert windows or window")
	} else {
		for message := range messageIds {
			if !windowP.IsDuplicate(int64(message)) {
				t.Errorf("Error insert window")
			}
		}
		if state.Id != MAX_STATES-1 {
			t.Errorf("Error insert states %v", state.Id)
		}
	}
	// Exceed the maximum number of valid states
	{
		id := MAX_STATES
		data := Data{
			Name: "Subject " + strconv.Itoa(id),
			Id:   id,
		}
		SaveState(stateHelper, data, windowData)
	}
	// Check window and state
	state, windowP = GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error clean files: %v", err)
	} else {
		for message := range messageIds {
			if !windowP.IsDuplicate(int64(message)) {
				t.Errorf("Error insert window")
			}
		}
		if state.Id != MAX_STATES {
			t.Errorf("Error insert states %v", state.Id)
		}
	}
	// Check files
	countLinesFile := countLines(stateHelper.filePath)
	if countLinesFile != 1 {
		t.Errorf("Error file must have one line %v", countLinesFile)
	}
	countLinesAuxFile := countLines(stateHelper.auxFilePath)
	if countLinesAuxFile != 1 {
		t.Errorf("Error aux file must be empty %v", countLinesAuxFile)
	}
	// Close files
	stateHelper.Dispose()
}

func TestTimeStampFileGreaterThanAux(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	// original file
	filePath := GenerateFilePath(clientId, moduleName, shard)
	os.Remove(filePath)
	fileWr, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer fileWr.Close()
	data := `{"State":{"Name":"Subject 6","Id":7},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415296"}` + "\n"
	fileWr.WriteString(data)
	// aux file
	auxFilePath := GenerateAuxFilePath(clientId, moduleName, shard)
	os.Remove(auxFilePath)
	auxFileWr, err := os.OpenFile(auxFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer auxFileWr.Close()
	data = `{"State":{"Name":"Subject 6","Id":6},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415294"}` + "\n"
	auxFileWr.WriteString(data)
	// Check loader
	stateHelper := NewStateHelper[Data](clientId, moduleName, shard)
	state, windowP := GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error load state or window")
	} else {
		if state.Id != 7 {
			t.Errorf("Error states %v", state.Id)
		}
	}
}

func TestTimeStampFileLessThanAux(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	// original file
	filePath := GenerateFilePath(clientId, moduleName, shard)
	os.Remove(filePath)
	fileWr, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer fileWr.Close()
	data := `{"State":{"Name":"Subject 6","Id":7},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415296"}` + "\n"
	fileWr.WriteString(data)
	// aux file
	auxFilePath := GenerateAuxFilePath(clientId, moduleName, shard)
	os.Remove(auxFilePath)
	auxFileWr, err := os.OpenFile(auxFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer auxFileWr.Close()
	data = `{"State":{"Name":"Subject 6","Id":6},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415298"}` + "\n"
	auxFileWr.WriteString(data)
	// Check loader
	stateHelper := NewStateHelper[Data](clientId, moduleName, shard)
	state, windowP := GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error load state or window")
	} else {
		if state.Id != 6 {
			t.Errorf("Error states %v", state.Id)
		}
	}
}

func TestStateOriginalFileBroken(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	// original file
	filePath := GenerateFilePath(clientId, moduleName, shard)
	os.Remove(filePath)
	fileWr, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer fileWr.Close()
	data := `{"State":{"Name":"Subject 6","Id":7},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415296"` + "\n"
	fileWr.WriteString(data)
	// aux file
	auxFilePath := GenerateAuxFilePath(clientId, moduleName, shard)
	os.Remove(auxFilePath)
	auxFileWr, err := os.OpenFile(auxFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer auxFileWr.Close()
	data = `{"State":{"Name":"Subject 6","Id":6},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415298"}` + "\n"
	auxFileWr.WriteString(data)
	// Check loader
	stateHelper := NewStateHelper[Data](clientId, moduleName, shard)
	state, windowP := GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error load state or window")
	} else {
		if state.Id != 6 {
			t.Errorf("Error states %v", state.Id)
		}
	}
}

func TestStateAuxFileBroken(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	// original file
	filePath := GenerateFilePath(clientId, moduleName, shard)
	os.Remove(filePath)
	fileWr, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer fileWr.Close()
	data := `{"State":{"Name":"Subject 6","Id":7},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415296"}` + "\n"
	fileWr.WriteString(data)
	// aux file
	auxFilePath := GenerateAuxFilePath(clientId, moduleName, shard)
	os.Remove(auxFilePath)
	auxFileWr, err := os.OpenFile(auxFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer auxFileWr.Close()
	data = `{"State":{"Name":"Subject 6","Id":6},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415298"}1` + "\n"
	auxFileWr.WriteString(data)
	// Check loader
	stateHelper := NewStateHelper[Data](clientId, moduleName, shard)
	state, windowP := GetLastValidState(stateHelper)
	if state == nil || windowP == nil {
		t.Errorf("Error load state or window")
	} else {
		if state.Id != 7 {
			t.Errorf("Error states %v", state.Id)
		}
	}
}

func TestStateBohtFilesBroken(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	// original file
	filePath := GenerateFilePath(clientId, moduleName, shard)
	os.Remove(filePath)
	fileWr, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer fileWr.Close()
	data := `{"State":{"Name":"Subject 6","Id":7},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415296"}2` + "\n"
	fileWr.WriteString(data)
	// aux file
	auxFilePath := GenerateAuxFilePath(clientId, moduleName, shard)
	os.Remove(auxFilePath)
	auxFileWr, err := os.OpenFile(auxFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer auxFileWr.Close()
	data = `{"State":{"Name":"Subject 6","Id":6},"Window":{"0":"2025-06-09 14:28:12.807811640","1":"2025-06-09 14:28:12.807813293","2":"2025-06-09 14:28:12.807813925"},"TimeStamp":"2025-06-09 14:28:21.161415298"}1` + "\n"
	auxFileWr.WriteString(data)
	// Check loader
	stateHelper := NewStateHelper[Data](clientId, moduleName, shard)
	state, windowP := GetLastValidState(stateHelper)
	if state != nil || windowP != nil {
		t.Errorf("Error load state or window")
	}
}

func countLines(filePath string) int {
	file, err := os.Open(filePath)
	if err != nil {
		return 0
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	lineCount := 0
	for scanner.Scan() {
		lineCount++
	}
	return lineCount
}
