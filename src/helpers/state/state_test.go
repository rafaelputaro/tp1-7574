package state

import (
	//"os"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"tp1/helpers/window"
)

// go test -timeout 30m

type Data struct {
	Name string
	Id   int
}

type UpdateArgs struct {
	ConcatToName int64
	MessageId    int64
}

func UpdateState(state *Data, messageWindow *window.MessageWindow, updateArgs *UpdateArgs) {
	state.Name = "Pepe" + strconv.FormatInt(updateArgs.ConcatToName, 10)
	messageWindow.AddMessage(updateArgs.MessageId)
}

func TestStateCorrectFilesWithoutExceedingMaximumCapacity(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper(clientId, moduleName, shard, UpdateState)
	state, windowMessage := GetLastValidState(stateHelper)
	// Check new helper
	if state != nil || !windowMessage.IsEmpty() {
		t.Errorf("Error new state helper")
	}
	state = &Data{
		Name: "Pepe",
		Id:   1,
	}
	// Window to test
	windowData := window.NewMessageWindow()
	// Insert states
	for id := range MAX_STATES - 1 {
		args := UpdateArgs{
			ConcatToName: int64(id + 10),
			MessageId:    int64(id),
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose()
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper(clientId, moduleName, shard, UpdateState)
	// Check window and state
	stateReaded, windowReaded = GetLastValidState(stateHelper)
	if stateReaded == nil {
		t.Errorf("Error on reload state")
	} else {
		if stateReaded.Id != state.Id || stateReaded.Name != state.Name {
			t.Errorf("Error in reloaded state")
		}
	}
	// Check window
	if windowReaded.IsEmpty() {
		t.Errorf("Error on reload window")
	} else {
		for id := range MAX_STATES - 1 {
			if windowData.IsDuplicate(int64(id)) != windowReaded.IsDuplicate(int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose()
}

func TestStateCorrectFilesExceedingMaximumCapacity(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper(clientId, moduleName, shard, UpdateState)
	state, windowMessage := GetLastValidState(stateHelper)
	// Check new helper
	if state != nil || !windowMessage.IsEmpty() {
		t.Errorf("Error new state helper")
	}
	state = &Data{
		Name: "Pepe",
		Id:   1,
	}
	// Window to test
	windowData := window.NewMessageWindow()
	// Insert states
	for id := range MAX_STATES + 1 {
		args := UpdateArgs{
			ConcatToName: int64(id + 10),
			MessageId:    int64(id),
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose()
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper(clientId, moduleName, shard, UpdateState)
	// Check window and state
	stateReaded, windowReaded = GetLastValidState(stateHelper)
	if stateReaded == nil {
		t.Errorf("Error on reload state")
	} else {
		if stateReaded.Id != state.Id || stateReaded.Name != state.Name {
			t.Errorf("Error in reloaded state")
		}
	}
	// Check window
	if windowReaded.IsEmpty() {
		t.Errorf("Error on reload window")
	} else {
		for id := range MAX_STATES - 1 {
			if windowData.IsDuplicate(int64(id)) != windowReaded.IsDuplicate(int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose()
}

func TestTimeStampFileGreaterThanAux(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper(clientId, moduleName, shard, UpdateState)
	state, windowMessage := GetLastValidState(stateHelper)
	// Check new helper
	if state != nil || !windowMessage.IsEmpty() {
		t.Errorf("Error new state helper")
	}
	state = &Data{
		Name: "Pepe",
		Id:   1,
	}
	// Window to test
	windowData := window.NewMessageWindow()
	// Insert states
	for id := range MAX_STATES + 1 {
		args := UpdateArgs{
			ConcatToName: int64(id + 10),
			MessageId:    int64(id),
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose()
	MakeTimeStampBigger(t, GenerateFilePath(clientId, moduleName, shard))
	/*
	   // Set env
	   CleanOnStart = false
	   // Reload
	   stateHelper = NewStateHelper(clientId, moduleName, shard, UpdateState)
	   // Check window and state
	   stateReaded, windowReaded = GetLastValidState(stateHelper)

	   	if stateReaded == nil {
	   		t.Errorf("Error on reload state")
	   	} else {

	   		if stateReaded.Id != state.Id || stateReaded.Name != state.Name {
	   			t.Errorf("Error in reloaded state")
	   		}
	   	}

	   // Check window

	   	if windowReaded.IsEmpty() {
	   		t.Errorf("Error on reload window")
	   	} else {

	   		for id := range MAX_STATES - 1 {
	   			if windowData.IsDuplicate(int64(id)) != windowReaded.IsDuplicate(int64(id)) {
	   				t.Errorf("Error in reloaded window")
	   			}
	   		}
	   	}

	   // Close files
	   stateHelper.Dispose()
	*/
}

/*
func TestTimeStampFileLessThanAux(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
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
	if state == nil || windowP.IsEmpty() {
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
	StatesDir = "/tmp/states_test"
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
	if state == nil || windowP.IsEmpty() {
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
	StatesDir = "/tmp/states_test"
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
	if state == nil || windowP.IsEmpty() {
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
	StatesDir = "/tmp/states_test"
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
	if state != nil || !windowP.IsEmpty() {
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
*/

func MakeTimeStampBigger(t *testing.T, filePath string) {
	// read file
	fileRd, err := os.Open(filePath)
	if err != nil {
		t.Errorf("Error on open file %v: ", err)
		return
	}
	reader := NewReader(fileRd)
	line, err := reader.ReadLine()
	if err != nil {
		t.Errorf("Error on read file %v: ", err)
		return
	}
	fileRd.Close()
	// change date
	newContent := strings.ReplaceAll(line, `"TimeStamp":"2025-`, `"TimeStamp":"2030-`)
	fileWr, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Errorf("Error on open file %v: ", err)
		return
	}
	fileWr.Truncate(0)
	fileWr.WriteString(newContent)
	fileWr.Close()
}

func TestReader(t *testing.T) {
	seed := int64(12345) // Example seed
	randSource := rand.New(rand.NewSource(seed))
	filePath := "/tmp/reader_test.ndjson"
	// lines
	lines := []string{}
	amountLines := 15000
	// generate file
	fileWr, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	fileWr.Truncate(0)
	if err != nil {
		t.Errorf("Error on open file %v: ", err)
		return
	}
	for lineNum := range amountLines {
		line := `{"line": ` + strconv.Itoa(9+lineNum) + "," + strings.Repeat(`"field": "value", `, randSource.Intn(5000)) + `"lineEnd": ` + strconv.Itoa(lineNum) + "}"
		if _, err := fileWr.WriteString(line + "\n"); err != nil {
			t.Errorf("Error on write file")
		} else {
			lines = append(lines, line)
		}
	}
	fileWr.Close()
	// Test Reader
	fileRd, err := os.Open(filePath)
	if err != nil {
		t.Errorf("Error on open file")
		return
	}
	defer fileRd.Close()
	reader := NewReader(fileRd)
	for index := range lines {
		readed, _ := reader.ReadLine()
		if lines[index] != readed {
			t.Errorf("Error on line readed. Expected(%v): %v Readed(%v): %v", len(lines[index]), lines[index], len(readed), readed)
		}
	}
	for range 10000 {
		if _, error := reader.ReadLine(); error != io.EOF {
			t.Errorf("Error on read EOF: %v", error)
		}
	}
}
