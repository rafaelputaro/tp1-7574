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

	amqp "github.com/rabbitmq/amqp091-go"
)

// go test -timeout 30m

type Data struct {
	Name string
	Id   int
}

type UpdateArgs struct {
	ConcatToName int64
	MessageId    int64
	ClientId     string
}

func UpdateState(state *Data, messageWindow *window.MessageWindow, updateArgs *UpdateArgs) {
	state.Name = "Pepe" + strconv.FormatInt(updateArgs.ConcatToName, 10)
	messageWindow.AddMessage(updateArgs.ClientId, updateArgs.MessageId)
}

func DummySendAck(msg amqp.Delivery) error { return nil }

func TestStateCorrectFilesWithoutExceedingMaximumCapacity(t *testing.T) {
	// Configuration
	moduleId := "1"
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	var dummyMsg = amqp.Delivery{}
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			ConcatToName: int64(id),
			MessageId:    int64(id),
			ClientId:     clientId,
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, dummyMsg, DummySendAck, *windowData, args)
		stateReaded, _ := GetLastValidState(stateHelper)
		if stateReaded.Id != state.Id || stateReaded.Name != state.Name {
			t.Errorf("Error in reloaded state")
		}
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			if windowData.IsDuplicate(clientId, int64(id)) != windowReaded.IsDuplicate(clientId, int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
}

func TestStateCorrectFilesExceedingMaximumCapacity(t *testing.T) {
	// Configuration
	moduleId := "1"
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	var dummyMsg = amqp.Delivery{}
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			ClientId:     clientId,
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, dummyMsg, DummySendAck, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			if windowData.IsDuplicate(clientId, int64(id)) != windowReaded.IsDuplicate(clientId, int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
}

func TestTimeStampFileGreaterThanAux(t *testing.T) {
	// Configuration
	moduleId := "1"
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	var dummyMsg = amqp.Delivery{}
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			ClientId:     clientId,
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, dummyMsg, DummySendAck, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
	// Filepath is the one that contains the newest state
	MakeTimeStampBigger(t, GenerateFilePath(moduleId, moduleName, shard))
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
	// Check window and state
	stateReaded, windowReaded = GetLastValidState(stateHelper)
	if stateReaded == nil {
		t.Errorf("Error on reload state")
	} else {
		if stateReaded.Id != state.Id || stateReaded.Name == state.Name {
			t.Errorf("Error in reloaded state")
		}
	}
	// Check window
	if windowReaded.IsEmpty() {
		t.Errorf("Error on reload window")
	} else {
		for id := range MAX_STATES - 1 {
			if windowData.IsDuplicate(clientId, int64(id)) != windowReaded.IsDuplicate(clientId, int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
}

func TestTimeStampFileLowerThanAux(t *testing.T) {
	// Configuration
	moduleId := "1"
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	var dummyMsg = amqp.Delivery{}
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			ClientId:     clientId,
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, dummyMsg, DummySendAck, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
	// Filepath is the one that contains the newest state
	MakeTimeStampBigger(t, GenerateAuxFilePath(moduleId, moduleName, shard))
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
	// Check window and state
	stateReaded, windowReaded = GetLastValidState(stateHelper)
	if stateReaded == nil {
		t.Errorf("Error on reload state")
	} else {
		if stateReaded.Id != state.Id || stateReaded.Name == state.Name {
			t.Errorf("Error in reloaded state")
		}
	}
	// Check window
	if windowReaded.IsEmpty() {
		t.Errorf("Error on reload window")
	} else {
		for id := range MAX_STATES - 1 {
			if windowData.IsDuplicate(clientId, int64(id)) != windowReaded.IsDuplicate(clientId, int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
}

func TestStateOriginalFileBroken(t *testing.T) {
	// Configuration
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	var dummyMsg = amqp.Delivery{}
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper[Data, UpdateArgs, amqp.Delivery](clientId, moduleName, shard, UpdateState)
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
			ClientId:     clientId,
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, dummyMsg, DummySendAck, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
	// Filepath is broken
	BreakFile(t, GenerateFilePath(clientId, moduleName, shard))
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper[Data, UpdateArgs, amqp.Delivery](clientId, moduleName, shard, UpdateState)
	// Check window and state
	stateReaded, windowReaded = GetLastValidState(stateHelper)
	if stateReaded == nil {
		t.Errorf("Error on reload state")
	} else {
		if stateReaded.Id != state.Id || stateReaded.Name == state.Name {
			t.Errorf("Error in reloaded state -  Readed %v %v - Expected %v %v", stateReaded.Id, stateReaded.Name, state.Id, state.Name)
		}
	}
	// Check window
	if windowReaded.IsEmpty() {
		t.Errorf("Error on reload window")
	} else {
		for id := range MAX_STATES - 1 {
			if windowData.IsDuplicate(clientId, int64(id)) != windowReaded.IsDuplicate(clientId, int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
}

func TestStateAuxFileBroken(t *testing.T) {
	// Configuration
	moduleId := "1"
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	var dummyMsg = amqp.Delivery{}
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			ClientId:     clientId,
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, dummyMsg, DummySendAck, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
	// AuxFilepath is broken
	BreakFile(t, GenerateAuxFilePath(moduleId, moduleName, shard))
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			if windowData.IsDuplicate(clientId, int64(id)) != windowReaded.IsDuplicate(clientId, int64(id)) {
				t.Errorf("Error in reloaded window")
			}
		}
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
}

func TestStateBohtFilesBroken(t *testing.T) {
	// Configuration
	moduleId := "1"
	clientId := "cli-1"
	moduleName := "testing"
	shard := "0"
	StatesDir = "/tmp/states_test"
	var dummyMsg = amqp.Delivery{}
	// Set env to clean previus files
	CleanOnStart = true
	// New State Helper
	stateHelper := NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
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
			ClientId:     clientId,
		}
		UpdateState(state, windowData, &args)
		SaveState(stateHelper, *state, dummyMsg, DummySendAck, *windowData, args)
	}
	// Check window and state
	stateReaded, windowReaded := GetLastValidState(stateHelper)
	if stateReaded == nil || windowReaded.IsEmpty() {
		t.Errorf("Error insert windows or state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
	// Both files are broken
	BreakFile(t, GenerateFilePath(moduleId, moduleName, shard))
	BreakFile(t, GenerateAuxFilePath(moduleId, moduleName, shard))
	// Set env
	CleanOnStart = false
	// Reload
	stateHelper = NewStateHelper[Data, UpdateArgs, amqp.Delivery](moduleId, moduleName, shard, UpdateState)
	// Check window and state
	stateReaded, windowReaded = GetLastValidState(stateHelper)
	if stateReaded != nil {
		t.Errorf("Error on reload state")
	}
	// Close files
	stateHelper.Dispose(DummySendAck)
}

func BreakFile(t *testing.T, filePath string) {
	// read file
	fileRd, err := os.Open(filePath)
	if err != nil {
		t.Errorf("Error on open file %v: ", err)
		return
	}
	reader := NewReader(fileRd)
	line, _, err := reader.ReadLine()
	if err != nil {
		t.Errorf("Error on read file %v: ", err)
		return
	}
	fileRd.Close()
	// change date
	newContent := strings.ReplaceAll(line, `"TimeStamp":"2025-`, `TimeStamp":"2025-`)
	fileWr, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		t.Errorf("Error on open file %v: ", err)
		return
	}
	fileWr.Truncate(0)
	fileWr.WriteString(newContent)
	fileWr.Sync()
	fileWr.Close()
}

func MakeTimeStampBigger(t *testing.T, filePath string) {
	// read file
	fileRd, err := os.Open(filePath)
	if err != nil {
		t.Errorf("Error on open file %v: ", err)
		return
	}
	reader := NewReader(fileRd)
	line, _, err := reader.ReadLine()
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
	fileWr.Sync()
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
		readed, lenReaded, _ := reader.ReadLine()
		if lines[index] != readed || len(lines[index]) != lenReaded {
			t.Errorf("Error on line readed. Expected(%v): %v Readed(%v): %v", len(lines[index]), lines[index], len(readed), readed)
		}
	}
	for range 10000 {
		if _, _, error := reader.ReadLine(); error != io.EOF {
			t.Errorf("Error on read EOF: %v", error)
		}
	}
}
