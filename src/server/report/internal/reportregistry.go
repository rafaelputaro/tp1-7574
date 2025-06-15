package internal

import (
	"slices"
	"sync"
	"time"
	"tp1/helpers/state"
	"tp1/helpers/window"
	pb "tp1/protobuf/protopb"

	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"
)

var logger = logging.MustGetLogger("report")

// ReportState holds all the report data that needs to be persisted
type ReportState struct {
	Reports     map[string]*pb.ReportResponse
	DoneReports map[string]int
}

// ReportUpdateArgs holds the incremental updates to be applied to the state
type ReportUpdateArgs struct {
	ClientID     string
	AnswerNumber int
	Data         []byte // Store serialized data from the specific Answer type
	IsDoneMarker bool   // Mark if this update is from DoneAnswer
	MessageID    int64  // Store message ID to prevent duplicates
}

// ReportRegistry provides storage for report data with resilience capabilities
type ReportRegistry struct {
	mu            sync.Mutex
	reports       map[string]*pb.ReportResponse
	doneReports   map[string]int
	stateHelper   *state.StateHelper[ReportState, ReportUpdateArgs]
	messageWindow window.MessageWindow
	moduleName    string
}

// NewReportRegistry creates a new report registry with resilience capabilities
func NewReportRegistry(moduleName string) *ReportRegistry {
	rr := &ReportRegistry{
		reports:       make(map[string]*pb.ReportResponse),
		doneReports:   make(map[string]int),
		messageWindow: *window.NewMessageWindow(),
		moduleName:    moduleName,
	}

	// Initialize state helper with generic state type and update function
	rr.stateHelper = state.NewStateHelper[ReportState, ReportUpdateArgs](
		"report", // Common client ID for the report service
		moduleName,
		"0", // Default shard
		rr.applyUpdate,
	)

	// Load existing state if available
	initialState, msgWindow := state.GetLastValidState(rr.stateHelper)
	if initialState != nil {
		rr.reports = initialState.Reports
		rr.doneReports = initialState.DoneReports
		rr.messageWindow = msgWindow
		logger.Infof("Recovered state for module %s with %d reports and %d done markers",
			moduleName, len(rr.reports), len(rr.doneReports))
	} else {
		logger.Infof("No existing state found for module %s, starting fresh", moduleName)
	}

	return rr
}

// applyUpdate applies incremental updates to the state
func (rr *ReportRegistry) applyUpdate(reportState *ReportState, msgWindow *window.MessageWindow, updateArgs *ReportUpdateArgs) {
	// Skip if we've already seen this message
	if msgWindow != nil && msgWindow.IsDuplicate(updateArgs.MessageID) {
		logger.Debugf("Skipping duplicate message ID: %d", updateArgs.MessageID)
		return
	}

	// Add message ID to window to prevent duplicate processing
	if msgWindow != nil {
		msgWindow.AddMessage(updateArgs.MessageID)
	}

	clientID := updateArgs.ClientID

	// If this is a "done" marker update
	if updateArgs.IsDoneMarker {
		if _, ok := reportState.DoneReports[clientID]; !ok {
			reportState.DoneReports[clientID] = 0
		}
		reportState.DoneReports[clientID]++
		return
	}

	// Ensure we have a report container for this client
	if _, ok := reportState.Reports[clientID]; !ok {
		reportState.Reports[clientID] = &pb.ReportResponse{}
	}

	// Based on answer number, update the appropriate part of the report
	switch updateArgs.AnswerNumber {
	case 1:
		// This is for Answer1 (Movies)
		if reportState.Reports[clientID].Answer1 == nil {
			reportState.Reports[clientID].Answer1 = &pb.Answer1{}
		}
		// Movies are added incrementally, so we need to unmarshal the MovieEntry and append it
		var entry pb.MovieEntry
		_ = unmarshalProto(updateArgs.Data, &entry)
		reportState.Reports[clientID].Answer1.Movies = append(
			reportState.Reports[clientID].Answer1.Movies, &entry)

	case 2:
		// Directly replace Answer2
		var answer2 pb.Answer2
		_ = unmarshalProto(updateArgs.Data, &answer2)
		reportState.Reports[clientID].Answer2 = &answer2

	case 3:
		// Directly replace Answer3
		var answer3 pb.Answer3
		_ = unmarshalProto(updateArgs.Data, &answer3)
		reportState.Reports[clientID].Answer3 = &answer3

	case 4:
		// Directly replace Answer4
		var answer4 pb.Answer4
		_ = unmarshalProto(updateArgs.Data, &answer4)
		reportState.Reports[clientID].Answer4 = &answer4

	case 5:
		// Directly replace Answer5
		var answer5 pb.Answer5
		_ = unmarshalProto(updateArgs.Data, &answer5)
		reportState.Reports[clientID].Answer5 = &answer5
	}
}

// Helper to unmarshal protobuf data
func unmarshalProto(data []byte, msg proto.Message) error {
	err := proto.Unmarshal(data, msg)
	if err != nil {
		logger.Errorf("Error unmarshaling protobuf message: %v", err)
	}
	return err
}

// Dispose cleans up resources
func (rr *ReportRegistry) Dispose() {
	rr.stateHelper.Dispose()
}

// WaitForReport waits for a report to be ready and returns it
func (rr *ReportRegistry) WaitForReport(clientID string) *pb.ReportResponse {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	timeoutChan := time.After(120 * time.Second)

	for {
		select {
		case <-ticker.C:
			if report := rr.GetReport(clientID); report != nil {
				return report
			}
		case <-timeoutChan:
			return nil
		}
	}
}

// GetReport returns a report if it's ready
func (rr *ReportRegistry) GetReport(clientID string) *pb.ReportResponse {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	r, ok := rr.reports[clientID]
	if !ok {
		return nil
	}

	d, ok := rr.doneReports[clientID]
	if !ok {
		return nil
	}

	if r.Answer1 == nil || r.Answer2 == nil || r.Answer3 == nil || r.Answer4 == nil || r.Answer5 == nil {
		return nil
	}

	if d < 5 {
		return nil
	}

	SortReport(r)
	return r
}

// DoneAnswer marks an answer as done for a client
func (rr *ReportRegistry) DoneAnswer(clientID string) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.doneReports[clientID]; !ok {
		rr.doneReports[clientID] = 0
	}
	rr.doneReports[clientID]++

	// Save the state update
	updateArgs := ReportUpdateArgs{
		ClientID:     clientID,
		IsDoneMarker: true,
		MessageID:    generateMessageID(),
	}

	reportState := ReportState{
		Reports:     rr.reports,
		DoneReports: rr.doneReports,
	}

	// Save the incremental update
	_ = state.SaveState(rr.stateHelper, reportState, rr.messageWindow, updateArgs)
}

// AddToAnswer1 adds a movie entry to answer 1
func (rr *ReportRegistry) AddToAnswer1(clientID string, entry *pb.MovieEntry) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}

	if rr.reports[clientID].Answer1 == nil {
		rr.reports[clientID].Answer1 = &pb.Answer1{}
	}

	rr.reports[clientID].Answer1.Movies = append(rr.reports[clientID].Answer1.Movies, entry)

	// Create serialized movie entry data for update
	data := serializeProto(entry)

	// Save the state update
	updateArgs := ReportUpdateArgs{
		ClientID:     clientID,
		AnswerNumber: 1,
		Data:         data,
		MessageID:    generateMessageID(),
	}

	reportState := ReportState{
		Reports:     rr.reports,
		DoneReports: rr.doneReports,
	}

	// Save the incremental update
	_ = state.SaveState(rr.stateHelper, reportState, rr.messageWindow, updateArgs)
}

// AddAnswer2 adds country data to answer 2
func (rr *ReportRegistry) AddAnswer2(clientID string, answer2 *pb.Answer2) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}

	rr.reports[clientID].Answer2 = answer2

	// Save the state update
	updateArgs := ReportUpdateArgs{
		ClientID:     clientID,
		AnswerNumber: 2,
		Data:         serializeProto(answer2),
		MessageID:    generateMessageID(),
	}

	reportState := ReportState{
		Reports:     rr.reports,
		DoneReports: rr.doneReports,
	}

	// Save the incremental update
	_ = state.SaveState(rr.stateHelper, reportState, rr.messageWindow, updateArgs)
}

// AddAnswer3 adds rating data to answer 3
func (rr *ReportRegistry) AddAnswer3(clientID string, answer3 *pb.Answer3) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}

	rr.reports[clientID].Answer3 = answer3

	// Save the state update
	updateArgs := ReportUpdateArgs{
		ClientID:     clientID,
		AnswerNumber: 3,
		Data:         serializeProto(answer3),
		MessageID:    generateMessageID(),
	}

	reportState := ReportState{
		Reports:     rr.reports,
		DoneReports: rr.doneReports,
	}

	// Save the incremental update
	_ = state.SaveState(rr.stateHelper, reportState, rr.messageWindow, updateArgs)
}

// AddAnswer4 adds actor data to answer 4
func (rr *ReportRegistry) AddAnswer4(clientID string, answer4 *pb.Answer4) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}

	rr.reports[clientID].Answer4 = answer4

	// Save the state update
	updateArgs := ReportUpdateArgs{
		ClientID:     clientID,
		AnswerNumber: 4,
		Data:         serializeProto(answer4),
		MessageID:    generateMessageID(),
	}

	reportState := ReportState{
		Reports:     rr.reports,
		DoneReports: rr.doneReports,
	}

	// Save the incremental update
	_ = state.SaveState(rr.stateHelper, reportState, rr.messageWindow, updateArgs)
}

// AddAnswer5 adds sentiment data to answer 5
func (rr *ReportRegistry) AddAnswer5(clientID string, answer5 *pb.Answer5) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}

	rr.reports[clientID].Answer5 = answer5

	// Save the state update
	updateArgs := ReportUpdateArgs{
		ClientID:     clientID,
		AnswerNumber: 5,
		Data:         serializeProto(answer5),
		MessageID:    generateMessageID(),
	}

	reportState := ReportState{
		Reports:     rr.reports,
		DoneReports: rr.doneReports,
	}

	// Save the incremental update
	_ = state.SaveState(rr.stateHelper, reportState, rr.messageWindow, updateArgs)
}

// Used to generate unique IDs for messages
var messageIDCounter int64 = 0
var messageIDMutex sync.Mutex

// Helper function to generate a unique message ID
func generateMessageID() int64 {
	messageIDMutex.Lock()
	defer messageIDMutex.Unlock()

	// Combine timestamp and counter for better uniqueness
	messageIDCounter++
	return (time.Now().UnixNano() << 16) | (messageIDCounter & 0xFFFF)
}

// Helper function to serialize protobuf messages
func serializeProto(msg proto.Message) []byte {
	data, err := proto.Marshal(msg)
	if err != nil {
		logger.Errorf("Error serializing protobuf message: %v", err)
		return []byte{}
	}
	return data
}

// SortReport sorts the different sections of a report response
func SortReport(report *pb.ReportResponse) {
	slices.SortFunc(report.Answer1.Movies, func(a, b *pb.MovieEntry) int {
		return int(a.GetId() - b.GetId())
	})

	slices.SortFunc(report.Answer2.Countries, func(a, b *pb.CountryEntry) int {
		return int(b.GetBudget() - a.GetBudget())
	})

	slices.SortFunc(report.Answer4.Actors, func(a, b *pb.ActorEntry) int {
		if a.GetCount() != b.GetCount() {
			return int(b.GetCount() - a.GetCount())
		}

		if a.GetName() < b.GetName() {
			return -1
		} else if a.GetName() > b.GetName() {
			return 1
		}

		return 0
	})
}
