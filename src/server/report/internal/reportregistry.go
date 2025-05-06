package internal

import (
	"sync"
	"time"
	pb "tp1/protobuf/protopb"
)

type ReportRegistry struct {
	mu      sync.Mutex
	reports map[string]*pb.ReportResponse
}

func NewReportRegistry() *ReportRegistry {
	return &ReportRegistry{reports: make(map[string]*pb.ReportResponse)}
}

func (rr *ReportRegistry) WaitForReport(clientID string, timeout time.Duration) *pb.ReportResponse {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	timeoutChan := time.After(timeout)

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

func (rr *ReportRegistry) GetReport(clientID string) *pb.ReportResponse {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	r, ok := rr.reports[clientID]
	if !ok {
		return nil
	}

	if r.Answer1 == nil || r.Answer2 == nil || r.Answer3 == nil || r.Answer4 == nil || r.Answer5 == nil {
		return nil
	}

	return r
}

func (rr *ReportRegistry) AddAnswer1(clientID string, answer1 *pb.Answer1) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}
	rr.reports[clientID].Answer1 = answer1
}

func (rr *ReportRegistry) AddAnswer2(clientID string, answer2 *pb.Answer2) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}
	rr.reports[clientID].Answer2 = answer2
}

func (rr *ReportRegistry) AddAnswer3(clientID string, answer3 *pb.Answer3) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}
	rr.reports[clientID].Answer3 = answer3
}

func (rr *ReportRegistry) AddAnswer4(clientID string, answer4 *pb.Answer4) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}
	rr.reports[clientID].Answer4 = answer4
}

func (rr *ReportRegistry) AddAnswer5(clientID string, answer5 *pb.Answer5) {
	rr.mu.Lock()
	defer rr.mu.Unlock()

	if _, ok := rr.reports[clientID]; !ok {
		rr.reports[clientID] = &pb.ReportResponse{}
	}
	rr.reports[clientID].Answer5 = answer5
}
