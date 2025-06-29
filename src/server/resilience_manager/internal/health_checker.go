package internal

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/op/go-logging"
)

// Container status constants
const (
	StatusHealthy   = "HEALTHY"
	StatusPending   = "PENDING"
	StatusUnhealthy = "UNHEALTHY"
)

type ServiceStatus struct {
	Name         string
	Status       string // HEALTHY, PENDING, UNHEALTHY
	LastChecked  time.Time
	ErrorCount   int
	PendingSince time.Time // When the service entered PENDING state
}

type HealthChecker struct {
	services           map[string]string // service name -> health check URL
	serviceStatus      map[string]*ServiceStatus
	checkInterval      time.Duration
	unhealthyThreshold int
	client             *http.Client
	log                *logging.Logger
}

func NewHealthChecker(config *Config, log *logging.Logger) *HealthChecker {
	return &HealthChecker{
		services:           make(map[string]string),
		serviceStatus:      make(map[string]*ServiceStatus),
		checkInterval:      config.CheckInterval,
		unhealthyThreshold: config.UnhealthyThreshold,
		log:                log,
		client: &http.Client{
			Timeout: 20 * time.Second, // Short timeout to detect unresponsive services quickly
		},
	}
}

func (h *HealthChecker) AddService(name, hostPort string, config *Config) {
	url := fmt.Sprintf("http://%s:%d%s", hostPort, config.HealthPort, config.HealthEndpoint)
	h.services[name] = url
	h.serviceStatus[name] = &ServiceStatus{
		Name:         name,
		Status:       StatusHealthy, // Assume initially healthy
		LastChecked:  time.Now(),
		PendingSince: time.Time{}, // Zero time
	}
	h.log.Infof("Added service for health monitoring: %s at %s", name, url)
}

func (h *HealthChecker) StartChecking(ctx context.Context) <-chan string {
	unhealthyCh := make(chan string)

	go func() {
		ticker := time.NewTicker(h.checkInterval)
		defer ticker.Stop()

		h.log.Infof("Health checker started with interval: %v, threshold: %d", h.checkInterval, h.unhealthyThreshold)

		for {
			select {
			case <-ctx.Done():
				h.log.Info("Health checker stopping...")
				close(unhealthyCh)
				return
			case <-ticker.C:
				h.checkAllServices(unhealthyCh)
			}
		}
	}()

	return unhealthyCh
}

func (h *HealthChecker) checkAllServices(unhealthyCh chan<- string) {
	for name, url := range h.services {
		status := h.serviceStatus[name]
		healthy := h.checkHealth(url)

		// Get previous state for transition detection
		previousStatus := status.Status

		// Health check logic
		if healthy {
			if status.ErrorCount > 0 {
				h.log.Infof("Service %s is healthy again after %d errors", name, status.ErrorCount)
			}
			status.ErrorCount = 0

			// If it was pending and now healthy, transition to healthy
			if status.Status == StatusPending {
				h.log.Infof("Service %s recovered from PENDING state", name)
				status.Status = StatusHealthy
				status.PendingSince = time.Time{} // Reset pending timestamp
			} else if status.Status == StatusUnhealthy {
				// Recovered directly from unhealthy without pending transition
				h.log.Infof("Service %s recovered from UNHEALTHY state", name)
				status.Status = StatusHealthy
			}
		} else {
			status.ErrorCount++
			h.log.Warningf("Health check failed for service %s (%d/%d failures)", name, status.ErrorCount, h.unhealthyThreshold)

			if status.ErrorCount >= h.unhealthyThreshold {
				if status.Status == StatusHealthy {
					h.log.Errorf("Service %s is now UNHEALTHY after %d consecutive failures", name, status.ErrorCount)
					status.Status = StatusUnhealthy
					unhealthyCh <- name
				} else if status.Status == StatusPending {
					// Check if service has been in PENDING state too long
					pendingDuration := time.Since(status.PendingSince)
					if pendingDuration > 30*time.Second { // TODO: Make configurable
						h.log.Errorf("Service %s stuck in PENDING state for %v, marking as UNHEALTHY", name, pendingDuration)
						status.Status = StatusUnhealthy
						unhealthyCh <- name
					}
				} else if status.Status == StatusUnhealthy && status.ErrorCount%h.unhealthyThreshold == 0 {
					h.log.Errorf("Service %s remains UNHEALTHY after %d consecutive failures, requesting another restart", name, status.ErrorCount)
					unhealthyCh <- name
				}
			}
		}

		// Log status transitions
		if previousStatus != status.Status {
			h.log.Infof("Service %s transitioned from %s to %s", name, previousStatus, status.Status)
		}

		status.LastChecked = time.Now()
	}
}

// checkHealth performs a single health check against the given URL
func (h *HealthChecker) checkHealth(url string) bool {
	resp, err := h.client.Get(url)
	if err != nil {
		h.log.Debugf("Health check failed for %s: %v", url, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		h.log.Debugf("Health check for %s returned status code %d", url, resp.StatusCode)
		return false
	}

	return true
}

// GetServiceStatus returns the current status of all monitored services
func (h *HealthChecker) GetServiceStatus() map[string]*ServiceStatus {
	result := make(map[string]*ServiceStatus)

	for name, status := range h.serviceStatus {
		result[name] = &ServiceStatus{
			Name:         status.Name,
			Status:       status.Status,
			LastChecked:  status.LastChecked,
			ErrorCount:   status.ErrorCount,
			PendingSince: status.PendingSince,
		}
	}

	return result
}

// SetServicePending marks a service as pending (during restart)
func (h *HealthChecker) SetServicePending(name string) {
	if status, exists := h.serviceStatus[name]; exists {
		status.Status = StatusPending
		status.PendingSince = time.Now()
		h.log.Infof("Service %s marked as PENDING", name)
	}
}

// IsServiceHealthy returns whether a service is currently healthy
func (h *HealthChecker) IsServiceHealthy(name string) bool {
	if status, exists := h.serviceStatus[name]; exists {
		return status.Status == StatusHealthy
	}
	return false
}
