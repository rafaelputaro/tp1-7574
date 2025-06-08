package internal

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/op/go-logging"
)

type ServiceStatus struct {
	Name        string
	Healthy     bool
	LastChecked time.Time
	ErrorCount  int
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
			Timeout: 2 * time.Second, // Short timeout to detect unresponsive services quickly
		},
	}
}

func (h *HealthChecker) AddService(name, hostPort string, config *Config) {
	url := fmt.Sprintf("http://%s:%d%s", hostPort, config.HealthPort, config.HealthEndpoint)
	h.services[name] = url
	h.serviceStatus[name] = &ServiceStatus{
		Name:        name,
		Healthy:     true, // Assume initially healthy
		LastChecked: time.Now(),
	}
	h.log.Infof("Added service for health monitoring: %s at %s", name, url)
}

func (h *HealthChecker) StartChecking(ctx context.Context) <-chan string {
	unhealthyCh := make(chan string)

	go func() {
		ticker := time.NewTicker(h.checkInterval)
		defer ticker.Stop()

		h.log.Infof("Health checker started with interval: %v, threshold: %d",
			h.checkInterval, h.unhealthyThreshold)

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

		// Previous state
		wasHealthy := status.Healthy

		if healthy {
			// Reset error count if service is healthy
			if status.ErrorCount > 0 {
				h.log.Infof("Service %s is healthy again after %d errors",
					name, status.ErrorCount)
			}
			status.ErrorCount = 0
			status.Healthy = true
		} else {
			status.ErrorCount++
			h.log.Warningf("Health check failed for service %s (%d/%d failures)",
				name, status.ErrorCount, h.unhealthyThreshold)

			// Mark as unhealthy after reaching threshold
			if status.ErrorCount >= h.unhealthyThreshold {
				status.Healthy = false

				// Only send notification on state change from healthy to unhealthy
				if wasHealthy {
					h.log.Errorf("Service %s is now UNHEALTHY after %d consecutive failures",
						name, status.ErrorCount)
					unhealthyCh <- name
				}
			}
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
			Name:        status.Name,
			Healthy:     status.Healthy,
			LastChecked: status.LastChecked,
			ErrorCount:  status.ErrorCount,
		}
	}

	return result
}
