package internal

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/op/go-logging"
)

// ServiceInfo holds the service metadata for management
type ServiceInfo struct {
	ServiceName   string
	ContainerName string
	HostPort      string
	LastRestart   time.Time
}

// ResilienceManager is the core component that handles service monitoring and recovery
type ResilienceManager struct {
	healthChecker *HealthChecker
	dockerClient  *DockerClient
	log           *logging.Logger
	config        *Config
	services      map[string]*ServiceInfo // Key is service name
	mutex         sync.RWMutex
}

func NewResilienceManager(config *Config, log *logging.Logger) (*ResilienceManager, error) {
	docker, err := NewDockerClient(config, log)
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	healthChecker := NewHealthChecker(config, log)

	return &ResilienceManager{
		healthChecker: healthChecker,
		dockerClient:  docker,
		log:           log,
		config:        config,
		services:      make(map[string]*ServiceInfo),
	}, nil
}

func (r *ResilienceManager) RegisterService(serviceName, containerName, hostPort string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	serviceInfo := &ServiceInfo{
		ServiceName:   serviceName,
		ContainerName: containerName,
		HostPort:      hostPort,
	}

	r.services[serviceName] = serviceInfo
	r.healthChecker.AddService(serviceName, hostPort, r.config)

	r.log.Infof("Registered service %s (container: %s) at %s", serviceName, containerName, hostPort)
}

func (r *ResilienceManager) Start(ctx context.Context) {
	r.log.Info("Starting resilience manager...")

	unhealthyCh := r.healthChecker.StartChecking(ctx)

	go func() {
		for {
			select {
			case <-ctx.Done():
				r.log.Info("Resilience manager stopping...")
				return
			case serviceName, ok := <-unhealthyCh:
				if !ok {
					// Channel closed
					return
				}
				r.handleUnhealthyService(ctx, serviceName)
			}
		}
	}()
}

func (r *ResilienceManager) isResponsibleFor(serviceName string) bool {
	// If only one node, this node is responsible for everything
	if r.config.TotalNodes <= 1 {
		return true
	}

	// Use FNV hash to calculate a hash value for the service name
	h := fnv.New32a()
	_, _ = h.Write([]byte(serviceName))
	hashValue := h.Sum32()

	// Use modulo to determine which node is responsible
	responsibleNodeIndex := int(hashValue) % r.config.TotalNodes

	// Convert nodeID to index
	// Assuming node IDs are in the format "node-X" where X is a number
	// Or directly numeric if desired
	nodeIndex := 0
	// Attempt to parse suffix after last dash, if not just use 0
	nodeIdParts := strings.Split(r.config.NodeID, "-")
	if len(nodeIdParts) > 1 {
		nodeIndex, _ = strconv.Atoi(nodeIdParts[len(nodeIdParts)-1])
	}

	isResponsible := (responsibleNodeIndex == nodeIndex)
	if !isResponsible {
		r.log.Debugf("Node %s not responsible for service %s (hash: %d mod %d = %d)",
			r.config.NodeID, serviceName, hashValue, r.config.TotalNodes, responsibleNodeIndex)
	}
	return isResponsible
}

func (r *ResilienceManager) handleUnhealthyService(ctx context.Context, serviceName string) {
	r.mutex.RLock()
	serviceInfo, exists := r.services[serviceName]
	r.mutex.RUnlock()

	if !exists {
		r.log.Errorf("Received unhealthy notification for unknown service: %s", serviceName)
		return
	}

	// Check if this node is responsible for the unhealthy service
	if !r.isResponsibleFor(serviceName) {
		r.log.Infof("Node %s not responsible for handling service %s, ignoring", r.config.NodeID, serviceName)
		return
	}

	// Check if service was recently restarted to avoid restart loops
	if time.Since(serviceInfo.LastRestart) < r.config.RestartWaitTime*2 {
		r.log.Warningf("Service %s was recently restarted, waiting before another restart", serviceName)
		return
	}

	// Mark the service as pending before restart
	r.healthChecker.SetServicePending(serviceName)

	r.log.Infof("Node %s attempting to restart unhealthy service %s (container: %s)",
		r.config.NodeID, serviceName, serviceInfo.ContainerName)

	err := r.dockerClient.RestartContainer(ctx, serviceInfo.ContainerName)
	if err != nil {
		r.log.Errorf("Failed to restart container %s: %v", serviceInfo.ContainerName, err)
		// Even if restart fails, we leave it in PENDING state for the health check to detect later
		return
	}

	// Update last restart time
	r.mutex.Lock()
	serviceInfo.LastRestart = time.Now()
	r.mutex.Unlock()

	r.log.Infof("Waiting %v for service %s to become healthy...", r.config.RestartWaitTime, serviceName)

	// Give the service time to start up
	time.Sleep(r.config.RestartWaitTime)

	// The health checker will update the status to HEALTHY or keep it as PENDING
	// If it remains PENDING for too long, it will be marked UNHEALTHY
}

func (r *ResilienceManager) Stop() {
	if r.dockerClient != nil {
		_ = r.dockerClient.Close()
	}
}

func (r *ResilienceManager) GetServiceStatuses() map[string]bool {
	statuses := r.healthChecker.GetServiceStatus()
	result := make(map[string]bool)

	for name, status := range statuses {
		// Convert three-state status to boolean for backwards compatibility
		result[name] = status.Status == StatusHealthy
	}

	return result
}

// GetServiceDetailedStatuses returns the detailed status for each service
func (r *ResilienceManager) GetServiceDetailedStatuses() map[string]string {
	statuses := r.healthChecker.GetServiceStatus()
	result := make(map[string]string)

	for name, status := range statuses {
		result[name] = status.Status
	}

	return result
}
