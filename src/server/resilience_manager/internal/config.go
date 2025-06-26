package internal

import (
	"os"
	"strconv"
	"time"

	"github.com/op/go-logging"
)

type Config struct {
	// Health check interval in seconds
	CheckInterval time.Duration
	// Number of failed health checks before restarting
	UnhealthyThreshold int
	// How long to wait for a service to become healthy after restart
	RestartWaitTime time.Duration
	// Docker socket path
	DockerSocket string
	// Health check endpoint
	HealthEndpoint string
	// Port on which services expose health checks
	HealthPort int
	// Node ID/name for this resilience manager instance
	NodeID string
	// Total number of resilience manager nodes in the cluster
	TotalNodes int
}

func CreateDefaultConfig(log *logging.Logger) *Config {
	checkInterval := getEnvInt("RESILIENCE_CHECK_INTERVAL", 5, log)
	unhealthyThreshold := getEnvInt("RESILIENCE_UNHEALTHY_THRESHOLD", 3, log)
	restartWait := getEnvInt("RESILIENCE_RESTART_WAIT", 20, log)
	healthPort := getEnvInt("RESILIENCE_HEALTH_PORT", 8081, log)
	totalNodes := getEnvInt("RESILIENCE_TOTAL_NODES", 1, log)

	// Get Docker socket path from env or use default
	dockerSocket := getEnvString("DOCKER_SOCKET", "/var/run/docker.sock", log)
	healthEndpoint := getEnvString("HEALTH_ENDPOINT", "/ping", log)

	// Node identity is required when running in a multi-node setup (totalNodes > 1)
	nodeID := getEnvString("RESILIENCE_NODE_ID", "node-0", log)
	if totalNodes > 1 && os.Getenv("RESILIENCE_NODE_ID") == "" {
		log.Critical("RESILIENCE_NODE_ID environment variable is required when running with multiple resilience manager nodes")
	}

	return &Config{
		CheckInterval:      time.Duration(checkInterval) * time.Second,
		UnhealthyThreshold: unhealthyThreshold,
		RestartWaitTime:    time.Duration(restartWait) * time.Second,
		DockerSocket:       dockerSocket,
		HealthEndpoint:     healthEndpoint,
		HealthPort:         healthPort,
		NodeID:             nodeID,
		TotalNodes:         totalNodes,
	}
}

// Helper function to get environment variable as int with default value
func getEnvInt(key string, defaultValue int, log *logging.Logger) int {
	valStr := os.Getenv(key)
	if valStr == "" {
		return defaultValue
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		log.Warningf("Invalid value for %s: %s, using default: %d", key, valStr, defaultValue)
		return defaultValue
	}

	return val
}

// Helper function to get environment variable as string with default value
func getEnvString(key string, defaultValue string, log *logging.Logger) string {
	val := os.Getenv(key)
	if val == "" {
		return defaultValue
	}
	return val
}
