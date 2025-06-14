package main

import (
	"context"
	"fmt"
	"github.com/op/go-logging"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"tp1/health"
	"tp1/server/resilience_manager/internal"
)

var logger = logging.MustGetLogger("resilience_manager")

func main() {
	logger.Info("Starting...")

	// Start health server
	healthSrv := health.New(logger)
	healthSrv.Start()

	// Create configuration
	config := internal.CreateDefaultConfig(logger)

	// Create resilience manager
	manager, err := internal.NewResilienceManager(config, logger)
	if err != nil {
		logger.Fatalf("Failed to create resilience manager: %v", err)
	}

	// Create context that can be canceled
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Register services to monitor
	registerServices(manager)

	// Start resilience manager
	manager.Start(ctx)

	healthSrv.MarkReady()

	// Wait for termination signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down...")
	cancel()
	manager.Stop()
}

func registerServices(manager *internal.ResilienceManager) {
	registeredServices := make(map[string]bool)

	serviceTypes := []string{"FILTER", "JOINER", "AGGREGATOR", "CONTROLLER", "REPORT"}

	for _, serviceType := range serviceTypes {
		registerServiceType(manager, serviceType, registeredServices)
	}

	logger.Infof("Registered %d services for monitoring", len(registeredServices))
}

func registerServiceType(manager *internal.ResilienceManager, serviceType string, registered map[string]bool) {
	serviceCount := 0
	maxAttempts := 100 // Reasonable upper limit to avoid infinite loop

	// Iterate through sequentially numbered environment variables
	for i := 1; i <= maxAttempts; i++ {
		// Construct environment variable name
		envVar := fmt.Sprintf("MONITOR_SERVICE_%s_%d", serviceType, i)
		containerName := os.Getenv(envVar)

		// Stop when we don't find the next numbered service
		if containerName == "" {
			// Only log if we expected to find services
			if i == 1 {
				logger.Debugf("No %s services found", strings.ToLower(serviceType))
			}
			break
		}

		// Service name used within resilience manager
		serviceName := fmt.Sprintf("%s_%d", strings.ToLower(serviceType), i)

		// Register service
		manager.RegisterService(serviceName, containerName, containerName)
		registered[serviceName] = true
		serviceCount++
		logger.Infof("Registered %s service: %s (container: %s)",
			strings.ToLower(serviceType), serviceName, containerName)
	}

	if serviceCount > 0 {
		logger.Infof("Discovered %d %s services", serviceCount, strings.ToLower(serviceType))
	}
}
