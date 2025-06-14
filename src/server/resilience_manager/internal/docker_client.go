package internal

import (
	"context"
	"fmt"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/client"
	"github.com/op/go-logging"
)

// DockerClient manages container operations using Docker API
type DockerClient struct {
	client *client.Client
	log    *logging.Logger
	config *Config
}

// NewDockerClient creates a new Docker client
func NewDockerClient(config *Config, log *logging.Logger) (*DockerClient, error) {
	// Set API version explicitly to avoid version compatibility issues
	cli, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithHost("unix://"+config.DockerSocket),
		client.WithVersion("1.48"))
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	// Verify Docker API connection
	_, err = cli.Ping(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Docker daemon: %w", err)
	}

	log.Info("Successfully connected to Docker daemon")

	return &DockerClient{
		client: cli,
		log:    log,
		config: config,
	}, nil
}

func (d *DockerClient) RestartContainer(ctx context.Context, containerName string) error {

	f := filters.NewArgs()
	f.Add("name", containerName)

	containers, err := d.client.ContainerList(ctx, container.ListOptions{
		Filters: f,
		All:     true, // Include stopped containers
	})
	if err != nil {
		return fmt.Errorf("failed to list containers: %w", err)
	}

	if len(containers) == 0 {
		d.log.Errorf("Container %s not found", containerName)
		return fmt.Errorf("container %s not found", containerName)
	}

	containerID := containers[0].ID
	d.log.Infof("Found container %s with ID %s, restarting...", containerName, containerID)

	// Check if container is running
	isRunning := containers[0].State == "running" // todo: me parece que no se puede usar docker para ver el estado

	if !isRunning {
		// If container is stopped, start it
		d.log.Infof("Container %s is not running, starting it...", containerName)
		err = d.client.ContainerStart(ctx, containerID, container.StartOptions{})
		if err != nil {
			return fmt.Errorf("failed to start container %s: %w", containerName, err)
		}
	} else {
		// If container is running, restart it
		timeoutSeconds := int(d.config.RestartWaitTime.Seconds())
		opts := container.StopOptions{Timeout: &timeoutSeconds}
		err = d.client.ContainerRestart(ctx, containerID, opts)
		if err != nil {
			return fmt.Errorf("failed to restart container %s: %w", containerName, err)
		}
	}

	d.log.Infof("Container %s restarted successfully", containerName)

	return nil
}

// Close closes the Docker client
func (d *DockerClient) Close() error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}
