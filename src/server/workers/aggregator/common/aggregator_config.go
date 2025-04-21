package common

import (
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type AggregatorConfig struct {
	ID             string
	AggregatorType string
	AmountSources  uint32
	InputQueue     QueueConfig
	InputQueueSec  QueueConfig
	OutputQueue    QueueConfig
	// TODO Probablemente una cola de control
}

// Returns new aggregator config
func NewAggregatorConfig(
	id string,
	aggregator_type string,
	amountSources uint32,
	inputQueue QueueConfig,
	inputQueueSec QueueConfig,
	outputQueue QueueConfig) *AggregatorConfig {
	config := &AggregatorConfig{
		ID:             id,
		AggregatorType: aggregator_type,
		AmountSources:  amountSources,
		InputQueue:     inputQueue,
		InputQueueSec:  inputQueueSec,
		OutputQueue:    outputQueue,
	}
	return config
}

// Read configuration from config.yaml or environment
func LoadAggregatorConfig() (*AggregatorConfig, error) {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvPrefix("aggregator")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.BindEnv("type")
	v.BindEnv("id")
	v.BindEnv("amount_sources")
	// input queue
	v.BindEnv("input_queue", "delete_when_unused")
	v.BindEnv("input_queue", "durable")
	v.BindEnv("input_queue", "exclusive")
	v.BindEnv("input_queue", "name")
	v.BindEnv("input_queue", "no_wait")
	// input queue secondary
	v.BindEnv("input_queue_sec", "delete_when_unused")
	v.BindEnv("input_queue_sec", "durable")
	v.BindEnv("input_queue_sec", "exclusive")
	v.BindEnv("input_queue_sec", "name")
	v.BindEnv("input_queue_sec", "no_wait")
	// output queue
	v.BindEnv("output_queue", "delete_when_unused")
	v.BindEnv("output_queue", "durable")
	v.BindEnv("output_queue", "exclusive")
	v.BindEnv("output_queue", "name")
	v.BindEnv("output_queue", "no_wait")
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		return nil, err
	} else {
		id := v.GetString("id")
		aggregatorType := v.GetString("type")
		amountSources := v.GetUint32("amount_sources")
		inputQueue := *NewQueueConfig(
			v.GetBool("input_queue.delete_when_unused"),
			v.GetBool("input_queue.durable"),
			v.GetBool("input_queue.exclusive"),
			v.GetString("input_queue.name"),
			v.GetBool("input_queue.no_wait"),
		)
		var inputQueueSec QueueConfig
		if aggregatorType == METRICS {
			inputQueueSec = *NewQueueConfig(
				v.GetBool("input_queue_sec.delete_when_unused"),
				v.GetBool("input_queue_sec.durable"),
				v.GetBool("input_queue_sec.exclusive"),
				v.GetString("input_queue_sec.name"),
				v.GetBool("input_queue_sec.no_wait"),
			)
		} else {
			inputQueueSec = *DummyQueueConfig()
		}
		outputQueue := *NewQueueConfig(
			v.GetBool("output_queue.delete_when_unused"),
			v.GetBool("output_queue.durable"),
			v.GetBool("output_queue.exclusive"),
			v.GetString("output_queue.name"),
			v.GetBool("output_queue.no_wait"),
		)
		var config *AggregatorConfig = NewAggregatorConfig(
			id,
			aggregatorType,
			amountSources,
			inputQueue,
			inputQueueSec,
			outputQueue,
		)
		return config, nil
	}
}

func (config *AggregatorConfig) LogConfig(log *logging.Logger) {
	log.Debugf("ID: %v | AggregatorType: %v | AmountSources: %v",
		config.ID,
		config.AggregatorType,
		config.AmountSources,
	)
	log.Debugf("InputQueue: %v", config.InputQueue.ToString())
	if config.AggregatorType == METRICS {
		log.Debugf("InputQueueSec: %v", config.InputQueueSec.ToString())
	}
	log.Debugf("OutputQueue: %v", config.OutputQueue.ToString())
}
