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
	InputQueue     string
	InputQueueSec  string
	OutputQueue    string
}

// Returns new aggregator config
func NewAggregatorConfig(
	id string,
	aggregator_type string,
	amountSources uint32,
	inputQueue string,
	inputQueueSec string,
	outputQueue string) *AggregatorConfig {
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
	v.BindEnv("input_queue", "name")
	// input queue secondary
	v.BindEnv("input_queue_sec", "name")
	// output queue
	v.BindEnv("output_queue", "name")
	id := v.GetString("id")
	aggregatorType := v.GetString("type")
	amountSources := v.GetUint32("amount_sources")
	inputQueue := v.GetString("input_queue.name")
	var inputQueueSec string
	if aggregatorType == METRICS {
		inputQueueSec = v.GetString("input_queue_sec.name")
	} else {
		inputQueueSec = ""
	}
	outputQueue := v.GetString("output_queue.name")
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

func (config *AggregatorConfig) LogConfig(log *logging.Logger) {
	log.Debugf("ID: %v | AggregatorType: %v | AmountSources: %v",
		config.ID,
		config.AggregatorType,
		config.AmountSources,
	)
	log.Debugf("InputQueue: %v", config.InputQueue)
	if config.AggregatorType == METRICS {
		log.Debugf("InputQueueSec: %v", config.InputQueueSec)
	}
	log.Debugf("OutputQueue: %v", config.OutputQueue)
}
