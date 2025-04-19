package common

import (
	"strings"

	"github.com/spf13/viper"
)

type AggregatorConfig struct {
	ID             string
	AggregatorType string
	AmqUrl         string
	AmountSources  uint32
	InputQueue     QueueConfig
	OutputQueue    QueueConfig
	// TODO Probablemente una cola de control
}

// Returns new aggregator config
func NewAggregatorConfig(
	id string,
	aggregator_type string,
	amqUrl string,
	amountSources uint32,
	inputQueue QueueConfig,
	outputQueue QueueConfig) *AggregatorConfig {
	config := &AggregatorConfig{
		ID:             id,
		AggregatorType: aggregator_type,
		AmqUrl:         amqUrl,
		AmountSources:  amountSources,
		InputQueue:     inputQueue,
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
	v.BindEnv("amqp_url")
	v.BindEnv("amount_sources")
	// input queue
	v.BindEnv("input_queue", "delete_when_unused")
	v.BindEnv("input_queue", "durable")
	v.BindEnv("input_queue", "exclusive")
	v.BindEnv("input_queue", "name")
	v.BindEnv("input_queue", "no_wait")
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
		var config *AggregatorConfig = NewAggregatorConfig(
			v.GetString("id"),
			v.GetString("type"),
			v.GetString("amqp_url"),
			v.GetUint32("amount_sources"),
			*NewQueueConfig(
				v.GetBool("input_queue.delete_when_unused"),
				v.GetBool("input_queue.durable"),
				v.GetBool("input_queue.exclusive"),
				v.GetString("input_queue.name"),
				v.GetBool("input_queue.no_wait"),
			),
			*NewQueueConfig(
				v.GetBool("input_queue.delete_when_unused"),
				v.GetBool("input_queue.durable"),
				v.GetBool("input_queue.exclusive"),
				v.GetString("input_queue.name"),
				v.GetBool("input_queue.no_wait"),
			),
		)
		return config, nil
	}
}
