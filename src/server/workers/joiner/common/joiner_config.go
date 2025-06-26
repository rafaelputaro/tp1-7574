package common

import (
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

type JoinerConfig struct {
	ID                string
	JoinerType        string
	InputQueueName    string
	InputQueueSecName string
	OutputQueueName   string
}

func NewJoinerConfig(id string, joiner_type string, inputQueueName string, inputQueueSecName string, outputQueueName string) *JoinerConfig {
	config := &JoinerConfig{
		ID:                id,
		JoinerType:        joiner_type,
		InputQueueName:    inputQueueName,
		InputQueueSecName: inputQueueSecName,
		OutputQueueName:   outputQueueName,
	}
	return config
}

// Read configuration environment
func LoadJoinerConfig() *JoinerConfig {
	v := viper.New()
	v.AutomaticEnv()
	v.SetEnvPrefix("joiner")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.BindEnv("type")
	v.BindEnv("id")
	// input queue
	v.BindEnv("input_queue", "name")
	// input queue secondary
	v.BindEnv("input_queue_sec", "name")
	// output queue
	v.BindEnv("output_queue", "name")
	// get values
	id := v.GetString("id")
	joinerType := v.GetString("type")
	inputQueue := v.GetString("input_queue.name")
	inputQueueSec := v.GetString("input_queue_sec.name")
	outputQueue := v.GetString("output_queue.name")
	var config = NewJoinerConfig(id, joinerType, inputQueue, inputQueueSec, outputQueue)
	return config
}

func (config *JoinerConfig) LogConfig(log *logging.Logger) {
	log.Debugf("ID: %v | JoinerType: %v",
		config.ID,
		config.JoinerType,
	)
	log.Debugf("InputQueue: %v", config.InputQueueName)
	log.Debugf("InputQueueSec: %v", config.InputQueueSecName)
	log.Debugf("OutputQueue: %v", config.OutputQueueName)
}
