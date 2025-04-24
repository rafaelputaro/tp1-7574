package common

import (
	"fmt"
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

// Returns new joiner config
func NewJoinerConfig(
	id string,
	joiner_type string,
	inputQueueName string,
	inputQueueSecName string,
	outputQueueName string) *JoinerConfig {
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
	v.BindEnv("input_queue", "base_name")
	// input queue secondary
	v.BindEnv("input_queue_sec", "base_name")
	// output queue
	v.BindEnv("output_queue", "base_name")
	// get values
	id := v.GetString("id")
	joinerType := v.GetString("type")
	inputQueue := getQueueName(v.GetString("input_queue.base_name"), id)
	inputQueueSec := getQueueName(v.GetString("input_queue_sec.base_name"), id)
	outputQueue := getQueueName(v.GetString("output_queue.base_name"), id)
	var config *JoinerConfig = NewJoinerConfig(
		id,
		joinerType,
		inputQueue,
		inputQueueSec,
		outputQueue,
	)
	return config
}

func getQueueName(baseName string, id string) string {
	return fmt.Sprintf("%s_shard_%s", baseName, id)
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
