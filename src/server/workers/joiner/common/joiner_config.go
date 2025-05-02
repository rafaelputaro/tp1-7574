package common

import (
	"fmt"
	"strings"

	"github.com/op/go-logging"
	"github.com/spf13/viper"
)

const INPUT_EXCHANGE_BASE_NAME = "joiner_input_exchange"

type JoinerConfig struct {
	ID                  string
	JoinerType          string
	InputQueueName      string
	InputQueueSecName   string
	OutputQueueName     string
	InputQueuesExchange string
}

// Returns new joiner config
func NewJoinerConfig(
	id string,
	joiner_type string,
	inputQueueName string,
	inputQueueSecName string,
	outputQueueName string,
	inputQueuesExchange string) *JoinerConfig {
	config := &JoinerConfig{
		ID:                  id,
		JoinerType:          joiner_type,
		InputQueueName:      inputQueueName,
		InputQueueSecName:   inputQueueSecName,
		OutputQueueName:     outputQueueName,
		InputQueuesExchange: inputQueuesExchange,
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
	v.BindEnv("input_queue_sec", "name")
	// output queue
	v.BindEnv("output_queue", "name")
	// get values
	id := v.GetString("id")
	joinerType := v.GetString("type")
	inputQueue := getShardQueueName(v.GetString("input_queue.base_name"), id) // Esta usando el ID para calcular el shard. En el compose el ID es 2 a pesar de que el shard es 1.
	inputQueueSec := v.GetString("input_queue_sec.name")
	outputQueue := v.GetString("output_queue.name")
	inputQueuesExchange := getExchangeName(id)
	var config *JoinerConfig = NewJoinerConfig(
		id,
		joinerType,
		inputQueue,
		inputQueueSec,
		outputQueue,
		inputQueuesExchange,
	)
	return config
}

func getExchangeName(id string) string {
	return fmt.Sprintf("%s_%s", INPUT_EXCHANGE_BASE_NAME, id)
}

func getShardQueueName(baseName string, id string) string {
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
	log.Debugf("InputQueuesExchange: %v", config.InputQueuesExchange)
}
