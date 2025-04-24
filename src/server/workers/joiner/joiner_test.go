package main

import (
	"fmt"
	"os"
	"testing"
	"tp1/server/workers/joiner/common"
)

func TestJoinerConfig(t *testing.T) {
	common.Log.Infof("Testing joiner")
	common.Log.Info("Starting joiner...")
	fmt.Println("Testing Joiner")
	common.InitLogger()
	// configuration
	const JOINER_ID string = "1"
	const JOINER_TYPE string = "group_by_movie_id_ratings"
	const JOINER_INPUT_QUEUE_BASE_NAME = "ar_movies_2000_and_later"
	const JOINER_INPUT_QUEUE_SEC_NAME = "ratings"
	const JOINER_OUTPUT_QUEUE_NAME = "movies_top_and_bottom"
	// expected:
	const INPUT_QUEUE_NAME = "ar_movies_2000_and_later_shard_1"
	const INPUT_QUEUES_EXCHANGE = "joiner_input_exchange_1"
	//set env
	os.Setenv("JOINER_ID", JOINER_ID)
	os.Setenv("JOINER_TYPE", JOINER_TYPE)
	os.Setenv("JOINER_INPUT_QUEUE_BASE_NAME", JOINER_INPUT_QUEUE_BASE_NAME)
	os.Setenv("JOINER_INPUT_QUEUE_SEC_NAME", JOINER_INPUT_QUEUE_SEC_NAME)
	os.Setenv("JOINER_OUTPUT_QUEUE_NAME", JOINER_OUTPUT_QUEUE_NAME)
	config := common.LoadJoinerConfig()
	if config.ID != JOINER_ID || config.JoinerType != JOINER_TYPE || config.InputQueueName != INPUT_QUEUE_NAME ||
		config.InputQueueSecName != JOINER_INPUT_QUEUE_SEC_NAME || config.OutputQueueName != JOINER_OUTPUT_QUEUE_NAME ||
		config.InputQueuesExchange != INPUT_QUEUES_EXCHANGE {
		t.Fatal("Error on config joiner")
	}
	fmt.Printf("%v", config)
}
