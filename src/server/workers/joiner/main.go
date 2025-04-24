package main

import (
	"os"
	"sync"
	"tp1/server/workers/joiner/common"
)

const MSG_ERROR_CREATE_JOINER = "Error on creating joiner"

/*
This module builds the full input's queue name based on the name in the environment
variables plus the joiner ID. Example: "ar_movies_2000_and_later" + "_shard_" <JOINER_ID>
Environment variables:
JOINER_ID;
JOINER_TYPE: "group_by_movie_id_ratings" "group_by_movie_id_credits";
JOINER_INPUT_QUEUE_BASE_NAME: movies queue name. Example: ar_movies_2000_and_later;
JOINER_INPUT_QUEUE_SEC_NAME: ratings or credits;
JOINER_OUTPUT_QUEUE_NAME: "actor_movies_count" or "movies_top_and_bottom" ;
*/
func main() {
	common.Log.Info("Starting joiner...")
	common.InitLogger()
	var joiner, err = common.NewJoiner(common.Log)
	if err != nil {
		common.Log.Infof("%v: %v", MSG_ERROR_CREATE_JOINER, err)
		os.Exit(1)
	} else {
		common.Log.Infof("Joiner type: %s | Joiner ID: %s\n",
			joiner.Config.JoinerType,
			joiner.Config.ID,
		)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			joiner.Start()
		}()
		// Wait for go routine to finish TODO: or SIGKILL signals
		wg.Wait()
		joiner.Dispose()
	}
}
