package globalconfig

import "fmt"

const (
	RatingsQueue = "ratings"
	CreditsQueue = "credits"
	Movies1Queue = "movies1"
	Movies2Queue = "movies2"
	Movies3Queue = "movies3"
	Report1Queue = "movies_report"
	Report2Queue = "top_5_report"
	Report3Queue = "top_and_bottom_report"
	Report4Queue = "top_10_report"
	Report5Queue = "metrics_report"

	CoordinationExchange = "coordination_exchange"
)

var MoviesQueues = []string{Movies1Queue, Movies2Queue, Movies3Queue}
var ReportQueues = []string{Report1Queue, Report2Queue, Report3Queue, Report4Queue, Report5Queue}

func GetShardedQueueName(baseQueue string, shards int64, key int64) string {
	shard := (key % shards) + 1
	return fmt.Sprintf("%s_%d", baseQueue, shard)
}

func GetAllShardedQueueNames(baseQueue string, shards int64) []string {
	result := make([]string, shards)
	for i := int64(0); i < shards; i++ {
		result[i] = fmt.Sprintf("%s_%d", baseQueue, i+1)
	}
	return result
}
