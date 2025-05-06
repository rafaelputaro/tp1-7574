package globalconfig

const (
	RatingsExchange = "ratings_exchange"
	CreditsExchange = "credits_exchange"
	Movies1Queue    = "movies1"
	Movies2Queue    = "movies2"
	Movies3Queue    = "movies3"
	Report1Queue    = "movies_report"
	Report2Queue    = "top_5_report"
	Report3Queue    = "top_and_bottom_report"
	Report4Queue    = "top_10_report"
	Report5Queue    = "metrics_report"
)

var MoviesQueues = []string{Movies1Queue, Movies2Queue, Movies3Queue}
var Exchanges = []string{RatingsExchange, CreditsExchange}
var ReportQueues = []string{Report1Queue, Report2Queue, Report3Queue, Report4Queue, Report5Queue}
