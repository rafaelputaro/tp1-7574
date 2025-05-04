package globalconfig

const (
	RatingsExchange = "ratings_exchange"
	CreditsExchange = "credits_exchange"
	Movies1Queue    = "movies1"
	Movies2Queue    = "movies2"
	Movies3Queue    = "movies3"
)

var MoviesQueues = []string{Movies1Queue, Movies2Queue, Movies3Queue}
var Exchanges = []string{RatingsExchange, CreditsExchange}
