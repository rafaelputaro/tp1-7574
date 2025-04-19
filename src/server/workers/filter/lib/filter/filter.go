package filter

import (
	"tp1/rabbitmq"

	"github.com/op/go-logging"
)

type FilterConfig struct {
	Type string
	ID   int
}

type Filter struct {
	config FilterConfig
	log    *logging.Logger
}

func NewFilter(config *FilterConfig, log *logging.Logger) *Filter {
	return &Filter{
		config: *config,
		log:    log,
	}
}

func (f *Filter) StartFilterLoop() {
	f.log.Info("Connecting to RabbitMQ...")

	conn, err := rabbitmq.ConnectRabbitMQ(f.log)
	if err != nil {
		f.log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	f.log.Info("Successful connection with RabbitMQ")

	switch f.config.Type {
	case "2000s_filter":
		f.process2000sFilter()
	case "ar_es_filter":
		f.processArEsFilter()
	case "ar_filter":
		f.processArFilter()
	case "top-5-investors-filter":
		f.processTop5InvestorsFilter()
	default:
		f.log.Errorf("Unknown filter type: %s", f.config.Type)
	}
}

// Jobs --------------------------------------------------------------------------------------------

func (f *Filter) process2000sFilter() {
	f.log.Infof("[2000s_filter] Starting job for ID: %d", f.config.ID)
}

func (f *Filter) processArEsFilter() {
	f.log.Infof("[ar_es_filter] Starting job for ID: %d", f.config.ID)
}

func (f *Filter) processArFilter() {
	f.log.Infof("[ar_filter] Starting job for ID: %d", f.config.ID)
}

func (f *Filter) processTop5InvestorsFilter() {
	f.log.Infof("[top_5_investors_filter] Starting job for ID: %d", f.config.ID)
}
