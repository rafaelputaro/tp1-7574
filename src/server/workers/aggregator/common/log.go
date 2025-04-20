package common

import (
	"os"

	"github.com/op/go-logging"
)

var Log = logging.MustGetLogger("aggregator")

func InitLogger() {
	format := logging.MustStringFormatter(
		`%{level:.5s} | %{shortfunc} | %{message}`,
	)
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}
