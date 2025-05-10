package common

import (
	"os"

	"github.com/op/go-logging"
)

var Log = logging.MustGetLogger("joiner")

func InitLogger() {
	format := logging.MustStringFormatter(
		`%{message}`,
	)
	backend := logging.NewLogBackend(os.Stderr, "", 0)
	backendFormatter := logging.NewBackendFormatter(backend, format)
	logging.SetBackend(backendFormatter)
}
