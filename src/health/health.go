package health

import (
	"context"
	"errors"
	"github.com/op/go-logging"
	"io"
	"net/http"
	"sync/atomic"
)

// Server exposes /ping and tracks readiness.
type Server struct {
	httpSrv *http.Server
	ready   atomic.Bool
	logger  *logging.Logger
}

// New creates a health server bound to :8081
func New(l *logging.Logger) *Server {
	// todo add logger like in coordinator.go
	s := &Server{
		logger: l,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/ping", func(w http.ResponseWriter, r *http.Request) {
		if !s.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "pong") // todo log error if needed
	})

	s.httpSrv = &http.Server{Addr: ":8081", Handler: mux}
	return s
}

// Start runs ListenAndServe in a goroutine.
func (s *Server) Start() {
	s.logger.Infof("starting health server")
	go func() {
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// todo log, panic, or ignore
		}
	}()
}

// MarkReady flips readiness so /ping returns 200.
func (s *Server) MarkReady() {
	s.logger.Infof("marking health server as ready")
	s.ready.Store(true)
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.httpSrv.Shutdown(ctx)
}
