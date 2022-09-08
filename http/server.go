package http

import (
	"context"
	"fmt"
	nhttp "net/http"
	"time"

	mux "github.com/gorilla/mux"
	"go.uber.org/zap"
)

// Route is an HTTP route
type Route struct {
	Path    string
	Method  string
	Handler nhttp.Handler
}

// Server is the app server implementation
type Server struct {
	id     string
	srv    *nhttp.Server
	router *mux.Router
	routes *[]Route
	logger *zap.Logger
}

// NewServer creates a new server
func NewServer(id string, l *zap.Logger, port int, routes *[]Route) (*Server, error) {
	r := mux.NewRouter()
	s := Server{
		id:     id,
		logger: l,
		srv: &nhttp.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: r,
		},
		router: r,
		routes: routes,
	}
	return &s, nil
}

func registerMuxRoutes(r *mux.Router, routes *[]Route) {
	for _, route := range *routes {
		r.Handle(route.Path, route.Handler).Methods(route.Method)
	}
}

// Run implements the custom server run logic
func (s *Server) Run(done <-chan struct{}) error {
	registerMuxRoutes(s.router, s.routes)

	var errors = make(chan error, 1)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil {
			errors <- err
		}
	}()

	s.logger.Info("HTTP Server started", zap.Any("id", s.id))

	select {
	case err := <-errors:
		return err
	case <-done:
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer func() {
			cancel()
			s.logger.Info("HTTP Server stopped", zap.Any("id", s.id))
		}()

		if err := s.srv.Shutdown(ctx); err != nil {
			return err
		}
		return nil
	}
}
