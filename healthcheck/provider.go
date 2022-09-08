package healthcheck

import "go.uber.org/zap"

var (
	// NeverFailHealthCheck empty health check function that will never error
	NeverFailHealthCheck = func() error { return nil }
)

// EnableProvider enables the provider of healthcheck data
func EnableProvider(logger *zap.Logger, checker Checker, done <-chan struct{}) <-chan error {
	return EnableHTTPProvider(logger, checker, done)
}
