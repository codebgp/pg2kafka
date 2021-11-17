package healthcheck

var (
	// NeverFailHealthCheck empty health check function that will never error
	NeverFailHealthCheck = func() error { return nil }
)

// EnableProvider enables the provider of healthcheck data
func EnableProvider(checker Checker, done <-chan struct{}) <-chan error {
	return EnableHTTPProvider(checker, done)
}
