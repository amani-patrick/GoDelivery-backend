// Command healthcheck is a minimal container health probe for the scratch
// runtime image. The production image contains no shell, no wget and no
// libc — Docker's HEALTHCHECK needs an executable it can run directly, and
// this ~2 MB static binary is it.
//
// It GETs /health on the configured port and exits 0 when the server reports
// ok/degraded (the server returns 503 when a dependency is down — that still
// means the process itself is up; Kubernetes-style liveness vs readiness can
// be layered later via the exit code).
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	client := &http.Client{Timeout: 3 * time.Second}
	url := fmt.Sprintf("http://127.0.0.1:%s/health", port)

	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	// 200 = all dependencies reachable. 503 = process alive but a dependency
	// is down — treat as healthy for liveness (restarting the container will
	// not fix Postgres), which matches the compose healthcheck semantics.
	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusServiceUnavailable {
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "healthcheck: unexpected status %d\n", resp.StatusCode)
	os.Exit(1)
}
