// Package matching implements the two-tier driver dispatch pipeline.
// It is a pure application-layer package: it imports domain types and the
// tracking SpatialIndex, but never writes to any database directly.
package matching

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// ── OSRM Table API client ─────────────────────────────────────────────────────

// RouteMatrix is the result of a single OSRM Table API call.
// Index [i] corresponds to the i-th origin in the request.
type RouteMatrix struct {
	// ETAsSeconds[i] is the estimated travel time in seconds from origin i
	// to the single destination (the pickup point).
	ETAsSeconds []float64
	// DistancesM[i] is the road distance in metres from origin i to the destination.
	DistancesM []float64
}

// OSRMClient calls the self-hosted OSRM (or Valhalla) Table/Matrix API.
// It is intentionally thin — no retries, no circuit breakers — because the
// engine falls back to haversine ETAs when OSRM is unreachable.
type OSRMClient struct {
	baseURL    string
	httpClient *http.Client
	log        *slog.Logger
}

// NewOSRMClient constructs the client.
// timeoutMs is the per-request deadline in milliseconds.
func NewOSRMClient(baseURL string, timeoutMs int, log *slog.Logger) *OSRMClient {
	return &OSRMClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutMs) * time.Millisecond,
		},
		log: log,
	}
}

// osrmTableResponse is the JSON structure returned by the OSRM /table/v1 endpoint.
// We only decode what we need — durations and distances.
type osrmTableResponse struct {
	Code      string        `json:"code"`
	Durations [][]float64   `json:"durations"` // [sources][destinations]
	Distances [][]float64   `json:"distances"` // [sources][destinations]
}

// Table calls OSRM's /table/v1/driving endpoint with N sources (driver positions)
// and 1 destination (pickup point), returning travel times and road distances.
//
// Coordinate format: OSRM expects lon,lat (not lat,lon).
//
// The sources slice contains (lat, lng) pairs for each driver.
// The destination is the pickup point (lat, lng).
//
// Returns an error when OSRM is unreachable or returns a non-OK status code.
// The engine handles this error by falling back to haversine ETA estimation.
func (c *OSRMClient) Table(
	ctx context.Context,
	sources [][2]float64, // each element is [lat, lng]
	destLat, destLng float64,
) (*RouteMatrix, error) {
	if len(sources) == 0 {
		return &RouteMatrix{}, nil
	}

	// Build the OSRM coordinate string: lon,lat;lon,lat;...
	// OSRM uses longitude-first notation (GeoJSON standard).
	coords := make([]string, 0, len(sources)+1)
	for _, s := range sources {
		coords = append(coords, fmt.Sprintf("%.6f,%.6f", s[1], s[0])) // lng,lat
	}
	// Destination is appended last; its index = len(sources)
	destIdx := len(sources)
	coords = append(coords, fmt.Sprintf("%.6f,%.6f", destLng, destLat))

	// Build source indices: 0..N-1
	srcIdxList := make([]string, len(sources))
	for i := range sources {
		srcIdxList[i] = fmt.Sprintf("%d", i)
	}

	// Construct URL:
	// /table/v1/driving/{coords}?sources=0;1;2&destinations=N&annotations=duration,distance
	url := fmt.Sprintf(
		"%s/table/v1/driving/%s?sources=%s&destinations=%d&annotations=duration,distance",
		c.baseURL,
		strings.Join(coords, ";"),
		strings.Join(srcIdxList, ";"),
		destIdx,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("osrm: build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		c.log.Warn("OSRM Table API unreachable — engine will fall back to haversine",
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("osrm: http: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		c.log.Warn("OSRM Table API non-200 response",
			slog.Int("status", resp.StatusCode),
			slog.String("body", string(body)),
		)
		return nil, fmt.Errorf("osrm: status %d", resp.StatusCode)
	}

	var osrmResp osrmTableResponse
	if err := json.NewDecoder(resp.Body).Decode(&osrmResp); err != nil {
		return nil, fmt.Errorf("osrm: decode response: %w", err)
	}
	if osrmResp.Code != "Ok" {
		return nil, fmt.Errorf("osrm: response code %q", osrmResp.Code)
	}

	// Extract the single column (destination index = 0 in our destination slice)
	// from the durations and distances matrices.
	matrix := &RouteMatrix{
		ETAsSeconds: make([]float64, len(sources)),
		DistancesM:  make([]float64, len(sources)),
	}
	for i := range sources {
		if i < len(osrmResp.Durations) && len(osrmResp.Durations[i]) > 0 {
			matrix.ETAsSeconds[i] = osrmResp.Durations[i][0]
		}
		if i < len(osrmResp.Distances) && len(osrmResp.Distances[i]) > 0 {
			matrix.DistancesM[i] = osrmResp.Distances[i][0]
		}
	}
	return matrix, nil
}

// ── OSRM Nearest API 
type osrmNearestResponse struct {
	Code      string `json:"code"`
	Waypoints []struct {
		Distance float64   `json:"distance"` 
		Location []float64 `json:"location"` 
	} `json:"waypoints"`
}

type SnapResult struct {
	DistanceM float64  
	SnappedLat float64 
	SnappedLng float64 
}


func (c *OSRMClient) SnapToRoad(ctx context.Context, lat, lng float64) (*SnapResult, error) {
	url := fmt.Sprintf("%s/nearest/v1/driving/%.6f,%.6f?number=1",
		c.baseURL, lng, lat) // OSRM: lon,lat
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("osrm nearest: build request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("osrm nearest: http: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
		return nil, fmt.Errorf("osrm nearest: status %d: %s", resp.StatusCode, body)
	}
	var nr osrmNearestResponse
	if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
		return nil, fmt.Errorf("osrm nearest: decode: %w", err)
	}
	if nr.Code != "Ok" || len(nr.Waypoints) == 0 {
		return nil, fmt.Errorf("osrm nearest: code=%q", nr.Code)
	}
	wp := nr.Waypoints[0]
	result := &SnapResult{
		DistanceM:  wp.Distance,
		SnappedLng: wp.Location[0],
		SnappedLat: wp.Location[1],
	}
	return result, nil
}
