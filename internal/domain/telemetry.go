package domain

import "time"

// ── Telemetry value objects ───────────────────────────────────────────────────

// TelemetryFrame is a single GPS observation emitted by a driver's device.
// It is the raw ingest unit processed by the tracking module.
type TelemetryFrame struct {
	DeliveryID string    `json:"delivery_id"`
	DriverID   string    `json:"driver_id"`
	Lat        float64   `json:"lat"`
	Lng        float64   `json:"lng"`
	SpeedKmh   float64   `json:"speed_kmh"`
	Bearing    float64   `json:"bearing"`  // compass degrees 0–360
	Accuracy   float32   `json:"accuracy_m"`
	CapturedAt time.Time `json:"captured_at"`
}

// PanicPayload is the ultra-lightweight emergency structure emitted by the
// client's silent panic bridge. It carries the last N frames to help
// dispatchers locate the driver during a robbery or assault event.
type PanicPayload struct {
	DeliveryID    string           `json:"delivery_id"`
	DriverID      string           `json:"driver_id"`
	RecentFrames  []TelemetryFrame `json:"recent_frames"`
	TriggeredAt   time.Time        `json:"triggered_at"`
	DeviceBattery float32          `json:"device_battery_pct"`
}

// AnomalyAlert is raised by the telemetry anomaly detector and forwarded
// to the dispatcher's notification channel.
type AnomalyAlert struct {
	DeliveryID   string    `json:"delivery_id"`
	DriverID     string    `json:"driver_id"`
	AlertType    string    `json:"alert_type"` // "STATIONARY" | "ROUTE_DEVIATION" | "PANIC"
	LastKnownLat float64   `json:"last_known_lat"`
	LastKnownLng float64   `json:"last_known_lng"`
	DetectedAt   time.Time `json:"detected_at"`
	Details      string    `json:"details"`
}

// AnomalyThresholds are the runtime-configurable detection parameters.
// They are loaded from environment variables and injected at startup.
type AnomalyThresholds struct {
	// StationaryMinutes: alert if a driver on an active trip is stationary
	// for longer than this duration at a non-designated location.
	StationaryMinutes int `json:"stationary_minutes"`
	// RouteDeviationMeters: alert if the driver's position deviates more than
	// this distance from the planned OSRM route polyline.
	RouteDeviationMeters float64 `json:"route_deviation_meters"`
	// PanicBufferSize: number of recent frames to include in a panic payload.
	// At 1 frame per 10 seconds, 30 frames ≈ 5 minutes of history.
	PanicBufferSize int `json:"panic_buffer_size"`
}

// DefaultThresholds provides safe production defaults tuned for Kigali road conditions.
var DefaultThresholds = AnomalyThresholds{
	StationaryMinutes:    5,
	RouteDeviationMeters: 300,
	PanicBufferSize:      30,
}
