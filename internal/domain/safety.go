package domain

import (
	"context"
	"time"
)

type DangerZone struct {
	ID             string    `json:"id"`
	CenterLat      float64   `json:"center_lat"`
	CenterLng      float64   `json:"center_lng"`
	RadiusMeters   float64   `json:"radius_meters"`
	ThreatLevel    float64   `json:"threat_level"`
	LastIncidentAt time.Time `json:"last_incident_at"`
}

type SafeHub struct {
	ID   string  `json:"id"`
	Name string  `json:"name"`
	Lat  float64 `json:"lat"`
	Lng  float64 `json:"lng"`
}

type SafetyRepository interface {
	// ReportIncident records a robbery/incident, creating or bumping a danger zone.
	ReportIncident(ctx context.Context, lat, lng float64) error
	// IsDangerZone checks if a coordinate falls inside any active danger zone.
	IsDangerZone(ctx context.Context, lat, lng float64) (bool, error)
	// FindNearestSafeHub returns the closest safe hub to a given coordinate.
	FindNearestSafeHub(ctx context.Context, lat, lng float64) (*SafeHub, error)
	// DecayThreatLevels reduces all threat levels by a factor and deletes zones below threshold.
	DecayThreatLevels(ctx context.Context, decayAmount float64) error
}
