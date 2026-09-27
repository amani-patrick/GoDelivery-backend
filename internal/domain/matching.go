package domain

import "time"

// ── MatchConfig ───────────────────────────────────────────────────────────────

// MatchConfig is the runtime-configurable parameter set for the matching engine.
type MatchConfig struct {
	// score = (ETA_minutes × WeightTime) + (road_km × WeightDistance) + (fuel_cost × WeightFuel)
	WeightTime     float64
	WeightDistance float64
	WeightFuel     float64 // fuel cost contribution to score (task 8)

	// Tier-1 coarse GEOSEARCH radius by vehicle class.
	Tier1RadiusKm map[VehicleType]float64

	// Maximum allowed road distance from driver to pickup (anti-deadheading).
	MaxDeadheadKm map[VehicleType]float64

	// Fuel consumption in litres per kilometre, per vehicle type (task 8).
	FuelLitresPerKm map[VehicleType]float64

	// Fuel price in Rwandan Francs per litre (task 8).
	// Source: Rwanda Energy Group published pump price.
	FuelPriceRWFPerLitre float64

	// Highway-passing paradox penalty parameters.
	HeadingPenaltyThresholdDeg float64
	HeadingPenaltyMinSpeedKmh  float64
	HeadingPenaltyMultiplier   float64

	// BatchWindowMs: order queue hold time before matching batch runs.
	BatchWindowMs int

	// RedispatchTimeoutMs: how long to wait for the winner to accept before
	// offering to the next ranked candidate. (task 1)
	RedispatchTimeoutMs int

	// OSRMTimeoutMs is the per-request HTTP timeout for the OSRM Table API.
	OSRMTimeoutMs int

	// MaxStackDelayMinutes: maximum additional delay allowed on a stacked
	// order before the existing customer's ETA (task 4).
	MaxStackDelayMinutes float64

	// MaxStackDirectionDeg: maximum angular divergence between two dropoff
	// vectors before the engine rejects a stack (dot-product gate, task 4).
	MaxStackDirectionDeg float64

	// PremiumScoreBoost: score reduction applied to premium delivery winners
	// so they beat standard candidates in the batch (task 7). Lower = better.
	PremiumScoreBoost float64
}

// ── ScoredCandidate ───────────────────────────────────────────────────────────

// ScoredCandidate is the output of the scoring stage for a single driver.
type ScoredCandidate struct {
	DispatchCandidate

	ETASeconds  float64 `json:"eta_seconds"`
	RoadDistKm  float64 `json:"road_dist_km"`
	FuelCostRWF float64 `json:"fuel_cost_rwf"` // task 8

	SpeedKmh float64 `json:"speed_kmh"`
	Bearing  float64 `json:"bearing"`

	ETAMinutes       float64 `json:"eta_minutes"`
	HeadingPenalised bool    `json:"heading_penalised"`
	Score            float64 `json:"score"` // lower = better
}

// ── MatchResult ───────────────────────────────────────────────────────────────

// MatchResult is the final output of the matching engine for a single delivery.
type MatchResult struct {
	DeliveryID string            `json:"delivery_id"`
	Winner     *ScoredCandidate  `json:"winner"`
	Ranked     []ScoredCandidate `json:"ranked"`
	OSRMUsed   bool              `json:"osrm_used"`
	ComputedAt time.Time         `json:"computed_at"`
}

// ── BatchOrder ────────────────────────────────────────────────────────────────

// BatchOrder is the minimal record queued in Redis during the batch window.
type BatchOrder struct {
	DeliveryID          string      `json:"delivery_id"`
	PickupLat           float64     `json:"pickup_lat"`
	PickupLng           float64     `json:"pickup_lng"`
	DropoffLat          float64     `json:"dropoff_lat"` // needed for stacking geometry
	DropoffLng          float64     `json:"dropoff_lng"`
	WeightKg            float64     `json:"weight_kg"`
	VehicleTypeRequired VehicleType `json:"vehicle_type_required"`
	IsPremium           bool        `json:"is_premium"` // task 7
	EnqueuedAt          time.Time   `json:"enqueued_at"`
}

// ── HeadingState ─────────────────────────────────────────────────────────────

// HeadingState carries the latest motion vector for a driver.
type HeadingState struct {
	SpeedKmh float64
	Bearing  float64
}

// ── StackRequest ─────────────────────────────────────────────────────────────

// StackRequest represents a candidate stacking opportunity: an ON_TRIP driver
// whose current route passes near a new pickup point.
// The engine evaluates whether the incremental delay to the existing customer
// and the direction divergence between the two dropoffs are acceptable.
type StackRequest struct {
	// New order being evaluated for stacking
	NewDeliveryID  string
	NewPickupLat   float64
	NewPickupLng   float64
	NewDropoffLat  float64
	NewDropoffLng  float64
	NewWeightKg    float64

	// Driver already on a trip
	DriverID        string
	ActiveDeliveryID string

	// Existing customer's committed dropoff (for SLA delay calculation)
	ExistingDropoffLat float64
	ExistingDropoffLng float64

	// Driver's current position
	DriverLat float64
	DriverLng float64
}

// StackEvaluation is the result of evaluating a StackRequest.
type StackEvaluation struct {
	Feasible             bool    `json:"feasible"`
	IncrementalDelayMin  float64 `json:"incremental_delay_min"` // added to existing customer ETA
	DirectionAngleDeg    float64 `json:"direction_angle_deg"`   // angle between two dropoff vectors
	RejectReason         string  `json:"reject_reason,omitempty"`
}

// ── Trust / reputation types (task 9) ─────────────────────────────────────────

// TrustLevel classifies an actor's current trust standing.
type TrustLevel string

const (
	TrustLevelGood       TrustLevel = "GOOD"
	TrustLevelWatch      TrustLevel = "WATCH"       // elevated monitoring, no block
	TrustLevelRestricted TrustLevel = "RESTRICTED"  // limited functionality
	TrustLevelBanned     TrustLevel = "BANNED"       // blocked from platform
)

// TrustSignal is a single fraud or quality event recorded against an actor.
type TrustSignal struct {
	ActorID    string    `json:"actor_id"`
	ActorType  string    `json:"actor_type"` // "DRIVER" | "MERCHANT" | "CUSTOMER"
	SignalType string    `json:"signal_type"`
	Severity   int       `json:"severity"` // 1 (minor) – 5 (critical)
	Details    string    `json:"details"`
	CreatedAt  time.Time `json:"created_at"`
}

// Defined TrustSignal types — used as SignalType values.
const (
	SignalHandshakeFail     = "HANDSHAKE_FAIL"      // QR/PIN mismatch attempt
	SignalWeightFraud       = "WEIGHT_FRAUD"         // declared vs actual weight mismatch
	SignalPhantomCargo      = "PHANTOM_CARGO"        // repeated deliveries to same recipient
	SignalClaimNeverReceived = "CLAIM_NEVER_RECEIVED" // customer denied receipt post-PIN
	SignalSLABreach         = "SLA_BREACH"           // driver consistently late vs OSRM ETA
	SignalMultiAppShadow    = "MULTI_APP_SHADOW"      // very slow vs predicted ETA pattern
	SignalDisputeRaised     = "DISPUTE_RAISED"        // delivery reached DISPUTED state
)
