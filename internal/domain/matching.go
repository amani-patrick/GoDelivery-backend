package domain

import "time"


type MatchConfig struct {
	WeightTime     float64
	WeightDistance float64
	WeightFuel     float64 

	Tier1RadiusKm map[VehicleType]float64

	MaxDeadheadKm map[VehicleType]float64

	FuelLitresPerKm map[VehicleType]float64
	FuelPriceRWFPerLitre float64

	HeadingPenaltyThresholdDeg float64
	HeadingPenaltyMinSpeedKmh  float64
	HeadingPenaltyMultiplier   float64
	BatchWindowMs int

	RedispatchTimeoutMs int

	OSRMTimeoutMs int
	MaxStackDelayMinutes float64
	MaxStackDirectionDeg float64

	PremiumScoreBoost float64
}

type ScoredCandidate struct {
	DispatchCandidate

	ETASeconds  float64 `json:"eta_seconds"`
	RoadDistKm  float64 `json:"road_dist_km"`
	FuelCostRWF float64 `json:"fuel_cost_rwf"` 

	SpeedKmh float64 `json:"speed_kmh"`
	Bearing  float64 `json:"bearing"`

	ETAMinutes       float64 `json:"eta_minutes"`
	HeadingPenalised bool    `json:"heading_penalised"`
	Score            float64 `json:"score"` // lower = better
}

type MatchResult struct {
	DeliveryID string            `json:"delivery_id"`
	Winner     *ScoredCandidate  `json:"winner"`
	Ranked     []ScoredCandidate `json:"ranked"`
	OSRMUsed   bool              `json:"osrm_used"`
	ComputedAt time.Time         `json:"computed_at"`
}

type BatchOrder struct {
	DeliveryID          string      `json:"delivery_id"`
	PickupLat           float64     `json:"pickup_lat"`
	PickupLng           float64     `json:"pickup_lng"`
	DropoffLat          float64     `json:"dropoff_lat"`
	DropoffLng          float64     `json:"dropoff_lng"`
	WeightKg            float64     `json:"weight_kg"`
	VehicleTypeRequired VehicleType `json:"vehicle_type_required"`
	IsPremium           bool        `json:"is_premium"` 
	EnqueuedAt          time.Time   `json:"enqueued_at"`
	ReadyAt             time.Time   `json:"ready_at"`
}

type HeadingState struct {
	SpeedKmh float64
	Bearing  float64
}

type StackRequest struct {
	NewDeliveryID  string
	NewPickupLat   float64
	NewPickupLng   float64
	NewDropoffLat  float64
	NewDropoffLng  float64
	NewWeightKg    float64

	DriverID        string
	ActiveDeliveryID string

	ExistingDropoffLat float64
	ExistingDropoffLng float64

	DriverLat float64
	DriverLng float64
}
type StackEvaluation struct {
	Feasible             bool    `json:"feasible"`
	IncrementalDelayMin  float64 `json:"incremental_delay_min"` 
	DirectionAngleDeg    float64 `json:"direction_angle_deg"`  
	RejectReason         string  `json:"reject_reason,omitempty"`
}

// TrustLevel classifies an actor's current trust standing.
type TrustLevel string

const (
	TrustLevelGood       TrustLevel = "GOOD"
	TrustLevelWatch      TrustLevel = "WATCH"      
	TrustLevelRestricted TrustLevel = "RESTRICTED"  
	TrustLevelBanned     TrustLevel = "BANNED"       
)

// TrustSignal is a single fraud or quality event recorded against an actor.
type TrustSignal struct {
	ActorID    string    `json:"actor_id"`
	ActorType  string    `json:"actor_type"` 
	SignalType string    `json:"signal_type"`
	Severity   int       `json:"severity"` 
	Details    string    `json:"details"`
	CreatedAt  time.Time `json:"created_at"`
}

const (
	SignalHandshakeFail     = "HANDSHAKE_FAIL"      
	SignalWeightFraud       = "WEIGHT_FRAUD"        
	SignalPhantomCargo      = "PHANTOM_CARGO"      
	SignalClaimNeverReceived = "CLAIM_NEVER_RECEIVED" 
	SignalSLABreach         = "SLA_BREACH"          
	SignalMultiAppShadow    = "MULTI_APP_SHADOW"      
	SignalDisputeRaised     = "DISPUTE_RAISED"   
)
