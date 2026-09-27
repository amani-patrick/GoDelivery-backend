package domain

import (
	"context"
	"errors"
	"time"
)


var (
	ErrDriverNotEligible    = errors.New("driver not eligible for this delivery")
	ErrDriverNotActive      = errors.New("driver account is not in ACTIVE status")
	ErrInsufficientCapacity = errors.New("driver vehicle capacity is below the required weight")
	ErrVehicleTypeMismatch  = errors.New("driver vehicle type does not match delivery requirement")
)


type Role string

const (
	RoleMerchant   Role = "MERCHANT"
	RoleDriver     Role = "DRIVER"
	RoleCustomer   Role = "CUSTOMER"
	RoleDispatcher Role = "DISPATCHER"
)

// IsValid reports whether r is one of the defined constants.
func (r Role) IsValid() bool {
	switch r {
	case RoleMerchant, RoleDriver, RoleCustomer, RoleDispatcher:
		return true
	}
	return false
}

// VehicleType classifies a driver's vehicle for capacity-filtered dispatch.
// The matching engine uses this to ensure a single-iPhone shipment never gets
// routed to a Fuso truck, and 50 bags of cement never go to a motorcycle.
type VehicleType string

const (
	VehicleMotorcycle VehicleType = "MOTORCYCLE"
	VehicleCar        VehicleType = "CAR"
	VehicleVan        VehicleType = "VAN"
	VehicleTruck      VehicleType = "TRUCK"
)

func (v VehicleType) IsValid() bool {
	switch v {
	case VehicleMotorcycle, VehicleCar, VehicleVan, VehicleTruck:
		return true
	}
	return false
}

// DefaultMaxWeightKg returns the practical maximum payload for each vehicle
// class under Rwanda road conditions. Used as a fallback when a driver has
// not provided their specific vehicle capacity.
func (v VehicleType) DefaultMaxWeightKg() float64 {
	switch v {
	case VehicleMotorcycle:
		return 50
	case VehicleCar:
		return 200
	case VehicleVan:
		return 800
	case VehicleTruck:
		return 5000
	}
	return 0
}

// DriverStatus is the operational lifecycle state of a driver account.
// Drivers cannot start accepting orders until a human administrator transitions
// them from PENDING_VERIFICATION to ACTIVE after document review.
type DriverStatus string

const (
	// DriverPendingVerification is the initial state after self-registration.
	// The driver cannot accept orders while in this state.
	DriverPendingVerification DriverStatus = "PENDING_VERIFICATION"

	// DriverActive means the driver has passed document review and can work.
	DriverActive DriverStatus = "ACTIVE"

	// DriverSuspended means the account has been administratively suspended
	// (e.g. after a confirmed theft incident or serious complaint).
	DriverSuspended DriverStatus = "SUSPENDED"

	// DriverOnTrip means the driver is currently executing an active delivery.
	// The system transitions to this automatically when a delivery reaches IN_TRANSIT.
	DriverOnTrip DriverStatus = "ON_TRIP"
)

// CanAcceptOrders returns true only when the driver's status permits accepting
// new job dispatches. Called by AcceptOrder before the Redis lock is even attempted.
func (s DriverStatus) CanAcceptOrders() bool {
	return s == DriverActive
}
// User is the shared identity aggregate for all actor types.
// PasswordHash carries json:"-" to prevent accidental serialisation.
type User struct {
	ID           string    `json:"id"`
	FullName     string    `json:"full_name"`
	Phone        string    `json:"phone"`          // Rwanda format: +2507XXXXXXXX
	Email        string    `json:"email,omitempty"`
	Role         Role      `json:"role"`
	PasswordHash string    `json:"-"`              
	IsActive     bool      `json:"is_active"`
	IsVerified   bool      `json:"is_verified"`   
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// ── DriverProfile ─────────────────────────────────────────────────────────────

// DriverProfile is the operational sub-aggregate for a driver.
// It extends User identity with legal documents, vehicle specifications,
// and real-time operational state.
//
// Legal compliance:
//   NationalID is the 16-digit Rwandan NID required for background checks.
//   LicenseNumber is verified by an administrator during PENDING_VERIFICATION review.
//   Neither field is returned in public API responses.
type DriverProfile struct {
	UserID      string       `json:"user_id"`
	Status      DriverStatus `json:"status"`

	NationalID    string `json:"-"`
	LicenseNumber string `json:"-"`

	VehicleType VehicleType `json:"vehicle_type"`
	PlateNumber string      `json:"plate_number"`
	MaxWeightKg float64     `json:"max_weight_kg"`

	IsOnline        bool     `json:"is_online"`
	CurrentLocation Location `json:"current_location"`

	Rating          float64    `json:"rating"`
	TotalDeliveries int        `json:"total_deliveries"`
	TrustScore      float64    `json:"trust_score"`  
	TrustLevel      TrustLevel `json:"trust_level"` 
}

// IsEligibleFor returns nil when the driver can be dispatched for a delivery
// with the given weight and vehicle-type requirement.
func (p *DriverProfile) IsEligibleFor(weightKg float64, required VehicleType) error {
	if !p.Status.CanAcceptOrders() {
		return ErrDriverNotActive
	}
	if required != "" && p.VehicleType != required {
		return ErrVehicleTypeMismatch
	}
	if p.MaxWeightKg < weightKg {
		return ErrInsufficientCapacity
	}
	return nil
}

// ── BusinessProfile ───────────────────────────────────────────────────────────

// BusinessProfile holds merchant/business-specific data.
// TINNumber is required for invoicing under Rwanda Revenue Authority rules.
type BusinessProfile struct {
	UserID        string   `json:"user_id"`
	CompanyName   string   `json:"company_name"`
	TINNumber     string   `json:"-"`
	ContactName   string   `json:"contact_name"`
	PickupAddress string   `json:"pickup_address"`
	Location      Location `json:"location"`
	District      string   `json:"district"`
	IsVerified    bool     `json:"is_verified"`
	TrustScore    float64  `json:"trust_score"`
	TrustLevel    TrustLevel `json:"trust_level"`
}

// ── CustomerProfile ───────────────────────────────────────────────────────────

// CustomerProfile is a lightweight profile for delivery recipients.
// SavedLocation is the customer's most recently used drop-off coordinate,
// used to pre-fill the address field on new orders.
//
// Privacy: SavedLocation must be cleared from client-side storage the moment
// a delivery reaches a terminal state (DELIVERED / CANCELLED).
type CustomerProfile struct {
	UserID        string     `json:"user_id"`
	SavedLocation Location   `json:"saved_location,omitempty"`
	TrustScore    float64    `json:"trust_score"` // 
	TrustLevel    TrustLevel `json:"trust_level"`
}

// ── DispatchCandidate ─────────────────────────────────────────────────────────

// DispatchCandidate is the projection returned by FindEligibleDrivers.
// It carries just enough information for the notification layer to alert
// the driver without exposing sensitive profile data.
type DispatchCandidate struct {
	UserID      string      `json:"user_id"`
	FullName    string      `json:"full_name"`
	Phone       string      `json:"phone"`
	VehicleType VehicleType `json:"vehicle_type"`
	PlateNumber string      `json:"plate_number"`
	MaxWeightKg float64     `json:"max_weight_kg"`
	DistanceKm  float64     `json:"distance_km"` // from SpatialIndex geo query
}

// ── Repository interfaces ─────────────────────────────────────────────────────

// UserRepository is the persistence contract for the User aggregate.
type UserRepository interface {
	Create(ctx context.Context, u *User) error
	GetByID(ctx context.Context, id string) (*User, error)
	GetByPhone(ctx context.Context, phone string) (*User, error)
	Update(ctx context.Context, u *User) error
}

// DriverProfileRepository is the persistence contract for DriverProfile.
type DriverProfileRepository interface {
	Upsert(ctx context.Context, p *DriverProfile) error
	GetByUserID(ctx context.Context, userID string) (*DriverProfile, error)
	SetOnlineStatus(ctx context.Context, userID string, online bool) error
	SetStatus(ctx context.Context, userID string, status DriverStatus) error
	SetOnTrip(ctx context.Context, userID string, onTrip bool) error

	// FindEligibleDrivers returns ACTIVE, online, capacity-matching drivers
	// from the candidateIDs slice (from Redis geo). Ordered by rating DESC.
	FindEligibleDrivers(
		ctx context.Context,
		candidateIDs []string,
		minWeightKg float64,
		required VehicleType,
	) ([]*DriverProfile, error)

	// UpdateRating updates the rating and total_deliveries using Bayesian rolling
	// average after a delivery is confirmed .
	UpdateRating(ctx context.Context, userID string, newRating float64, newTotal int) error

	// UpdateTrustScore persists a recalculated trust score and level .
	UpdateTrustScore(ctx context.Context, userID string, score float64, level TrustLevel) error
}

// BusinessProfileRepository is the persistence contract for BusinessProfile.
type BusinessProfileRepository interface {
	Upsert(ctx context.Context, p *BusinessProfile) error
	GetByUserID(ctx context.Context, userID string) (*BusinessProfile, error)
	SetVerified(ctx context.Context, userID string, verified bool) error
	UpdateTrustScore(ctx context.Context, userID string, score float64, level TrustLevel) error // 
}

// CustomerProfileRepository is the persistence contract for CustomerProfile.
type CustomerProfileRepository interface {
	Upsert(ctx context.Context, p *CustomerProfile) error
	GetByUserID(ctx context.Context, userID string) (*CustomerProfile, error)
	UpdateTrustScore(ctx context.Context, userID string, score float64, level TrustLevel) error // 
}

// TrustRepository persists fraud signals for all actor types .
type TrustRepository interface {
	AppendSignal(ctx context.Context, signal *TrustSignal) error
	CountSignals(ctx context.Context, actorID, signalType string, since time.Time) (int, error)
	CountDeliveriesByMerchantToRecipient(ctx context.Context, merchantID, recipientPhone string) (int, error)
	MaxDeliveriesToSingleRecipient(ctx context.Context, merchantID string, since time.Time) (int, error)
}
