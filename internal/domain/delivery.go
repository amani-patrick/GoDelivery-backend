
package domain

import (
	"context"
	"errors"
	"time"
)


// Package domain contains the enterprise core: aggregate roots, value objects,
// domain events, sentinel errors, and repository interfaces.
// Nothing in this package imports from any other internal package.

// All errors are defined here so any layer can do errors.Is() comparisons
// without importing concrete infrastructure packages.

var (
	ErrInvalidStateTransition = errors.New("state transition logic violation")
	ErrHandshakeFailed        = errors.New("cryptographic handshake token mismatch")
	ErrLockAcquisitionFailed  = errors.New("resource locked by concurrent worker")
	ErrDeliveryNotFound       = errors.New("delivery not found")
	ErrUserNotFound           = errors.New("user not found")
	ErrUnauthorized           = errors.New("actor not authorised for this operation")
	ErrAccountSuspended       = errors.New("account is suspended")
	ErrInvalidInput           = errors.New("invalid input")
	ErrWeightMismatch         = errors.New("confirmed package weight exceeds driver vehicle capacity")
	ErrStackNotFeasible       = errors.New("order stacking not feasible for this route")
	ErrActorBanned            = errors.New("actor is banned from the platform")
)


type ErrDangerZone struct {
	NearestSafeHub *SafeHub
}

func (e *ErrDangerZone) Error() string {
	if e.NearestSafeHub != nil {
		return "dropoff location is in an active danger zone — nearest safe hub: " + e.NearestSafeHub.Name
	}
	return "dropoff location is in an active danger zone"
}


// Location is an immutable geographic coordinate pair.
// It is a value object: equality is structural, it has no identity.
type Location struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// IsZero returns true when the location has not been set.
func (l Location) IsZero() bool {
	return l.Lat == 0 && l.Lng == 0
}

// DeliveryState represents a single node in the delivery lifecycle graph.
type DeliveryState string

const (
	StateCreated   DeliveryState = "CREATED"
	StateAssigned  DeliveryState = "ASSIGNED"
	StateInTransit DeliveryState = "IN_TRANSIT"
	StateDelivered DeliveryState = "DELIVERED"
	StateCancelled DeliveryState = "CANCELLED"
	StateDisputed  DeliveryState = "DISPUTED"
)

// validTransitions is the canonical directed graph of allowed state moves.
// It is the single source of truth — no other layer may hard-code transitions.
var validTransitions = map[DeliveryState]map[DeliveryState]bool{
	StateCreated:   {StateAssigned: true, StateCancelled: true},
	StateAssigned:  {StateInTransit: true, StateCancelled: true},
	StateInTransit: {StateDelivered: true, StateDisputed: true, StateCancelled: true},
	StateDelivered: {},
	StateCancelled: {},
	StateDisputed:  {StateCancelled: true},
}

// PackageCategory classifies the type of goods being shipped.
// Used by the matching engine to select appropriate vehicle types and
// as metadata for insurance and customs compliance.
type PackageCategory string

const (
	PackageCategoryGeneral     PackageCategory = "GENERAL"
	PackageCategoryElectronics PackageCategory = "ELECTRONICS"
	PackageCategoryFragile     PackageCategory = "FRAGILE"
	PackageCategoryPerishable  PackageCategory = "PERISHABLE"
	PackageCategoryDocuments   PackageCategory = "DOCUMENTS"
	PackageCategoryBulk        PackageCategory = "BULK"       
)

// Delivery is the central aggregate root of the system.
//
// Security contract:
//   - PickupQRCode stores a bcrypt hash. The plaintext is returned exactly once
//     at creation time and is never persisted or re-transmitted.
//   - DeliveryPIN stores a bcrypt hash. Same contract.
//   - Both fields carry json:"-" to prevent accidental serialisation into
//     API responses, logs, or error messages.
type Delivery struct {
	ID           string          `json:"id"`
	MerchantID   string          `json:"merchant_id"`
	DriverID     string          `json:"driver_id,omitempty"`
	CustomerID   string          `json:"customer_id"`
	CurrentState DeliveryState   `json:"current_state"`
	PickupQRCode string          `json:"-"`
	DeliveryPIN  string          `json:"-"`
	PickupLoc    Location        `json:"pickup_location"`
	DropoffLoc   Location        `json:"dropoff_location"`
	Description  string          `json:"description"`
	WeightKg     float64         `json:"weight_kg"`

	// Zero means not yet confirmed. Set during ConfirmPickup.
	ConfirmedWeightKg float64 `json:"confirmed_weight_kg,omitempty"`

	// Matching engine constraints
	VehicleTypeRequired VehicleType     `json:"vehicle_type_required,omitempty"`
	PackageCategory     PackageCategory `json:"package_category"`

	// 0 = standard, 1 = priority, 2 = premium.
	PriorityLevel int `json:"priority_level"`

	StackSequence int    `json:"stack_sequence,omitempty"`
	StackGroupID  string `json:"stack_group_id,omitempty"`

	PrepTimeMinutes int       `json:"prep_time_minutes"`
	ReadyAt         time.Time `json:"ready_at"`

	// FareRWF is the gross fare locked at creation time and escrowed from the
	// merchant wallet in the same transaction that inserts this row. Written
	// once at creation — never mutated (the invoice worker and escrow release
	// both read it).
	FareRWF int64 `json:"fare_rwf"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Transition enforces the state machine and cryptographic custody checks.
//
// Custody token protocol:
//   - ASSIGNED → IN_TRANSIT: the usecase layer must bcrypt-verify the scanned
//     QR token against PickupQRCode BEFORE calling Transition. It then passes
//     the stored hash as inputToken so the domain-level defence-in-depth check
//     (hash == hash) passes. An empty or mismatched token fails immediately.
//   - IN_TRANSIT → DELIVERED: same protocol using DeliveryPIN.
//   - All other transitions: inputToken is ignored; pass "".

func (d *Delivery) Transition(next DeliveryState, inputToken string) error {
	allowed, stateKnown := validTransitions[d.CurrentState]
	if !stateKnown || !allowed[next] {
		return ErrInvalidStateTransition
	}

	switch {
	case d.CurrentState == StateAssigned && next == StateInTransit:
		if inputToken == "" || inputToken != d.PickupQRCode {
			return ErrHandshakeFailed
		}
	case d.CurrentState == StateInTransit && next == StateDelivered:
		if inputToken == "" || inputToken != d.DeliveryPIN {
			return ErrHandshakeFailed
		}
	}

	d.CurrentState = next
	// NOTE: Transition deliberately does NOT touch d.UpdatedAt. The repository's
	// optimistic lock (UPDATE ... WHERE updated_at = <value read by GetByID>)
	// compares against the timestamp fetched from the database. Mutating it here
	// would make every post-transition Update match zero rows and fail with
	// ErrDeliveryNotFound. Update() stamps the new timestamp on write.
	return nil
}

// IsTerminal returns true when the delivery has reached a final state
// from which no further transitions are possible.
func (d *Delivery) IsTerminal() bool {
	allowed := validTransitions[d.CurrentState]
	return len(allowed) == 0
}

// Interfaces are owned by the domain layer.
// Concrete implementations live in internal/delivery/repository.

// DeliveryRepository is the persistence contract for the Delivery aggregate.
type DeliveryRepository interface {
	Create(ctx context.Context, d *Delivery) error
	// CreateWithinTx inserts the delivery inside the caller's transaction so
	// the row and its wallet escrow (order funding) commit atomically: an
	// order can never exist unfunded, and funding can never orphan an order.
	CreateWithinTx(ctx context.Context, tx Tx, d *Delivery) error
	GetByID(ctx context.Context, id string) (*Delivery, error)
	Update(ctx context.Context, d *Delivery) error
	UpdateWithinTx(ctx context.Context, tx Tx, d *Delivery) error
	ListByDriver(ctx context.Context, driverID string, states []DeliveryState) ([]*Delivery, error)
	ListByMerchant(ctx context.Context, merchantID string, states []DeliveryState) ([]*Delivery, error)

	// UpdateConfirmedWeight sets the confirmed_weight_kg field after physical
	// weighing at pickup. Distinct from Update to minimise write surface 
	UpdateConfirmedWeight(ctx context.Context, deliveryID string, confirmedKg float64) error

	// UpdateStackInfo writes the stack_group_id and stack_sequence for a stacked
	// delivery after the matching engine approves a multi-drop route 
	UpdateStackInfo(ctx context.Context, deliveryID, stackGroupID string, sequence int) error

	// UpdateOptimisedSequence persists the backend-enforced drop-off order
	// (1-based) computed by the multi-drop TSP optimiser.
	UpdateOptimisedSequence(ctx context.Context, deliveryID string, sequence int) error

	// ListActiveByDriver returns all non-terminal deliveries for a driver.
	// Used by the stacking engine to find a driver's current active orders.
	ListActiveByDriver(ctx context.Context, driverID string) ([]*Delivery, error)
}
