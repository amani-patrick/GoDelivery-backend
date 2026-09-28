package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/umurinzi/backend/internal/domain"
	"github.com/umurinzi/backend/internal/matching"
	"github.com/umurinzi/backend/internal/middleware"
)


// DriverUsecase orchestrates all driver-lifecycle operations: registration,
// administrator approval/suspension, online/offline toggling, and the
// capacity-filtered matching engine query used by dispatch.

type DriverUsecase struct {
	driverRepo   domain.DriverProfileRepository
	businessRepo domain.BusinessProfileRepository
	customerRepo domain.CustomerProfileRepository
	userRepo     domain.UserRepository
	ledger       domain.LedgerRepository
	engine       *matching.Engine // nil means matching engine not configured
	log          *slog.Logger
}

func NewDriverUsecase(
	driverRepo domain.DriverProfileRepository,
	businessRepo domain.BusinessProfileRepository,
	customerRepo domain.CustomerProfileRepository,
	userRepo domain.UserRepository,
	ledger domain.LedgerRepository,
	engine *matching.Engine,
	log *slog.Logger,
) *DriverUsecase {
	return &DriverUsecase{
		driverRepo:   driverRepo,
		businessRepo: businessRepo,
		customerRepo: customerRepo,
		userRepo:     userRepo,
		ledger:       ledger,
		engine:       engine,
		log:          log,
	}
}

//RegisterDriverInput carries the self-submitted registration data from a
//driver who has already created a User account.
type RegisterDriverInput struct {
	UserID        string
	NationalID    string          
	LicenseNumber string
	VehicleType   domain.VehicleType
	PlateNumber   string
	MaxWeightKg   float64          //optional: defaults to VehicleType.DefaultMaxWeightKg()
}

//RegisterDriver creates a DriverProfile in PENDING_VERIFICATION state.
func (uc *DriverUsecase) RegisterDriver(ctx context.Context, in RegisterDriverInput) (*domain.DriverProfile, error) {
	if !in.VehicleType.IsValid() {
		return nil, fmt.Errorf("%w: unknown vehicle type %q", domain.ErrInvalidInput, in.VehicleType)
	}
	if in.NationalID == "" {
		return nil, fmt.Errorf("%w: national_id is required for driver registration", domain.ErrInvalidInput)
	}
	if in.LicenseNumber == "" {
		return nil, fmt.Errorf("%w: license_number is required for driver registration", domain.ErrInvalidInput)
	}
	if in.PlateNumber == "" {
		return nil, fmt.Errorf("%w: plate_number is required", domain.ErrInvalidInput)
	}

	maxWeight := in.MaxWeightKg
	if maxWeight <= 0 {
		maxWeight = in.VehicleType.DefaultMaxWeightKg()
	}

	p := &domain.DriverProfile{
		UserID:        in.UserID,
		Status:        domain.DriverPendingVerification,
		NationalID:    in.NationalID,
		LicenseNumber: in.LicenseNumber,
		VehicleType:   in.VehicleType,
		PlateNumber:   in.PlateNumber,
		MaxWeightKg:   maxWeight,
		IsOnline:      false,
		Rating:        5.0,
	}

	if err := uc.driverRepo.Upsert(ctx, p); err != nil {
		uc.log.Error("RegisterDriver: upsert failed",
			slog.String("user_id", in.UserID),
			slog.String("reason", err.Error()),
		)
		return nil, err
	}

	uc.appendAudit(ctx, in.UserID, in.UserID, "DRIVER_REGISTERED",
		"", string(domain.DriverPendingVerification),
		fmt.Sprintf("vehicle=%s plate=%s", in.VehicleType, in.PlateNumber))

	uc.log.Info("driver registered — awaiting verification",
		slog.String("user_id", in.UserID),
		slog.String("vehicle_type", string(in.VehicleType)),
		slog.String("plate", in.PlateNumber),
	)
	return p, nil
}

// requireAdmin enforces the ADMIN gate on privileged operations. The role
// comes from the JWT claims (request context) and is re-verified against the
// users table so a token minted before a demotion is honoured immediately.
func (uc *DriverUsecase) requireAdmin(ctx context.Context, callerID, op string) error {
	role, _ := ctx.Value(middleware.CtxUserRole).(string)
	if role != string(domain.RoleAdmin) {
		uc.log.Warn("privileged operation denied — caller role is not ADMIN",
			slog.String("op", op),
			slog.String("caller_id", callerID),
			slog.String("token_role", role),
		)
		return domain.ErrUnauthorized
	}
	u, err := uc.userRepo.GetByID(ctx, callerID)
	if err != nil || u == nil || u.Role != domain.RoleAdmin {
		uc.log.Warn("privileged operation denied — DB role check failed",
			slog.String("op", op),
			slog.String("caller_id", callerID),
			slog.String("reason", err.Error()),
		)
		return domain.ErrUnauthorized
	}
	return nil
}

func (uc *DriverUsecase) ApproveDriver(ctx context.Context, driverUserID, approvedByUserID string) error {
	if err := uc.requireAdmin(ctx, approvedByUserID, "ApproveDriver"); err != nil {
		return err
	}
	profile, err := uc.driverRepo.GetByUserID(ctx, driverUserID)
	if err != nil {
		return err
	}

	if profile.Status != domain.DriverPendingVerification {
		return fmt.Errorf("%w: cannot approve a driver in %s status",
			domain.ErrInvalidInput, profile.Status)
	}

	if err := uc.driverRepo.SetStatus(ctx, driverUserID, domain.DriverActive); err != nil {
		uc.log.Error("ApproveDriver: SetStatus failed",
			slog.String("driver_user_id", driverUserID),
			slog.String("reason", err.Error()),
		)
		return err
	}

	uc.appendAudit(ctx, driverUserID, approvedByUserID, "DRIVER_APPROVED",
		string(domain.DriverPendingVerification), string(domain.DriverActive), "")

	uc.log.Info("driver approved",
		slog.String("driver_user_id", driverUserID),
		slog.String("approved_by", approvedByUserID),
	)
	return nil
}

func (uc *DriverUsecase) SuspendDriver(ctx context.Context, driverUserID, suspendedByUserID, reason string) error {
	if err := uc.requireAdmin(ctx, suspendedByUserID, "SuspendDriver"); err != nil {
		return err
	}
	if reason == "" {
		return fmt.Errorf("%w: suspension reason is required", domain.ErrInvalidInput)
	}

	profile, err := uc.driverRepo.GetByUserID(ctx, driverUserID)
	if err != nil {
		return err
	}

	prevStatus := profile.Status
	if err := uc.driverRepo.SetStatus(ctx, driverUserID, domain.DriverSuspended); err != nil {
		uc.log.Error("SuspendDriver: SetStatus failed",
			slog.String("driver_user_id", driverUserID),
			slog.String("reason", err.Error()),
		)
		return err
	}

	// Force the driver offline immediately
	_ = uc.driverRepo.SetOnlineStatus(ctx, driverUserID, false)

	uc.appendAudit(ctx, driverUserID, suspendedByUserID, "DRIVER_SUSPENDED",
		string(prevStatus), string(domain.DriverSuspended), reason)

	uc.log.Warn("driver suspended",
		slog.String("driver_user_id", driverUserID),
		slog.String("suspended_by", suspendedByUserID),
		slog.String("reason", reason),
	)
	return nil
}

func (uc *DriverUsecase) SetOnline(ctx context.Context, driverUserID string) error {
	profile, err := uc.driverRepo.GetByUserID(ctx, driverUserID)
	if err != nil {
		return err
	}
	if profile.Status != domain.DriverActive {
		return fmt.Errorf("%w: only ACTIVE drivers can go online (current: %s)",
			domain.ErrDriverNotActive, profile.Status)
	}
	if err := uc.driverRepo.SetOnlineStatus(ctx, driverUserID, true); err != nil {
		return err
	}
	uc.log.Info("driver online", slog.String("user_id", driverUserID))
	return nil
}

func (uc *DriverUsecase) SetOffline(ctx context.Context, driverUserID string) error {
	if err := uc.driverRepo.SetOnlineStatus(ctx, driverUserID, false); err != nil {
		return err
	}
	uc.log.Info("driver offline", slog.String("user_id", driverUserID))
	return nil
}

func (uc *DriverUsecase) GetDriverProfile(ctx context.Context, userID string) (*domain.DriverProfile, error) {
	return uc.driverRepo.GetByUserID(ctx, userID)
}

type FindEligibleDriversInput struct {
	NearbyDriverIDs []string
	MinWeightKg float64
	RequiredVehicleType domain.VehicleType // optional = any type
}

// This usecase deliberately does NOT call Redis or the
// SpatialIndex directly. The caller is responsible for supplying NearbyDriverIDs
// from a prior geo-search. This keeps the usecase layer infrastructure-agnostic
// and makes the matching logic independently testable.
func (uc *DriverUsecase) FindEligibleDrivers(
	ctx context.Context, in FindEligibleDriversInput,
) ([]*domain.DispatchCandidate, error) {
	if len(in.NearbyDriverIDs) == 0 {
		return nil, nil
	}

	profiles, err := uc.driverRepo.FindEligibleDrivers(
		ctx, in.NearbyDriverIDs, in.MinWeightKg, in.RequiredVehicleType,
	)
	if err != nil {
		return nil, err
	}

	// Build a lookup from userID → User for name/phone enrichment.
	candidates := make([]*domain.DispatchCandidate, 0, len(profiles))
	for _, p := range profiles {
		u, err := uc.userRepo.GetByID(ctx, p.UserID)
		if err != nil {
			uc.log.Error("FindEligibleDrivers: user not found for driver profile",
				slog.String("user_id", p.UserID),
				slog.String("reason", err.Error()),
			)
			continue
		}
		candidates = append(candidates, &domain.DispatchCandidate{
			UserID:      p.UserID,
			FullName:    u.FullName,
			Phone:       u.Phone,
			VehicleType: p.VehicleType,
			PlateNumber: p.PlateNumber,
			MaxWeightKg: p.MaxWeightKg,
		})
	}
	return candidates, nil
}

// DispatchForDelivery runs the full two-tier matching pipeline for a delivery
// and returns a ranked list of scored candidates.
//
// When the matching engine is configured (uc.engine != nil) it runs:
//   Tier-1 Redis GEOSEARCH → Tier-2 OSRM matrix → dynamic scoring → heading penalty
//
// When the engine is nil (e.g. local dev without OSRM) it falls back to the
// simple Postgres FindEligibleDrivers path and returns unsorted candidates.

func (uc *DriverUsecase) DispatchForDelivery(
	ctx context.Context,
	deliveryID string,
	pickupLat, pickupLng float64,
	weightKg float64,
	vehicleTypeRequired domain.VehicleType,
) (*domain.MatchResult, error) {
	if uc.engine == nil {
		uc.log.Warn("DispatchForDelivery: matching engine not configured — using simple dispatch",
			slog.String("delivery_id", deliveryID),
		)
		// Simple fallback: return nil result, caller uses FindEligibleDrivers.
		return &domain.MatchResult{
			DeliveryID: deliveryID,
		}, nil
	}

	order := domain.BatchOrder{
		DeliveryID:          deliveryID,
		PickupLat:           pickupLat,
		PickupLng:           pickupLng,
		WeightKg:            weightKg,
		VehicleTypeRequired: vehicleTypeRequired,
	}

	result, err := uc.engine.Match(ctx, order)
	if err != nil {
		uc.log.Error("DispatchForDelivery: matching engine error",
			slog.String("delivery_id", deliveryID),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("dispatch: %w", err)
	}

	if result.Winner != nil {
		uc.log.Info("dispatch winner selected",
			slog.String("delivery_id", deliveryID),
			slog.String("winner_id", result.Winner.UserID),
			slog.Float64("score", result.Winner.Score),
			slog.Float64("eta_min", result.Winner.ETAMinutes),
			slog.Float64("road_km", result.Winner.RoadDistKm),
			slog.Bool("osrm_used", result.OSRMUsed),
			slog.Bool("heading_penalised", result.Winner.HeadingPenalised),
		)
	} else {
		uc.log.Warn("dispatch: no eligible driver found",
			slog.String("delivery_id", deliveryID),
		)
	}
	return result, nil
}

func (uc *DriverUsecase) EnqueueForDispatch(
	ctx context.Context,
	deliveryID string,
	pickupLat, pickupLng float64,
	weightKg float64,
	vehicleTypeRequired domain.VehicleType,
	readyAt time.Time,
) error {
	if uc.engine == nil {
		return nil 
	}
	return uc.engine.Enqueue(ctx, domain.BatchOrder{
		DeliveryID:          deliveryID,
		PickupLat:           pickupLat,
		PickupLng:           pickupLng,
		WeightKg:            weightKg,
		VehicleTypeRequired: vehicleTypeRequired,
		EnqueuedAt:          time.Now().UTC(),
		ReadyAt:             readyAt,
	})
}

type UpsertBusinessProfileInput struct {
	UserID        string
	CompanyName   string
	TINNumber     string
	ContactName   string
	PickupAddress string
	Lat           float64
	Lng           float64
	District      string
}

func (uc *DriverUsecase) UpsertBusinessProfile(ctx context.Context, in UpsertBusinessProfileInput) (*domain.BusinessProfile, error) {
	if in.CompanyName == "" {
		return nil, fmt.Errorf("%w: company_name is required", domain.ErrInvalidInput)
	}

	p := &domain.BusinessProfile{
		UserID:        in.UserID,
		CompanyName:   in.CompanyName,
		TINNumber:     in.TINNumber,
		ContactName:   in.ContactName,
		PickupAddress: in.PickupAddress,
		Location:      domain.Location{Lat: in.Lat, Lng: in.Lng},
		District:      in.District,
		IsVerified:    false, 
	}

	if err := uc.businessRepo.Upsert(ctx, p); err != nil {
		return nil, err
	}

	uc.appendAudit(ctx, in.UserID, in.UserID, "BUSINESS_PROFILE_UPSERTED",
		"", "", fmt.Sprintf("company=%s district=%s", in.CompanyName, in.District))

	return p, nil
}

func (uc *DriverUsecase) GetBusinessProfile(ctx context.Context, userID string) (*domain.BusinessProfile, error) {
	return uc.businessRepo.GetByUserID(ctx, userID)
}

func (uc *DriverUsecase) UpsertCustomerSavedLocation(ctx context.Context, userID string, lat, lng float64) error {
	p := &domain.CustomerProfile{
		UserID:        userID,
		SavedLocation: domain.Location{Lat: lat, Lng: lng},
	}
	return uc.customerRepo.Upsert(ctx, p)
}

func (uc *DriverUsecase) GetCustomerProfile(ctx context.Context, userID string) (*domain.CustomerProfile, error) {
	return uc.customerRepo.GetByUserID(ctx, userID)
}

func (uc *DriverUsecase) appendAudit(
	ctx context.Context,
	entityID, actorID, action, oldState, newState, meta string,
) {
	event := &domain.AuditEvent{
		ID:         uuid.NewString(),
		EntityID:   entityID,
		EntityType: "DRIVER",
		ActorID:    actorID,
		Action:     action,
		OldState:   oldState,
		NewState:   newState,
		Metadata:   meta,
		CreatedAt:  time.Now().UTC(),
	}
	if err := uc.ledger.Append(ctx, event); err != nil {
		uc.log.Error("driver.appendAudit: ledger write failed",
			slog.String("entity_id", entityID),
			slog.String("action", action),
			slog.String("reason", err.Error()),
		)
	}
}
