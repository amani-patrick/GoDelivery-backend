package usecase

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/umurinzi/backend/internal/domain"
	"golang.org/x/crypto/bcrypt"
)

// Package usecase orchestrates the delivery lifecycle application logic.
// It coordinates domain rules, distributed locking, and persistence —
// never touching the database directly; always through repository interfaces.
// Cross-module communication happens through Go channels or explicit interface
// calls — never through direct cross-module database writes.

// Constants 

const (
	lockTTL           = 5 * time.Second  // Redis NX lock expiry — prevents deadlock on crash
	qrTokenBytes      = 16               // 128-bit random pickup token
	pinDigits         = 6                // numeric OTP length for customer delivery confirmation
	idempotencyTTL    = 24 * time.Hour   // how long to cache CreateDelivery results
	idempotencyPrefix = "idem:create:"   // Redis key prefix for idempotency records
)



// Returns 1 if the key was deleted (we owned it), 0 if not (already expired/stolen).
const luaReleaseLock = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end`

// DispatchEnqueuerFunc is the function signature for enqueuing a delivery
// for batch matching. Using a function type keeps delivery_usecase free of
// a dependency on matching or driver_usecase packages.
type DispatchEnqueuerFunc func(
	ctx context.Context,
	deliveryID string,
	pickupLat, pickupLng float64,
	weightKg float64,
	vehicleTypeRequired domain.VehicleType,
) error

//DeliveryUsecase orchestrates all delivery lifecycle operations.
type DeliveryUsecase struct {
	repo             domain.DeliveryRepository
	driverRepo       domain.DriverProfileRepository
	outbox           domain.OutboxRepository
	trustRepo        domain.TrustRepository    //nil when not wired
	ledger           domain.LedgerRepository
	redisClient      *redis.Client
	pool             *pgxpool.Pool
	bcryptCost       int
	dispatchEnqueuer DispatchEnqueuerFunc
	clearPending     func(ctx context.Context, deliveryID string) 
	getIntended      func(ctx context.Context, deliveryID string) string
	log              *slog.Logger
}

func NewDeliveryUsecase(
	repo domain.DeliveryRepository,
	driverRepo domain.DriverProfileRepository,
	outbox domain.OutboxRepository,
	trustRepo domain.TrustRepository,
	ledger domain.LedgerRepository,
	redisClient *redis.Client,
	pool *pgxpool.Pool,
	bcryptCost int,
	dispatchEnqueuer DispatchEnqueuerFunc,
	clearPending func(ctx context.Context, deliveryID string),
	getIntended func(ctx context.Context, deliveryID string) string,
	log *slog.Logger,
) *DeliveryUsecase {
	return &DeliveryUsecase{
		repo:             repo,
		driverRepo:       driverRepo,
		outbox:           outbox,
		trustRepo:        trustRepo,
		ledger:           ledger,
		redisClient:      redisClient,
		pool:             pool,
		bcryptCost:       bcryptCost,
		dispatchEnqueuer: dispatchEnqueuer,
		clearPending:     clearPending,
		getIntended:      getIntended,
		log:              log,
	}
}

//CreateDeliveryInput carries the caller-supplied creation parameters.
type CreateDeliveryInput struct {
	MerchantID          string
	CustomerID          string
	PickupLat           float64
	PickupLng           float64
	DropoffLat          float64
	DropoffLng          float64
	Description         string
	WeightKg            float64
	VehicleTypeRequired domain.VehicleType   //empty for any type 
	PackageCategory     domain.PackageCategory
	// IdempotencyKey is optional. When provided and a matching record exists in
	// Redis, the original Delivery is returned without creating a new one.
	// The plaintext tokens are NOT re-served on a duplicate — the caller must
	// use GetDelivery to inspect the current state.
	IdempotencyKey string
}

// CreateDeliveryOutput is returned to the caller on success.
// PlaintextQRCode and PlaintextPIN are one-time-use values returned
// exactly once and never stored in plaintext anywhere in the system.
// On a duplicate idempotent request, both fields are empty strings and
// IsDuplicate is true.
type CreateDeliveryOutput struct {
	Delivery        *domain.Delivery
	PlaintextQRCode string // shown to merchant ONCE for QR generation
	PlaintextPIN    string // sent to customer ONCE via SMS / push notification
	IsDuplicate     bool   // true when this result came from the idempotency cache
}

// idempotencyRecord is what we store in Redis — only the delivery ID so we
// can re-fetch the full aggregate on a duplicate request. We deliberately
// never cache plaintext tokens.
type idempotencyRecord struct {
	DeliveryID string `json:"delivery_id"`
}


// Idempotency protocol:
//   - If in.IdempotencyKey is non-empty, we check Redis for a cached result
//     under "idem:create:<key>" before doing any work.
//   - On cache hit: fetch the delivery from Postgres and return it with
//     IsDuplicate=true. Plaintext tokens are NOT re-served (they are gone).
//   - On cache miss: proceed normally, then write the delivery ID to Redis
//     with a 24-hour TTL before returning.
//   - If the Redis write fails, we log and proceed — idempotency is best-effort
//     and must never abort a successful business operation.
func (uc *DeliveryUsecase) CreateDelivery(
	ctx context.Context, in CreateDeliveryInput,
) (*CreateDeliveryOutput, error) {
	//  Idempotency cache lookup ─
	if in.IdempotencyKey != "" {
		if out, hit, err := uc.checkIdempotencyCache(ctx, in.IdempotencyKey); err != nil {
			// Cache read failure is non-fatal — log and proceed to create.
			uc.log.Warn("CreateDelivery: idempotency cache read failed — proceeding without cache",
				slog.String("idempotency_key", in.IdempotencyKey),
				slog.String("reason", err.Error()),
			)
		} else if hit {
			uc.log.Info("CreateDelivery: duplicate request — returning cached delivery",
				slog.String("idempotency_key", in.IdempotencyKey),
				slog.String("delivery_id", out.Delivery.ID),
			)
			return out, nil
		}
	}

	if in.MerchantID == "" || in.CustomerID == "" {
		return nil, fmt.Errorf("%w: merchant_id and customer_id are required", domain.ErrInvalidInput)
	}
	if in.WeightKg <= 0 {
		return nil, fmt.Errorf("%w: weight_kg must be positive", domain.ErrInvalidInput)
	}

	rawQR, err := generateSecureToken(qrTokenBytes)
	if err != nil {
		return nil, fmt.Errorf("generate qr token: %w", err)
	}
	rawPIN := generateNumericPIN(pinDigits)

	//Hash both tokens 
	qrHash, err := bcrypt.GenerateFromPassword([]byte(rawQR), uc.bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hash qr token: %w", err)
	}
	pinHash, err := bcrypt.GenerateFromPassword([]byte(rawPIN), uc.bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("hash delivery pin: %w", err)
	}

	now := time.Now().UTC()

	category := in.PackageCategory
	if category == "" {
		category = domain.PackageCategoryGeneral
	}

	d := &domain.Delivery{
		ID:                  uuid.NewString(),
		MerchantID:          in.MerchantID,
		CustomerID:          in.CustomerID,
		CurrentState:        domain.StateCreated,
		PickupQRCode:        string(qrHash),
		DeliveryPIN:         string(pinHash),
		PickupLoc:           domain.Location{Lat: in.PickupLat, Lng: in.PickupLng},
		DropoffLoc:          domain.Location{Lat: in.DropoffLat, Lng: in.DropoffLng},
		Description:         in.Description,
		WeightKg:            in.WeightKg,
		VehicleTypeRequired: in.VehicleTypeRequired,
		PackageCategory:     category,
		CreatedAt:           now,
		UpdatedAt:           now,
	}

	if err := uc.repo.Create(ctx, d); err != nil {
		return nil, err
	}

	//Write idempotency record to Redis
	if in.IdempotencyKey != "" {
		if err := uc.writeIdempotencyCache(ctx, in.IdempotencyKey, d.ID); err != nil {
			uc.log.Warn("CreateDelivery: idempotency cache write failed — non-fatal",
				slog.String("idempotency_key", in.IdempotencyKey),
				slog.String("delivery_id", d.ID),
				slog.String("reason", err.Error()),
			)
		}
	}

	if uc.dispatchEnqueuer != nil {
		if err := uc.dispatchEnqueuer(ctx, d.ID, d.PickupLoc.Lat, d.PickupLoc.Lng,
			d.WeightKg, d.VehicleTypeRequired); err != nil {
			uc.log.Warn("CreateDelivery: dispatch enqueue failed — non-fatal",
				slog.String("delivery_id", d.ID),
				slog.String("reason", err.Error()),
			)
		}
	}

	uc.appendAudit(ctx, d.ID, in.MerchantID, "DELIVERY_CREATED",
		"", string(domain.StateCreated), "")

	return &CreateDeliveryOutput{
		Delivery:        d,
		PlaintextQRCode: rawQR,
		PlaintextPIN:    rawPIN,
	}, nil
}

//Returns (nil, false, nil) on a cache miss.
func (uc *DeliveryUsecase) checkIdempotencyCache(
	ctx context.Context, key string,
) (*CreateDeliveryOutput, bool, error) {
	cacheKey := idempotencyPrefix + key
	raw, err := uc.redisClient.Get(ctx, cacheKey).Result()
	if err != nil {
		// redis.Nil means key doesn't exist = a normal cache miss.
		if err.Error() == "redis: nil" {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("redis GET: %w", err)
	}

	var rec idempotencyRecord
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		return nil, false, fmt.Errorf("unmarshal idempotency record: %w", err)
	}

	d, err := uc.repo.GetByID(ctx, rec.DeliveryID)
	if err != nil {
		return nil, false, fmt.Errorf("re-fetch delivery for idempotency: %w", err)
	}

	return &CreateDeliveryOutput{
		Delivery:    d,
		IsDuplicate: true,
	}, true, nil
}

func (uc *DeliveryUsecase) writeIdempotencyCache(
	ctx context.Context, key, deliveryID string,
) error {
	rec := idempotencyRecord{DeliveryID: deliveryID}
	b, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal idempotency record: %w", err)
	}
	cacheKey := idempotencyPrefix + key
	return uc.redisClient.Set(ctx, cacheKey, string(b), idempotencyTTL).Err()
}

// AcceptOrder assigns a driver to a delivery. It enforces three layers of
// protection before writing anything:
//
//  1. Driver eligibility: the driver must be ACTIVE with a vehicle that can
//     carry the package weight and matches the required vehicle type.
//  2. Redis distributed lock: prevents two drivers from accepting the same
//     order simultaneously.
//  3. State machine: the delivery must still be in CREATED state after the
//     lock is acquired .

// On success the driver's status is moved to ON_TRIP.
func (uc *DeliveryUsecase) AcceptOrder(ctx context.Context, deliveryID, driverID string) error {
	//Driver eligibility check 
	driverProfile, err := uc.driverRepo.GetByUserID(ctx, driverID)
	if err != nil {
		uc.log.Warn("AcceptOrder: driver profile not found",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("AcceptOrder: fetch driver profile: %w", err)
	}

	delivery, err := uc.repo.GetByID(ctx, deliveryID)
	if err != nil {
		return err
	}

	if eligErr := driverProfile.IsEligibleFor(delivery.WeightKg, delivery.VehicleTypeRequired); eligErr != nil {
		uc.log.Warn("AcceptOrder: driver not eligible",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("driver_status", string(driverProfile.Status)),
			slog.String("vehicle_type", string(driverProfile.VehicleType)),
			slog.Float64("driver_max_kg", driverProfile.MaxWeightKg),
			slog.Float64("required_kg", delivery.WeightKg),
			slog.String("required_vehicle", string(delivery.VehicleTypeRequired)),
			slog.String("reason", eligErr.Error()),
		)
		return eligErr
	}
	
	// If the matching engine registered an intended winner and this driver is
	// not that winner, block them while the TTL is still active.
	// After TTL expires (getIntended returns ""), any driver may accept.
	if uc.getIntended != nil {
		intended := uc.getIntended(ctx, deliveryID)
		if intended != "" && intended != driverID {
			uc.log.Warn("AcceptOrder: driver is not the intended winner — blocked",
				slog.String("delivery_id", deliveryID),
				slog.String("driver_id", driverID),
				slog.String("intended_driver", intended),
			)
			return domain.ErrLockAcquisitionFailed
		}
	}

	//Redis distributed lock 
	lockKey := "lock:delivery:" + deliveryID

	acquired, err := uc.redisClient.SetNX(ctx, lockKey, driverID, lockTTL).Result()
	if err != nil {
		uc.log.Error("AcceptOrder: redis lock error",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("lock acquisition: %w", err)
	}
	if !acquired {
		uc.log.Warn("AcceptOrder: delivery already locked by concurrent driver",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
		)
		return domain.ErrLockAcquisitionFailed
	}
	//Conditional Lua release.
	defer func() {
		released, delErr := uc.redisClient.Eval(
			ctx, luaReleaseLock, []string{lockKey}, driverID,
		).Int()
		if delErr != nil {
			uc.log.Error("AcceptOrder: lock release script failed",
				slog.String("delivery_id", deliveryID),
				slog.String("reason", delErr.Error()),
			)
		} else if released == 0 {
			uc.log.Warn("AcceptOrder: lock had already expired — another driver may have acquired it",
				slog.String("delivery_id", deliveryID),
				slog.String("driver_id", driverID),
			)
		}
	}()

	// Re-read delivery inside the lock and run state machine 
	delivery, err = uc.repo.GetByID(ctx, deliveryID)
	if err != nil {
		return err
	}

	prevState := delivery.CurrentState
	delivery.DriverID = driverID
	if err := delivery.Transition(domain.StateAssigned, ""); err != nil {
		uc.log.Warn("AcceptOrder: state transition rejected",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("current_state", string(prevState)),
			slog.String("reason", err.Error()),
		)
		return err
	}

	if err := uc.repo.Update(ctx, delivery); err != nil {
		return err
	}

	// Move driver status to ON_TRIP
	if err := uc.driverRepo.SetOnTrip(ctx, driverID, true); err != nil {
		uc.log.Error("AcceptOrder: SetOnTrip failed — delivery assigned but driver status not updated",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
	}

	// Clear re-dispatch pending key
	// The driver accepted — stop the re-dispatch countdown.
	if uc.clearPending != nil {
		uc.clearPending(ctx, deliveryID)
	}

	uc.appendAudit(ctx, deliveryID, driverID, "DELIVERY_ASSIGNED",
		string(prevState), string(domain.StateAssigned),
		fmt.Sprintf("vehicle=%s weight_capacity=%.1fkg", driverProfile.VehicleType, driverProfile.MaxWeightKg))

	return nil
}

// ConfirmPickup validates the QR token scanned by the driver, records the
// confirmed package weight, and transitions the delivery to IN_TRANSIT.
//
// Token protocol:
//  1. Driver scans the physical QR code; the app submits the raw token string.
//  2. We bcrypt-compare it against the stored hash.
//  3. On match, we pass the stored hash itself as inputToken to Transition().
func (uc *DeliveryUsecase) ConfirmPickup(ctx context.Context, deliveryID, driverID, scannedToken string, confirmedWeightKg float64) error {
	delivery, err := uc.repo.GetByID(ctx, deliveryID)
	if err != nil {
		return err
	}

	if delivery.DriverID != driverID {
		uc.log.Warn("ConfirmPickup: unauthorized attempt",
			slog.String("delivery_id", deliveryID),
			slog.String("requesting_driver", driverID),
			slog.String("assigned_driver", delivery.DriverID),
		)
		return domain.ErrUnauthorized
	}

	// Constant-time hash comparison 
	if err := bcrypt.CompareHashAndPassword([]byte(delivery.PickupQRCode), []byte(scannedToken)); err != nil {
		uc.log.Warn("ConfirmPickup: QR code mismatch",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
		)
		uc.appendAudit(ctx, deliveryID, driverID, "HANDSHAKE_FAILED_PICKUP",
			string(delivery.CurrentState), "", "qr_token_mismatch")
		//Emit trust signal for repeated handshake failures
		uc.emitTrustSignal(ctx, driverID, "DRIVER", domain.SignalHandshakeFail, 2,
			fmt.Sprintf("QR mismatch on delivery %s", deliveryID))
		return domain.ErrHandshakeFailed
	}

	//  Weight fraud guard
	if confirmedWeightKg > 0 && confirmedWeightKg != delivery.WeightKg {
		// Fetch driver profile to check vehicle capacity against actual weight.
		driverProfile, err := uc.driverRepo.GetByUserID(ctx, driverID)
		if err == nil && driverProfile.MaxWeightKg < confirmedWeightKg {
			uc.log.Warn("ConfirmPickup: confirmed weight exceeds vehicle capacity — weight fraud suspected",
				slog.String("delivery_id", deliveryID),
				slog.String("driver_id", driverID),
				slog.Float64("declared_kg", delivery.WeightKg),
				slog.Float64("confirmed_kg", confirmedWeightKg),
				slog.Float64("vehicle_max_kg", driverProfile.MaxWeightKg),
			)
			uc.appendAudit(ctx, deliveryID, driverID, "WEIGHT_FRAUD_DETECTED",
				string(delivery.CurrentState), "",
				fmt.Sprintf("declared=%.1fkg confirmed=%.1fkg vehicle_max=%.1fkg",
					delivery.WeightKg, confirmedWeightKg, driverProfile.MaxWeightKg))
			// Emit weight fraud trust signal against the merchant
			uc.emitTrustSignal(ctx, delivery.MerchantID, "MERCHANT", domain.SignalWeightFraud, 3,
				fmt.Sprintf("delivery %s: declared %.1f kg but confirmed %.1f kg at pickup",
					deliveryID, delivery.WeightKg, confirmedWeightKg))
			return domain.ErrWeightMismatch
		}
		// Weight is different but vehicle can still carry it — update the record.
		if persistErr := uc.repo.UpdateConfirmedWeight(ctx, deliveryID, confirmedWeightKg); persistErr != nil {
			uc.log.Warn("ConfirmPickup: UpdateConfirmedWeight failed — non-fatal",
				slog.String("delivery_id", deliveryID),
				slog.String("reason", persistErr.Error()),
			)
		}
	} else if confirmedWeightKg > 0 {
		// still persist it for audit completeness.
		if persistErr := uc.repo.UpdateConfirmedWeight(ctx, deliveryID, confirmedWeightKg); persistErr != nil {
			uc.log.Warn("ConfirmPickup: UpdateConfirmedWeight failed — non-fatal",
				slog.String("delivery_id", deliveryID),
				slog.String("reason", persistErr.Error()),
			)
		}
	}

	prevState := delivery.CurrentState
	if err := delivery.Transition(domain.StateInTransit, delivery.PickupQRCode); err != nil {
		uc.log.Error("ConfirmPickup: transition failed after valid handshake",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return err
	}

	if err := uc.repo.Update(ctx, delivery); err != nil {
		return err
	}

	uc.appendAudit(ctx, deliveryID, driverID, "PICKUP_CONFIRMED",
		string(prevState), string(domain.StateInTransit), "custody_transferred_to_driver")

	uc.log.Info("custody transferred to driver",
		slog.String("delivery_id", deliveryID),
		slog.String("driver_id", driverID),
	)
	return nil
}



// If uc.outbox is nil (payment integration not yet wired), the method falls
// back to a plain Update — safe for development, not for production.
func (uc *DeliveryUsecase) ConfirmDelivery(ctx context.Context, deliveryID, driverID, customerPIN string) error {
	delivery, err := uc.repo.GetByID(ctx, deliveryID)
	if err != nil {
		return err
	}

	if delivery.DriverID != driverID {
		return domain.ErrUnauthorized
	}

	if err := bcrypt.CompareHashAndPassword([]byte(delivery.DeliveryPIN), []byte(customerPIN)); err != nil {
		uc.log.Warn("ConfirmDelivery: PIN mismatch",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
		)
		uc.appendAudit(ctx, deliveryID, driverID, "HANDSHAKE_FAILED_DELIVERY",
			string(delivery.CurrentState), "", "pin_mismatch")
		uc.emitTrustSignal(ctx, driverID, "DRIVER", domain.SignalHandshakeFail, 2,
			fmt.Sprintf("delivery PIN mismatch on delivery %s", deliveryID))
		return domain.ErrHandshakeFailed
	}

	prevState := delivery.CurrentState
	if err := delivery.Transition(domain.StateDelivered, delivery.DeliveryPIN); err != nil {
		uc.log.Error("ConfirmDelivery: transition to DELIVERED failed",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return err
	}

	//  Atomic outbox write
	if uc.outbox != nil && uc.pool != nil {
		if err := uc.confirmDeliveryWithOutbox(ctx, delivery, prevState); err != nil {
			return err
		}
	} else {
		uc.log.Warn("ConfirmDelivery: outbox not configured — using plain update (payment not guaranteed)",
			slog.String("delivery_id", deliveryID),
		)
		if err := uc.repo.Update(ctx, delivery); err != nil {
			return err
		}
		uc.appendAudit(ctx, deliveryID, driverID, "DELIVERY_COMPLETED",
			string(prevState), string(domain.StateDelivered), "outbox=disabled; pii_purge_required=true")
	}

	uc.log.Info("delivery completed — client must purge customer PII",
		slog.String("delivery_id", deliveryID),
	)

	//Bayesian rolling rating update(for drivers' rankings and ratings) 
	uc.updateDriverRating(ctx, driverID, 5.0) 

	if err := uc.driverRepo.SetOnTrip(ctx, driverID, false); err != nil {
		uc.log.Error("ConfirmDelivery: SetOnTrip(false) failed",
			slog.String("delivery_id", deliveryID),
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
	}

	return nil
}

func (uc *DeliveryUsecase) confirmDeliveryWithOutbox(
	ctx context.Context,
	delivery *domain.Delivery,
	prevState domain.DeliveryState,
) error {
	tx, err := uc.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("ConfirmDelivery: begin tx: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			uc.log.Warn("ConfirmDelivery: tx rollback",
				slog.String("delivery_id", delivery.ID),
				slog.String("reason", rbErr.Error()),
			)
		}
	}()
	domainTx := wrapPgxTx(tx)

	// Update delivery state inside the transaction
	if err := uc.repo.UpdateWithinTx(ctx, domainTx, delivery); err != nil {
		return err
	}

	// Insert payment outbox event inside the same transaction
	// AmountRWF is 0 here — the actual fare calculation will be implemented
	// when the pricing service is wired in. The worker will skip events with
	// amount = 0 and re-queue them until pricing is resolved.
	idemKey := buildPaymentIdempotencyKey(delivery.ID, delivery.DriverID)
	now := time.Now().UTC()
	event := &domain.PaymentEvent{
		ID:                   uuid.NewString(),
		DeliveryID:           delivery.ID,
		DriverID:             delivery.DriverID,
		AmountRWF:            0, // TODO: inject fare from pricing service
		RecipientPhone:       "", // TODO: inject from driver profile lookup
		OutboxIdempotencyKey: idemKey,
		Status:               domain.PaymentPending,
		AttemptCount:         0,
		MaxAttempts:          5,
		NextRetryAt:          now,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	if err := uc.outbox.CreateWithinTx(ctx, domainTx, event); err != nil {
		return fmt.Errorf("ConfirmDelivery: outbox insert: %w", err)
	}

	// Commit both writes atomically
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("ConfirmDelivery: commit: %w", err)
	}

	uc.appendAudit(ctx, delivery.ID, delivery.DriverID, "DELIVERY_COMPLETED",
		string(prevState), string(domain.StateDelivered),
		"outbox_event_id="+event.ID+"; pii_purge_required=true")

	return nil
}

// RaiseDispute transitions an IN_TRANSIT delivery to DISPUTED state.
// The reason string is persisted in the audit ledger for dispatcher review.
func (uc *DeliveryUsecase) RaiseDispute(ctx context.Context, deliveryID, actorID, reason string) error {
	delivery, err := uc.repo.GetByID(ctx, deliveryID)
	if err != nil {
		return err
	}

	prevState := delivery.CurrentState
	if err := delivery.Transition(domain.StateDisputed, ""); err != nil {
		uc.log.Warn("RaiseDispute: transition rejected",
			slog.String("delivery_id", deliveryID),
			slog.String("actor_id", actorID),
			slog.String("current_state", string(prevState)),
			slog.String("reason", err.Error()),
		)
		return err
	}

	if err := uc.repo.Update(ctx, delivery); err != nil {
		return err
	}

	uc.appendAudit(ctx, deliveryID, actorID, "DISPUTE_RAISED",
		string(prevState), string(domain.StateDisputed), reason)

	//Emit dispute trust signal against the driver
	if delivery.DriverID != "" {
		uc.emitTrustSignal(ctx, delivery.DriverID, "DRIVER", domain.SignalDisputeRaised, 3,
			fmt.Sprintf("dispute on delivery %s: %s", deliveryID, reason))
	}

	return nil
}

// GetDelivery returns a single delivery by ID.
func (uc *DeliveryUsecase) GetDelivery(ctx context.Context, id string) (*domain.Delivery, error) {
	return uc.repo.GetByID(ctx, id)
}

// Returns active deliveries for a driver.
func (uc *DeliveryUsecase) ListDriverDeliveries(
	ctx context.Context, driverID string, states []domain.DeliveryState,
) ([]*domain.Delivery, error) {
	return uc.repo.ListByDriver(ctx, driverID, states)
}

// Returns deliveries for a merchant.
func (uc *DeliveryUsecase) ListMerchantDeliveries(
	ctx context.Context, merchantID string, states []domain.DeliveryState,
) ([]*domain.Delivery, error) {
	return uc.repo.ListByMerchant(ctx, merchantID, states)
}

//  Audit helpers 
func (uc *DeliveryUsecase) appendAudit(
	ctx context.Context,
	entityID, actorID, action, oldState, newState, meta string,
) {
	event := &domain.AuditEvent{
		ID:         uuid.NewString(),
		EntityID:   entityID,
		EntityType: "DELIVERY",
		ActorID:    actorID,
		Action:     action,
		OldState:   oldState,
		NewState:   newState,
		Metadata:   meta,
		CreatedAt:  time.Now().UTC(),
	}
	if err := uc.ledger.Append(ctx, event); err != nil {
		uc.log.Error("appendAudit: ledger write failed — audit record lost",
			slog.String("entity_id", entityID),
			slog.String("action", action),
			slog.String("reason", err.Error()),
		)
	}
}

func generateSecureToken(nBytes int) (string, error) {
	b := make([]byte, nBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}

func generateNumericPIN(digits int) string {
	b := make([]byte, digits)
	_, _ = rand.Read(b)
	pin := make([]byte, digits)
	for i := range b {
		pin[i] = '0' + (b[i] % 10)
	}
	return string(pin)
}

// buildPaymentIdempotencyKey derives a stable, unique key for the payment
// provider from the delivery and driver IDs. Using SHA-256 of the combined
// string guarantees the same delivery always produces the same key, making
// the at-least-once outbox worker safe to run multiple times.
func buildPaymentIdempotencyKey(deliveryID, driverID string) string {
	sum := sha256.Sum256([]byte(deliveryID + ":" + driverID))
	return hex.EncodeToString(sum[:])
}

// pgxTxWrapper adapts a pgx.Tx to the domain.Tx interface.
// Defined here so the usecase can begin a transaction from the pool and pass
// it to both the delivery and outbox repositories without either repo needing
// to know about the other.
type pgxTxWrapper struct{ inner pgx.Tx }

func (w *pgxTxWrapper) Exec(ctx context.Context, sql string, args ...interface{}) error {
	_, err := w.inner.Exec(ctx, sql, args...)
	return err
}

// wrapPgxTx converts a pgx.Tx into the domain.Tx abstraction.
func wrapPgxTx(tx pgx.Tx) domain.Tx {
	return &pgxTxWrapper{inner: tx}
}

// Rating & trust helpers

func (uc *DeliveryUsecase) updateDriverRating(ctx context.Context, driverID string, deliveryScore float64) {
	p, err := uc.driverRepo.GetByUserID(ctx, driverID)
	if err != nil {
		uc.log.Error("updateDriverRating: fetch driver profile failed",
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return
	}
	newTotal := p.TotalDeliveries + 1
	newRating := (p.Rating*float64(p.TotalDeliveries) + deliveryScore) / float64(newTotal)
	if newRating > 5.0 {
		newRating = 5.0
	}
	if newRating < 1.0 {
		newRating = 1.0
	}
	if err := uc.driverRepo.UpdateRating(ctx, driverID, newRating, newTotal); err != nil {
		uc.log.Error("updateDriverRating: UpdateRating failed",
			slog.String("driver_id", driverID),
			slog.Float64("new_rating", newRating),
			slog.String("reason", err.Error()),
		)
	} else {
		uc.log.Info("driver rating updated",
			slog.String("driver_id", driverID),
			slog.Float64("old_rating", p.Rating),
			slog.Float64("new_rating", newRating),
			slog.Int("total_deliveries", newTotal),
		)
	}
}

func (uc *DeliveryUsecase) emitTrustSignal(
	ctx context.Context,
	actorID, actorType, signalType string,
	severity int,
	details string,
) {
	if uc.trustRepo == nil {
		return
	}
	signal := &domain.TrustSignal{
		ActorID:    actorID,
		ActorType:  actorType,
		SignalType: signalType,
		Severity:   severity,
		Details:    details,
	}
	if err := uc.trustRepo.AppendSignal(ctx, signal); err != nil {
		uc.log.Error("emitTrustSignal: AppendSignal failed",
			slog.String("actor_id", actorID),
			slog.String("signal_type", signalType),
			slog.String("reason", err.Error()),
		)
	}
}
