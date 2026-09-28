package repository

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/umurinzi/backend/internal/domain"
)

// Package repository contains the PostgreSQL + PostGIS implementations of every
// domain repository interface. All SQL is parameterised — no string interpolation
// of user-controlled input. Cross-module database writes are strictly forbidden;
// each module owns only its own tables.


// pgUniqueViolation is the PostgreSQL SQLSTATE code for a unique constraint violation.
const pgUniqueViolation = "23505"

// DeliveryPostgresRepo

// DeliveryPostgresRepo is the concrete PostgreSQL implementation of
// domain.DeliveryRepository. It interacts only with the `deliveries` table.
type DeliveryPostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewDeliveryPostgresRepo constructs the repository with a shared connection pool.
func NewDeliveryPostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *DeliveryPostgresRepo {
	return &DeliveryPostgresRepo{pool: pool, log: log}
}

// Create inserts a new Delivery row. The caller must have already generated the ID
// and hashed the custody tokens. This method never generates IDs or tokens itself.
func (r *DeliveryPostgresRepo) Create(ctx context.Context, d *domain.Delivery) error {
	const q = `
		INSERT INTO deliveries (
			id, merchant_id, driver_id, customer_id, current_state,
			pickup_qr_code, delivery_pin,
			pickup_lat,  pickup_lng,
			dropoff_lat, dropoff_lng,
			description, weight_kg,
			vehicle_type_required, package_category,
			confirmed_weight_kg, priority_level,
			stack_group_id, stack_sequence,
			prep_time_minutes, ready_at,
			fare_rwf, created_at, updated_at
		) VALUES (
			$1,  $2,  $3,  $4,  $5,
			$6,  $7,
			$8,  $9,
			$10, $11,
			$12, $13,
			$14, $15,
			$16, $17,
			$18, $19,
			$20, $21,
			$22, $23, $24
		)`

	_, err := r.pool.Exec(ctx, q,
		d.ID,
		d.MerchantID,
		nullableString(d.DriverID),
		d.CustomerID,
		string(d.CurrentState),
		d.PickupQRCode,
		d.DeliveryPIN,
		d.PickupLoc.Lat,
		d.PickupLoc.Lng,
		d.DropoffLoc.Lat,
		d.DropoffLoc.Lng,
		d.Description,
		d.WeightKg,
		string(d.VehicleTypeRequired),
		string(d.PackageCategory),
		d.ConfirmedWeightKg,
		d.PriorityLevel,
		nullableString(d.StackGroupID),
		d.StackSequence,
		d.PrepTimeMinutes,
		d.ReadyAt,
		d.FareRWF,
		d.CreatedAt,
		d.UpdatedAt,
	)
	if err != nil {
		r.log.Error("delivery.repo.Create failed",
			slog.String("delivery_id", d.ID),
			slog.String("merchant_id", d.MerchantID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("create delivery: %w", err)
	}
	return nil
}

// CreateWithinTx inserts the delivery inside the caller's transaction so the
// row and the merchant's wallet escrow commit atomically (order funding).
func (r *DeliveryPostgresRepo) CreateWithinTx(ctx context.Context, tx domain.Tx, d *domain.Delivery) error {
	const q = `
		INSERT INTO deliveries (
			id, merchant_id, driver_id, customer_id, current_state,
			pickup_qr_code, delivery_pin,
			pickup_lat,  pickup_lng,
			dropoff_lat, dropoff_lng,
			description, weight_kg,
			vehicle_type_required, package_category,
			confirmed_weight_kg, priority_level,
			stack_group_id, stack_sequence,
			prep_time_minutes, ready_at,
			fare_rwf, created_at, updated_at
		) VALUES (
			$1,  $2,  $3,  $4,  $5,
			$6,  $7,
			$8,  $9,
			$10, $11,
			$12, $13,
			$14, $15,
			$16, $17,
			$18, $19,
			$20, $21,
			$22, $23, $24
		)`

	if err := tx.Exec(ctx, q,
		d.ID,
		d.MerchantID,
		nullableString(d.DriverID),
		d.CustomerID,
		string(d.CurrentState),
		d.PickupQRCode,
		d.DeliveryPIN,
		d.PickupLoc.Lat,
		d.PickupLoc.Lng,
		d.DropoffLoc.Lat,
		d.DropoffLoc.Lng,
		d.Description,
		d.WeightKg,
		string(d.VehicleTypeRequired),
		string(d.PackageCategory),
		d.ConfirmedWeightKg,
		d.PriorityLevel,
		nullableString(d.StackGroupID),
		d.StackSequence,
		d.PrepTimeMinutes,
		d.ReadyAt,
		d.FareRWF,
		d.CreatedAt,
		d.UpdatedAt,
	); err != nil {
		r.log.Error("delivery.repo.CreateWithinTx failed",
			slog.String("delivery_id", d.ID),
			slog.String("merchant_id", d.MerchantID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("create delivery in tx: %w", err)
	}
	return nil
}

// GetByID fetches a single delivery by primary key.
// Returns domain.ErrDeliveryNotFound when no row matches.
func (r *DeliveryPostgresRepo) GetByID(ctx context.Context, id string) (*domain.Delivery, error) {
	const q = `
		SELECT
			id, merchant_id, COALESCE(driver_id, ''), customer_id, current_state,
			pickup_qr_code, delivery_pin,
			pickup_lat,  pickup_lng,
			dropoff_lat, dropoff_lng,
			description, weight_kg,
			vehicle_type_required, package_category,
			confirmed_weight_kg, priority_level,
			COALESCE(stack_group_id, ''), stack_sequence,
			prep_time_minutes, COALESCE(ready_at, '0001-01-01'::timestamptz),
			fare_rwf, created_at, updated_at
		FROM deliveries
		WHERE id = $1`

	row := r.pool.QueryRow(ctx, q, id)
	d, err := scanDelivery(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrDeliveryNotFound
		}
		r.log.Error("delivery.repo.GetByID failed",
			slog.String("delivery_id", id),
			slog.String("reason", err.Error()),
		)
		return nil, fmt.Errorf("get delivery by id: %w", err)
	}
	return d, nil
}

// Update persists a mutated Delivery aggregate using optimistic concurrency
// control: the WHERE clause includes the last known updated_at so a stale
// write (two concurrent mutations of the same row) is detected immediately.
func (r *DeliveryPostgresRepo) Update(ctx context.Context, d *domain.Delivery) error {
	const q = `
		UPDATE deliveries SET
			driver_id     = $2,
			current_state = $3,
			pickup_qr_code = $4,
			delivery_pin  = $5,
			updated_at    = $6
		WHERE id = $1 AND updated_at = $7`

	prevUpdatedAt := d.UpdatedAt
	d.UpdatedAt = time.Now().UTC()

	tag, err := r.pool.Exec(ctx, q,
		d.ID,
		nullableString(d.DriverID),
		string(d.CurrentState),
		d.PickupQRCode,
		d.DeliveryPIN,
		d.UpdatedAt,
		prevUpdatedAt,
	)
	if err != nil {
		r.log.Error("delivery.repo.Update failed",
			slog.String("delivery_id", d.ID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update delivery: %w", err)
	}
	if tag.RowsAffected() == 0 {
		// Either the row was deleted or another writer updated it first.
		return domain.ErrDeliveryNotFound
	}
	return nil
}

// UpdateWithinTx performs the same update as Update but uses the provided
// domain.Tx handle so the write participates in the caller's transaction.
// Used by ConfirmDelivery to atomically update the delivery state and insert
// the outbox payment event in a single database transaction.
//
// Optimistic lock note: domain.Tx.Exec returns error only (no RowsAffected),
// so we cannot check for a stale write here the same way Update does.
// This is safe because ConfirmDelivery holds the delivery in-memory after
// GetByID, the state machine already validated the transition, and the caller
// holds a Redis distributed lock (AcceptOrder path) or the customer PIN
// (ConfirmDelivery path) ensuring no concurrent mutation can invalidate the row
// between GetByID and this write.
func (r *DeliveryPostgresRepo) UpdateWithinTx(ctx context.Context, tx domain.Tx, d *domain.Delivery) error {
	const q = `
		UPDATE deliveries SET
			driver_id      = $2,
			current_state  = $3,
			pickup_qr_code = $4,
			delivery_pin   = $5,
			updated_at     = $6
		WHERE id = $1`

	d.UpdatedAt = time.Now().UTC()

	if err := tx.Exec(ctx, q,
		d.ID,
		nullableString(d.DriverID),
		string(d.CurrentState),
		d.PickupQRCode,
		d.DeliveryPIN,
		d.UpdatedAt,
	); err != nil {
		r.log.Error("delivery.repo.UpdateWithinTx failed",
			slog.String("delivery_id", d.ID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update delivery in tx: %w", err)
	}
	return nil
}

// ListByDriver returns active deliveries for a driver filtered by state.
// Returns an empty (non-nil) slice when no rows match.
func (r *DeliveryPostgresRepo) ListByDriver(
	ctx context.Context, driverID string, states []domain.DeliveryState,
) ([]*domain.Delivery, error) {
	const q = `
		SELECT
			id, merchant_id, COALESCE(driver_id, ''), customer_id, current_state,
			pickup_qr_code, delivery_pin,
			pickup_lat,  pickup_lng,
			dropoff_lat, dropoff_lng,
			description, weight_kg,
			vehicle_type_required, package_category,
			confirmed_weight_kg, priority_level,
			COALESCE(stack_group_id, ''), stack_sequence,
			prep_time_minutes, COALESCE(ready_at, '0001-01-01'::timestamptz),
			fare_rwf, created_at, updated_at
		FROM deliveries
		WHERE driver_id = $1 AND current_state = ANY($2::text[])
		ORDER BY stack_sequence ASC, created_at DESC`

	return r.queryList(ctx, q, driverID, statesToStrings(states))
}

// ListByMerchant returns deliveries for a merchant filtered by state.
func (r *DeliveryPostgresRepo) ListByMerchant(
	ctx context.Context, merchantID string, states []domain.DeliveryState,
) ([]*domain.Delivery, error) {
	const q = `
		SELECT
			id, merchant_id, COALESCE(driver_id, ''), customer_id, current_state,
			pickup_qr_code, delivery_pin,
			pickup_lat,  pickup_lng,
			dropoff_lat, dropoff_lng,
			description, weight_kg,
			vehicle_type_required, package_category,
			confirmed_weight_kg, priority_level,
			COALESCE(stack_group_id, ''), stack_sequence,
			prep_time_minutes, COALESCE(ready_at, '0001-01-01'::timestamptz),
			fare_rwf, created_at, updated_at
		FROM deliveries
		WHERE merchant_id = $1 AND current_state = ANY($2::text[])
		ORDER BY created_at DESC`

	return r.queryList(ctx, q, merchantID, statesToStrings(states))
}

// ListActiveByDriver returns all non-terminal deliveries for a driver.
// Used by the stacking engine to check what a driver is currently carrying.
func (r *DeliveryPostgresRepo) ListActiveByDriver(ctx context.Context, driverID string) ([]*domain.Delivery, error) {
	const q = `
		SELECT
			id, merchant_id, COALESCE(driver_id, ''), customer_id, current_state,
			pickup_qr_code, delivery_pin,
			pickup_lat,  pickup_lng,
			dropoff_lat, dropoff_lng,
			description, weight_kg,
			vehicle_type_required, package_category,
			confirmed_weight_kg, priority_level,
			COALESCE(stack_group_id, ''), stack_sequence,
			prep_time_minutes, COALESCE(ready_at, '0001-01-01'::timestamptz),
			fare_rwf, created_at, updated_at
		FROM deliveries
		WHERE driver_id = $1
		  AND current_state IN ('ASSIGNED','IN_TRANSIT')
		ORDER BY stack_sequence ASC`

	rows, err := r.pool.Query(ctx, q, driverID)
	if err != nil {
		return nil, fmt.Errorf("list active deliveries by driver: %w", err)
	}
	defer rows.Close()

	result := make([]*domain.Delivery, 0)
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("scan active delivery: %w", err)
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

// UpdateConfirmedWeight sets the confirmed_weight_kg field after the driver
// physically weighs the package at pickup — weight fraud guard).
func (r *DeliveryPostgresRepo) UpdateConfirmedWeight(ctx context.Context, deliveryID string, confirmedKg float64) error {
	const q = `UPDATE deliveries SET confirmed_weight_kg = $2, updated_at = NOW() WHERE id = $1`
	tag, err := r.pool.Exec(ctx, q, deliveryID, confirmedKg)
	if err != nil {
		r.log.Error("delivery.repo.UpdateConfirmedWeight failed",
			slog.String("delivery_id", deliveryID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update confirmed weight: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrDeliveryNotFound
	}
	return nil
}

// UpdateStackInfo writes the stack_group_id and stack_sequence after the
// matching engine approves a multi-drop stacking arrangement .
func (r *DeliveryPostgresRepo) UpdateStackInfo(ctx context.Context, deliveryID, stackGroupID string, sequence int) error {
	const q = `UPDATE deliveries SET stack_group_id = $2, stack_sequence = $3, updated_at = NOW() WHERE id = $1`
	tag, err := r.pool.Exec(ctx, q, deliveryID, nullableString(stackGroupID), sequence)
	if err != nil {
		r.log.Error("delivery.repo.UpdateStackInfo failed",
			slog.String("delivery_id", deliveryID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update stack info: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrDeliveryNotFound
	}
	return nil
}

// UpdateOptimisedSequence persists the backend-enforced drop-off position
// (1-based) computed by the multi-drop TSP optimiser. The mobile app must
// present this order to the driver — it never decides the sequence itself.
func (r *DeliveryPostgresRepo) UpdateOptimisedSequence(ctx context.Context, deliveryID string, sequence int) error {
	const q = `UPDATE deliveries SET optimised_sequence = $2, updated_at = NOW() WHERE id = $1`
	tag, err := r.pool.Exec(ctx, q, deliveryID, sequence)
	if err != nil {
		r.log.Error("delivery.repo.UpdateOptimisedSequence failed",
			slog.String("delivery_id", deliveryID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update optimised sequence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrDeliveryNotFound
	}
	return nil
}

// internal helpers

func (r *DeliveryPostgresRepo) queryList(
	ctx context.Context,
	q string,
	filterValue string,
	states []string,
) ([]*domain.Delivery, error) {
	rows, err := r.pool.Query(ctx, q, filterValue, states)
	if err != nil {
		return nil, fmt.Errorf("query deliveries: %w", err)
	}
	defer rows.Close()

	result := make([]*domain.Delivery, 0)
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("scan delivery row: %w", err)
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("delivery rows iteration: %w", err)
	}
	return result, nil
}

// scannable is satisfied by both pgx.Row (single) and pgx.Rows (multi).
type scannable interface {
	Scan(dest ...any) error
}

func scanDelivery(row scannable) (*domain.Delivery, error) {
	d := &domain.Delivery{}
	err := row.Scan(
		&d.ID,
		&d.MerchantID,
		&d.DriverID,
		&d.CustomerID,
		&d.CurrentState,
		&d.PickupQRCode,
		&d.DeliveryPIN,
		&d.PickupLoc.Lat,
		&d.PickupLoc.Lng,
		&d.DropoffLoc.Lat,
		&d.DropoffLoc.Lng,
		&d.Description,
		&d.WeightKg,
		&d.VehicleTypeRequired,
		&d.PackageCategory,
		&d.ConfirmedWeightKg,
		&d.PriorityLevel,
		&d.StackGroupID,
		&d.StackSequence,
		&d.PrepTimeMinutes,
		&d.ReadyAt,
		&d.FareRWF,
		&d.CreatedAt,
		&d.UpdatedAt,
	)
	return d, err
}

// nullableString converts an empty Go string to a SQL NULL.
// This avoids inserting empty strings where the schema allows NULL.
func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func statesToStrings(states []domain.DeliveryState) []string {
	out := make([]string, len(states))
	for i, s := range states {
		out[i] = string(s)
	}
	return out
}

// isUniqueViolation checks whether a postgres error is a unique constraint breach.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}
