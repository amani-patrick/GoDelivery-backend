package repository

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/umurinzi/backend/internal/domain"
)

// OutboxPostgresRepo implements domain.OutboxRepository against the
// payment_events table. The most critical method — CreateWithinTx — accepts
// a domain.Tx so the insert always runs inside the same transaction as the
// delivery state update, preserving atomicity.
//
// Note on tx adapter ownership: the usecase layer is responsible for beginning
// the transaction (pgxpool.Pool.Begin) and converting pgx.Tx → domain.Tx via
// its own wrapPgxTx helper. This repository never creates or owns transactions.
type OutboxPostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewOutboxPostgresRepo constructs the repository.
func NewOutboxPostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *OutboxPostgresRepo {
	return &OutboxPostgresRepo{pool: pool, log: log}
}

// CreateWithinTx inserts a PaymentEvent using the caller-provided transaction.
// Must be called inside the same BEGIN/COMMIT block as the delivery state update.
func (r *OutboxPostgresRepo) CreateWithinTx(ctx context.Context, tx domain.Tx, event *domain.PaymentEvent) error {
	const q = `
		INSERT INTO payment_events (
			id, delivery_id, driver_id, amount_rwf,
			recipient_phone, outbox_idempotency_key,
			status, attempt_count, max_attempts,
			next_retry_at, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6,
			$7, $8, $9,
			$10, $11, $12
		)`

	if err := tx.Exec(ctx, q,
		event.ID,
		event.DeliveryID,
		event.DriverID,
		event.AmountRWF,
		event.RecipientPhone,
		event.OutboxIdempotencyKey,
		string(event.Status),
		event.AttemptCount,
		event.MaxAttempts,
		event.NextRetryAt,
		event.CreatedAt,
		event.UpdatedAt,
	); err != nil {
		r.log.Error("outbox.CreateWithinTx failed",
			slog.String("delivery_id", event.DeliveryID),
			slog.String("driver_id", event.DriverID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("outbox create: %w", err)
	}
	return nil
}

// ClaimPending selects up to limit PENDING events using SELECT FOR UPDATE SKIP LOCKED,
// marks them PROCESSING atomically, and returns them to the caller (OutboxWorker).
// SKIP LOCKED ensures multiple worker instances never process the same event simultaneously.
func (r *OutboxPostgresRepo) ClaimPending(ctx context.Context, limit int) ([]*domain.PaymentEvent, error) {
	// Use a transaction so the UPDATE and SELECT are atomic.
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("outbox claim: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	const selectQ = `
		SELECT id, delivery_id, driver_id, amount_rwf,
		       recipient_phone, outbox_idempotency_key,
		       status, attempt_count, max_attempts,
		       last_attempted_at, next_retry_at,
		       provider_reference, failure_reason,
		       created_at, updated_at
		FROM payment_events
		WHERE status = 'PENDING' AND next_retry_at <= NOW()
		ORDER BY next_retry_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED`

	rows, err := tx.Query(ctx, selectQ, limit)
	if err != nil {
		return nil, fmt.Errorf("outbox claim: query: %w", err)
	}
	defer rows.Close()

	var events []*domain.PaymentEvent
	var ids []string
	for rows.Next() {
		e := &domain.PaymentEvent{}
		if err := rows.Scan(
			&e.ID, &e.DeliveryID, &e.DriverID, &e.AmountRWF,
			&e.RecipientPhone, &e.OutboxIdempotencyKey,
			&e.Status, &e.AttemptCount, &e.MaxAttempts,
			&e.LastAttemptedAt, &e.NextRetryAt,
			&e.ProviderRef, &e.FailureReason,
			&e.CreatedAt, &e.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("outbox claim: scan: %w", err)
		}
		events = append(events, e)
		ids = append(ids, e.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox claim: rows: %w", err)
	}

	if len(ids) == 0 {
		return nil, tx.Commit(ctx)
	}

	// Mark all selected rows as PROCESSING in one UPDATE.
	const updateQ = `
		UPDATE payment_events
		SET status = 'PROCESSING',
		    last_attempted_at = NOW(),
		    attempt_count = attempt_count + 1,
		    updated_at = NOW()
		WHERE id = ANY($1)`

	if _, err := tx.Exec(ctx, updateQ, ids); err != nil {
		return nil, fmt.Errorf("outbox claim: mark processing: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("outbox claim: commit: %w", err)
	}
	return events, nil
}

// MarkSucceeded records a successful payment and transitions the event to SUCCEEDED.
func (r *OutboxPostgresRepo) MarkSucceeded(ctx context.Context, id, providerRef string) error {
	const q = `
		UPDATE payment_events
		SET status = 'SUCCEEDED',
		    provider_reference = $2,
		    updated_at = NOW()
		WHERE id = $1`

	tag, err := r.pool.Exec(ctx, q, id, providerRef)
	if err != nil {
		return fmt.Errorf("outbox mark succeeded: %w", err)
	}
	if tag.RowsAffected() == 0 {
		r.log.Warn("outbox.MarkSucceeded: event not found", slog.String("id", id))
	}
	return nil
}

// MarkFailed transitions a terminal failure — all retries exhausted.
func (r *OutboxPostgresRepo) MarkFailed(ctx context.Context, id, reason string) error {
	const q = `
		UPDATE payment_events
		SET status = 'FAILED',
		    failure_reason = $2,
		    updated_at = NOW()
		WHERE id = $1`

	_, err := r.pool.Exec(ctx, q, id, reason)
	if err != nil {
		return fmt.Errorf("outbox mark failed: %w", err)
	}
	return nil
}

// ScheduleRetry sets the next retry time using exponential back-off.
// The caller (OutboxWorker) computes nextRetryAt:
//
//	nextRetryAt = now + (2^attempt * baseIntervalSeconds)
func (r *OutboxPostgresRepo) ScheduleRetry(ctx context.Context, id string, nextRetryAt time.Time) error {
	const q = `
		UPDATE payment_events
		SET status = 'PENDING',
		    next_retry_at = $2,
		    updated_at = NOW()
		WHERE id = $1`

	_, err := r.pool.Exec(ctx, q, id, nextRetryAt)
	if err != nil {
		return fmt.Errorf("outbox schedule retry: %w", err)
	}
	return nil
}
