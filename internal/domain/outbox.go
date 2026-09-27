package domain

import (
	"context"
	"time"
)

type PaymentEventStatus string

const (
	PaymentPending PaymentEventStatus = "PENDING"
	PaymentProcessing PaymentEventStatus = "PROCESSING"
	PaymentSucceeded PaymentEventStatus = "SUCCEEDED"
	PaymentFailed PaymentEventStatus = "FAILED"
)


// PaymentEvent is an outbox record created atomically with the DELIVERED state
// transition. It represents the intent to pay a driver for a completed delivery.
//
// Atomicity contract:
//   The INSERT into payment_events must happen in the SAME Postgres transaction
//   as the UPDATE to deliveries.current_state = 'DELIVERED'. This is enforced
//   by OutboxRepository.CreateWithDeliveryUpdate, which accepts a pgx.Tx.
//
// At-least-once semantics:
//   The OutboxWorker may process this event more than once. The
//   OutboxIdempotencyKey must be forwarded to the payment provider so they
//   can deduplicate on their end. MTN MoMo and Airtel Money both support
//   X-Reference-Id idempotency headers for this purpose.
type PaymentEvent struct {
	ID         string    `json:"id"`
	DeliveryID string    `json:"delivery_id"`
	DriverID   string    `json:"driver_id"`
	AmountRWF  int64     `json:"amount_rwf"`

	// RecipientPhone is the driver's MoMo-registered number (+2507XXXXXXXX).
	RecipientPhone string `json:"recipient_phone"`

	OutboxIdempotencyKey string `json:"outbox_idempotency_key"`

	Status           PaymentEventStatus `json:"status"`
	AttemptCount     int                `json:"attempt_count"`
	MaxAttempts      int                `json:"max_attempts"`
	LastAttemptedAt  *time.Time         `json:"last_attempted_at,omitempty"`
	NextRetryAt      time.Time          `json:"next_retry_at"`
	ProviderRef      string             `json:"provider_reference,omitempty"`
	FailureReason    string             `json:"failure_reason,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// OutboxRepository defines the persistence contract for the payment outbox.
// The critical method is CreateWithinTx — it must execute inside the same
// database transaction that updates the delivery state to DELIVERED.
type OutboxRepository interface {
	// CreateWithinTx inserts a PaymentEvent using the provided transaction handle.
	// The caller is responsible for beginning and committing/rolling back the tx.
	// This is the ONLY correct way to insert an outbox event — any other path
	// breaks the atomicity guarantee.
	CreateWithinTx(ctx context.Context, tx Tx, event *PaymentEvent) error

	// ClaimPending atomically marks up to limit PENDING events as PROCESSING
	ClaimPending(ctx context.Context, limit int) ([]*PaymentEvent, error)
	
	// MarkSucceeded records a successful payment confirmation from the provider.
	MarkSucceeded(ctx context.Context, id, providerRef string) error

	// MarkFailed records a terminal failure after all retries are exhausted.
	MarkFailed(ctx context.Context, id, reason string) error

	// ScheduleRetry increments attempt_count and sets next_retry_at for
	// exponential back-off. Called when a payment attempt fails transiently.
	ScheduleRetry(ctx context.Context, id string, nextRetryAt time.Time) error
}

// Tx is an abstraction over a database transaction handle. The concrete
// implementation wraps pgx.Tx. Defined here in the domain layer so the
// OutboxRepository interface remains infrastructure-agnostic.
type Tx interface {
	// Exec runs a parameterised SQL statement inside the transaction.
	Exec(ctx context.Context, sql string, args ...interface{}) error
}
