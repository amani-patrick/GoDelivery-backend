package repository

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/umurinzi/backend/internal/domain"
)

// TrustPostgresRepo implements domain.TrustRepository against the
// trust_signals table (created in migration 007).
//
// The table is append-only: trust signals are never updated or deleted.
// This mirrors the audit_events immutability contract.
type TrustPostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewTrustPostgresRepo constructs the repository.
func NewTrustPostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *TrustPostgresRepo {
	return &TrustPostgresRepo{pool: pool, log: log}
}

// AppendSignal inserts a single trust signal. The ID is generated here so
// callers never need to manage signal IDs.
func (r *TrustPostgresRepo) AppendSignal(ctx context.Context, signal *domain.TrustSignal) error {
	if signal.CreatedAt.IsZero() {
		signal.CreatedAt = time.Now().UTC()
	}
	const q = `
		INSERT INTO trust_signals (id, actor_id, actor_type, signal_type, severity, details, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := r.pool.Exec(ctx, q,
		uuid.NewString(),
		signal.ActorID,
		signal.ActorType,
		signal.SignalType,
		signal.Severity,
		signal.Details,
		signal.CreatedAt,
	)
	if err != nil {
		r.log.Error("trust.repo.AppendSignal failed",
			slog.String("actor_id", signal.ActorID),
			slog.String("signal_type", signal.SignalType),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("append trust signal: %w", err)
	}
	r.log.Info("trust signal recorded",
		slog.String("actor_id", signal.ActorID),
		slog.String("actor_type", signal.ActorType),
		slog.String("signal_type", signal.SignalType),
		slog.Int("severity", signal.Severity),
	)
	return nil
}

// CountSignals returns the number of signals of a given type for an actor
// since the provided time boundary. Used by the trust scoring service to
// calculate score degradation (e.g. 3 HANDSHAKE_FAIL in 7 days → WATCH).
func (r *TrustPostgresRepo) CountSignals(
	ctx context.Context, actorID, signalType string, since time.Time,
) (int, error) {
	const q = `
		SELECT COUNT(*) FROM trust_signals
		WHERE actor_id = $1 AND signal_type = $2 AND created_at >= $3`

	var count int
	if err := r.pool.QueryRow(ctx, q, actorID, signalType, since).Scan(&count); err != nil {
		return 0, fmt.Errorf("count trust signals: %w", err)
	}
	return count, nil
}

// CountDeliveriesByMerchantToRecipient counts how many deliveries a given
// merchant has sent to the same recipient phone number.
// Used to detect the phantom cargo scam: a merchant sending fake orders to
// a co-conspirator to inflate transaction volume.
func (r *TrustPostgresRepo) CountDeliveriesByMerchantToRecipient(
	ctx context.Context, merchantID, recipientPhone string,
) (int, error) {
	// Join deliveries with users on customer_id to look up the customer's phone.
	const q = `
		SELECT COUNT(*)
		FROM deliveries d
		JOIN users u ON u.id = d.customer_id
		WHERE d.merchant_id = $1
		  AND u.phone = $2
		  AND d.current_state = 'DELIVERED'`

	var count int
	if err := r.pool.QueryRow(ctx, q, merchantID, recipientPhone).Scan(&count); err != nil {
		return 0, fmt.Errorf("count merchant-to-recipient deliveries: %w", err)
	}
	return count, nil
}
