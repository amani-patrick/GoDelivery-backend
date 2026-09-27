// Package ledger implements the append-only audit trail for all significant
// system events. Records are never mutated or deleted — only inserted.
// This satisfies the evidentiary requirements of the Rwanda Data Protection
// Law (Law No. 058/2021) and provides a forensic timeline for security dispatchers.
package ledger

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/umurinzi/backend/internal/domain"
)

// PostgresLedger implements domain.LedgerRepository against an append-only
// PostgreSQL table. The table has no UPDATE or DELETE grants at the DB role level.
type PostgresLedger struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewPostgresLedger constructs the ledger repository.
func NewPostgresLedger(pool *pgxpool.Pool, log *slog.Logger) *PostgresLedger {
	return &PostgresLedger{pool: pool, log: log}
}

// Append inserts a single AuditEvent. It never updates or deletes.
// If the insert fails, it is logged at ERROR level and the error is returned
// so the caller can decide whether to abort the enclosing operation.
func (l *PostgresLedger) Append(ctx context.Context, event *domain.AuditEvent) error {
	const q = `
		INSERT INTO audit_events (
			id, entity_id, entity_type, actor_id,
			action, old_state, new_state, metadata, created_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9
		)`

	_, err := l.pool.Exec(ctx, q,
		event.ID,
		event.EntityID,
		event.EntityType,
		event.ActorID,
		event.Action,
		nullIfEmpty(event.OldState),
		nullIfEmpty(event.NewState),
		nullIfEmpty(event.Metadata),
		event.CreatedAt,
	)
	if err != nil {
		l.log.Error("ledger.Append failed — audit record lost",
			slog.String("event_id", event.ID),
			slog.String("entity_id", event.EntityID),
			slog.String("action", event.Action),
			slog.String("actor_id", event.ActorID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("ledger append: %w", err)
	}

	l.log.Info("audit event recorded",
		slog.String("event_id", event.ID),
		slog.String("entity_id", event.EntityID),
		slog.String("entity_type", event.EntityType),
		slog.String("action", event.Action),
		slog.String("actor_id", event.ActorID),
		slog.String("old_state", event.OldState),
		slog.String("new_state", event.NewState),
	)
	return nil
}

// QueryByEntity returns the full ordered history for a given entity (e.g. a delivery ID).
// Results are ordered oldest-first to reconstruct the lifecycle timeline.
func (l *PostgresLedger) QueryByEntity(ctx context.Context, entityID string) ([]*domain.AuditEvent, error) {
	const q = `
		SELECT
			id, entity_id, entity_type, actor_id,
			action,
			COALESCE(old_state, ''),
			COALESCE(new_state, ''),
			COALESCE(metadata, ''),
			created_at
		FROM audit_events
		WHERE entity_id = $1
		ORDER BY created_at ASC`

	rows, err := l.pool.Query(ctx, q, entityID)
	if err != nil {
		return nil, fmt.Errorf("ledger query by entity: %w", err)
	}
	defer rows.Close()

	var events []*domain.AuditEvent
	for rows.Next() {
		e := &domain.AuditEvent{}
		if err := rows.Scan(
			&e.ID,
			&e.EntityID,
			&e.EntityType,
			&e.ActorID,
			&e.Action,
			&e.OldState,
			&e.NewState,
			&e.Metadata,
			&e.CreatedAt,
		); err != nil {
			l.log.Error("ledger.QueryByEntity scan failed",
				slog.String("entity_id", entityID),
				slog.String("reason", err.Error()),
			)
			return nil, fmt.Errorf("ledger scan: %w", err)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ledger rows error: %w", err)
	}
	return events, nil
}

// QueryByActor returns all events recorded for a specific actor (dispatcher forensics).
func (l *PostgresLedger) QueryByActor(ctx context.Context, actorID string) ([]*domain.AuditEvent, error) {
	const q = `
		SELECT
			id, entity_id, entity_type, actor_id,
			action,
			COALESCE(old_state, ''),
			COALESCE(new_state, ''),
			COALESCE(metadata, ''),
			created_at
		FROM audit_events
		WHERE actor_id = $1
		ORDER BY created_at DESC
		LIMIT 500`

	rows, err := l.pool.Query(ctx, q, actorID)
	if err != nil {
		return nil, fmt.Errorf("ledger query by actor: %w", err)
	}
	defer rows.Close()

	var events []*domain.AuditEvent
	for rows.Next() {
		e := &domain.AuditEvent{}
		if err := rows.Scan(
			&e.ID, &e.EntityID, &e.EntityType, &e.ActorID,
			&e.Action, &e.OldState, &e.NewState, &e.Metadata,
			&e.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("ledger actor scan: %w", err)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// ── helpers ───────────────────────────────────────────────────────────────────

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
