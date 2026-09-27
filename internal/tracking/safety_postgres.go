package tracking

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/umurinzi/backend/internal/domain"
)

type SafetyPostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

func NewSafetyPostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *SafetyPostgresRepo {
	return &SafetyPostgresRepo{pool: pool, log: log}
}

func (r *SafetyPostgresRepo) ReportIncident(ctx context.Context, lat, lng float64) error {
	// If an existing danger zone is within 500m, bump its threat level and update center/time.
	// Else, create a new one.
	const q = `
		WITH updated AS (
			UPDATE danger_zones
			SET threat_level = threat_level + 1.0,
			    last_incident_at = NOW()
			WHERE ST_DWithin(center, ST_SetSRID(ST_MakePoint($1, $2), 4326), 500, true)
			RETURNING id
		)
		INSERT INTO danger_zones (id, center, radius_meters, threat_level)
		SELECT $3, ST_SetSRID(ST_MakePoint($1, $2), 4326), 500.0, 1.0
		WHERE NOT EXISTS (SELECT 1 FROM updated);
	`
	_, err := r.pool.Exec(ctx, q, lng, lat, uuid.NewString())
	if err != nil {
		return fmt.Errorf("ReportIncident: %w", err)
	}
	return nil
}

func (r *SafetyPostgresRepo) IsDangerZone(ctx context.Context, lat, lng float64) (bool, error) {
	const q = `
		SELECT EXISTS (
			SELECT 1 FROM danger_zones
			WHERE ST_DWithin(center, ST_SetSRID(ST_MakePoint($1, $2), 4326), radius_meters, true)
		)
	`
	var exists bool
	if err := r.pool.QueryRow(ctx, q, lng, lat).Scan(&exists); err != nil {
		return false, fmt.Errorf("IsDangerZone: %w", err)
	}
	return exists, nil
}

func (r *SafetyPostgresRepo) FindNearestSafeHub(ctx context.Context, lat, lng float64) (*domain.SafeHub, error) {
	const q = `
		SELECT id, name, ST_Y(geom::geometry) as lat, ST_X(geom::geometry) as lng
		FROM safe_hubs
		ORDER BY geom <-> ST_SetSRID(ST_MakePoint($1, $2), 4326)
		LIMIT 1
	`
	var hub domain.SafeHub
	if err := r.pool.QueryRow(ctx, q, lng, lat).Scan(&hub.ID, &hub.Name, &hub.Lat, &hub.Lng); err != nil {
		return nil, fmt.Errorf("FindNearestSafeHub: %w", err)
	}
	return &hub, nil
}

func (r *SafetyPostgresRepo) DecayThreatLevels(ctx context.Context, decayAmount float64) error {
	const updateQ = `UPDATE danger_zones SET threat_level = threat_level - $1`
	const deleteQ = `DELETE FROM danger_zones WHERE threat_level <= 0`

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, updateQ, decayAmount); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, deleteQ); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
