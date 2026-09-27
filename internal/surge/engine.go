package surge

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	SampleInterval = 2 * time.Minute // how often we record demand
	WindowSize     = 5              
	MaxMultiplier  = 3.0             // hard ceiling
	MinMultiplier  = 1.0
	MaxDeltaPerTick = 0.25           // max multiplier change per tick 
)

// Engine holds the current surge multiplier and handles background sampling.
type Engine struct {
	pool    *pgxpool.Pool
	log     *slog.Logger
	mu      sync.RWMutex
	current float64 // protected by mu
}

func NewEngine(pool *pgxpool.Pool, log *slog.Logger) *Engine {
	return &Engine{pool: pool, log: log, current: 1.0}
}

// Current returns the live surge multiplier (thread-safe).
func (e *Engine) Current() float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.current
}

// Apply multiplies a base fare by the current surge.
func (e *Engine) Apply(baseFareRWF int64) int64 {
	m := e.Current()
	return int64(math.Round(float64(baseFareRWF) * m))
}

// RecordSample writes one demand data point to the DB.
func (e *Engine) RecordSample(ctx context.Context, openOrders, onlineDrivers int) error {
	const q = `
		INSERT INTO surge_demand_samples (region_key, open_orders, online_drivers)
		VALUES ('global', $1, $2)`
	_, err := e.pool.Exec(ctx, q, openOrders, onlineDrivers)
	return err
}

// Recompute reads the last WindowSize samples and recalculates the multiplier
// using a moving-average smoothing filter.
func (e *Engine) Recompute(ctx context.Context) error {
	const q = `
		SELECT open_orders, online_drivers FROM surge_demand_samples
		WHERE region_key = 'global'
		ORDER BY sampled_at DESC
		LIMIT $1`
	rows, err := e.pool.Query(ctx, q, WindowSize)
	if err != nil {
		return fmt.Errorf("surge recompute: %w", err)
	}
	defer rows.Close()

	var totalOrders, totalDrivers, n int
	for rows.Next() {
		var o, d int
		if err := rows.Scan(&o, &d); err != nil {
			continue
		}
		totalOrders += o
		totalDrivers += d
		n++
	}
	if n == 0 {
		return nil
	}

	avgOrders := float64(totalOrders) / float64(n)
	avgDrivers := math.Max(1, float64(totalDrivers)/float64(n))
	ratio := avgOrders / avgDrivers

	// Map ratio → multiplier:
	// ratio < 1 → 1.0x  (supply exceeds demand)
	// ratio = 2 → 1.5x
	// ratio = 4 → 2.0x
	// ratio ≥ 8 → cap at MaxMultiplier
	target := 1.0 + (ratio-1.0)*0.2
	target = math.Max(MinMultiplier, math.Min(MaxMultiplier, target))

	e.mu.Lock()
	prev := e.current
	// Smoothing: clamp to ±MaxDeltaPerTick
	delta := target - prev
	if delta > MaxDeltaPerTick {
		delta = MaxDeltaPerTick
	} else if delta < -MaxDeltaPerTick {
		delta = -MaxDeltaPerTick
	}
	e.current = prev + delta
	e.mu.Unlock()

	e.log.Info("surge multiplier updated",
		slog.Float64("prev", prev),
		slog.Float64("target", target),
		slog.Float64("current", e.current),
		slog.Float64("avg_orders", avgOrders),
		slog.Float64("avg_drivers", avgDrivers),
	)
	return nil
}

// RunLoop periodically samples demand and recomputes the multiplier.
// openOrders and onlineDrivers are callbacks to avoid circular imports.
func (e *Engine) RunLoop(
	ctx context.Context,
	openOrders func(ctx context.Context) int,
	onlineDrivers func(ctx context.Context) int,
) {
	ticker := time.NewTicker(SampleInterval)
	defer ticker.Stop()
	e.log.Info("surge engine loop started", slog.Duration("interval", SampleInterval))
	for {
		select {
		case <-ctx.Done():
			e.log.Info("surge engine loop stopped")
			return
		case <-ticker.C:
			o := openOrders(ctx)
			d := onlineDrivers(ctx)
			if err := e.RecordSample(ctx, o, d); err != nil {
				e.log.Error("surge: RecordSample failed", slog.String("reason", err.Error()))
			}
			if err := e.Recompute(ctx); err != nil {
				e.log.Error("surge: Recompute failed", slog.String("reason", err.Error()))
			}
		}
	}
}
