package matching

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/umurinzi/backend/internal/domain"
	"github.com/umurinzi/backend/internal/tracking"
)

// ── Redis key prefixes ────────────────────────────────────────────────────────

const (
	batchQueueKey          = "match:queue"
	dispatchPendingPrefix  = "dispatch:pending:"  // task 1: re-dispatch timeout
	dispatchIntendedPrefix = "dispatch:intended:"  // task 2: concurrent acceptance guard
	osrmFallbackSpeedKmh   = 25.0
)

// ── Engine ────────────────────────────────────────────────────────────────────

// Engine implements the full production matching pipeline.
//
// Pipeline stages for each order:
//   1. Tier-1 haversine radius filter (Redis GEOSEARCH) — < 2 ms
//   2. Postgres capacity + status + trust filter         — < 5 ms
//   3. Heading vector check (highway-passing paradox)    — < 1 ms
//   4. OSRM Tier-2 routing matrix (real road ETAs)       — 5–50 ms
//   5. Deadheading guard (road dist > max → drop)        — < 1 ms
//   6. Fuel cost calculation                             — < 1 ms  (task 8)
//   7. Dynamic scoring with fuel + heading + premium     — < 1 ms  (tasks 7,8)
//   8. Re-dispatch timeout registration                  — < 1 ms  (task 1)
//   9. Intended winner registration                      — < 1 ms  (task 2)
//  10. Rank and return
type Engine struct {
	osrm     *OSRMClient
	spatial  *tracking.SpatialIndex
	driverDB domain.DriverProfileRepository
	userDB   domain.UserRepository
	rdb      *redis.Client
	cfg      domain.MatchConfig
	log      *slog.Logger
}

// NewEngine constructs the matching engine.
func NewEngine(
	osrm *OSRMClient,
	spatial *tracking.SpatialIndex,
	driverDB domain.DriverProfileRepository,
	userDB domain.UserRepository,
	rdb *redis.Client,
	cfg domain.MatchConfig,
	log *slog.Logger,
) *Engine {
	return &Engine{
		osrm:     osrm,
		spatial:  spatial,
		driverDB: driverDB,
		userDB:   userDB,
		rdb:      rdb,
		cfg:      cfg,
		log:      log,
	}
}

// ── Enqueue ───────────────────────────────────────────────────────────────────

// Enqueue pushes a new order into the Redis batch queue.
func (e *Engine) Enqueue(ctx context.Context, order domain.BatchOrder) error {
	order.EnqueuedAt = time.Now().UTC()
	b, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("enqueue: marshal: %w", err)
	}
	if err := e.rdb.LPush(ctx, batchQueueKey, string(b)).Err(); err != nil {
		return fmt.Errorf("enqueue: lpush: %w", err)
	}
	e.log.Info("order enqueued for batch matching",
		slog.String("delivery_id", order.DeliveryID),
		slog.Float64("weight_kg", order.WeightKg),
		slog.Bool("is_premium", order.IsPremium),
	)
	return nil
}

// ── Match ─────────────────────────────────────────────────────────────────────

// Match runs the full pipeline for a single order and returns a ranked result.
func (e *Engine) Match(ctx context.Context, order domain.BatchOrder) (*domain.MatchResult, error) {
	result := &domain.MatchResult{
		DeliveryID: order.DeliveryID,
		ComputedAt: time.Now().UTC(),
	}

	// ── Stage 1: Tier-1 haversine filter ─────────────────────────────────────
	radius := e.tier1RadiusFor(order.VehicleTypeRequired)
	candidateIDs, err := e.spatial.FindNearbyDrivers(ctx, order.PickupLat, order.PickupLng, radius)
	if err != nil {
		return nil, fmt.Errorf("match: tier-1 geo: %w", err)
	}
	if len(candidateIDs) == 0 {
		e.log.Info("match: no drivers in radius",
			slog.String("delivery_id", order.DeliveryID),
			slog.Float64("radius_km", radius),
		)
		return result, nil
	}

	// ── Stage 2: Postgres capacity + status + trust filter ───────────────────
	profiles, err := e.driverDB.FindEligibleDrivers(
		ctx, candidateIDs, order.WeightKg, order.VehicleTypeRequired,
	)
	if err != nil {
		return nil, fmt.Errorf("match: find eligible: %w", err)
	}
	if len(profiles) == 0 {
		e.log.Info("match: no eligible drivers", slog.String("delivery_id", order.DeliveryID))
		return result, nil
	}

	// Drop drivers with a BANNED trust level (task 9).
	eligible := profiles[:0]
	for _, p := range profiles {
		if p.TrustLevel == domain.TrustLevelBanned {
			e.log.Warn("match: skipping BANNED driver",
				slog.String("driver_id", p.UserID),
				slog.String("delivery_id", order.DeliveryID),
			)
			continue
		}
		eligible = append(eligible, p)
	}
	if len(eligible) == 0 {
		return result, nil
	}
	profiles = eligible

	// ── Stage 3: Heading vector fetch ─────────────────────────────────────────
	headingMap := e.fetchHeadings(ctx, profiles)

	// ── Stage 4: OSRM routing matrix ─────────────────────────────────────────
	sources := make([][2]float64, len(profiles))
	for i, p := range profiles {
		lat, lng := e.driverPosition(ctx, p)
		sources[i] = [2]float64{lat, lng}
	}

	matrix, osrmErr := e.osrm.Table(ctx, sources, order.PickupLat, order.PickupLng)
	if osrmErr != nil {
		e.log.Warn("match: OSRM unavailable — haversine fallback",
			slog.String("delivery_id", order.DeliveryID),
			slog.String("reason", osrmErr.Error()),
		)
		matrix = e.haversineFallback(sources, order.PickupLat, order.PickupLng)
	} else {
		result.OSRMUsed = true
	}

	// ── Stages 5–8: Deadhead → fuel → score → sort ───────────────────────────
	maxDeadhead  := e.maxDeadheadFor(order.VehicleTypeRequired)
	scored := make([]domain.ScoredCandidate, 0, len(profiles))

	for i, p := range profiles {
		roadDistKm := matrix.DistancesM[i] / 1000.0
		etaSec     := matrix.ETAsSeconds[i]
		etaMin     := etaSec / 60.0

		u, err := e.userDB.GetByID(ctx, p.UserID)
		if err != nil {
			e.log.Error("match: user lookup failed",
				slog.String("user_id", p.UserID),
				slog.String("reason", err.Error()),
			)
			continue
		}

		// Stage 5: Deadhead guard
		if roadDistKm > maxDeadhead {
			e.log.Info("match: driver exceeds deadhead limit",
				slog.String("driver_id", p.UserID),
				slog.Float64("road_km", roadDistKm),
				slog.Float64("max_km", maxDeadhead),
			)
			continue
		}

		// Stage 6: Fuel cost calculation (task 8)
		fuelCostRWF := e.fuelCost(p.VehicleType, roadDistKm)

		// Stage 7a: Base score
		score := (etaMin * e.cfg.WeightTime) +
			(roadDistKm * e.cfg.WeightDistance) +
			(fuelCostRWF/1000.0 * e.cfg.WeightFuel) // normalise RWF to ~km scale

		// Stage 7b: Heading penalty (highway-passing paradox)
		penalised := false
		hs := headingMap[p.UserID]
		if hs.SpeedKmh >= e.cfg.HeadingPenaltyMinSpeedKmh {
			bearing := bearingBetween(sources[i][0], sources[i][1], order.PickupLat, order.PickupLng)
			if angularDifference(hs.Bearing, bearing) > e.cfg.HeadingPenaltyThresholdDeg {
				score *= e.cfg.HeadingPenaltyMultiplier
				penalised = true
				e.log.Info("match: heading penalty applied",
					slog.String("driver_id", p.UserID),
					slog.Float64("speed_kmh", hs.SpeedKmh),
				)
			}
		}

		// Stage 7c: Premium score boost (task 7) — subtract a fixed amount so
		// premium winners beat standard candidates unconditionally.
		if order.IsPremium {
			score -= e.cfg.PremiumScoreBoost
		}

		scored = append(scored, domain.ScoredCandidate{
			DispatchCandidate: domain.DispatchCandidate{
				UserID:      p.UserID,
				FullName:    u.FullName,
				Phone:       u.Phone,
				VehicleType: p.VehicleType,
				PlateNumber: p.PlateNumber,
				MaxWeightKg: p.MaxWeightKg,
				DistanceKm:  roadDistKm,
			},
			ETASeconds:       etaSec,
			RoadDistKm:       roadDistKm,
			FuelCostRWF:      fuelCostRWF,
			SpeedKmh:         hs.SpeedKmh,
			Bearing:          hs.Bearing,
			ETAMinutes:       etaMin,
			HeadingPenalised: penalised,
			Score:            score,
		})
	}

	if len(scored) == 0 {
		e.log.Info("match: all candidates eliminated", slog.String("delivery_id", order.DeliveryID))
		return result, nil
	}

	sort.Slice(scored, func(i, j int) bool { return scored[i].Score < scored[j].Score })
	result.Ranked = scored
	result.Winner = &scored[0]

	// Stage 8: Register pending dispatch with TTL for re-dispatch (task 1).
	e.registerPending(ctx, order.DeliveryID, result)

	// Stage 9: Register intended winner to block concurrent acceptance (task 2).
	e.registerIntended(ctx, order.DeliveryID, result.Winner.UserID)

	e.log.Info("match: winner selected",
		slog.String("delivery_id", order.DeliveryID),
		slog.String("winner_id", result.Winner.UserID),
		slog.Float64("score", result.Winner.Score),
		slog.Float64("eta_min", result.Winner.ETAMinutes),
		slog.Float64("road_km", result.Winner.RoadDistKm),
		slog.Float64("fuel_rwf", result.Winner.FuelCostRWF),
		slog.Bool("heading_penalised", result.Winner.HeadingPenalised),
		slog.Bool("osrm_used", result.OSRMUsed),
		slog.Bool("is_premium", order.IsPremium),
	)
	return result, nil
}

// ── Re-dispatch timeout (task 1) ──────────────────────────────────────────────

// pendingDispatch is what is stored in Redis under dispatch:pending:<deliveryID>.
// It carries the full ranked list so re-dispatch can offer the next candidate
// without running the entire matching pipeline again.
type pendingDispatch struct {
	DeliveryID string                 `json:"delivery_id"`
	Ranked     []domain.ScoredCandidate `json:"ranked"`
	Attempt    int                    `json:"attempt"` // which ranked index to try next
	ExpiresAt  time.Time              `json:"expires_at"`
}

// registerPending writes the ranked list to Redis with the re-dispatch TTL.
func (e *Engine) registerPending(ctx context.Context, deliveryID string, result *domain.MatchResult) {
	pd := pendingDispatch{
		DeliveryID: deliveryID,
		Ranked:     result.Ranked,
		Attempt:    0,
		ExpiresAt:  time.Now().UTC().Add(time.Duration(e.cfg.RedispatchTimeoutMs) * time.Millisecond),
	}
	b, err := json.Marshal(pd)
	if err != nil {
		e.log.Error("registerPending: marshal failed", slog.String("delivery_id", deliveryID))
		return
	}
	ttl := time.Duration(e.cfg.RedispatchTimeoutMs) * time.Millisecond
	if err := e.rdb.Set(ctx, dispatchPendingPrefix+deliveryID, string(b), ttl).Err(); err != nil {
		e.log.Error("registerPending: redis SET failed",
			slog.String("delivery_id", deliveryID),
			slog.String("reason", err.Error()),
		)
	}
}

// RedispatchExpired is called by the background scanner (RunRedispatchLoop).
// It reads deliveries whose pending key has expired (meaning the winner did not
// accept within the timeout) and offers to the next ranked candidate.
// Returns the next dispatch result, or nil if all candidates were exhausted.
func (e *Engine) RedispatchExpired(ctx context.Context, deliveryID string) *domain.AnomalyAlert {
	key := dispatchPendingPrefix + deliveryID
	raw, err := e.rdb.Get(ctx, key).Result()
	if err != nil {
		return nil // key gone — delivery was accepted or cancelled
	}

	var pd pendingDispatch
	if err := json.Unmarshal([]byte(raw), &pd); err != nil {
		e.log.Error("RedispatchExpired: unmarshal failed", slog.String("delivery_id", deliveryID))
		return nil
	}

	nextIdx := pd.Attempt + 1
	if nextIdx >= len(pd.Ranked) {
		e.log.Warn("RedispatchExpired: all candidates exhausted",
			slog.String("delivery_id", deliveryID),
		)
		_ = e.rdb.Del(ctx, key)
		return nil
	}

	next := pd.Ranked[nextIdx]
	pd.Attempt = nextIdx
	pd.ExpiresAt = time.Now().UTC().Add(time.Duration(e.cfg.RedispatchTimeoutMs) * time.Millisecond)

	b, _ := json.Marshal(pd)
	ttl := time.Duration(e.cfg.RedispatchTimeoutMs) * time.Millisecond
	_ = e.rdb.Set(ctx, key, string(b), ttl)

	// Update intended winner key (task 2).
	e.registerIntended(ctx, deliveryID, next.UserID)

	e.log.Info("RedispatchExpired: offering next candidate",
		slog.String("delivery_id", deliveryID),
		slog.String("next_driver", next.UserID),
		slog.Int("attempt", nextIdx),
	)

	// Return an alert the result consumer uses to push WebSocket notification.
	return &domain.AnomalyAlert{
		DeliveryID:  deliveryID,
		DriverID:    next.UserID,
		AlertType:   "DISPATCH",
		DetectedAt:  time.Now().UTC(),
		Details:     fmt.Sprintf(`{"delivery_id":%q,"eta_minutes":%.1f,"score":%.2f,"attempt":%d}`, deliveryID, next.ETAMinutes, next.Score, nextIdx),
	}
}

// ClearPending removes the pending dispatch key once a driver accepts.
// Called by AcceptOrder after a successful state transition.
func (e *Engine) ClearPending(ctx context.Context, deliveryID string) {
	_ = e.rdb.Del(ctx, dispatchPendingPrefix+deliveryID)
	_ = e.rdb.Del(ctx, dispatchIntendedPrefix+deliveryID)
}

// RunRedispatchLoop scans for expired pending keys and re-offers to next candidates.
// Run in a dedicated goroutine: go engine.RunRedispatchLoop(ctx, resultCh)
func (e *Engine) RunRedispatchLoop(ctx context.Context, alertCh chan<- domain.AnomalyAlert) {
	// Scan every (RedispatchTimeoutMs / 2) to catch expirations promptly.
	interval := time.Duration(e.cfg.RedispatchTimeoutMs/2) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	e.log.Info("redispatch loop started", slog.Duration("scan_interval", interval))

	for {
		select {
		case <-ctx.Done():
			e.log.Info("redispatch loop stopped")
			return
		case <-ticker.C:
			// SCAN for expired dispatch:pending:* keys.
			// We use SCAN instead of KEYS to avoid blocking Redis on large keyspaces.
			var cursor uint64
			for {
				keys, nextCursor, err := e.rdb.Scan(ctx, cursor, dispatchPendingPrefix+"*", 50).Result()
				if err != nil {
					e.log.Error("redispatch scan failed", slog.String("reason", err.Error()))
					break
				}
				for _, key := range keys {
					ttl, err := e.rdb.TTL(ctx, key).Result()
					if err != nil {
						continue
					}
					// A TTL of -2 means the key expired but SCAN still returned it
					// briefly. A TTL of 0–1 means it is about to expire — treat as expired.
					if ttl <= 1*time.Second {
						deliveryID := key[len(dispatchPendingPrefix):]
						alert := e.RedispatchExpired(ctx, deliveryID)
						if alert != nil {
							select {
							case alertCh <- *alert:
							default:
								e.log.Error("redispatch alert channel full — alert dropped",
									slog.String("delivery_id", deliveryID),
								)
							}
						}
					}
				}
				cursor = nextCursor
				if cursor == 0 {
					break
				}
			}
		}
	}
}

// ── Intended winner guard (task 2) ────────────────────────────────────────────

// registerIntended writes the intended winner driver ID to Redis with the same
// TTL as the pending dispatch. AcceptOrder reads this key to block a different
// driver from accepting the order while the intended winner has time to respond.
func (e *Engine) registerIntended(ctx context.Context, deliveryID, driverID string) {
	key := dispatchIntendedPrefix + deliveryID
	ttl := time.Duration(e.cfg.RedispatchTimeoutMs) * time.Millisecond
	if err := e.rdb.Set(ctx, key, driverID, ttl).Err(); err != nil {
		e.log.Error("registerIntended: redis SET failed",
			slog.String("delivery_id", deliveryID),
			slog.String("reason", err.Error()),
		)
	}
}

// GetIntendedDriver returns the driverID currently intended for a delivery,
// or empty string if no intended winner is registered (TTL expired or never set).
// Called by AcceptOrder before the Redis NX lock.
func (e *Engine) GetIntendedDriver(ctx context.Context, deliveryID string) string {
	val, err := e.rdb.Get(ctx, dispatchIntendedPrefix+deliveryID).Result()
	if err != nil {
		return "" // cache miss = TTL expired, any driver may now accept
	}
	return val
}

// ── Order stacking (task 4) ───────────────────────────────────────────────────

// EvaluateStack decides whether a new order can be stacked onto an ON_TRIP driver.
//
// Two gates must both pass:
//  1. Direction gate: the dot-product angle between (driver→dropoffA) and
//     (driver→dropoffB) must be < MaxStackDirectionDeg. This filters out
//     opposite-direction stacks like the (-7, 0) vs (8, 0) example.
//  2. Delay gate: the OSRM-computed incremental delay added to the first
//     customer's ETA must be < MaxStackDelayMinutes.
//
// Returns a StackEvaluation with Feasible=true if both gates pass.
func (e *Engine) EvaluateStack(ctx context.Context, req domain.StackRequest) (domain.StackEvaluation, error) {
	// ── Gate 1: Direction dot-product ────────────────────────────────────────
	// Compute bearing from driver's current position to each dropoff.
	bearingA := bearingBetween(req.DriverLat, req.DriverLng, req.ExistingDropoffLat, req.ExistingDropoffLng)
	bearingB := bearingBetween(req.DriverLat, req.DriverLng, req.NewDropoffLng, req.NewDropoffLat)
	angleDeg := angularDifference(bearingA, bearingB)

	if angleDeg > e.cfg.MaxStackDirectionDeg {
		reason := fmt.Sprintf(
			"dropoff directions diverge by %.1f° (max %.1f°) — opposite-direction stack rejected",
			angleDeg, e.cfg.MaxStackDirectionDeg,
		)
		e.log.Info("EvaluateStack: direction gate rejected",
			slog.String("new_delivery_id", req.NewDeliveryID),
			slog.String("driver_id", req.DriverID),
			slog.Float64("angle_deg", angleDeg),
		)
		return domain.StackEvaluation{
			Feasible:            false,
			DirectionAngleDeg:   angleDeg,
			RejectReason:        reason,
		}, nil
	}

	// ── Gate 2: Incremental delay via OSRM ───────────────────────────────────
	// Route A (solo):   driver → dropoffA
	// Route B (stacked): driver → newPickup → dropoffA → dropoffB
	//
	// We need three OSRM queries collapsed into two Table calls:
	//  - Table([driver, newPickup], dropoffA) → etaA_solo, etaNewPickupToDropoffA
	//  - etaNewPickupToDropoffA comes from second row of the same call.
	//
	// Incremental delay = (driver→newPickup ETA) + (newPickup→dropoffA ETA) - (driver→dropoffA ETA)

	sources := [][2]float64{
		{req.DriverLat, req.DriverLng},
		{req.NewPickupLat, req.NewPickupLng},
	}
	matrix, err := e.osrm.Table(ctx, sources, req.ExistingDropoffLat, req.ExistingDropoffLng)
	if err != nil {
		// OSRM unavailable — use haversine estimates
		matrix = e.haversineFallback(sources, req.ExistingDropoffLat, req.ExistingDropoffLng)
	}

	// matrix.ETAsSeconds[0] = driver → dropoffA (solo ETA in seconds)
	// matrix.ETAsSeconds[1] = newPickup → dropoffA
	driverToDropoffA  := matrix.ETAsSeconds[0] / 60.0
	newPickupToDropA  := matrix.ETAsSeconds[1] / 60.0

	// Driver to new pickup (haversine fallback, fast)
	driverToNewPickup := haversineM(req.DriverLat, req.DriverLng, req.NewPickupLat, req.NewPickupLng) /
		1000.0 / (osrmFallbackSpeedKmh / 60.0)

	incrementalDelay := driverToNewPickup + newPickupToDropA - driverToDropoffA

	if incrementalDelay > e.cfg.MaxStackDelayMinutes {
		reason := fmt.Sprintf(
			"stacking adds %.1f min delay to existing customer (max %.1f min)",
			incrementalDelay, e.cfg.MaxStackDelayMinutes,
		)
		e.log.Info("EvaluateStack: delay gate rejected",
			slog.String("new_delivery_id", req.NewDeliveryID),
			slog.String("driver_id", req.DriverID),
			slog.Float64("incremental_delay_min", incrementalDelay),
		)
		return domain.StackEvaluation{
			Feasible:            false,
			IncrementalDelayMin: incrementalDelay,
			DirectionAngleDeg:   angleDeg,
			RejectReason:        reason,
		}, nil
	}

	e.log.Info("EvaluateStack: APPROVED",
		slog.String("new_delivery_id", req.NewDeliveryID),
		slog.String("driver_id", req.DriverID),
		slog.Float64("incremental_delay_min", incrementalDelay),
		slog.Float64("direction_angle_deg", angleDeg),
	)
	return domain.StackEvaluation{
		Feasible:            true,
		IncrementalDelayMin: incrementalDelay,
		DirectionAngleDeg:   angleDeg,
	}, nil
}

// ── Batch loop ────────────────────────────────────────────────────────────────

// RunBatch drains the Redis batch queue, runs Match for each order, and
// handles intra-batch driver collisions using a greedy cascade.
func (e *Engine) RunBatch(ctx context.Context) []domain.MatchResult {
	raw, err := e.rdb.LRange(ctx, batchQueueKey, 0, -1).Result()
	if err != nil || len(raw) == 0 {
		return nil
	}
	_ = e.rdb.Del(ctx, batchQueueKey)

	orders := make([]domain.BatchOrder, 0, len(raw))
	for _, r := range raw {
		var o domain.BatchOrder
		if err := json.Unmarshal([]byte(r), &o); err != nil {
			e.log.Warn("RunBatch: malformed entry", slog.String("reason", err.Error()))
			continue
		}
		orders = append(orders, o)
	}

	// Sort: premium orders first so they get first pick of the best driver.
	sort.Slice(orders, func(i, j int) bool {
		return orders[i].IsPremium && !orders[j].IsPremium
	})

	results       := make([]domain.MatchResult, 0, len(orders))
	assignedDrivers := make(map[string]bool)

	for _, order := range orders {
		res, err := e.Match(ctx, order)
		if err != nil {
			e.log.Error("RunBatch: match failed",
				slog.String("delivery_id", order.DeliveryID),
				slog.String("reason", err.Error()),
			)
			continue
		}
		if res.Winner == nil {
			results = append(results, *res)
			continue
		}

		winnerID := res.Winner.UserID
		if assignedDrivers[winnerID] {
			reassigned := false
			for _, alt := range res.Ranked[1:] {
				if !assignedDrivers[alt.UserID] {
					w := alt
					res.Winner = &w
					winnerID  = w.UserID
					reassigned = true
					// Update intended winner key for the new selection.
					e.registerIntended(ctx, order.DeliveryID, winnerID)
					break
				}
			}
			if !reassigned {
				e.log.Info("RunBatch: all candidates taken — re-queuing",
					slog.String("delivery_id", order.DeliveryID),
				)
				_ = e.Enqueue(ctx, order)
				continue
			}
		}

		assignedDrivers[winnerID] = true
		results = append(results, *res)
	}

	e.log.Info("RunBatch complete",
		slog.Int("orders", len(orders)),
		slog.Int("matched", len(results)),
	)
	return results
}

// RunBatchLoop starts the periodic batch processing goroutine.
func (e *Engine) RunBatchLoop(ctx context.Context, resultCh chan<- []domain.MatchResult) {
	interval := time.Duration(e.cfg.BatchWindowMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	e.log.Info("matching engine batch loop started", slog.Duration("interval", interval))

	for {
		select {
		case <-ctx.Done():
			e.log.Info("matching engine batch loop stopped")
			return
		case <-ticker.C:
			if results := e.RunBatch(ctx); len(results) > 0 {
				select {
				case resultCh <- results:
				default:
					e.log.Error("match result channel full", slog.Int("dropped", len(results)))
				}
			}
		}
	}
}

// ── Internal helpers ──────────────────────────────────────────────────────────

func (e *Engine) tier1RadiusFor(vt domain.VehicleType) float64 {
	if r, ok := e.cfg.Tier1RadiusKm[vt]; ok && r > 0 {
		return r
	}
	max := 5.0
	for _, r := range e.cfg.Tier1RadiusKm {
		if r > max {
			max = r
		}
	}
	return max
}

func (e *Engine) maxDeadheadFor(vt domain.VehicleType) float64 {
	if vt == "" {
		max := 5.0
		for _, d := range e.cfg.MaxDeadheadKm {
			if d > max {
				max = d
			}
		}
		return max
	}
	if d, ok := e.cfg.MaxDeadheadKm[vt]; ok && d > 0 {
		return d
	}
	return 5.0
}

// fuelCost calculates the deadhead fuel cost in RWF for the given vehicle
// type and road distance to pickup. Used in the score formula (task 8).
func (e *Engine) fuelCost(vt domain.VehicleType, roadDistKm float64) float64 {
	litresPerKm, ok := e.cfg.FuelLitresPerKm[vt]
	if !ok || litresPerKm == 0 {
		return 0
	}
	return litresPerKm * roadDistKm * e.cfg.FuelPriceRWFPerLitre
}

func (e *Engine) fetchHeadings(ctx context.Context, profiles []*domain.DriverProfile) map[string]domain.HeadingState {
	m := make(map[string]domain.HeadingState, len(profiles))
	for _, p := range profiles {
		frames, err := e.spatial.GetRecentFrames(ctx, p.UserID, 1)
		if err != nil || len(frames) == 0 {
			m[p.UserID] = domain.HeadingState{}
			continue
		}
		m[p.UserID] = domain.HeadingState{SpeedKmh: frames[0].SpeedKmh, Bearing: frames[0].Bearing}
	}
	return m
}

func (e *Engine) driverPosition(ctx context.Context, p *domain.DriverProfile) (float64, float64) {
	frames, err := e.spatial.GetRecentFrames(ctx, p.UserID, 1)
	if err == nil && len(frames) > 0 {
		return frames[0].Lat, frames[0].Lng
	}
	return p.CurrentLocation.Lat, p.CurrentLocation.Lng
}

func (e *Engine) haversineFallback(sources [][2]float64, destLat, destLng float64) *RouteMatrix {
	m := &RouteMatrix{
		ETAsSeconds: make([]float64, len(sources)),
		DistancesM:  make([]float64, len(sources)),
	}
	for i, src := range sources {
		distM := haversineM(src[0], src[1], destLat, destLng)
		m.DistancesM[i]  = distM
		m.ETAsSeconds[i] = (distM * 1.3) / (osrmFallbackSpeedKmh / 3.6)
	}
	return m
}

// ── Geometry ──────────────────────────────────────────────────────────────────

func haversineM(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6_371_000.0
	φ1, φ2 := toRadians(lat1), toRadians(lat2)
	dφ := toRadians(lat2 - lat1)
	dλ := toRadians(lng2 - lng1)
	a := math.Sin(dφ/2)*math.Sin(dφ/2) +
		math.Cos(φ1)*math.Cos(φ2)*math.Sin(dλ/2)*math.Sin(dλ/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func bearingBetween(lat1, lng1, lat2, lng2 float64) float64 {
	φ1, φ2 := toRadians(lat1), toRadians(lat2)
	dλ := toRadians(lng2 - lng1)
	y := math.Sin(dλ) * math.Cos(φ2)
	x := math.Cos(φ1)*math.Sin(φ2) - math.Sin(φ1)*math.Cos(φ2)*math.Cos(dλ)
	return math.Mod(toDegrees(math.Atan2(y, x))+360, 360)
}

func angularDifference(a, b float64) float64 {
	diff := math.Abs(a - b)
	if diff > 180 {
		diff = 360 - diff
	}
	return diff
}

func toRadians(deg float64) float64 { return deg * math.Pi / 180 }
func toDegrees(rad float64) float64 { return rad * 180 / math.Pi }

// uuid is used for stack group IDs
var _ = uuid.NewString // prevent unused import if caller doesn't call it directly
