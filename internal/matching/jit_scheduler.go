package matching

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/umurinzi/backend/internal/domain"
)

// jitDispatchBufferSec is the fixed buffer added to account for driver
// acceptance lag (looking at screen, evaluating pay, tapping accept).
const jitDispatchBufferSec = 60

// jitPendingKey is the hidden PENDING_DISPATCH pool. Orders whose ReadyAt is
// still in the future live here, invisible to RunBatch, until the JIT trigger
// zone is reached. This is what actually enforces the JIT gate: RunBatch only
// ever sees orders the scheduler has released.
const jitPendingKey = "jit:pending"

// matchReadyKey is the handoff queue of JIT-released orders. RunBatch drains
// ONLY this list — never match:queue directly — so an order created a
// microsecond after the JIT pass cannot slip past the trigger-zone gate; it
// simply waits for the next tick (≤ one batch window) and is evaluated with
// fresh driver-ETA data.
const matchReadyKey = "jit:match:ready"

// jitMaxTriggerHorizonSec bounds JIT evaluation. The driver ETA can never
// exceed this (max Tier-1 radius is 10 km ≈ 30+ min at motorbike speeds), so
// an order whose remaining prep exceeds the horizon cannot possibly be inside
// the trigger zone yet — we skip the OSRM estimate entirely to bound the
// per-tick cost of evaluating a large pending pool.
const jitMaxTriggerHorizonSec = 2400

// jitNoDriverFallbackETA is the conservative default ETA (10 minutes) used
// when no eligible drivers are online around a pickup point.
const jitNoDriverFallbackETA = 600

// GetLocalDriverETAEstimate returns the average ETA in seconds for the
// nearest cluster of drivers around a given order's pickup point.
// Used by the JIT scheduler to decide when to release an order from
// the pending pool into the active matching queue.
func (e *Engine) GetLocalDriverETAEstimate(ctx context.Context, pickupLat, pickupLng float64, vehicleType domain.VehicleType) int {
	radius := e.tier1RadiusFor(vehicleType)
	candidateIDs, err := e.spatial.FindNearbyDrivers(ctx, pickupLat, pickupLng, radius)
	if err != nil || len(candidateIDs) == 0 {
		// No nearby drivers — return a conservative default.
		return jitNoDriverFallbackETA
	}

	// Limit to top 5 closest for a representative average.
	if len(candidateIDs) > 5 {
		candidateIDs = candidateIDs[:5]
	}

	profiles, err := e.driverDB.FindEligibleDrivers(ctx, candidateIDs, 0, vehicleType)
	if err != nil || len(profiles) == 0 {
		return jitNoDriverFallbackETA
	}

	sources := make([][2]float64, len(profiles))
	for i, p := range profiles {
		lat, lng := e.driverPosition(ctx, p)
		sources[i] = [2]float64{lat, lng}
	}

	matrix, osrmErr := e.osrm.Table(ctx, sources, pickupLat, pickupLng)
	if osrmErr != nil {
		matrix = e.haversineFallback(sources, pickupLat, pickupLng)
	}

	var totalSec float64
	for _, eta := range matrix.ETAsSeconds {
		totalSec += eta
	}
	avgSec := totalSec / float64(len(matrix.ETAsSeconds))
	return int(avgSec)
}

// ProcessJITPendingPool evaluates every queued order against the JIT trigger
// zone and releases only the ones whose prep window is about to close:
//
//	remainingPrepSec <= closestDriverETASec + acceptanceBufferSec
//
// The engine's batch loop calls this immediately before RunBatch on every
// tick. Released orders land back in the batch queue so RunBatch matches them
// in the same tick; the rest wait in the hidden jit:pending pool.
//
// Drains use RPop loops (one atomic pop per item) instead of LRange+Del so a
// concurrent Enqueue — which LPushes to the head — can never be lost.
func (e *Engine) ProcessJITPendingPool(ctx context.Context) {
	now := time.Now().UTC()

	ready := make([]string, 0)
	deferred := make([]string, 0)

	// ── Phase 1: classify the intake queue ───────────────────────────────────
	for {
		raw, err := e.rdb.RPop(ctx, batchQueueKey).Result()
		if err != nil {
			break // redis.Nil → queue drained; any other error also stops the loop
		}
		if e.jitEvaluate(ctx, raw, now) {
			ready = append(ready, raw)
		} else {
			deferred = append(deferred, raw)
		}
	}

	// ── Phase 2: re-evaluate the hidden pending pool ─────────────────────────
	for {
		raw, err := e.rdb.RPop(ctx, jitPendingKey).Result()
		if err != nil {
			break
		}
		if e.jitEvaluate(ctx, raw, now) {
			ready = append(ready, raw)
		} else {
			deferred = append(deferred, raw)
		}
	}

	// ── Phase 3: push back in two pipelined round-trips ──────────────────────
	// Crash window: if the process dies between the pops above and these
	// pushes, the in-flight batch is lost. The window is milliseconds wide
	// (one tick of work), versus the previous LRange+Del design which lost
	// every order enqueued during the drain.
	if len(deferred) > 0 {
		if err := e.rdb.RPush(ctx, jitPendingKey, toIfaces(deferred)...).Err(); err != nil {
			e.log.Error("JIT: failed to push deferred orders back to pending pool",
				slog.Int("count", len(deferred)),
				slog.String("reason", err.Error()),
			)
		}
	}
	if len(ready) > 0 {
		if err := e.rdb.RPush(ctx, matchReadyKey, toIfaces(ready)...).Err(); err != nil {
			e.log.Error("JIT: failed to push released orders to match handoff queue",
				slog.Int("count", len(ready)),
				slog.String("reason", err.Error()),
			)
		}
		e.log.Info("JIT: released orders into the trigger zone",
			slog.Int("released", len(ready)),
			slog.Int("still_deferred", len(deferred)),
		)
	}
}

// jitEvaluate decides whether one raw queue entry may be released to the
// matching pipeline. Malformed entries are dropped (never re-queued) so a
// poison payload cannot wedge the loop.
func (e *Engine) jitEvaluate(ctx context.Context, raw string, now time.Time) bool {
	var o domain.BatchOrder
	if err := json.Unmarshal([]byte(raw), &o); err != nil {
		e.log.Warn("JIT: malformed queue entry — dropping",
			slog.String("reason", err.Error()),
		)
		return true
	}

	// No prep time specified — dispatch immediately (Approach A fallback).
	if o.ReadyAt.IsZero() {
		return true
	}

	remainingPrepSec := int(o.ReadyAt.Sub(now).Seconds())
	if remainingPrepSec <= 0 {
		// Already past ready time — dispatch immediately. This also guarantees
		// deferred orders cannot starve: once ReadyAt passes they always release.
		return true
	}

	// Far outside the trigger zone — defer without paying for an OSRM call.
	if remainingPrepSec > jitMaxTriggerHorizonSec {
		return false
	}

	// Query the local driver ETA to calculate the optimal dispatch window.
	driverETASec := e.GetLocalDriverETAEstimate(ctx, o.PickupLat, o.PickupLng, o.VehicleTypeRequired)

	// The "Trigger Zone": dispatch when remaining prep ≤ driver travel + acceptance buffer.
	triggerThresholdSec := driverETASec + jitDispatchBufferSec
	if remainingPrepSec <= triggerThresholdSec {
		e.log.Info("JIT: order entered the trigger zone — releasing for dispatch",
			slog.String("delivery_id", o.DeliveryID),
			slog.Int("remaining_prep_sec", remainingPrepSec),
			slog.Int("driver_eta_sec", driverETASec),
			slog.Int("trigger_threshold_sec", triggerThresholdSec),
		)
		return true
	}
	return false
}

func toIfaces(ss []string) []interface{} {
	ifaces := make([]interface{}, len(ss))
	for i, v := range ss {
		ifaces[i] = v
	}
	return ifaces
}

// MerchantWaitBountyPerMinRWF is the per-minute fee charged to merchants
// when their package isn't ready by the time the JIT-dispatched driver arrives.
const MerchantWaitBountyPerMinRWF = 50.0

// MerchantWaitBounty represents a wait-time micro-fee charged to a merchant.
type MerchantWaitBounty struct {
	DeliveryID    string    `json:"delivery_id"`
	DriverID      string    `json:"driver_id"`
	MerchantID    string    `json:"merchant_id"`
	WaitStartedAt time.Time `json:"wait_started_at"`
	WaitEndedAt   time.Time `json:"wait_ended_at"`
	WaitMinutes   float64   `json:"wait_minutes"`
	BountyRWF     float64   `json:"bounty_rwf"`
}

// ComputeMerchantWaitBounty calculates the wait bounty when a driver's physical
// presence at the shop (proven by the QR handshake) happens after the promised
// ReadyAt. Returns nil when the merchant was on time or the wait is under the
// 1-minute grace period.
func ComputeMerchantWaitBounty(deliveryID, driverID, merchantID string, readyAt time.Time, driverArrivedAt time.Time) *MerchantWaitBounty {
	if readyAt.IsZero() || driverArrivedAt.Before(readyAt) {
		// Driver arrived early — no bounty, the merchant is on time.
		return nil
	}
	waitMinutes := driverArrivedAt.Sub(readyAt).Minutes()
	if waitMinutes < 1.0 {
		return nil // grace period: under 1 minute, no charge
	}
	bountyRWF := waitMinutes * MerchantWaitBountyPerMinRWF
	return &MerchantWaitBounty{
		DeliveryID:    deliveryID,
		DriverID:      driverID,
		MerchantID:    merchantID,
		WaitStartedAt: readyAt,
		WaitEndedAt:   driverArrivedAt,
		WaitMinutes:   waitMinutes,
		BountyRWF:     bountyRWF,
	}
}

// PrepTimeForCategory returns the dynamic preparation time in minutes
// based on the package category and item count. This replaces the static
// blanket prep time approach with category-aware estimation.
func PrepTimeForCategory(category domain.PackageCategory, itemCount int) int {
	if itemCount <= 0 {
		itemCount = 1
	}

	// Base minutes per category.
	baseMins := map[domain.PackageCategory]int{
		domain.PackageCategoryDocuments:   2,
		domain.PackageCategoryGeneral:     5,
		domain.PackageCategoryElectronics: 15, // diagnostic checks + tamper-proof packaging
		domain.PackageCategoryFragile:     10, // careful wrapping
		domain.PackageCategoryPerishable:  8,  // insulated packaging
		domain.PackageCategoryBulk:        20, // heavy loading
	}

	base, ok := baseMins[category]
	if !ok {
		base = 5
	}

	// Scale by item count with diminishing returns (√n-like tiers):
	// 1 item = base, 4 items ≈ 1.7x, 9 items = 2.5x, 16+ = 3-4x.
	if itemCount > 1 {
		multiplier := 1.0
		switch {
		case itemCount <= 2:
			multiplier = 1.3
		case itemCount <= 4:
			multiplier = 1.7
		case itemCount <= 9:
			multiplier = 2.5
		case itemCount <= 16:
			multiplier = 3.0
		default:
			multiplier = 4.0
		}
		base = int(float64(base) * multiplier)
	}

	return base
}
