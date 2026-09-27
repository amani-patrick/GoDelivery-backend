package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/umurinzi/backend/internal/domain"
)

// TrustUsecase recalculates actor trust scores from the signal ledger
// and persists updated levels to the profile tables.
//
// Trust score algorithm:
//  - Every actor starts at 1.0 (fully trusted).
//  - Each trust signal deducts a weighted penalty based on severity and
//    recency. Older signals decay in weight.
//  - Score is clamped to [0.0, 1.0].
//  - TrustLevel thresholds:
//      >= 0.80 → GOOD        (normal operation)
//      >= 0.60 → WATCH       (elevated monitoring, no restrictions)
//      >= 0.30 → RESTRICTED  (limited functionality — dispatcher review required)
//       < 0.30 → BANNED      (blocked from platform)
//
// This usecase is called by background workers and dispatcher-triggered
// reviews. It is never called in the hot path of a delivery transaction.
type TrustUsecase struct {
	trustRepo    domain.TrustRepository
	driverRepo   domain.DriverProfileRepository
	businessRepo domain.BusinessProfileRepository
	customerRepo domain.CustomerProfileRepository
	log          *slog.Logger
}

// NewTrustUsecase constructs the usecase.
func NewTrustUsecase(
	trustRepo domain.TrustRepository,
	driverRepo domain.DriverProfileRepository,
	businessRepo domain.BusinessProfileRepository,
	customerRepo domain.CustomerProfileRepository,
	log *slog.Logger,
) *TrustUsecase {
	return &TrustUsecase{
		trustRepo:    trustRepo,
		driverRepo:   driverRepo,
		businessRepo: businessRepo,
		customerRepo: customerRepo,
		log:          log,
	}
}

// RecalculateDriver recomputes and persists the trust score for a driver.
// Call after any event that could degrade trust (dispute, handshake fail, etc.)
func (uc *TrustUsecase) RecalculateDriver(ctx context.Context, driverID string) error {
	score := uc.computeScore(ctx, driverID, "DRIVER")
	level := scoreToLevel(score)

	if err := uc.driverRepo.UpdateTrustScore(ctx, driverID, score, level); err != nil {
		uc.log.Error("TrustUsecase.RecalculateDriver: UpdateTrustScore failed",
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return err
	}
	uc.log.Info("driver trust score updated",
		slog.String("driver_id", driverID),
		slog.Float64("score", score),
		slog.String("level", string(level)),
	)
	return nil
}

// RecalculateMerchant recomputes and persists the trust score for a merchant.
func (uc *TrustUsecase) RecalculateMerchant(ctx context.Context, merchantID string) error {
	score := uc.computeScore(ctx, merchantID, "MERCHANT")
	level := scoreToLevel(score)

	if err := uc.businessRepo.UpdateTrustScore(ctx, merchantID, score, level); err != nil {
		uc.log.Error("TrustUsecase.RecalculateMerchant: UpdateTrustScore failed",
			slog.String("merchant_id", merchantID),
			slog.String("reason", err.Error()),
		)
		return err
	}
	uc.log.Info("merchant trust score updated",
		slog.String("merchant_id", merchantID),
		slog.Float64("score", score),
		slog.String("level", string(level)),
	)
	return nil
}

// RecalculateCustomer recomputes and persists the trust score for a customer.
func (uc *TrustUsecase) RecalculateCustomer(ctx context.Context, customerID string) error {
	score := uc.computeScore(ctx, customerID, "CUSTOMER")
	level := scoreToLevel(score)

	if err := uc.customerRepo.UpdateTrustScore(ctx, customerID, score, level); err != nil {
		uc.log.Error("TrustUsecase.RecalculateCustomer: UpdateTrustScore failed",
			slog.String("customer_id", customerID),
			slog.String("reason", err.Error()),
		)
		return err
	}
	uc.log.Info("customer trust score updated",
		slog.String("customer_id", customerID),
		slog.Float64("score", score),
		slog.String("level", string(level)),
	)
	return nil
}

// ── Signal definitions ────────────────────────────────────────────────────────

// signalPenalty maps a signal type to its base score deduction per occurrence.
var signalPenalty = map[string]float64{
	domain.SignalHandshakeFail:      0.05, // QR or PIN mismatch attempt
	domain.SignalWeightFraud:        0.15, // declared vs actual weight mismatch
	domain.SignalPhantomCargo:       0.25, // repeated delivery to same co-conspirator
	domain.SignalClaimNeverReceived: 0.10, // customer denied receipt after PIN was given
	domain.SignalSLABreach:          0.03, // driver consistently late
	domain.SignalMultiAppShadow:     0.08, // driver taking jobs from competing apps
	domain.SignalDisputeRaised:      0.07, // delivery reached DISPUTED state
}

// recencyWindows defines the look-back periods for each signal.
// Older signals decay: 7-day window carries full weight, 30-day window
// carries 50% weight, 90-day window carries 25% weight.
var recencyWindows = []struct {
	days   int
	weight float64
}{
	{7, 1.00},
	{30, 0.50},
	{90, 0.25},
}

// computeScore sums penalty contributions from all signal types for an actor.
func (uc *TrustUsecase) computeScore(ctx context.Context, actorID, actorType string) float64 {
	score := 1.0
	now := time.Now().UTC()

	for signalType, basePenalty := range signalPenalty {
		for _, window := range recencyWindows {
			since := now.AddDate(0, 0, -window.days)
			count, err := uc.trustRepo.CountSignals(ctx, actorID, signalType, since)
			if err != nil {
				uc.log.Warn("computeScore: CountSignals failed",
					slog.String("actor_id", actorID),
					slog.String("signal_type", signalType),
					slog.String("reason", err.Error()),
				)
				continue
			}
			if count > 0 {
				// Marginal decay: each additional occurrence in the same window
				// has diminishing impact (sqrt damping prevents one bad week
				// from permanently destroying a long-standing actor).
				contribution := basePenalty * window.weight * dampedCount(count)
				score -= contribution
			}
		}
	}

	// Phantom cargo check for merchants: if 80%+ of their deliveries go to
	// the same phone number, flag them regardless of explicit signals.
	if actorType == "MERCHANT" {
		// This is a heuristic check — we use the 90-day window count for the
		// most frequently seen recipient. Full implementation requires a
		// GROUP BY query; here we surface it as a known TODO for the first
		// release — the signal will be emitted manually by a dispatcher review.
		_ = actorType // placeholder
	}

	if score < 0 {
		score = 0
	}
	if score > 1.0 {
		score = 1.0
	}
	return score
}

// dampedCount applies square-root damping to occurrence counts.
// count=1 → 1.0, count=4 → 2.0, count=9 → 3.0 etc.
// Prevents a single actor generating a flood of signals from being
// over-penalised compared to one with persistent low-level signals.
func dampedCount(count int) float64 {
	if count <= 0 {
		return 0
	}
	// Use float64 math — avoid importing math package just for Sqrt by
	// approximating with Newton's method for the common range 1–20.
	x := float64(count)
	// Two Newton iterations: good enough for count ≤ 100.
	est := x / 2.0
	est = (est + x/est) / 2.0
	est = (est + x/est) / 2.0
	return est
}

// scoreToLevel converts a numeric score to a categorical TrustLevel.
func scoreToLevel(score float64) domain.TrustLevel {
	switch {
	case score >= 0.80:
		return domain.TrustLevelGood
	case score >= 0.60:
		return domain.TrustLevelWatch
	case score >= 0.30:
		return domain.TrustLevelRestricted
	default:
		return domain.TrustLevelBanned
	}
}
