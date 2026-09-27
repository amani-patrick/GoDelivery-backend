package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/umurinzi/backend/internal/domain"
)

// Trust score algorithm 
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

var signalPenalty = map[string]float64{
	domain.SignalHandshakeFail:      0.05, // QR or PIN mismatch attempt
	domain.SignalWeightFraud:        0.15, // declared vs actual weight mismatch
	domain.SignalPhantomCargo:       0.25, // repeated delivery to same co-conspirator
	domain.SignalClaimNeverReceived: 0.10, // customer denied receipt after PIN was given
	domain.SignalSLABreach:          0.03, // driver consistently late
	domain.SignalMultiAppShadow:     0.08, // driver taking jobs from competing apps
	domain.SignalDisputeRaised:      0.07, // delivery reached DISPUTED state
}

var recencyWindows = []struct {
	days   int
	weight float64
}{
	{7, 1.00},
	{30, 0.50},
	{90, 0.25},
}

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
				contribution := basePenalty * window.weight * dampedCount(count)
				score -= contribution
			}
		}
	}

	if actorType == "MERCHANT" {
		// This is a heuristic check — we use the 90-day window count for the
		// most frequently seen recipient. Full implementation requires a
		// GROUP BY query; here we surface it as a known TODO for the first
		// release — the signal will be emitted manually by a dispatcher review.
		_ = actorType 
	}

	if score < 0 {
		score = 0
	}
	if score > 1.0 {
		score = 1.0
	}
	return score
}

func dampedCount(count int) float64 {
	if count <= 0 {
		return 0
	}
	x := float64(count)
	est := x / 2.0
	est = (est + x/est) / 2.0
	est = (est + x/est) / 2.0
	return est
}

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
