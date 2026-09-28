package wallet

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/umurinzi/backend/internal/domain"
)

// ── Payout provider seam ──────────────────────────────────────────────────────

// StubPayoutProvider is the no-external-rails implementation of
// domain.PayoutProvider. It "succeeds" deterministically without touching a
// real payment network, so the entire money pipeline (ledger, wallets,
// earnings, cashout history) runs end-to-end today. When MTN MoMo lands,
// implement the same interface against the Disbursements API and rebind in
// main.go — no call-site changes.
type StubPayoutProvider struct {
	log *slog.Logger
}

// NewStubPayoutProvider constructs the stub.
func NewStubPayoutProvider(log *slog.Logger) *StubPayoutProvider {
	return &StubPayoutProvider{log: log}
}

// Payout records a deterministic local success. The reference encodes the
// stub identity so reports can distinguish stub payouts from real ones.
func (s *StubPayoutProvider) Payout(ctx context.Context, phone string, amountRWF int64, reference string) (string, error) {
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("payout cancelled: %w", ctx.Err())
	default:
	}
	ref := "STUB-" + reference
	s.log.Info("payout provider (STUB) — no external transfer performed",
		slog.String("phone", phone),
		slog.Int64("amount_rwf", amountRWF),
		slog.String("ref", ref),
	)
	return ref, nil
}

// ── usecase ───────────────────────────────────────────────────────────────────

// Usecase is the wallet application service. It composes the repository's
// transactional movements with the payout provider seam.
type Usecase struct {
	repo    domain.WalletRepository
	payouts domain.PayoutProvider
	log     *slog.Logger
}

// NewUsecase constructs the wallet usecase.
func NewUsecase(repo domain.WalletRepository, payouts domain.PayoutProvider, log *slog.Logger) *Usecase {
	return &Usecase{repo: repo, payouts: payouts, log: log}
}

// MyWallet returns the caller's balance + escrow snapshot, creating the row
// on first view so a fresh account always has a wallet.
func (uc *Usecase) MyWallet(ctx context.Context, userID string) (*domain.Wallet, error) {
	return uc.repo.EnsureWallet(ctx, userID)
}

// TopUp credits the wallet via the stubbed external rails.
func (uc *Usecase) TopUp(ctx context.Context, userID string, amountRWF int64) (*domain.Wallet, error) {
	w, err := uc.repo.TopUp(ctx, userID, amountRWF)
	if err != nil {
		return nil, err
	}
	uc.log.Info("wallet topped up",
		slog.String("user_id", userID),
		slog.Int64("amount_rwf", amountRWF),
	)
	return w, nil
}

// MyEarnings answers "how much did I make today / this week / all-time".
func (uc *Usecase) MyEarnings(ctx context.Context, userID, period string) (*domain.WalletSummary, error) {
	return uc.repo.MySummary(ctx, userID, period)
}

// RequestCashout performs an instant self-service withdrawal: debit →
// provider payout → PAID (or refund on failure). No admin approval — the
// funds are the user's, and the provider seam is where real-rail risk lives.
func (uc *Usecase) RequestCashout(ctx context.Context, userID, phone string, amountRWF int64) (*domain.CashoutRequest, error) {
	phone = strings.TrimSpace(phone)
	if !isValidRwandaPhone(phone) {
		return nil, fmt.Errorf("%w: phone must be a Rwandan MSISDN (+2507XXXXXXXX or 07XXXXXXXX)", domain.ErrInvalidInput)
	}

	req, err := uc.repo.RequestCashout(ctx, userID, phone, amountRWF)
	if err != nil {
		return nil, err
	}

	// Money has left the wallet — now move it out through the rails.
	providerRef, payoutErr := uc.payouts.Payout(ctx, req.Phone, req.AmountRWF, req.ID)
	if payoutErr != nil {
		uc.log.Error("cashout payout failed — refunding wallet",
			slog.String("cashout_id", req.ID),
			slog.String("user_id", userID),
			slog.String("reason", payoutErr.Error()),
		)
		if refundErr := uc.repo.FailCashout(ctx, req.ID, payoutErr.Error()); refundErr != nil {
			// PROCESSING row stuck with debited wallet: ops-recoverable via
			// FailCashout replay (status guard makes it idempotent).
			uc.log.Error("cashout refund ALSO failed — row stuck PROCESSING",
				slog.String("cashout_id", req.ID),
				slog.String("reason", refundErr.Error()),
			)
			return nil, fmt.Errorf("cashout failed and refund failed: %w", payoutErr)
		}
		return nil, fmt.Errorf("%w: %v", domain.ErrPayoutProvider, payoutErr)
	}

	if err := uc.repo.CompleteCashout(ctx, req.ID, providerRef); err != nil {
		uc.log.Error("cashout completion update failed (provider succeeded)",
			slog.String("cashout_id", req.ID),
			slog.String("reason", err.Error()),
		)
		return nil, err
	}

	req.Status = domain.CashoutPaid
	req.ProviderRef = providerRef
	req.UpdatedAt = time.Now().UTC()

	uc.log.Info("cashout paid instantly",
		slog.String("cashout_id", req.ID),
		slog.String("user_id", userID),
		slog.Int64("amount_rwf", req.AmountRWF),
		slog.String("phone", req.Phone),
	)
	return req, nil
}

// MyCashouts lists the caller's withdrawal history.
func (uc *Usecase) MyCashouts(ctx context.Context, userID string, limit int) ([]*domain.CashoutRequest, error) {
	return uc.repo.ListCashouts(ctx, userID, limit)
}

// PlatformStats gives an ADMIN the money-flow view: platform commission
// (GGR), escrow float, and payout exposure, straight from the ledger.
func (uc *Usecase) PlatformStats(ctx context.Context, adminID string) (*domain.WalletSummary, error) {
	return uc.repo.MySummary(ctx, adminID, "all")
}

// isValidRwandaPhone accepts +2507XXXXXXXX / 2507XXXXXXXX / 07XXXXXXXX.
func isValidRwandaPhone(p string) bool {
	p = strings.ReplaceAll(p, " ", "")
	switch {
	case strings.HasPrefix(p, "+2507") && len(p) == 13:
		return allDigits(p[4:])
	case strings.HasPrefix(p, "2507") && len(p) == 12:
		return allDigits(p[3:])
	case strings.HasPrefix(p, "07") && len(p) == 10:
		return allDigits(p[1:])
	default:
		return false
	}
}

func allDigits(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
