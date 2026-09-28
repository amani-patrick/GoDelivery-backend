// Package wallet implements the financial core: an append-only ledger
// (ledger_entries) with a per-user balance projection (wallets) and instant
// self-service cashouts (cashout_requests).
//
// Atomicity rules:
//   - Every ledger write and its balance projection happen in one transaction.
//   - Escrow hold/release join the CALLER's transaction (domain.Tx) so money
//     state commits atomically with delivery state: an order can only be
//     created if the merchant can fund it, and the driver is credited in the
//     same commit that marks the delivery DELIVERED.
//   - Cashout debits the wallet first, then hands off to the payout provider;
//     a provider failure refunds via CASHOUT_REVERSAL. No approval queue —
//     admin approval applies to driver verification, not money movement.
package wallet

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/umurinzi/backend/internal/domain"
)

// PostgresRepo implements domain.WalletRepository.
type PostgresRepo struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewPostgresRepo constructs the wallet repository.
func NewPostgresRepo(pool *pgxpool.Pool, log *slog.Logger) *PostgresRepo {
	return &PostgresRepo{pool: pool, log: log}
}

// ── wallet row ────────────────────────────────────────────────────────────────

// EnsureWallet returns the wallet row, creating it (0/0) if missing.
func (r *PostgresRepo) EnsureWallet(ctx context.Context, userID string) (*domain.Wallet, error) {
	return r.upsertWallet(ctx, r.pool, userID)
}

// GetWallet returns the wallet row.
func (r *PostgresRepo) GetWallet(ctx context.Context, userID string) (*domain.Wallet, error) {
	return r.getWallet(ctx, r.pool, userID)
}

func (r *PostgresRepo) upsertWallet(ctx context.Context, q txQuerier, userID string) (*domain.Wallet, error) {
	const qUpsert = `INSERT INTO wallets (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`
	if _, err := q.Exec(ctx, qUpsert, userID); err != nil {
		return nil, fmt.Errorf("wallet upsert: %w", err)
	}
	return r.getWallet(ctx, q, userID)
}

func (r *PostgresRepo) getWallet(ctx context.Context, q txQuerier, userID string) (*domain.Wallet, error) {
	const qGet = `SELECT user_id, balance_rwf, escrow_rwf, updated_at FROM wallets WHERE user_id = $1`
	w := &domain.Wallet{}
	err := q.QueryRow(ctx, qGet, userID).Scan(&w.UserID, &w.BalanceRWF, &w.EscrowRWF, &w.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrUserNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("wallet get: %w", err)
	}
	return w, nil
}

// ── escrow (joins the caller's delivery transaction) ─────────────────────────

// HoldEscrowWithinTx debits balance → escrow and writes the ESCROW_HOLD entry
// inside the caller's transaction (order creation). The conditional UPDATE
// enforces the sufficient-balance rule at the row level — no read-modify-write
// race, and the delivery insert + funding commit atomically or not at all.
func (r *PostgresRepo) HoldEscrowWithinTx(
	ctx context.Context, tx domain.Tx, userID, deliveryID string, amountRWF int64,
) error {
	if amountRWF <= 0 {
		return fmt.Errorf("%w: escrow amount must be positive", domain.ErrInvalidInput)
	}
	const q = `
		UPDATE wallets
		SET balance_rwf = balance_rwf - $2,
		    escrow_rwf  = escrow_rwf  + $2,
		    updated_at  = NOW()
		WHERE user_id = $1 AND balance_rwf >= $2`
	tag, err := tx.ExecTag(ctx, q, userID, amountRWF)
	if err != nil {
		return fmt.Errorf("escrow hold: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrInsufficientFunds
	}
	return r.insertEntryTx(ctx, tx, &domain.LedgerEntry{
		DeliveryID: deliveryID,
		ActorID:    userID,
		ActorType:  "MERCHANT",
		EntryType:  domain.LedgerEscrowHold,
		AmountRWF:  -amountRWF,
		Notes:      "fare escrowed at order creation",
	})
}

// ReleaseEscrowWithinTx moves escrow → driver (net) + platform (commission)
// inside the caller's transaction (ConfirmDelivery's outbox tx), so the
// driver is paid in the same commit that marks the delivery DELIVERED.
func (r *PostgresRepo) ReleaseEscrowWithinTx(
	ctx context.Context, tx domain.Tx, merchantID, driverID, deliveryID string, grossFareRWF int64,
) error {
	if grossFareRWF <= 0 {
		return fmt.Errorf("%w: fare must be positive", domain.ErrInvalidInput)
	}
	commission := domain.CommissionOf(grossFareRWF)
	driverNet := grossFareRWF - commission

	// The ADMIN (platform treasury) row is created by migration 012 and is
	// static during normal operation, so reading it outside the delivery tx
	// via the pool is safe and keeps domain.Tx write-only.
	platformID, err := r.platformUserID(ctx)
	if err != nil {
		return err
	}

	// 1. merchant escrow -= fare (conditional: catch accounting drift loudly)
	const qMerchant = `UPDATE wallets SET escrow_rwf = escrow_rwf - $2, updated_at = NOW()
	                   WHERE user_id = $1 AND escrow_rwf >= $2`
	tag, err := tx.ExecTag(ctx, qMerchant, merchantID, grossFareRWF)
	if err != nil {
		return fmt.Errorf("escrow release (merchant): %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("escrow release: merchant %s escrow cannot cover fare %d", merchantID, grossFareRWF)
	}

	// 2. driver balance += net (wallet row guaranteed by migration 012 bootstrap)
	if err := r.creditTx(ctx, tx, driverID, driverNet); err != nil {
		return err
	}
	// 3. platform balance += commission
	if err := r.creditTx(ctx, tx, platformID, commission); err != nil {
		return err
	}

	// 4. ledger: driver net credit + platform commission + escrow release note
	if err := r.insertEntryTx(ctx, tx, &domain.LedgerEntry{
		DeliveryID: deliveryID, ActorID: driverID, ActorType: "DRIVER",
		EntryType: domain.LedgerFareEarned, AmountRWF: driverNet,
		Notes: "fare credited on delivery confirmation",
	}); err != nil {
		return err
	}
	if commission > 0 {
		if err := r.insertEntryTx(ctx, tx, &domain.LedgerEntry{
			DeliveryID: deliveryID, ActorID: platformID, ActorType: "PLATFORM",
			EntryType: domain.LedgerCommission, AmountRWF: commission,
			Notes: "platform commission",
		}); err != nil {
			return err
		}
	}
	return r.insertEntryTx(ctx, tx, &domain.LedgerEntry{
		DeliveryID: deliveryID, ActorID: merchantID, ActorType: "MERCHANT",
		EntryType: domain.LedgerEscrowRelease, AmountRWF: 0,
		Notes: fmt.Sprintf("escrow released: driver %d RWF, platform %d RWF", driverNet, commission),
	})
}

func (r *PostgresRepo) creditTx(ctx context.Context, tx domain.Tx, userID string, amount int64) error {
	// Credit-or-create: users registered after migration 012 have no wallet row
	// until their first credit. One atomic statement covers both branches.
	const q = `INSERT INTO wallets (user_id, balance_rwf) VALUES ($1, $2)
	           ON CONFLICT (user_id) DO UPDATE
	           SET balance_rwf = wallets.balance_rwf + $2, updated_at = NOW()`
	if _, err := tx.ExecTag(ctx, q, userID, amount); err != nil {
		return fmt.Errorf("wallet credit: %w", err)
	}
	return nil
}

// platformUserID resolves the wallet that receives platform commission.
// The first ADMIN user acts as the platform treasury (migration 012 seeds it).
func (r *PostgresRepo) platformUserID(ctx context.Context) (string, error) {
	const q = `SELECT w.user_id FROM wallets w JOIN users u ON u.id = w.user_id
	           WHERE u.role = 'ADMIN' ORDER BY u.created_at LIMIT 1`
	var id string
	err := r.pool.QueryRow(ctx, q).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("platform treasury lookup (seed an ADMIN user): %w", err)
	}
	return id, nil
}

// ── instant movements (self-contained transactions) ───────────────────────────

// TopUp credits external money in. Stubbed rails — a real gateway will call
// the same method from its webhook with a verified transaction reference.
func (r *PostgresRepo) TopUp(ctx context.Context, userID string, amountRWF int64) (*domain.Wallet, error) {
	if amountRWF < domain.MinTopUpRWF || amountRWF > domain.MaxTopUpRWF {
		return nil, fmt.Errorf("%w: top-up must be between %d and %d RWF",
			domain.ErrInvalidInput, domain.MinTopUpRWF, domain.MaxTopUpRWF)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("topup begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // safe after commit

	// creditPgxTx credit-or-creates the wallet row (lazy bootstrap for users
	// registered after migration 012's backfill).
	if err := r.creditPgxTx(ctx, tx, userID, amountRWF); err != nil {
		return nil, err
	}
	if err := r.insertEntry(ctx, tx, &domain.LedgerEntry{
		ActorID:   userID,
		ActorType: actorTypeFor(r.roleOf(ctx, tx, userID)),
		EntryType: domain.LedgerTopUp,
		AmountRWF: amountRWF,
		Notes:     "wallet top-up (stubbed external rails)",
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("topup commit: %w", err)
	}
	return r.getWallet(ctx, r.pool, userID)
}

// PayWaitBounty debits the merchant and credits the driver instantly.
// Idempotency is provided upstream: the bounty row exists once per delivery
// and this method is only invoked when that row is first written.
func (r *PostgresRepo) PayWaitBounty(ctx context.Context, merchantID, driverID, deliveryID string, amountRWF int64) error {
	if amountRWF <= 0 {
		return fmt.Errorf("%w: bounty must be positive", domain.ErrInvalidInput)
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("bounty begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Platform-enforced fee: allow the merchant wallet to go negative rather
	// than failing the driver's payout. Logged for ops visibility.
	const qDebit = `UPDATE wallets SET balance_rwf = balance_rwf - $2, updated_at = NOW()
	                WHERE user_id = $1 AND balance_rwf >= $2`
	tag, err := tx.Exec(ctx, qDebit, merchantID, amountRWF)
	if err != nil {
		return fmt.Errorf("bounty debit: %w", err)
	}
	if tag.RowsAffected() == 0 {
		r.log.Warn("wait bounty pushed merchant wallet negative",
			slog.String("merchant_id", merchantID),
			slog.Int64("amount_rwf", amountRWF),
		)
		const qForced = `UPDATE wallets SET balance_rwf = balance_rwf - $2, updated_at = NOW()
		                WHERE user_id = $1`
		if _, err := tx.Exec(ctx, qForced, merchantID, amountRWF); err != nil {
			return fmt.Errorf("bounty forced debit: %w", err)
		}
	}
	if err := r.creditPgxTx(ctx, tx, driverID, amountRWF); err != nil {
		return err
	}
	if err := r.insertEntry(ctx, tx, &domain.LedgerEntry{
		DeliveryID: deliveryID, ActorID: merchantID, ActorType: "MERCHANT",
		EntryType: domain.LedgerWaitBounty, AmountRWF: -amountRWF,
		Notes: "driver wait bounty (late merchant)",
	}); err != nil {
		return err
	}
	if err := r.insertEntry(ctx, tx, &domain.LedgerEntry{
		DeliveryID: deliveryID, ActorID: driverID, ActorType: "DRIVER",
		EntryType: domain.LedgerWaitBounty, AmountRWF: amountRWF,
		Notes: "driver wait bounty earned",
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ── cashout (instant, self-service — no approval queue) ───────────────────────

// RequestCashout debits the wallet and creates the PROCESSING cashout row in
// one transaction. The usecase then calls the payout provider; provider
// failure refunds via FailCashout.
func (r *PostgresRepo) RequestCashout(
	ctx context.Context, userID, phone string, amountRWF int64,
) (*domain.CashoutRequest, error) {
	if amountRWF < domain.MinCashoutRWF {
		return nil, fmt.Errorf("%w: minimum cashout is %d RWF", domain.ErrCashoutTooSmall, domain.MinCashoutRWF)
	}
	if amountRWF > domain.MaxCashoutRWF {
		return nil, fmt.Errorf("%w: maximum cashout is %d RWF", domain.ErrCashoutTooLarge, domain.MaxCashoutRWF)
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("cashout begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// Conditional debit — balance check and debit are one atomic statement,
	// so concurrent cashouts can never overdraw the wallet.
	const q = `UPDATE wallets SET balance_rwf = balance_rwf - $2, updated_at = NOW()
	           WHERE user_id = $1 AND balance_rwf >= $2`
	tag, err := tx.Exec(ctx, q, userID, amountRWF)
	if err != nil {
		return nil, fmt.Errorf("cashout debit: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return nil, domain.ErrCashoutTooLarge // = insufficient funds
	}

	req := &domain.CashoutRequest{
		ID:        uuid.NewString(),
		UserID:    userID,
		AmountRWF: amountRWF,
		Phone:     phone,
		Provider:  "MOMO_STUB",
		Status:    domain.CashoutProcessing,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	const qIns = `INSERT INTO cashout_requests
		(id, user_id, amount_rwf, phone, provider, status, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`
	if _, err := tx.Exec(ctx, qIns,
		req.ID, req.UserID, req.AmountRWF, req.Phone, req.Provider, req.Status, req.CreatedAt, req.UpdatedAt,
	); err != nil {
		return nil, fmt.Errorf("cashout insert: %w", err)
	}
	if err := r.insertEntry(ctx, tx, &domain.LedgerEntry{
		ActorID:   userID,
		ActorType: actorTypeFor(r.roleOf(ctx, tx, userID)),
		EntryType: domain.LedgerCashout,
		AmountRWF: -amountRWF,
		Notes:     fmt.Sprintf("instant cashout to %s", phone),
	}); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("cashout commit: %w", err)
	}
	return req, nil
}

// CompleteCashout marks a cashout PAID (provider success).
func (r *PostgresRepo) CompleteCashout(ctx context.Context, cashoutID, providerRef string) error {
	const q = `UPDATE cashout_requests SET status = $2, provider_ref = $3, updated_at = NOW()
	           WHERE id = $1 AND status = $4`
	tag, err := r.pool.Exec(ctx, q, cashoutID, domain.CashoutPaid, providerRef, domain.CashoutProcessing)
	if err != nil {
		return fmt.Errorf("cashout complete: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrCashoutNotFound
	}
	return nil
}

// FailCashout marks the cashout FAILED and refunds the wallet atomically.
func (r *PostgresRepo) FailCashout(ctx context.Context, cashoutID, reason string) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("cashout fail begin: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var userID string
	var amount int64
	err = tx.QueryRow(ctx,
		`UPDATE cashout_requests SET status = $2, failure_reason = $3, updated_at = NOW()
		 WHERE id = $1 AND status = $4
		 RETURNING user_id, amount_rwf`,
		cashoutID, domain.CashoutFailed, reason, domain.CashoutProcessing,
	).Scan(&userID, &amount)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrCashoutNotFound
	}
	if err != nil {
		return fmt.Errorf("cashout fail update: %w", err)
	}
	if err := r.creditPgxTx(ctx, tx, userID, amount); err != nil {
		return err
	}
	if err := r.insertEntry(ctx, tx, &domain.LedgerEntry{
		ActorID: userID, ActorType: "SYSTEM",
		EntryType: domain.LedgerCashoutReversal, AmountRWF: amount,
		Notes: fmt.Sprintf("cashout failed — refunded: %s", reason),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListCashouts returns the user's recent cashout history.
func (r *PostgresRepo) ListCashouts(ctx context.Context, userID string, limit int) ([]*domain.CashoutRequest, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	const q = `SELECT id, user_id, amount_rwf, phone, provider, status,
	                   COALESCE(provider_ref, ''), COALESCE(failure_reason, ''), created_at, updated_at
	           FROM cashout_requests WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2`
	rows, err := r.pool.Query(ctx, q, userID, limit)
	if err != nil {
		return nil, fmt.Errorf("cashout list: %w", err)
	}
	defer rows.Close()

	var out []*domain.CashoutRequest
	for rows.Next() {
		c := &domain.CashoutRequest{}
		if err := rows.Scan(&c.ID, &c.UserID, &c.AmountRWF, &c.Phone, &c.Provider,
			&c.Status, &c.ProviderRef, &c.FailureReason, &c.CreatedAt, &c.UpdatedAt); err != nil {
			return nil, fmt.Errorf("cashout scan: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ── earnings summary ──────────────────────────────────────────────────────────

// MySummary aggregates the actor's ledger entries for a period. One indexed
// scan (idx_ledger_actor), folded in Go — trivially correct and fast at
// per-user volumes.
func (r *PostgresRepo) MySummary(ctx context.Context, userID, period string) (*domain.WalletSummary, error) {
	sinceExpr := map[string]string{
		"today": "date_trunc('day', NOW())",
		"week":  "date_trunc('week', NOW())",
		"month": "date_trunc('month', NOW())",
		"all":   "'-infinity'::timestamptz",
	}[period]
	if sinceExpr == "" {
		return nil, fmt.Errorf("%w: unknown period %q (today|week|month|all)", domain.ErrInvalidInput, period)
	}

	q := fmt.Sprintf(`SELECT entry_type, amount_rwf FROM ledger_entries
	                  WHERE actor_id = $1 AND created_at >= %s`, sinceExpr)
	rows, err := r.pool.Query(ctx, q, userID)
	if err != nil {
		return nil, fmt.Errorf("summary query: %w", err)
	}
	defer rows.Close()

	s := &domain.WalletSummary{Period: period}
	for rows.Next() {
		var et string
		var amt int64
		if err := rows.Scan(&et, &amt); err != nil {
			return nil, fmt.Errorf("summary scan: %w", err)
		}
		switch et {
		case domain.LedgerFareEarned:
			s.TripsCompleted++
			s.NetRWF += amt
			// Reconstruct gross from net: net = gross − 10% ⇒ gross = net × 100/90.
			s.GrossRWF += amt * 100 / 90
		case domain.LedgerCommission:
			s.CommissionRWF += amt
		case domain.LedgerWaitBounty:
			if amt >= 0 {
				s.BountyRWF += amt
			} else {
				s.SpentRWF += -amt
			}
		case domain.LedgerEscrowHold:
			s.SpentRWF += -amt
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("summary rows: %w", err)
	}
	return s, nil
}

// ── internals ─────────────────────────────────────────────────────────────────

// insertEntry writes a ledger row on a self-owned pgx transaction.
func (r *PostgresRepo) insertEntry(ctx context.Context, tx pgx.Tx, e *domain.LedgerEntry) error {
	const q = `INSERT INTO ledger_entries
		(delivery_id, actor_id, actor_type, entry_type, amount_rwf, notes)
		VALUES ($1,$2,$3,$4,$5,$6)`
	if _, err := tx.Exec(ctx, q,
		nullable(e.DeliveryID), e.ActorID, e.ActorType, e.EntryType, e.AmountRWF, nullable(e.Notes),
	); err != nil {
		return fmt.Errorf("ledger insert (%s): %w", e.EntryType, err)
	}
	return nil
}

// insertEntryTx writes a ledger row on the caller's domain.Tx (delivery txs).
func (r *PostgresRepo) insertEntryTx(ctx context.Context, tx domain.Tx, e *domain.LedgerEntry) error {
	const q = `INSERT INTO ledger_entries
		(delivery_id, actor_id, actor_type, entry_type, amount_rwf, notes)
		VALUES ($1,$2,$3,$4,$5,$6)`
	if err := tx.Exec(ctx, q,
		nullable(e.DeliveryID), e.ActorID, e.ActorType, e.EntryType, e.AmountRWF, nullable(e.Notes),
	); err != nil {
		return fmt.Errorf("ledger insert (%s): %w", e.EntryType, err)
	}
	return nil
}

func (r *PostgresRepo) creditPgxTx(ctx context.Context, tx pgx.Tx, userID string, amount int64) error {
	// Credit-or-create (same rationale as creditTx).
	const q = `INSERT INTO wallets (user_id, balance_rwf) VALUES ($1, $2)
	           ON CONFLICT (user_id) DO UPDATE
	           SET balance_rwf = wallets.balance_rwf + $2, updated_at = NOW()`
	if _, err := tx.Exec(ctx, q, userID, amount); err != nil {
		return fmt.Errorf("wallet credit: %w", err)
	}
	return nil
}

func (r *PostgresRepo) roleOf(ctx context.Context, tx pgx.Tx, userID string) string {
	var role string
	_ = tx.QueryRow(ctx, `SELECT role FROM users WHERE id = $1`, userID).Scan(&role)
	return role
}

// ── querier union for pool-or-tx reads ────────────────────────────────────────

// txQuerier covers the handles getWallet/upsertWallet run on: *pgxpool.Pool,
// pgx.Tx, or both — pgx.Tx and pool share this method surface.
type txQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

func nullable(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

func actorTypeFor(role string) string {
	switch strings.ToUpper(strings.TrimSpace(role)) {
	case "DRIVER":
		return "DRIVER"
	case "MERCHANT":
		return "MERCHANT"
	case "ADMIN":
		return "PLATFORM"
	default:
		return "CUSTOMER"
	}
}
