package domain

import (
	"context"
	"errors"
	"time"
)

// ── Money & wallet domain ─────────────────────────────────────────────────────
//
// The ledger_entries table (migration 009) is the single source of financial
// truth. Wallet balances are a projection kept in the SAME transaction as the
// ledger entries that move them, and — for deliveries — in the same
// transaction as the delivery state change itself:
//
//   CreateDelivery  tx: insert delivery + ESCROW_HOLD + debit merchant balance → escrow
//   ConfirmDelivery tx: update state + ESCROW_RELEASE + credit driver + credit platform
//
// Because escrow happens at order creation, a merchant can never place an
// order it cannot pay for, and a driver is paid the moment the PIN handshake
// completes. Cashout is self-service and instant: as long as funds are in the
// wallet, the money moves (via the PayoutProvider — a stub until MoMo lands).

var (
	// ErrInsufficientFunds — wallet cannot cover the requested amount.
	ErrInsufficientFunds = errors.New("insufficient wallet balance")
	// ErrCashoutTooLarge — requested more than the available balance.
	ErrCashoutTooLarge = errors.New("cashout amount exceeds available balance")
	// ErrCashoutTooSmall — below the minimum cashout amount.
	ErrCashoutTooSmall = errors.New("cashout amount is below the minimum")
	// ErrCashoutNotFound
	ErrCashoutNotFound = errors.New("cashout request not found")
	// ErrPayoutProvider — provider stub/real failed to process the payout.
	ErrPayoutProvider = errors.New("payout provider rejected the transfer")
)

// Ledger entry_type values (extends the sketch in migration 009).
const (
	LedgerFareEarned      = "FARE_EARNED"       // driver credit at delivery (net of commission)
	LedgerCommission      = "PLATFORM_COMMISSION" // platform credit at delivery
	LedgerEscrowHold      = "ESCROW_HOLD"       // merchant balance → escrow at order creation
	LedgerEscrowRelease   = "ESCROW_RELEASE"    // escrow → driver + platform at delivery
	LedgerWaitBounty      = "WAIT_BOUNTY"       // merchant debit → driver credit
	LedgerTopUp           = "TOPUP"             // external money in (stubbed until a gateway exists)
	LedgerCashout         = "CASHOUT"           // wallet → external money out
	LedgerCashoutReversal = "CASHOUT_REVERSAL"  // failed payout returns funds
)

// CommissionBPS is the platform commission in basis points (10%).
const CommissionBPS = 1000

// MinTopUpRWF / MinCashoutRWF guard rails for the stub rails.
const (
	MinTopUpRWF    = 100
	MinCashoutRWF  = 500
	MaxTopUpRWF    = 2_000_000
	MaxCashoutRWF  = 2_000_000
)

// CommissionOf returns the platform commission for a gross fare.
func CommissionOf(fareRWF int64) int64 { return fareRWF * CommissionBPS / 10000 }

// DriverNetOf returns the driver's share of a gross fare.
func DriverNetOf(fareRWF int64) int64 { return fareRWF - CommissionOf(fareRWF) }

// LedgerEntry is a single immutable money-movement record. Rows are only
// ever INSERTed — corrections are new compensating entries, never edits.
type LedgerEntry struct {
	ID         string
	DeliveryID string // optional correlation
	ActorID    string
	ActorType  string // DRIVER | MERCHANT | PLATFORM | CUSTOMER | SYSTEM
	EntryType  string
	AmountRWF  int64 // positive = credit to actor, negative = debit
	Notes      string
	CreatedAt  time.Time
}

// Wallet is the per-user balance projection.
type Wallet struct {
	UserID     string    `json:"user_id"`
	BalanceRWF int64     `json:"balance_rwf"`
	EscrowRWF  int64     `json:"escrow_rwf"`
	UpdatedAt  time.Time `json:"updated_at"`
}

func (w *Wallet) AvailableRWF() int64 { return w.BalanceRWF }

// WalletSummary answers "what did I earn / spend" for a period.
type WalletSummary struct {
	Period         string `json:"period"`
	TripsCompleted int    `json:"trips_completed"`
	GrossRWF       int64  `json:"gross_rwf"`
	CommissionRWF  int64  `json:"commission_rwf"`
	NetRWF         int64  `json:"net_rwf"`
	BountyRWF      int64  `json:"bounty_rwf"`
	SpentRWF       int64  `json:"spent_rwf"` // merchants: escrowed fares + bounties paid
}

// CashoutRequest is a self-service instant withdrawal — no approval queue.
type CashoutRequest struct {
	ID            string    `json:"id"`
	UserID        string    `json:"user_id"`
	AmountRWF     int64     `json:"amount_rwf"`
	Phone         string    `json:"phone"`
	Provider      string    `json:"provider"`
	Status        string    `json:"status"` // PROCESSING | PAID | FAILED
	ProviderRef   string    `json:"provider_ref,omitempty"`
	FailureReason string    `json:"failure_reason,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Cashout statuses.
const (
	CashoutProcessing = "PROCESSING"
	CashoutPaid       = "PAID"
	CashoutFailed     = "FAILED"
)

// WalletRepository is implemented over wallets + ledger_entries + cashout_requests.
// Every method is transactional internally: the entry write and the balance
// projection move together or not at all. Methods taking a domain.Tx join the
// CALLER's transaction (delivery creation / delivery completion), so money
// state and delivery state commit atomically.
type WalletRepository interface {
	// EnsureWallet returns the wallet row, creating it (0/0) if missing.
	EnsureWallet(ctx context.Context, userID string) (*Wallet, error)
	// GetWallet returns the wallet row.
	GetWallet(ctx context.Context, userID string) (*Wallet, error)

	// HoldEscrowWithinTx debits balance and credits escrow inside the caller's
	// transaction (order creation). Writes the ESCROW_HOLD ledger entry
	// atomically. Fails with ErrInsufficientFunds without touching anything.
	HoldEscrowWithinTx(ctx context.Context, tx Tx, userID, deliveryID string, amountRWF int64) error
	// ReleaseEscrowWithinTx moves escrow → driver balance (net) and → platform
	// treasury (commission) inside the caller's transaction (delivery
	// completion). Writes FARE_EARNED + PLATFORM_COMMISSION + ESCROW_RELEASE.
	ReleaseEscrowWithinTx(ctx context.Context, tx Tx, merchantID, driverID, deliveryID string, grossFareRWF int64) error

	// TopUp credits external money in (stubbed rails). Instant.
	TopUp(ctx context.Context, userID string, amountRWF int64) (*Wallet, error)
	// PayWaitBounty debits the merchant and credits the driver for a late-merchant
	// bounty. Instant; non-transactional across both wallets by design (each
	// side is individually atomic — the bounty is never double-charged).
	PayWaitBounty(ctx context.Context, merchantID, driverID, deliveryID string, amountRWF int64) error

	// MySummary aggregates ledger entries for a period ("today", "week", "all").
	MySummary(ctx context.Context, userID, period string) (*WalletSummary, error)

	// RequestCashout debits the wallet, inserts a PROCESSING cashout row and
	// its CASHOUT ledger entry atomically, then the usecase hands it to the
	// PayoutProvider. Instant by design — no approval queue.
	RequestCashout(ctx context.Context, userID, phone string, amountRWF int64) (*CashoutRequest, error)
	// CompleteCashout marks a cashout PAID (provider success).
	CompleteCashout(ctx context.Context, cashoutID, providerRef string) error
	// FailCashout marks a cashout FAILED and refunds the wallet
	// (CASHOUT_REVERSAL entry) atomically.
	FailCashout(ctx context.Context, cashoutID, reason string) error
	// ListCashouts returns the user's recent cashout history.
	ListCashouts(ctx context.Context, userID string, limit int) ([]*CashoutRequest, error)
}

// PayoutProvider abstracts the external money-out rails (MTN MoMo later).
// The stub implementation records success locally without touching a real
// payment network — swap the binding in main.go when MoMo is ready.
type PayoutProvider interface {
	// Payout sends amountRWF to phone. Returns a provider reference.
	Payout(ctx context.Context, phone string, amountRWF int64, reference string) (providerRef string, err error)
}
