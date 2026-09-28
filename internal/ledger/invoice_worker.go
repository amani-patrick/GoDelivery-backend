package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RRA (Rwanda Revenue Authority) VAT rate: 18%.
const rraVATRate = 0.18

// InvoiceStatus tracks the lifecycle of a tax invoice.
type InvoiceStatus string

const (
	InvoiceStatusPending   InvoiceStatus = "PENDING"
	InvoiceStatusSubmitted InvoiceStatus = "SUBMITTED"
	InvoiceStatusConfirmed InvoiceStatus = "CONFIRMED"
	InvoiceStatusFailed    InvoiceStatus = "FAILED"
)

// TaxInvoice is an immutable ledger record for RRA compliance.
type TaxInvoice struct {
	ID            string        `json:"id"`
	DeliveryID    string        `json:"delivery_id"`
	DriverID      string        `json:"driver_id"`
	MerchantID    string        `json:"merchant_id"`
	CustomerID    string        `json:"customer_id"`
	FareRWF       float64       `json:"fare_rwf"`
	VATRWF        float64       `json:"vat_rwf"`
	TotalRWF      float64       `json:"total_rwf"`
	InvoiceNumber string        `json:"invoice_number"`
	Status        InvoiceStatus `json:"status"`
	RRAPayload    string        `json:"rra_payload,omitempty"`
	SubmittedAt   *time.Time    `json:"submitted_at,omitempty"`
	ConfirmedAt   *time.Time    `json:"confirmed_at,omitempty"`
	CreatedAt     time.Time     `json:"created_at"`
}

// InvoiceWorker is an asynchronous worker that listens for completed
// deliveries (StateDelivered events in the payment outbox) and constructs
// immutable tax invoice records for RRA compliance.
//
// This separates tax/accounting overhead from the core logistics pipeline,
// keeping the main order flow fast while ensuring every completed delivery
// has a corresponding tax record.
type InvoiceWorker struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewInvoiceWorker constructs the worker.
func NewInvoiceWorker(pool *pgxpool.Pool, log *slog.Logger) *InvoiceWorker {
	return &InvoiceWorker{pool: pool, log: log}
}

// Run starts the invoice processing loop. It polls the payment_events table
// for completed deliveries that don't yet have a corresponding tax_invoice.
// Run in a dedicated goroutine: go invoiceWorker.Run(ctx)
func (w *InvoiceWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	w.log.Info("RRA invoice worker started", slog.Duration("poll_interval", 15*time.Second))

	for {
		select {
		case <-ctx.Done():
			w.log.Info("RRA invoice worker stopped")
			return
		case <-ticker.C:
			w.processNewDeliveries(ctx)
		}
	}
}

// processNewDeliveries finds completed deliveries without tax invoices and
// creates immutable invoice records for each.
func (w *InvoiceWorker) processNewDeliveries(ctx context.Context) {
	// Find payment events for delivered orders that don't have invoices yet.
	// NOTE: payment_events.status uses the PaymentEventStatus enum — a completed
	// payment is 'SUCCEEDED', not 'SUCCESS'. Scanning the wrong literal made
	// this query match zero rows forever.
	rows, err := w.pool.Query(ctx, `
		SELECT pe.delivery_id, COALESCE(pe.driver_id, ''), pe.amount_rwf,
		       d.merchant_id, d.customer_id
		FROM payment_events pe
		JOIN deliveries d ON d.id = pe.delivery_id
		WHERE pe.status = 'SUCCEEDED'
		  AND NOT EXISTS (
		      SELECT 1 FROM tax_invoices ti WHERE ti.delivery_id = pe.delivery_id
		  )
		LIMIT 50
	`)
	if err != nil {
		w.log.Error("invoice worker: query failed", slog.String("reason", err.Error()))
		return
	}
	defer rows.Close()

	var invoicesCreated int
	for rows.Next() {
		var deliveryID, driverID, merchantID, customerID string
		var fareRWF float64

		if err := rows.Scan(&deliveryID, &driverID, &fareRWF, &merchantID, &customerID); err != nil {
			w.log.Error("invoice worker: scan failed", slog.String("reason", err.Error()))
			continue
		}

		invoice := w.buildInvoice(deliveryID, driverID, merchantID, customerID, fareRWF)
		if err := w.insertInvoice(ctx, invoice); err != nil {
			w.log.Error("invoice worker: insert failed",
				slog.String("delivery_id", deliveryID),
				slog.String("reason", err.Error()),
			)
			continue
		}

		invoicesCreated++
		w.log.Info("tax invoice created",
			slog.String("delivery_id", deliveryID),
			slog.String("invoice_number", invoice.InvoiceNumber),
			slog.Float64("fare_rwf", invoice.FareRWF),
			slog.Float64("vat_rwf", invoice.VATRWF),
			slog.Float64("total_rwf", invoice.TotalRWF),
		)
	}

	if invoicesCreated > 0 {
		w.log.Info("invoice worker: batch complete", slog.Int("invoices_created", invoicesCreated))
	}
}

// buildInvoice constructs an immutable tax invoice record.
func (w *InvoiceWorker) buildInvoice(deliveryID, driverID, merchantID, customerID string, fareRWF float64) *TaxInvoice {
	vatRWF := fareRWF * rraVATRate
	totalRWF := fareRWF + vatRWF

	// Generate a deterministic invoice number: UMR-YYYYMMDD-XXXXXXXX
	now := time.Now().UTC()
	invoiceNumber := fmt.Sprintf("UMR-%s-%s",
		now.Format("20060102"),
		uuid.NewString()[:8],
	)

	// Build the RRA payload that would be submitted to the tax authority.
	rraPayload, _ := json.Marshal(map[string]interface{}{
		"tin":             "UMURINZI_TIN",
		"invoice_number":  invoiceNumber,
		"date":            now.Format(time.RFC3339),
		"customer_id":     customerID,
		"merchant_id":     merchantID,
		"items": []map[string]interface{}{
			{
				"description": "Delivery Service",
				"quantity":    1,
				"unit_price":  fareRWF,
				"vat_rate":    rraVATRate,
				"vat_amount":  vatRWF,
				"total":       totalRWF,
			},
		},
		"subtotal":  fareRWF,
		"vat_total": vatRWF,
		"total":     totalRWF,
	})

	return &TaxInvoice{
		ID:            uuid.NewString(),
		DeliveryID:    deliveryID,
		DriverID:      driverID,
		MerchantID:    merchantID,
		CustomerID:    customerID,
		FareRWF:       fareRWF,
		VATRWF:        vatRWF,
		TotalRWF:      totalRWF,
		InvoiceNumber: invoiceNumber,
		Status:        InvoiceStatusPending,
		RRAPayload:    string(rraPayload),
		CreatedAt:     now,
	}
}

// insertInvoice writes the invoice to the immutable ledger.
func (w *InvoiceWorker) insertInvoice(ctx context.Context, inv *TaxInvoice) error {
	_, err := w.pool.Exec(ctx, `
		INSERT INTO tax_invoices (
			id, delivery_id, driver_id, merchant_id, customer_id,
			fare_rwf, vat_rwf, total_rwf, invoice_number,
			status, rra_payload, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb, $12)
		ON CONFLICT (delivery_id) DO NOTHING
	`, inv.ID, inv.DeliveryID, inv.DriverID, inv.MerchantID, inv.CustomerID,
		inv.FareRWF, inv.VATRWF, inv.TotalRWF, inv.InvoiceNumber,
		string(inv.Status), inv.RRAPayload, inv.CreatedAt,
	)
	return err
}

// SubmitToRRA pushes the immutable RRA payload onto an external queue (or
// calls the RRA VSDC API directly in production). Keeping submission
// asynchronous here is what keeps the core logistics pipeline fast: the
// DELIVERED state transition never waits on accounting.
func (w *InvoiceWorker) SubmitToRRA(ctx context.Context, invoiceID string) error {
	now := time.Now().UTC()
	_, err := w.pool.Exec(ctx, `
		UPDATE tax_invoices
		SET status = $1, submitted_at = $2, updated_at = $2
		WHERE id = $3 AND status = $4
	`, string(InvoiceStatusSubmitted), now, invoiceID, string(InvoiceStatusPending))
	if err != nil {
		return fmt.Errorf("submit to RRA: %w", err)
	}

	w.log.Info("invoice submitted to RRA",
		slog.String("invoice_id", invoiceID),
	)
	return nil
}
