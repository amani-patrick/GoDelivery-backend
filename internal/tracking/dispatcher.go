package tracking

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/umurinzi/backend/internal/domain"
)

// AlertChannelSize is the buffer depth of the alert channel.
// At 256 slots it absorbs burst periods without dropping alerts
// while keeping backpressure visible in logs when it fills up.
const AlertChannelSize = 256

// Dispatcher is the single consumer of the AnomalyAlert channel.
// It fans out each alert to three destinations:
//  1. Structured log (visible to real-time security operations)
//  2. Immutable audit ledger (compliance + forensics)
//  3. Driver WebSocket connection (safety check-in prompt on device)
//
// The Dispatcher communicates with other modules only through the
// domain.LedgerRepository interface and TelemetryHandler.BroadcastAlert —
// never through direct cross-module database writes.
type Dispatcher struct {
	alertCh <-chan domain.AnomalyAlert
	ledger  domain.LedgerRepository
	handler *TelemetryHandler
	log     *slog.Logger
}

// NewDispatcher constructs the Dispatcher.
func NewDispatcher(
	alertCh <-chan domain.AnomalyAlert,
	ledger domain.LedgerRepository,
	handler *TelemetryHandler,
	log *slog.Logger,
) *Dispatcher {
	return &Dispatcher{
		alertCh: alertCh,
		ledger:  ledger,
		handler: handler,
		log:     log,
	}
}

// Run starts the event loop. It blocks until ctx is cancelled or the alert
// channel is closed. Call it in a dedicated goroutine:
//
//	go dispatcher.Run(ctx)
func (d *Dispatcher) Run(ctx context.Context) {
	d.log.Info("dispatcher started")
	for {
		select {
		case <-ctx.Done():
			d.log.Info("dispatcher: context cancelled — shutting down")
			return
		case alert, ok := <-d.alertCh:
			if !ok {
				d.log.Info("dispatcher: alert channel closed — exiting")
				return
			}
			d.handle(ctx, alert)
		}
	}
}

// handle processes a single alert. All three fan-out steps are attempted
// regardless of individual failures — a ledger write failure does not
// prevent the driver from receiving their push notification.
func (d *Dispatcher) handle(ctx context.Context, alert domain.AnomalyAlert) {
	// 1. Structured log — immediately visible to on-call dispatchers
	d.log.Error("DISPATCH ALERT",
		slog.String("alert_type", alert.AlertType),
		slog.String("delivery_id", alert.DeliveryID),
		slog.String("driver_id", alert.DriverID),
		slog.Float64("last_lat", alert.LastKnownLat),
		slog.Float64("last_lng", alert.LastKnownLng),
		slog.String("details", alert.Details),
		slog.Time("detected_at", alert.DetectedAt),
	)

	// 2. Append to immutable ledger
	event := &domain.AuditEvent{
		ID:         uuid.NewString(),
		EntityID:   alert.DeliveryID,
		EntityType: "DELIVERY",
		ActorID:    "SYSTEM",
		Action:     fmt.Sprintf("ANOMALY_%s", alert.AlertType),
		OldState:   string(domain.StateInTransit),
		NewState:   "",
		Metadata:   alert.Details,
		CreatedAt:  time.Now().UTC(),
	}
	if err := d.ledger.Append(ctx, event); err != nil {
		d.log.Error("dispatcher: ledger write failed",
			slog.String("delivery_id", alert.DeliveryID),
			slog.String("alert_type", alert.AlertType),
			slog.String("reason", err.Error()),
		)
	}

	// 3. Push alert to the driver's active WebSocket connection
	if d.handler != nil {
		d.handler.BroadcastAlert(alert.DriverID, alert)
	}
}
