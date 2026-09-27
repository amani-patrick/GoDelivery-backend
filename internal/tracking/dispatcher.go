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
const AlertChannelSize = 256

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

func (d *Dispatcher) handle(ctx context.Context, alert domain.AnomalyAlert) {
	// Structured log — immediately visible to on-call dispatchers
	d.log.Error("DISPATCH ALERT",
		slog.String("alert_type", alert.AlertType),
		slog.String("delivery_id", alert.DeliveryID),
		slog.String("driver_id", alert.DriverID),
		slog.Float64("last_lat", alert.LastKnownLat),
		slog.Float64("last_lng", alert.LastKnownLng),
		slog.String("details", alert.Details),
		slog.Time("detected_at", alert.DetectedAt),
	)

	//Append to immutable ledger
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

	// Push alert to the driver's active WebSocket connection
	if d.handler != nil {
		d.handler.BroadcastAlert(alert.DriverID, alert)
	}
}
