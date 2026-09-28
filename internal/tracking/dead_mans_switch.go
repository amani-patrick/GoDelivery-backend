package tracking

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/umurinzi/backend/internal/domain"
)

const (
	// deadMansSwitchPrefix is the Redis key prefix tracking the last telemetry
	// timestamp for each driver on an active IN_TRANSIT trip.
	deadMansSwitchPrefix = "dms:last_ping:"

	// deadMansSwitchTTL is the max time we keep the key alive. If no ping
	// refreshes it within this window, the key auto-expires.
	deadMansSwitchTTL = 10 * time.Minute

	// deadMansSilenceThreshold is the silence duration that triggers a
	// high-priority alert. 7 minutes of zero GPS pings on an active trip
	// indicates a real-world risk (accident, robbery, dead zone).
	deadMansSilenceThreshold = 7 * time.Minute

	// deadMansScanInterval is how often the background scanner checks for
	// silent drivers.
	deadMansScanInterval = 30 * time.Second
)

// DeadMansSwitch monitors GPS telemetry continuity for drivers on active
// IN_TRANSIT trips. If a driver's telemetry goes silent for more than
// deadMansSilenceThreshold, a high-priority alert is emitted.
type DeadMansSwitch struct {
	rdb     *redis.Client
	alertCh chan<- domain.AnomalyAlert
	log     *slog.Logger
}

// NewDeadMansSwitch constructs the switch.
func NewDeadMansSwitch(rdb *redis.Client, alertCh chan<- domain.AnomalyAlert, log *slog.Logger) *DeadMansSwitch {
	return &DeadMansSwitch{rdb: rdb, alertCh: alertCh, log: log}
}

// ArmForTrip starts tracking a driver when they begin an IN_TRANSIT trip.
// Called from ConfirmPickup after the QR handshake succeeds.
func (d *DeadMansSwitch) ArmForTrip(ctx context.Context, driverID, deliveryID string) {
	key := deadMansSwitchPrefix + driverID
	val := fmt.Sprintf("%s|%d", deliveryID, time.Now().UTC().UnixMilli())
	if err := d.rdb.Set(ctx, key, val, deadMansSwitchTTL).Err(); err != nil {
		d.log.Error("DMS: failed to arm",
			slog.String("driver_id", driverID),
			slog.String("delivery_id", deliveryID),
			slog.String("reason", err.Error()),
		)
	}
}

// RefreshPing resets the DMS timer on every valid telemetry frame received
// from a driver who has an active trip. Called from handleFrame after
// writing to the spatial index.
func (d *DeadMansSwitch) RefreshPing(ctx context.Context, driverID string) {
	key := deadMansSwitchPrefix + driverID
	// Only refresh if the key exists (driver is on an active trip).
	exists, err := d.rdb.Exists(ctx, key).Result()
	if err != nil || exists == 0 {
		return
	}
	// Read the existing value to preserve the delivery ID, update timestamp.
	val, err := d.rdb.Get(ctx, key).Result()
	if err != nil {
		return
	}
	// Parse out the delivery ID (before the pipe).
	deliveryID := val
	for i, c := range val {
		if c == '|' {
			deliveryID = val[:i]
			break
		}
	}
	newVal := fmt.Sprintf("%s|%d", deliveryID, time.Now().UTC().UnixMilli())
	_ = d.rdb.Set(ctx, key, newVal, deadMansSwitchTTL).Err()
}

// DisarmForTrip removes tracking when a trip completes (DELIVERED/CANCELLED).
func (d *DeadMansSwitch) DisarmForTrip(ctx context.Context, driverID string) {
	_ = d.rdb.Del(ctx, deadMansSwitchPrefix+driverID).Err()
}

// RunScanner periodically scans for drivers whose DMS keys are about to expire
// (meaning no telemetry pings were received). Emits DEAD_MANS_SWITCH alerts.
// Run in a dedicated goroutine: go dms.RunScanner(ctx)
func (d *DeadMansSwitch) RunScanner(ctx context.Context) {
	ticker := time.NewTicker(deadMansScanInterval)
	defer ticker.Stop()

	d.log.Info("Dead Man's Switch scanner started",
		slog.Duration("scan_interval", deadMansScanInterval),
		slog.Duration("silence_threshold", deadMansSilenceThreshold),
	)

	for {
		select {
		case <-ctx.Done():
			d.log.Info("Dead Man's Switch scanner stopped")
			return
		case <-ticker.C:
			d.scanForSilentDrivers(ctx)
		}
	}
}

// scanForSilentDrivers uses SCAN to find DMS keys that are about to expire
// and checks whether the last ping timestamp exceeds the silence threshold.
func (d *DeadMansSwitch) scanForSilentDrivers(ctx context.Context) {
	var cursor uint64
	for {
		keys, nextCursor, err := d.rdb.Scan(ctx, cursor, deadMansSwitchPrefix+"*", 100).Result()
		if err != nil {
			d.log.Error("DMS: scan failed", slog.String("reason", err.Error()))
			return
		}

		for _, key := range keys {
			ttl, err := d.rdb.TTL(ctx, key).Result()
			if err != nil {
				continue
			}

			// If the TTL is below the silence threshold, the driver hasn't
			// sent a ping in (deadMansSwitchTTL - remaining TTL) time.
			// Example: TTL=10min, remaining=2min → silence = 8min > 7min threshold
			silenceDuration := deadMansSwitchTTL - ttl
			if silenceDuration < deadMansSilenceThreshold {
				continue
			}

			// Parse the stored value for delivery ID and last ping time.
			val, err := d.rdb.Get(ctx, key).Result()
			if err != nil {
				continue
			}

			driverID := key[len(deadMansSwitchPrefix):]
			deliveryID := val
			for i, c := range val {
				if c == '|' {
					deliveryID = val[:i]
					break
				}
			}

			d.log.Warn("DEAD MAN'S SWITCH TRIGGERED — driver telemetry silent",
				slog.String("driver_id", driverID),
				slog.String("delivery_id", deliveryID),
				slog.Duration("silence", silenceDuration),
			)

			alert := domain.AnomalyAlert{
				DeliveryID: deliveryID,
				DriverID:   driverID,
				AlertType:  "DEAD_MANS_SWITCH",
				DetectedAt: time.Now().UTC(),
				Details: fmt.Sprintf(
					"GPS telemetry silent for %.0f minutes on active IN_TRANSIT trip %s — possible accident, robbery, or dead zone",
					silenceDuration.Minutes(), deliveryID,
				),
			}

			select {
			case d.alertCh <- alert:
			default:
				d.log.Error("DMS: alert channel full — alert dropped",
					slog.String("driver_id", driverID),
					slog.String("delivery_id", deliveryID),
				)
			}

			// Delete the key after alerting to avoid duplicate alerts.
			// If the driver reconnects, ArmForTrip or RefreshPing will re-create it.
			_ = d.rdb.Del(ctx, key)
		}

		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
}
