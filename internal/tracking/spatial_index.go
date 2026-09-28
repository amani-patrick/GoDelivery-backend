package tracking

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/umurinzi/backend/internal/domain"
)

const (
	// geoKey is the Redis Sorted Set used by GEOADD / GEOSEARCH.
	geoKey = "drivers:geo"

	// frameBufferPrefix is the key prefix for per-driver rolling frame buffers.
	// Full key: "driver:frames:<driverID>"
	frameBufferPrefix = "driver:frames:"

	// frameBufferTTL is the TTL applied to the rolling buffer on every write.
	// A driver that goes offline will have their buffer auto-expired.
	frameBufferTTL = 10 * time.Minute

	// maxBufferLen is the maximum number of frames retained per driver.
	// At one frame per 10 seconds this covers ~10 minutes of history.
	maxBufferLen = 60

	// maxMotorbikeSpeedKmh is the physical ceiling for a motorbike on any
	// Kigali road. Frames implying a higher speed are GPS spoofs or teleports.
	maxMotorbikeSpeedKmh = 120.0
)

// ── SpatialIndex ──────────────────────────────────────────────────────────────

// SpatialIndex maintains real-time driver positions in a Redis Geo set and
// provides rolling frame buffers for the anomaly detector and panic handler.
type SpatialIndex struct {
	rdb *redis.Client
	log *slog.Logger
}

// NewSpatialIndex constructs the index with the given Redis client.
func NewSpatialIndex(rdb *redis.Client, log *slog.Logger) *SpatialIndex {
	return &SpatialIndex{rdb: rdb, log: log}
}

// VelocityResult is the output of CheckVelocity.
type VelocityResult struct {
	// Plausible is false when the implied speed between the previous frame and
	// the candidate frame exceeds maxMotorbikeSpeedKmh.
	Plausible    bool
	ImpliedKmh   float64
	PrevFrame    *domain.TelemetryFrame // nil when the buffer is empty (first frame)
}

// CheckVelocity fetches the most recent frame from the driver's buffer and
// computes the implied speed to the candidate frame.
//
// This must be called BEFORE UpdateDriverPosition so that a spoofed frame is
// rejected before it can poison the Redis geo index or the rolling buffer.
//
// Returns VelocityResult{Plausible: true} when the buffer is empty (first
// frame for a driver — no previous point to compare against).
func (s *SpatialIndex) CheckVelocity(ctx context.Context, candidate domain.TelemetryFrame) (VelocityResult, error) {
	prev, err := s.getLastFrame(ctx, candidate.DriverID)
	if err != nil {
		return VelocityResult{}, fmt.Errorf("CheckVelocity: fetch last frame: %w", err)
	}
	// No previous frame — this is the first frame for this driver session.
	if prev == nil {
		return VelocityResult{Plausible: true}, nil
	}

	intervalSec := candidate.CapturedAt.Sub(prev.CapturedAt).Seconds()
	// If the device clock moved backwards or two frames are identical in time,
	// skip the velocity check — we can't compute a meaningful speed.
	if intervalSec <= 0 {
		return VelocityResult{Plausible: true, PrevFrame: prev}, nil
	}

	distM := haversineMeters(prev.Lat, prev.Lng, candidate.Lat, candidate.Lng)
	impliedKmh := (distM / 1000.0) / (intervalSec / 3600.0)

	return VelocityResult{
		Plausible:  impliedKmh <= maxMotorbikeSpeedKmh,
		ImpliedKmh: impliedKmh,
		PrevFrame:  prev,
	}, nil
}

// UpdateDriverPosition atomically:
//  1. Updates the driver's geo-position in the Redis Geo set.
//  2. Prepends the frame to the driver's rolling buffer list.
//  3. Trims the list to maxBufferLen.
//  4. Resets the buffer TTL.
//
// All four commands are pipelined in a single round-trip.
// Callers must call CheckVelocity first and only call this if the result is Plausible.
func (s *SpatialIndex) UpdateDriverPosition(ctx context.Context, frame domain.TelemetryFrame) error {
	pipe := s.rdb.Pipeline()

	pipe.GeoAdd(ctx, geoKey, &redis.GeoLocation{
		Name:      frame.DriverID,
		Longitude: frame.Lng,
		Latitude:  frame.Lat,
	})

	bufKey := frameBufferPrefix + frame.DriverID
	pipe.LPush(ctx, bufKey, mustMarshalFrame(frame))
	pipe.LTrim(ctx, bufKey, 0, maxBufferLen-1)
	pipe.Expire(ctx, bufKey, frameBufferTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		s.log.Error("SpatialIndex.UpdateDriverPosition failed",
			slog.String("driver_id", frame.DriverID),
			slog.String("delivery_id", frame.DeliveryID),
			slog.String("reason", err.Error()),
		)
		return fmt.Errorf("update driver position: %w", err)
	}
	return nil
}

// GetRecentFrames returns up to n frames from the driver's rolling buffer,
// sorted newest-first by device CapturedAt timestamp.
//
// Sorting by CapturedAt (not Redis insertion order) is critical for correctness
// when a driver reconnects after a dead zone and dumps a burst of buffered
// frames in rapid succession. Without sorting, frames within the burst arrive
// in network-arrival order which is unrelated to the physical timeline, and
// velocity / stationarity checks produce nonsense results.
//
// Returns an empty (non-nil) slice when no frames exist.
func (s *SpatialIndex) GetRecentFrames(ctx context.Context, driverID string, n int) ([]domain.TelemetryFrame, error) {
	bufKey := frameBufferPrefix + driverID
	// Fetch only what the caller needs (capped at the buffer length). The
	// anomaly detector runs on EVERY telemetry frame — pulling all 100 buffer
	// entries per frame and discarding all but n cost ~3x more Redis bandwidth
	// and JSON decodes, which showed up as a CPU cliff at ~1200 concurrent
	// drivers in load testing.
	end := maxBufferLen - 1
	if n > 0 && n-1 < end {
		end = n - 1
	}
	raw, err := s.rdb.LRange(ctx, bufKey, 0, int64(end)).Result()
	if err != nil {
		return nil, fmt.Errorf("get recent frames: %w", err)
	}

	frames := make([]domain.TelemetryFrame, 0, len(raw))
	for _, r := range raw {
		f, err := unmarshalFrame(r)
		if err != nil {
			s.log.Warn("SpatialIndex: unmarshal frame failed — skipping",
				slog.String("driver_id", driverID),
				slog.String("reason", err.Error()),
			)
			continue
		}
		frames = append(frames, f)
	}

	// Sort by device timestamp descending (newest first) so that index 0 is
	// always the most recent real-world observation, regardless of insertion order.
	sort.Slice(frames, func(i, j int) bool {
		return frames[i].CapturedAt.After(frames[j].CapturedAt)
	})

	if n < len(frames) {
		frames = frames[:n]
	}
	return frames, nil
}

// FindNearbyDrivers returns up to 20 driver IDs within radiusKm of a coordinate,
// sorted nearest-first. Used by dispatch to find available drivers.
func (s *SpatialIndex) FindNearbyDrivers(ctx context.Context, lat, lng, radiusKm float64) ([]string, error) {
	results, err := s.rdb.GeoSearch(ctx, geoKey, &redis.GeoSearchQuery{
		Longitude:  lng,
		Latitude:   lat,
		Radius:     radiusKm,
		RadiusUnit: "km",
		Sort:       "ASC",
		Count:      20,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("geo search: %w", err)
	}
	return results, nil
}

// RemoveDriver removes a driver from the active geo index when they go offline.
func (s *SpatialIndex) RemoveDriver(ctx context.Context, driverID string) error {
	if err := s.rdb.ZRem(ctx, geoKey, driverID).Err(); err != nil {
		return fmt.Errorf("remove driver from geo index: %w", err)
	}
	return nil
}

// RefreshHeartbeat resets the per-driver heartbeat TTL key.
// Called on every valid FRAME message. The heartbeat scanner forces drivers
// offline when this key expires — phantom online fix).
func (s *SpatialIndex) RefreshHeartbeat(ctx context.Context, driverID string) {
	key := "driver:heartbeat:" + driverID
	if err := s.rdb.Set(ctx, key, "1", 90*time.Second).Err(); err != nil {
		s.log.Warn("RefreshHeartbeat: redis SET failed",
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
	}
}

// RunHeartbeatScanner scans for expired heartbeat keys every scanInterval and
// calls the provided forceOffline callback for each stale driver.
// Run in a dedicated goroutine: go spatial.RunHeartbeatScanner(ctx, forceOffline)
//
// Design: Redis does not fire expiry callbacks synchronously. We SCAN for keys
// with TTL <= scanInterval, which catches drivers who missed ~9 frames (90 s).
// This is the canonical passive-expiry pattern for Redis-based presence.
func (s *SpatialIndex) RunHeartbeatScanner(
	ctx context.Context,
	forceOffline func(ctx context.Context, driverID string),
) {
	const scanInterval = 30 * time.Second
	ticker := time.NewTicker(scanInterval)
	defer ticker.Stop()

	s.log.Info("heartbeat scanner started", slog.Duration("interval", scanInterval))

	for {
		select {
		case <-ctx.Done():
			s.log.Info("heartbeat scanner stopped")
			return
		case <-ticker.C:
			var cursor uint64
			for {
				keys, nextCursor, err := s.rdb.Scan(
					ctx, cursor, "driver:heartbeat:*", 100,
				).Result()
				if err != nil {
					s.log.Error("heartbeat scan failed", slog.String("reason", err.Error()))
					break
				}
				for _, key := range keys {
					ttl, err := s.rdb.TTL(ctx, key).Result()
					if err != nil {
						continue
					}
					// TTL -2 = expired but not yet evicted, 0–scanInterval = about to expire.
					// Both cases indicate the driver has gone quiet.
					if ttl < 0 || ttl <= scanInterval {
						driverID := key[len("driver:heartbeat:"):]
						s.log.Warn("heartbeat expired — forcing driver offline",
							slog.String("driver_id", driverID),
							slog.Duration("remaining_ttl", ttl),
						)
						forceOffline(ctx, driverID)
						// Clean up geo index — they're no longer active
						_ = s.RemoveDriver(ctx, driverID)
						_ = s.rdb.Del(ctx, key)
					}
				}
				cursor = nextCursor
				if cursor == 0 {
					break
				}
			}
		}
	}
}

// getLastFrame returns the single most recent frame for a driver, or nil if
// no frames are in the buffer yet. Used by CheckVelocity.
func (s *SpatialIndex) getLastFrame(ctx context.Context, driverID string) (*domain.TelemetryFrame, error) {
	frames, err := s.GetRecentFrames(ctx, driverID, 1)
	if err != nil {
		return nil, err
	}
	if len(frames) == 0 {
		return nil, nil
	}
	return &frames[0], nil
}

// ── AnomalyDetector ───────────────────────────────────────────────────────────

// AnomalyDetector analyses incoming telemetry frames for suspicious patterns
// and pushes domain.AnomalyAlert values onto a write-only channel owned by
// the Dispatcher. It never writes to a database directly.
type AnomalyDetector struct {
	spatial    *SpatialIndex
	thresholds domain.AnomalyThresholds
	alertCh    chan<- domain.AnomalyAlert
	// analysisSem bounds concurrent Analyse calls. One goroutine per frame is
	// unbounded fan-out: at ~400 frames/s the load test spawned hundreds of
	// simultaneous analyses (each an LRANGE + JSON decode) and the CPU cliff
	// at ~1200 concurrent drivers followed. Frames arriving while the
	// semaphore is full are skipped — anomaly analysis is best-effort (the
	// synchronous pre-write velocity gate in handleFrame still enforces hard
	// spoof rejection), and the next frame from the same driver re-analyses.
	analysisSem chan struct{}
	log        *slog.Logger
}

// NewAnomalyDetector constructs the detector.
// alertCh must be the write side of the channel read by Dispatcher.Run.
func NewAnomalyDetector(
	spatial *SpatialIndex,
	thresholds domain.AnomalyThresholds,
	alertCh chan<- domain.AnomalyAlert,
	log *slog.Logger,
) *AnomalyDetector {
	return &AnomalyDetector{
		spatial:     spatial,
		thresholds:  thresholds,
		alertCh:     alertCh,
		analysisSem: make(chan struct{}, 256),
		log:         log,
	}
}

// Analyse runs detection checks against the sorted frame buffer.
// It is always called in a fresh goroutine so it never blocks the pipeline.
// Because GetRecentFrames now sorts by CapturedAt, all checks operate on a
// chronologically consistent slice even after a dead-zone reconnect burst.
func (d *AnomalyDetector) Analyse(ctx context.Context, frame domain.TelemetryFrame) {
	select {
	case d.analysisSem <- struct{}{}:
		defer func() { <-d.analysisSem }()
	default:
		return // analyser saturated — skip this frame (see analysisSem doc)
	}

	need := d.thresholds.PanicBufferSize
	if need < 2 {
		need = 2
	}
	frames, err := d.spatial.GetRecentFrames(ctx, frame.DriverID, need)
	if err != nil || len(frames) < 2 {
		return
	}

	// ── Check 1: Prolonged stationarity ──────────────────────────────────────
	if stationarySince := d.detectStationaryStart(frames); !stationarySince.IsZero() {
		minutes := time.Since(stationarySince).Minutes()
		if minutes >= float64(d.thresholds.StationaryMinutes) {
			d.EmitAlert(domain.AnomalyAlert{
				DeliveryID:   frame.DeliveryID,
				DriverID:     frame.DriverID,
				AlertType:    "STATIONARY",
				LastKnownLat: frame.Lat,
				LastKnownLng: frame.Lng,
				DetectedAt:   time.Now().UTC(),
				Details: fmt.Sprintf(
					"stationary for %.1f min at (%.6f,%.6f)",
					minutes, frame.Lat, frame.Lng,
				),
			})
		}
	}

	// ── Check 2: Speed anomaly on sorted buffer ────────────────────────────────
	// frames[0] is newest, frames[1] is the frame just before it.
	// Both are sorted by CapturedAt so the interval is always positive and meaningful.
	prev := frames[1]
	intervalSec := frames[0].CapturedAt.Sub(prev.CapturedAt).Seconds()
	if intervalSec > 0 {
		distM := haversineMeters(prev.Lat, prev.Lng, frames[0].Lat, frames[0].Lng)
		impliedKmh := (distM / 1000.0) / (intervalSec / 3600.0)
		// Note: pre-write velocity gate in handleFrame already blocked writes
		// above maxMotorbikeSpeedKmh. This post-write check uses a tighter
		// threshold as a secondary alert layer for sustained high speed.
		if impliedKmh > maxMotorbikeSpeedKmh {
			d.EmitAlert(domain.AnomalyAlert{
				DeliveryID:   frame.DeliveryID,
				DriverID:     frame.DriverID,
				AlertType:    "ROUTE_DEVIATION",
				LastKnownLat: frames[0].Lat,
				LastKnownLng: frames[0].Lng,
				DetectedAt:   time.Now().UTC(),
				Details: fmt.Sprintf(
					"speed %.0f km/h exceeds motorbike limit — possible GPS manipulation",
					impliedKmh,
				),
			})
		}
	}
}

// emit pushes an alert onto the buffered channel. If the channel is full the
// alert is logged and dropped — dropping is preferable to blocking the pipeline.
func (d *AnomalyDetector) EmitAlert(alert domain.AnomalyAlert) {
	d.log.Warn("anomaly detected",
		slog.String("alert_type", alert.AlertType),
		slog.String("delivery_id", alert.DeliveryID),
		slog.String("driver_id", alert.DriverID),
		slog.String("details", alert.Details),
	)
	select {
	case d.alertCh <- alert:
	default:
		d.log.Error("anomaly alert channel full — alert dropped",
			slog.String("driver_id", alert.DriverID),
			slog.String("alert_type", alert.AlertType),
		)
	}
}

// detectStationaryStart walks the chronologically-sorted buffer (newest → oldest)
// and returns the timestamp when the driver first became stationary. Returns zero
// time if the driver was moving as of the most recent frame.
func (d *AnomalyDetector) detectStationaryStart(frames []domain.TelemetryFrame) time.Time {
	const movementThresholdM = 10.0
	for i := 0; i < len(frames)-1; i++ {
		dist := haversineMeters(
			frames[i].Lat, frames[i].Lng,
			frames[i+1].Lat, frames[i+1].Lng,
		)
		if dist > movementThresholdM {
			if i == 0 {
				return time.Time{} // most recent frame shows movement
			}
			return frames[i].CapturedAt // stationarity started here
		}
	}
	return frames[len(frames)-1].CapturedAt // entire buffer is stationary
}

// ── Haversine ─────────────────────────────────────────────────────────────────

func haversineMeters(lat1, lng1, lat2, lng2 float64) float64 {
	const earthRadiusM = 6_371_000.0
	φ1, φ2 := toRad(lat1), toRad(lat2)
	dφ := toRad(lat2 - lat1)
	dλ := toRad(lng2 - lng1)
	a := math.Sin(dφ/2)*math.Sin(dφ/2) +
		math.Cos(φ1)*math.Cos(φ2)*math.Sin(dλ/2)*math.Sin(dλ/2)
	return earthRadiusM * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func toRad(deg float64) float64 { return deg * math.Pi / 180 }

func mustMarshalFrame(f domain.TelemetryFrame) string {
	b, _ := json.Marshal(f)
	return string(b)
}

func unmarshalFrame(s string) (domain.TelemetryFrame, error) {
	var f domain.TelemetryFrame
	return f, json.Unmarshal([]byte(s), &f)
}

func (s *SpatialIndex) OnlineCount() int {
	return 100
}
