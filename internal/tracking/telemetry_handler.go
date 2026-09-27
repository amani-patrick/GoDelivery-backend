package tracking

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/umurinzi/backend/internal/domain"
	"github.com/umurinzi/backend/internal/middleware"
)

const (
	// wsReadLimit caps inbound message size to protect against memory exhaustion.
	wsReadLimit = 4 * 1024 // 4 KB is generous for a GPS frame

	// wsPongWait is how long to wait for a pong before treating the connection dead.
	wsPongWait = 60 * time.Second

	// wsPingInterval controls how often the server sends a ping keepalive.
	wsPingInterval = 50 * time.Second

	// wsWriteWait is the deadline for any single write operation.
	wsWriteWait = 10 * time.Second
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// Origin check is delegated to the JWT middleware that sits in front of
	// this handler. By the time we get here the bearer token is already valid.
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Every field has an explicit type — no interface{} / any.
type inboundMsg struct {
	Type       string  `json:"type"`       
	DeliveryID string  `json:"delivery_id"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	SpeedKmh   float64 `json:"speed_kmh"`
	Bearing    float64 `json:"bearing"`  
	AccuracyM  float32 `json:"accuracy_m"`
	Battery    float32 `json:"battery_pct"`
	TimestampMs int64  `json:"ts_ms"` 
}

// outboundMsg is sent back to the driver device.
type outboundMsg struct {
	Type    string `json:"type"`    // "ACK" | "ALERT" | "PONG"
	Payload string `json:"payload"`
}

// ── TelemetryHandler ─────────────────────────────────────────────────────────

// DriverPresenceNotifier allows the telemetry handler to update driver
// online status in persistent storage without importing usecase or repository
// packages — maintaining strict DDD module boundaries.
type DriverPresenceNotifier interface {
	SetOnlineStatus(ctx context.Context, userID string, online bool) error
}

// heartbeatTTLKey is the Redis key prefix for per-driver heartbeat keys.
// Full key: "driver:heartbeat:<driverID>"
// TTL is refreshed on every valid FRAME message. When it expires the
// phantom online scanner forces the driver offline in Postgres.
const (
	heartbeatKeyPrefix = "driver:heartbeat:"
	heartbeatTTL       = 90 * time.Second // ~9 missed 10-second frames
)

// TelemetryHandler upgrades HTTP connections to WebSocket and processes the
// high-frequency GPS telemetry stream. One goroutine pair (read pump + write pump)
// is created per active driver connection.
type TelemetryHandler struct {
	spatial   *SpatialIndex
	detector  *AnomalyDetector
	ledger    domain.LedgerRepository
	presence  DriverPresenceNotifier // nil when not wired (dev mode)
	log       *slog.Logger

	// connMu guards activeConns for concurrent read/write.
	connMu      sync.RWMutex
	activeConns map[string]*websocket.Conn // driverID → connection
}

// NewTelemetryHandler constructs the handler with all dependencies injected.
// presence may be nil — when nil, online status is not updated on disconnect.
func NewTelemetryHandler(
	spatial *SpatialIndex,
	detector *AnomalyDetector,
	ledger domain.LedgerRepository,
	presence DriverPresenceNotifier,
	log *slog.Logger,
) *TelemetryHandler {
	return &TelemetryHandler{
		spatial:     spatial,
		detector:    detector,
		ledger:      ledger,
		presence:    presence,
		log:         log,
		activeConns: make(map[string]*websocket.Conn),
	}
}

// ServeHTTP upgrades the HTTP connection to WebSocket and starts the per-driver
// read/write pump pair. The JWT middleware must have already injected the user ID
// under middleware.CtxUserID before this handler is reached.
func (h *TelemetryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Read driverID using the same exported key type that JWTMiddleware injects.
	driverID, ok := r.Context().Value(middleware.CtxUserID).(string)
	if !ok || driverID == "" {
		http.Error(w, `{"error":"unauthenticated"}`, http.StatusUnauthorized)
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("websocket upgrade failed",
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
		return
	}

	h.registerConn(driverID, conn)
	defer h.deregisterConn(driverID, conn)

	h.log.Info("driver telemetry connection established",
		slog.String("driver_id", driverID),
		slog.String("remote_addr", r.RemoteAddr),
	)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go h.writePump(ctx, conn, driverID)
	h.readPump(ctx, conn, driverID)
}

// ── Read pump ─────────────────────────────────────────────────────────────────

func (h *TelemetryHandler) readPump(ctx context.Context, conn *websocket.Conn, driverID string) {
	conn.SetReadLimit(wsReadLimit)
	_ = conn.SetReadDeadline(time.Now().Add(wsPongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})

	for {
		_, raw, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err,
				websocket.CloseGoingAway,
				websocket.CloseNormalClosure,
				websocket.CloseNoStatusReceived,
			) {
				h.log.Warn("unexpected websocket close",
					slog.String("driver_id", driverID),
					slog.String("reason", err.Error()),
				)
			}
			return
		}

		var msg inboundMsg
		if err := json.Unmarshal(raw, &msg); err != nil {
			h.log.Warn("malformed telemetry message — discarded",
				slog.String("driver_id", driverID),
				slog.String("reason", err.Error()),
			)
			continue
		}

		switch msg.Type {
		case "FRAME":
			h.handleFrame(ctx, conn, driverID, msg)
		case "PANIC":
			h.handlePanic(ctx, conn, driverID, msg)
		case "PING":
			h.writeMsg(conn, outboundMsg{Type: "PONG"})
		default:
			h.log.Warn("unknown message type",
				slog.String("driver_id", driverID),
				slog.String("type", msg.Type),
			)
		}
	}
}

// ── Frame handler ─────────────────────────────────────────────────────────────

func (h *TelemetryHandler) handleFrame(
	ctx context.Context, conn *websocket.Conn, driverID string, msg inboundMsg,
) {
	capturedAt := time.UnixMilli(msg.TimestampMs).UTC()

	// ── Gate 1: timestamp plausibility ───────────────────────────────────────
	// Reject frames with a clock drift greater than 5 minutes in either
	// direction. This blocks replayed frames and severely mis-clocked devices.
	if drift := time.Since(capturedAt); drift < -5*time.Minute || drift > 5*time.Minute {
		h.log.Warn("frame rejected: implausible timestamp",
			slog.String("driver_id", driverID),
			slog.String("delivery_id", msg.DeliveryID),
			slog.Duration("drift", drift),
		)
		return
	}

	frame := domain.TelemetryFrame{
		DeliveryID: msg.DeliveryID,
		DriverID:   driverID,
		Lat:        msg.Lat,
		Lng:        msg.Lng,
		SpeedKmh:   msg.SpeedKmh,
		Bearing:    msg.Bearing,
		Accuracy:   msg.AccuracyM,
		CapturedAt: capturedAt,
	}

	// ── Gate 2: pre-write velocity check ─────────────────────────────────────
	// Check velocity BEFORE writing to Redis. A spoofed GPS teleport must be
	// rejected here so it never poisons the geo index or the frame buffer.
	// If CheckVelocity itself fails (Redis error), we fail open — the frame is
	// written anyway and the anomaly detector will catch it post-write.
	velocity, err := h.spatial.CheckVelocity(ctx, frame)
	if err != nil {
		h.log.Error("CheckVelocity failed — proceeding without pre-write gate",
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
	} else if !velocity.Plausible {
		h.log.Warn("frame rejected: impossible velocity — GPS spoof suspected",
			slog.String("driver_id", driverID),
			slog.String("delivery_id", msg.DeliveryID),
			slog.Float64("implied_kmh", velocity.ImpliedKmh),
		)
		// Emit an alert so dispatchers are notified even though the frame is dropped.
		h.detector.emit(domain.AnomalyAlert{
			DeliveryID:   msg.DeliveryID,
			DriverID:     driverID,
			AlertType:    "ROUTE_DEVIATION",
			LastKnownLat: msg.Lat,
			LastKnownLng: msg.Lng,
			DetectedAt:   time.Now().UTC(),
			Details: fmt.Sprintf(
				"frame rejected pre-write: implied speed %.0f km/h exceeds %.0f km/h motorbike limit — GPS spoof suspected",
				velocity.ImpliedKmh, maxMotorbikeSpeedKmh,
			),
		})
		return // do NOT write to Redis
	}

	// ── Write to Redis ────────────────────────────────────────────────────────
	if err := h.spatial.UpdateDriverPosition(ctx, frame); err != nil {
		h.log.Error("UpdateDriverPosition failed",
			slog.String("driver_id", driverID),
			slog.String("delivery_id", msg.DeliveryID),
			slog.String("reason", err.Error()),
		)
		return
	}

	// Task 3: Refresh heartbeat TTL — proves driver is still alive and connected.
	// The heartbeat scanner in SpatialIndex.RunHeartbeatScanner forces drivers
	// offline in Postgres when this key expires after 90 seconds of silence.
	h.spatial.RefreshHeartbeat(ctx, driverID)

	// Anomaly detection runs in a separate goroutine — the telemetry pipeline
	// must never block waiting for detection logic.
	go h.detector.Analyse(ctx, frame)

	h.writeMsg(conn, outboundMsg{Type: "ACK", Payload: msg.DeliveryID})
}

// ── Panic handler ─────────────────────────────────────────────────────────────

func (h *TelemetryHandler) handlePanic(
	ctx context.Context, conn *websocket.Conn, driverID string, msg inboundMsg,
) {
	h.log.Error("PANIC SIGNAL received",
		slog.String("driver_id", driverID),
		slog.String("delivery_id", msg.DeliveryID),
		slog.Float64("lat", msg.Lat),
		slog.Float64("lng", msg.Lng),
		slog.Float64("battery", float64(msg.Battery)),
	)

	frames, err := h.spatial.GetRecentFrames(ctx, driverID, domain.DefaultThresholds.PanicBufferSize)
	if err != nil {
		h.log.Error("panic: failed to retrieve frame buffer",
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
	}

	h.detector.emit(domain.AnomalyAlert{
		DeliveryID:   msg.DeliveryID,
		DriverID:     driverID,
		AlertType:    "PANIC",
		LastKnownLat: msg.Lat,
		LastKnownLng: msg.Lng,
		DetectedAt:   time.Now().UTC(),
		Details:      buildPanicDetails(frames, msg.Battery),
	})

	h.writeMsg(conn, outboundMsg{Type: "ACK", Payload: "panic_received"})
}

// ── Write pump ────────────────────────────────────────────────────────────────

func (h *TelemetryHandler) writePump(ctx context.Context, conn *websocket.Conn, driverID string) {
	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				h.log.Warn("ping failed — write pump exiting",
					slog.String("driver_id", driverID),
					slog.String("reason", err.Error()),
				)
				return
			}
		}
	}
}

// ── BroadcastAlert ────────────────────────────────────────────────────────────

// BroadcastAlert pushes a structured alert back to the driver's active connection.
// Called by the Dispatcher to trigger a safety check-in prompt on the driver app.
// If the driver is offline the call is a no-op.
func (h *TelemetryHandler) BroadcastAlert(driverID string, alert domain.AnomalyAlert) {
	h.connMu.RLock()
	conn, ok := h.activeConns[driverID]
	h.connMu.RUnlock()
	if !ok {
		return
	}

	payload, _ := json.Marshal(alert)
	h.writeMsg(conn, outboundMsg{Type: "ALERT", Payload: string(payload)})
}

// ── Connection registry ───────────────────────────────────────────────────────

func (h *TelemetryHandler) registerConn(driverID string, conn *websocket.Conn) {
	h.connMu.Lock()
	defer h.connMu.Unlock()
	if old, exists := h.activeConns[driverID]; exists {
		// Close the stale connection before replacing it.
		_ = old.Close()
	}
	h.activeConns[driverID] = conn
}

func (h *TelemetryHandler) deregisterConn(driverID string, conn *websocket.Conn) {
	h.connMu.Lock()
	defer h.connMu.Unlock()
	if current, ok := h.activeConns[driverID]; ok && current == conn {
		delete(h.activeConns, driverID)
	}
	_ = conn.Close()

	// Task 3: Phantom online fix — force driver offline in Postgres and remove
	// from Redis geo index the moment their WebSocket connection closes.
	// We use a background context because the request context is already cancelled.
	bgCtx := context.Background()
	if err := h.spatial.RemoveDriver(bgCtx, driverID); err != nil {
		h.log.Warn("deregisterConn: failed to remove driver from geo index",
			slog.String("driver_id", driverID),
			slog.String("reason", err.Error()),
		)
	}
	if h.presence != nil {
		if err := h.presence.SetOnlineStatus(bgCtx, driverID, false); err != nil {
			h.log.Warn("deregisterConn: failed to set driver offline in Postgres",
				slog.String("driver_id", driverID),
				slog.String("reason", err.Error()),
			)
		}
	}

	h.log.Info("driver telemetry connection closed — forced offline",
		slog.String("driver_id", driverID),
	)
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func (h *TelemetryHandler) writeMsg(conn *websocket.Conn, msg outboundMsg) {
	_ = conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
	b, err := json.Marshal(msg)
	if err != nil {
		return
	}
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		if !errors.Is(err, websocket.ErrCloseSent) {
			h.log.Warn("websocket write failed", slog.String("reason", err.Error()))
		}
	}
}

func buildPanicDetails(frames []domain.TelemetryFrame, battery float32) string {
	if len(frames) == 0 {
		return "panic_signal; no frame buffer available"
	}
	f := frames[0]
	return fmt.Sprintf(
		"panic_signal; last_pos=(%.6f,%.6f); frames=%d; battery=%.0f%%",
		f.Lat, f.Lng, len(frames), battery,
	)
}
