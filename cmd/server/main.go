// Package main is the Umurinzi backend entry point.
// Wiring order:
//  1. Config + Logger
//  2. PostgreSQL pool
//  3. Redis client
//  4. Infrastructure (repositories, ledger)
//  5. Tracking infrastructure (spatial index, anomaly detector)
//  6. Matching engine (OSRM + two-tier pipeline)
//  7. Application usecases
//  8. HTTP handler + router
//  9. Background goroutines
// 10. Serve + graceful drain
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/umurinzi/backend/internal/config"
	"github.com/umurinzi/backend/internal/delivery"
	deliveryrepo "github.com/umurinzi/backend/internal/delivery/repository"
	deliveryuc "github.com/umurinzi/backend/internal/delivery/usecase"
	"github.com/umurinzi/backend/internal/domain"
	"github.com/umurinzi/backend/internal/ledger"
	"github.com/umurinzi/backend/internal/matching"
	"github.com/umurinzi/backend/internal/middleware"
	"github.com/umurinzi/backend/internal/pricing"
	"github.com/umurinzi/backend/internal/surge"
	"github.com/umurinzi/backend/internal/tracking"
)

// decayInterval controls how often the danger zone threat level is reduced.
// Every tick subtracts decayAmount from each zone. Zones that drop to 0 are
// automatically deleted, resetting that area to safe.
const (
	safetyDecayInterval = 1 * time.Hour
	safetyDecayAmount   = 0.05 // threat drops from 1.0→0 in ~20 hours of no incidents
)

func main() {
	// ── 1. Logger ─────────────────────────────────────────────────────────────
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(log)

	// ── 2. Config ─────────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Error("configuration error", slog.String("reason", err.Error()))
		os.Exit(1)
	}
	log.Info("configuration loaded", slog.String("port", cfg.Server.Port))

	// ── 3. PostgreSQL ─────────────────────────────────────────────────────────
	poolCfg, err := pgxpool.ParseConfig(cfg.Postgres.DSN)
	if err != nil {
		log.Error("invalid postgres DSN", slog.String("reason", err.Error()))
		os.Exit(1)
	}
	poolCfg.MaxConns        = cfg.Postgres.MaxConns
	poolCfg.MinConns        = cfg.Postgres.MinConns
	poolCfg.MaxConnLifetime = cfg.Postgres.MaxConnLifetime

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		log.Error("postgres pool creation failed", slog.String("reason", err.Error()))
		os.Exit(1)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Error("postgres ping failed", slog.String("reason", err.Error()))
		os.Exit(1)
	}
	log.Info("postgres connected")

	// ── 4. Redis ──────────────────────────────────────────────────────────────
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})
	defer func() { _ = rdb.Close() }()
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Error("redis ping failed", slog.String("reason", err.Error()))
		os.Exit(1)
	}
	log.Info("redis connected")

	// ── 5. Infrastructure repositories ───────────────────────────────────────
	auditLedger  := ledger.NewPostgresLedger(pool, log)
	deliveryRepo := deliveryrepo.NewDeliveryPostgresRepo(pool, log)
	userRepo     := deliveryrepo.NewUserPostgresRepo(pool, log)
	driverRepo   := deliveryrepo.NewDriverProfilePostgresRepo(pool, log)
	businessRepo := deliveryrepo.NewBusinessProfilePostgresRepo(pool, log)
	customerRepo := deliveryrepo.NewCustomerProfilePostgresRepo(pool, log)
	outboxRepo   := deliveryrepo.NewOutboxPostgresRepo(pool, log)
	trustRepo    := deliveryrepo.NewTrustPostgresRepo(pool, log)

	// ── 6. Tracking (spatial index + anomaly detector) ────────────────────────
	// Built before the matching engine — both share the spatial index read-only.
	alertCh     := make(chan domain.AnomalyAlert, tracking.AlertChannelSize)
	spatialIndex := tracking.NewSpatialIndex(rdb, log)

	// Safety repository (PostGIS-backed danger zones + safe hubs).
	safetyRepo := tracking.NewSafetyPostgresRepo(pool, log)

	anomalyThresholds := domain.AnomalyThresholds{
		StationaryMinutes:    cfg.Anomaly.StationaryMinutes,
		RouteDeviationMeters: cfg.Anomaly.RouteDeviationMeters,
		PanicBufferSize:      cfg.Anomaly.PanicBufferSize,
	}
	anomalyDetector := tracking.NewAnomalyDetector(spatialIndex, anomalyThresholds, alertCh, log)

	// driverRepo satisfies tracking.DriverPresenceNotifier (has SetOnlineStatus).
	// This lets TelemetryHandler force drivers offline on WebSocket close .
	telemetryHandler := tracking.NewTelemetryHandler(
		spatialIndex, anomalyDetector, auditLedger, driverRepo, log,
	)
	dispatcher := tracking.NewDispatcher(alertCh, auditLedger, telemetryHandler, log)

	// ── 7. Matching engine ────────────────────────────────────────────────────
	matchCfg := domain.MatchConfig{
		WeightTime:     cfg.Matching.WeightTime,
		WeightDistance: cfg.Matching.WeightDistance,
		WeightFuel:     cfg.Matching.WeightFuel,
		Tier1RadiusKm: map[domain.VehicleType]float64{
			domain.VehicleMotorcycle: cfg.Matching.Tier1RadiusMotorcycleKm,
			domain.VehicleCar:        cfg.Matching.Tier1RadiusCarKm,
			domain.VehicleVan:        cfg.Matching.Tier1RadiusVanKm,
			domain.VehicleTruck:      cfg.Matching.Tier1RadiusTruckKm,
		},
		MaxDeadheadKm: map[domain.VehicleType]float64{
			domain.VehicleMotorcycle: cfg.Matching.MaxDeadheadMotorcycleKm,
			domain.VehicleCar:        cfg.Matching.MaxDeadheadCarKm,
			domain.VehicleVan:        cfg.Matching.MaxDeadheadVanKm,
			domain.VehicleTruck:      cfg.Matching.MaxDeadheadTruckKm,
		},
		FuelLitresPerKm: map[domain.VehicleType]float64{
			domain.VehicleMotorcycle: cfg.Matching.FuelLitresPerKmMotorcycle,
			domain.VehicleCar:        cfg.Matching.FuelLitresPerKmCar,
			domain.VehicleVan:        cfg.Matching.FuelLitresPerKmVan,
			domain.VehicleTruck:      cfg.Matching.FuelLitresPerKmTruck,
		},
		FuelPriceRWFPerLitre:       cfg.Matching.FuelPriceRWFPerLitre,
		HeadingPenaltyThresholdDeg: cfg.Matching.HeadingPenaltyThresholdDeg,
		HeadingPenaltyMinSpeedKmh:  cfg.Matching.HeadingPenaltyMinSpeedKmh,
		HeadingPenaltyMultiplier:   cfg.Matching.HeadingPenaltyMultiplier,
		BatchWindowMs:              cfg.Matching.BatchWindowMs,
		RedispatchTimeoutMs:        cfg.Matching.RedispatchTimeoutMs,
		MaxStackDelayMinutes:       cfg.Matching.MaxStackDelayMinutes,
		MaxStackDirectionDeg:       cfg.Matching.MaxStackDirectionDeg,
		PremiumScoreBoost:          cfg.Matching.PremiumScoreBoost,
		OSRMTimeoutMs:              cfg.Matching.OSRMTimeoutMs,
	}

	osrmClient  := matching.NewOSRMClient(cfg.Matching.OSRMBaseURL, cfg.Matching.OSRMTimeoutMs, log)
	matchEngine := matching.NewEngine(osrmClient, spatialIndex, driverRepo, userRepo, rdb, matchCfg, log)
	matchResultCh := make(chan []domain.MatchResult, 256)

	// Surge Pricing Engine (feature #7)
	surgeEngine := surge.NewEngine(pool, log)

	// ── 8. Application usecases ───────────────────────────────────────────────
	authCfg := deliveryuc.AuthConfig{
		JWTSecret:   cfg.Auth.JWTSecret,
		ExpiryHours: cfg.Auth.JWTExpiryHours,
		BcryptCost:  cfg.Auth.BcryptCost,
	}
	authUC := deliveryuc.NewAuthUsecase(userRepo, authCfg, log)

	// driverUC owns the engine — exposes EnqueueForDispatch and DispatchForDelivery.
	driverUC := deliveryuc.NewDriverUsecase(
		driverRepo, businessRepo, customerRepo, userRepo, auditLedger, matchEngine, log,
	)

	trustUC := deliveryuc.NewTrustUsecase(
		trustRepo, driverRepo, businessRepo, customerRepo, log,
	)
	_ = trustUC

	deliveryUC := deliveryuc.NewDeliveryUsecase(
		deliveryRepo,
		driverRepo,
		outboxRepo,
		trustRepo,
		userRepo,
		safetyRepo,
		pricing.NewEngine(surgeEngine),
		auditLedger,
		rdb,
		pool,
		cfg.Auth.BcryptCost,
		driverUC.EnqueueForDispatch,         // dispatch enqueuer 
		matchEngine.ClearPending,             // clear re-dispatch key on accept 
		matchEngine.GetIntendedDriver,        // concurrent acceptance guard 
		osrmClient,                           // osrm client for pin snapping
		log,
	)

	// ── 9. HTTP handler + router ──────────────────────────────────────────────
	deliveryHandler := delivery.NewHandler(deliveryUC, authUC, driverUC, log)

	r := chi.NewRouter()
	r.Use(chimw.RealIP)
	r.Use(chimw.RequestID)
	r.Use(middleware.Recovery(log))
	r.Use(middleware.RequestLogger(log))
	r.Use(chimw.Compress(5))

	r.Post("/auth/register", injectOperation(deliveryHandler, "Register"))
	r.Post("/auth/login",    injectOperation(deliveryHandler, "Login"))
	r.Get("/health",         healthHandler(pool, rdb, log))

	r.Group(func(r chi.Router) {
		r.Use(middleware.JWTMiddleware(cfg.Auth.JWTSecret))
		r.Post("/graphql", deliveryHandler.ServeHTTP)
	})
	r.Group(func(r chi.Router) {
		r.Use(middleware.JWTMiddleware(cfg.Auth.JWTSecret))
		r.Get("/ws/telemetry", telemetryHandler.ServeHTTP)
	})

	// ── 10. Background goroutines ─────────────────────────────────────────────
	bgCtx, cancelBg := context.WithCancel(ctx)
	defer cancelBg()

	// Anomaly dispatcher — consumes AnomalyAlert from tracking module.
	go dispatcher.Run(bgCtx)

	// Matching batch loop — drains Redis queue every BatchWindowMs.
	go matchEngine.RunBatchLoop(bgCtx, matchResultCh)

	// Re-dispatch timeout scanner — scans for expired pending keys .
	// The redispatch alert channel is shared with the regular alertCh so
	// BroadcastAlert reaches the driver's WebSocket connection.
	go matchEngine.RunRedispatchLoop(bgCtx, alertCh)

	// Match result consumer — pushes DISPATCH WebSocket notifications to winners.
	go runMatchResultConsumer(bgCtx, matchResultCh, telemetryHandler, log)

	// Heartbeat scanner — forces phantom-online drivers offline .
	go spatialIndex.RunHeartbeatScanner(bgCtx, func(hbCtx context.Context, driverID string) {
		if err := driverRepo.SetOnlineStatus(hbCtx, driverID, false); err != nil {
			log.Warn("heartbeat scanner: SetOnlineStatus failed",
				slog.String("driver_id", driverID),
				slog.String("reason", err.Error()),
			)
		}
	})

	// Safety decay loop — decays danger zone threat levels hourly.
	// Zones whose threat level drops to 0 are automatically deleted (area becomes safe).
	go runSafetyDecayLoop(bgCtx, safetyRepo, log)

	// Surge Pricing demand sampler loop (feature #7).
	go surgeEngine.RunLoop(bgCtx, func(ctx context.Context) int {
		// To avoid circular dependencies, we run a direct count query.
		var count int
		_ = pool.QueryRow(ctx, "SELECT COUNT(*) FROM deliveries WHERE current_state = 'CREATED' OR current_state = 'DISPATCHED'").Scan(&count)
		return count
	}, func(ctx context.Context) int {
		return spatialIndex.OnlineCount()
	})

	// ── 11. HTTP server ───────────────────────────────────────────────────────
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		log.Info("server listening", slog.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	// ── 12. Graceful shutdown ─────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-quit:
		log.Info("shutdown signal received", slog.String("signal", sig.String()))
	case err := <-serverErr:
		log.Error("server fatal error", slog.String("reason", err.Error()))
	}

	cancelBg()

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", slog.String("reason", err.Error()))
		os.Exit(1)
	}
	log.Info("server stopped cleanly")
}

func runSafetyDecayLoop(ctx context.Context, repo domain.SafetyRepository, log *slog.Logger) {
	ticker := time.NewTicker(safetyDecayInterval)
	defer ticker.Stop()
	log.Info("safety decay loop started",
		slog.Duration("interval", safetyDecayInterval),
		slog.Float64("decay_per_tick", safetyDecayAmount),
	)
	for {
		select {
		case <-ctx.Done():
			log.Info("safety decay loop stopped")
			return
		case <-ticker.C:
			if err := repo.DecayThreatLevels(ctx, safetyDecayAmount); err != nil {
				log.Error("safety decay: DecayThreatLevels failed",
					slog.String("reason", err.Error()),
				)
			} else {
				log.Info("safety decay: threat levels updated")
			}
		}
	}
}

// runMatchResultConsumer drains the matchResultCh and broadcasts DISPATCH
// alerts to matched drivers over their active WebSocket connections.
func runMatchResultConsumer(
	ctx context.Context,
	ch <-chan []domain.MatchResult,
	handler *tracking.TelemetryHandler,
	log *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-ch:
			if !ok {
				return
			}
			for _, result := range batch {
				if result.Winner == nil {
					continue
				}
				handler.BroadcastAlert(result.Winner.UserID, domain.AnomalyAlert{
					DeliveryID:  result.DeliveryID,
					DriverID:    result.Winner.UserID,
					AlertType:   "DISPATCH",
					DetectedAt:  result.ComputedAt,
					Details:     formatDispatchDetails(result),
				})
				log.Info("dispatch notification sent",
					slog.String("delivery_id", result.DeliveryID),
					slog.String("driver_id", result.Winner.UserID),
					slog.Float64("eta_min", result.Winner.ETAMinutes),
					slog.Float64("score", result.Winner.Score),
					slog.Bool("osrm_used", result.OSRMUsed),
				)
			}
		}
	}
}

func formatDispatchDetails(r domain.MatchResult) string {
	b, _ := json.Marshal(map[string]interface{}{
		"delivery_id":       r.DeliveryID,
		"eta_minutes":       r.Winner.ETAMinutes,
		"road_dist_km":      r.Winner.RoadDistKm,
		"fuel_cost_rwf":     r.Winner.FuelCostRWF,
		"score":             r.Winner.Score,
		"osrm_used":         r.OSRMUsed,
		"heading_penalised": r.Winner.HeadingPenalised,
	})
	return string(b)
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

func injectOperation(h *delivery.Handler, opName string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Header.Set("X-Gql-Operation", opName)
		h.ServeHTTP(w, r)
	}
}

func healthHandler(pool *pgxpool.Pool, rdb *redis.Client, log *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		type response struct {
			Status string            `json:"status"`
			Checks map[string]string `json:"checks"`
		}
		resp := response{
			Status: "ok",
			Checks: map[string]string{"postgres": "ok", "redis": "ok"},
		}
		code := http.StatusOK

		if err := pool.Ping(ctx); err != nil {
			resp.Checks["postgres"] = "unreachable"
			resp.Status = "degraded"
			code = http.StatusServiceUnavailable
			log.Error("health: postgres unreachable", slog.String("reason", err.Error()))
		}
		if err := rdb.Ping(ctx).Err(); err != nil {
			resp.Checks["redis"] = "unreachable"
			resp.Status = "degraded"
			code = http.StatusServiceUnavailable
			log.Error("health: redis unreachable", slog.String("reason", err.Error()))
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Error("health: encode failed", slog.String("reason", err.Error()))
		}
	}
}
