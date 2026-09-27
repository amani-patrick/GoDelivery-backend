// Package config loads all runtime configuration from environment variables.
// No config values are ever hard-coded or committed to source control.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the complete runtime configuration for the server.
type Config struct {
	Server   ServerConfig
	Postgres PostgresConfig
	Redis    RedisConfig
	Auth     AuthConfig
	Anomaly  AnomalyConfig
	Matching MatchingConfig
}

type ServerConfig struct {
	Port            string
	ShutdownTimeout time.Duration
}

type PostgresConfig struct {
	DSN             string 
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
}

type RedisConfig struct {
	Addr     string 
	Password string
	DB       int
}

type AuthConfig struct {
	JWTSecret          string
	JWTExpiryHours     int
	BcryptCost         int
}

type AnomalyConfig struct {
	StationaryMinutes    int
	RouteDeviationMeters float64
	PanicBufferSize      int
}

// MatchingConfig holds all parameters for the two-tier matching engine.
type MatchingConfig struct {
	OSRMBaseURL    string
	WeightTime     float64
	WeightDistance float64
	WeightFuel     float64 // task 8
	BatchWindowMs  int

	// Re-dispatch timeout in ms — how long to wait for winner to accept (task 1)
	RedispatchTimeoutMs int

	Tier1RadiusMotorcycleKm float64
	Tier1RadiusCarKm        float64
	Tier1RadiusVanKm        float64
	Tier1RadiusTruckKm      float64

	MaxDeadheadMotorcycleKm float64
	MaxDeadheadCarKm        float64
	MaxDeadheadVanKm        float64
	MaxDeadheadTruckKm      float64

	// Fuel litres per km per vehicle type (task 8)
	FuelLitresPerKmMotorcycle float64
	FuelLitresPerKmCar        float64
	FuelLitresPerKmVan        float64
	FuelLitresPerKmTruck      float64
	FuelPriceRWFPerLitre      float64

	HeadingPenaltyThresholdDeg float64
	HeadingPenaltyMinSpeedKmh  float64
	HeadingPenaltyMultiplier   float64

	// Stacking parameters (task 4)
	MaxStackDelayMinutes float64
	MaxStackDirectionDeg float64

	// Premium score reduction (task 7) — subtracted from winner score
	PremiumScoreBoost float64

	OSRMTimeoutMs int
}

// Load reads all required environment variables and returns a populated Config.
// Any missing required variable causes an immediate error.
func Load() (*Config, error) {
	cfg := &Config{}
	var missing []string

	// ── Server ────────────────────────────────────────────────────────────────
	cfg.Server.Port = envOr("PORT", "8080")
	cfg.Server.ShutdownTimeout = envDurationOr("SHUTDOWN_TIMEOUT_SECS", 30) * time.Second

	// ── Postgres ──────────────────────────────────────────────────────────────
	cfg.Postgres.DSN = requireEnv("DATABASE_URL", &missing)
	cfg.Postgres.MaxConns = int32(envIntOr("DB_MAX_CONNS", 25))
	cfg.Postgres.MinConns = int32(envIntOr("DB_MIN_CONNS", 5))
	cfg.Postgres.MaxConnLifetime = envDurationOr("DB_MAX_CONN_LIFETIME_MINS", 30) * time.Minute

	// ── Redis ─────────────────────────────────────────────────────────────────
	cfg.Redis.Addr = envOr("REDIS_ADDR", "localhost:6379")
	cfg.Redis.Password = os.Getenv("REDIS_PASSWORD") // optional
	cfg.Redis.DB = envIntOr("REDIS_DB", 0)

	// ── Auth ──────────────────────────────────────────────────────────────────
	cfg.Auth.JWTSecret = requireEnv("JWT_SECRET", &missing)
	cfg.Auth.JWTExpiryHours = envIntOr("JWT_EXPIRY_HOURS", 24)
	cfg.Auth.BcryptCost = envIntOr("BCRYPT_COST", 12)

	// ── Anomaly detection ─────────────────────────────────────────────────────
	cfg.Anomaly.StationaryMinutes = envIntOr("ANOMALY_STATIONARY_MINUTES", 5)
	cfg.Anomaly.RouteDeviationMeters = envFloat64Or("ANOMALY_DEVIATION_METERS", 300)
	cfg.Anomaly.PanicBufferSize = envIntOr("ANOMALY_PANIC_BUFFER_SIZE", 30)

	// ── Matching engine ───────────────────────────────────────────────────────
	cfg.Matching.OSRMBaseURL    = envOr("OSRM_BASE_URL", "http://localhost:5000")
	cfg.Matching.WeightTime     = envFloat64Or("MATCH_WEIGHT_TIME", 3.0)
	cfg.Matching.WeightDistance = envFloat64Or("MATCH_WEIGHT_DISTANCE", 1.0)
	cfg.Matching.WeightFuel     = envFloat64Or("MATCH_WEIGHT_FUEL", 0.5)
	cfg.Matching.BatchWindowMs  = envIntOr("MATCH_BATCH_WINDOW_MS", 12000)
	cfg.Matching.RedispatchTimeoutMs = envIntOr("MATCH_REDISPATCH_TIMEOUT_MS", 45000)

	cfg.Matching.Tier1RadiusMotorcycleKm = envFloat64Or("MATCH_RADIUS_MOTORCYCLE_KM", 3.0)
	cfg.Matching.Tier1RadiusCarKm        = envFloat64Or("MATCH_RADIUS_CAR_KM", 5.0)
	cfg.Matching.Tier1RadiusVanKm        = envFloat64Or("MATCH_RADIUS_VAN_KM", 7.0)
	cfg.Matching.Tier1RadiusTruckKm      = envFloat64Or("MATCH_RADIUS_TRUCK_KM", 10.0)

	cfg.Matching.MaxDeadheadMotorcycleKm = envFloat64Or("MATCH_MAX_DEADHEAD_MOTORCYCLE_KM", 3.0)
	cfg.Matching.MaxDeadheadCarKm        = envFloat64Or("MATCH_MAX_DEADHEAD_CAR_KM", 6.0)
	cfg.Matching.MaxDeadheadVanKm        = envFloat64Or("MATCH_MAX_DEADHEAD_VAN_KM", 10.0)
	cfg.Matching.MaxDeadheadTruckKm      = envFloat64Or("MATCH_MAX_DEADHEAD_TRUCK_KM", 15.0)

	// Fuel consumption — Rwanda pump price ~1,550 RWF/litre as of 2026
	cfg.Matching.FuelLitresPerKmMotorcycle = envFloat64Or("FUEL_L_PER_KM_MOTORCYCLE", 0.035)
	cfg.Matching.FuelLitresPerKmCar        = envFloat64Or("FUEL_L_PER_KM_CAR", 0.080)
	cfg.Matching.FuelLitresPerKmVan        = envFloat64Or("FUEL_L_PER_KM_VAN", 0.120)
	cfg.Matching.FuelLitresPerKmTruck      = envFloat64Or("FUEL_L_PER_KM_TRUCK", 0.250)
	cfg.Matching.FuelPriceRWFPerLitre      = envFloat64Or("FUEL_PRICE_RWF_PER_LITRE", 1550.0)

	cfg.Matching.HeadingPenaltyThresholdDeg = envFloat64Or("MATCH_HEADING_PENALTY_THRESHOLD_DEG", 90.0)
	cfg.Matching.HeadingPenaltyMinSpeedKmh  = envFloat64Or("MATCH_HEADING_PENALTY_MIN_SPEED_KMH", 30.0)
	cfg.Matching.HeadingPenaltyMultiplier   = envFloat64Or("MATCH_HEADING_PENALTY_MULTIPLIER", 1.8)

	cfg.Matching.MaxStackDelayMinutes = envFloat64Or("MATCH_MAX_STACK_DELAY_MINUTES", 5.0)
	cfg.Matching.MaxStackDirectionDeg = envFloat64Or("MATCH_MAX_STACK_DIRECTION_DEG", 45.0)
	cfg.Matching.PremiumScoreBoost    = envFloat64Or("MATCH_PREMIUM_SCORE_BOOST", 10.0)
	cfg.Matching.OSRMTimeoutMs        = envIntOr("OSRM_TIMEOUT_MS", 3000)

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %v", missing)
	}

	return cfg, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func requireEnv(key string, missing *[]string) string {
	v := os.Getenv(key)
	if v == "" {
		*missing = append(*missing, key)
	}
	return v
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envIntOr(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func envFloat64Or(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func envDurationOr(key string, fallbackSecs int) time.Duration {
	return time.Duration(envIntOr(key, fallbackSecs))
}
