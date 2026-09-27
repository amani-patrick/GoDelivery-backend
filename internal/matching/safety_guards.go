package matching

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/umurinzi/backend/internal/domain"
)

type CurfewConfig struct {
	CutoffHour   int 
	ResumeHour   int 
}

var DefaultCurfew = CurfewConfig{CutoffHour: 21, ResumeHour: 6}

func (c CurfewConfig) IsCurfewActive() bool {
	h := time.Now().UTC().Add(2 * time.Hour).Hour() // UTC+2 Kigali
	if c.CutoffHour > c.ResumeHour {
		return h >= c.CutoffHour || h < c.ResumeHour
	}
	return h >= c.CutoffHour && h < c.ResumeHour
}

func (c CurfewConfig) WouldExceedCurfew(etaSeconds float64) bool {
	arrival := time.Now().Add(time.Duration(etaSeconds) * time.Second)
	arrivalHour := arrival.UTC().Add(2 * time.Hour).Hour()
	if c.CutoffHour > c.ResumeHour {
		return arrivalHour >= c.CutoffHour || arrivalHour < c.ResumeHour
	}
	return arrivalHour >= c.CutoffHour && arrivalHour < c.ResumeHour
}



const (
	etaVarianceTicks = 3
	bearingConeDeg = 30.0
)

type ETAVarianceDetector struct {
	osrm    *OSRMClient
	alertCh chan<- domain.AnomalyAlert
	log     *slog.Logger
	worseningCount map[string]int
	lastDistKm map[string]float64
}

func NewETAVarianceDetector(osrm *OSRMClient, alertCh chan<- domain.AnomalyAlert, log *slog.Logger) *ETAVarianceDetector {
	return &ETAVarianceDetector{
		osrm:           osrm,
		alertCh:        alertCh,
		log:            log,
		worseningCount: make(map[string]int),
		lastDistKm:     make(map[string]float64),
	}
}

func (d *ETAVarianceDetector) Check(
	ctx context.Context,
	driverID, deliveryID string,
	driverLat, driverLng float64,
	dropoffLat, dropoffLng float64,
	bearing float64,
) {
	// Compute current road distance to destination via OSRM (or haversine fallback).
	var distKm float64
	matrix, err := d.osrm.Table(ctx, [][2]float64{{driverLat, driverLng}}, dropoffLat, dropoffLng)
	if err != nil || len(matrix.DistancesM) == 0 {
		// haversine fallback
		distKm = haversineKm(driverLat, driverLng, dropoffLat, dropoffLng)
	} else {
		distKm = matrix.DistancesM[0] / 1000.0
	}

	key := driverID + ":" + deliveryID
	prev, hasPrev := d.lastDistKm[key]
	d.lastDistKm[key] = distKm

	// Bearing check: is the driver heading generally toward the destination?
	optimalBearing := bearingBetween(driverLat, driverLng, dropoffLat, dropoffLng)
	offCone := angularDifference(bearing, optimalBearing) > bearingConeDeg

	if hasPrev && (distKm > prev || offCone) {
		d.worseningCount[key]++
		d.log.Info("ETA variance: driver moving away from destination",
			slog.String("driver_id", driverID),
			slog.Float64("prev_km", prev),
			slog.Float64("curr_km", distKm),
			slog.Int("consecutive_ticks", d.worseningCount[key]),
			slog.Bool("off_cone", offCone),
		)
	} else {
		d.worseningCount[key] = 0
	}

	if d.worseningCount[key] >= etaVarianceTicks {
		d.worseningCount[key] = 0 // reset so we don't spam
		d.alertCh <- domain.AnomalyAlert{
			DeliveryID:   deliveryID,
			DriverID:     driverID,
			AlertType:    "MULTI_APP_SHADOW",
			LastKnownLat: driverLat,
			LastKnownLng: driverLng,
			DetectedAt:   time.Now().UTC(),
			Details: fmt.Sprintf(
				`{"dist_km":%.2f,"off_bearing_cone":%v,"ticks":%d}`,
				distKm, offCone, etaVarianceTicks,
			),
		}
		d.log.Warn("MULTI_APP_SHADOW alert fired",
			slog.String("driver_id", driverID),
			slog.String("delivery_id", deliveryID),
		)
	}
}

// haversineKm local helper (mirrors engine.go's haversineM / 1000)
func haversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	return haversineM(lat1, lng1, lat2, lng2) / 1000.0
}
