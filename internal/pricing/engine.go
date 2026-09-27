package pricing

import (
	"context"
	"math"

	"github.com/umurinzi/backend/internal/domain"
	"github.com/umurinzi/backend/internal/surge"
)

// Engine calculates fare for a delivery.
type Engine struct{
	surge *surge.Engine
}

func NewEngine(s *surge.Engine) *Engine {
	return &Engine{surge: s}
}

// CalculateFare computes the amount in RWF for the delivery.
func (e *Engine) CalculateFare(ctx context.Context, d *domain.Delivery) int64 {
	distanceKm := haversineKm(d.PickupLoc.Lat, d.PickupLoc.Lng, d.DropoffLoc.Lat, d.DropoffLoc.Lng)
	
	baseFare := 500.0
	perKm := 300.0

	switch d.VehicleTypeRequired {
	case domain.VehicleMotorcycle:
		perKm = 300.0
	case domain.VehicleCar:
		perKm = 700.0
	case domain.VehicleVan:
		perKm = 1500.0
	case domain.VehicleTruck:
		perKm = 3000.0
	}

	fare := baseFare + (distanceKm * perKm)

	if d.PriorityLevel == 2 {
		fare *= 1.5 // premium
	}

	baseFareRWF := int64(fare)
	if e.surge != nil {
		return e.surge.Apply(baseFareRWF)
	}
	return baseFareRWF
}

func haversineKm(lat1, lng1, lat2, lng2 float64) float64 {
	const R = 6371.0
	φ1, φ2 := toRadians(lat1), toRadians(lat2)
	dφ := toRadians(lat2 - lat1)
	dλ := toRadians(lng2 - lng1)
	a := math.Sin(dφ/2)*math.Sin(dφ/2) +
		math.Cos(φ1)*math.Cos(φ2)*math.Sin(dλ/2)*math.Sin(dλ/2)
	return R * 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
}

func toRadians(deg float64) float64 { return deg * math.Pi / 180 }
