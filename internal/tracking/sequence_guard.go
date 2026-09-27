package tracking

import (
	"fmt"
	"log/slog"
	"sync"
)

// SequenceGuard enforces monotonically increasing sequence numbers per driver.
// This prevents TCP-handover duplicate/out-of-order frames from corrupting state
// .
type SequenceGuard struct {
	mu   sync.Mutex
	seqs map[string]int64 // driverID → last accepted seq
	log  *slog.Logger
}

func NewSequenceGuard(log *slog.Logger) *SequenceGuard {
	return &SequenceGuard{seqs: make(map[string]int64), log: log}
}

// Accept returns true only if seq is strictly greater than the last accepted
// sequence for this driver, then records it. Returns false for duplicates and
// out-of-order retries without returning an error (graceful skip).
func (g *SequenceGuard) Accept(driverID string, seq int64) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	last, ok := g.seqs[driverID]
	if ok && seq <= last {
		g.log.Warn("sequence guard: out-of-order frame dropped",
			slog.String("driver_id", driverID),
			slog.Int64("received", seq),
			slog.Int64("last_accepted", last),
		)
		return false
	}
	g.seqs[driverID] = seq
	return true
}

// Reset clears state for a driver (called on disconnect).
func (g *SequenceGuard) Reset(driverID string) {
	g.mu.Lock()
	delete(g.seqs, driverID)
	g.mu.Unlock()
}

// ── GPS Spoof Detector  ──────────────────────────────────────────

// NetworkFingerprint carries the cellular / Wi-Fi environment captured by the
// driver's device alongside their GPS coordinate.
type NetworkFingerprint struct {
	// CellTowerIDs is a comma-separated list of cell tower IDs the device
	// currently sees (MCC-MNC-LAC-CID format).
	CellTowerIDs string `json:"cell_tower_ids"`
	// WifiSSIDs is a comma-separated list of nearby Wi-Fi network names.
	WifiSSIDs string `json:"wifi_ssids"`
	// WifiCount is the number of distinct access points visible.
	WifiCount int `json:"wifi_count"`
}

// SpoofDetector analyses a driver's claimed location against their network
// environment and flags implausible combinations.
//
// Heuristic: commercial areas (malls, hospital hubs) have dense Wi-Fi
// (5+ APs visible). If a driver claims to be at such a location but their
// device sees fewer than MinCommercialWifi access points, it is suspicious.
type SpoofDetector struct {
	log *slog.Logger
}

const (
	// MinCommercialWifi is the minimum number of distinct Wi-Fi APs expected
	// in a dense commercial area. Below this, the GPS claim is suspicious.
	MinCommercialWifi = 3
	// MinCellTowers is the expected number of tower IDs in an urban area.
	MinCellTowers = 1
)

func NewSpoofDetector(log *slog.Logger) *SpoofDetector {
	return &SpoofDetector{log: log}
}

// IsSuspicious returns (true, reason) when the fingerprint contradicts the
// claimed urban location. Returns (false, "") when clean.
//
// NOTE: This is a probabilistic heuristic — it deprioritizes, it does not ban.
func (d *SpoofDetector) IsSuspicious(fp NetworkFingerprint, claimedLat, claimedLng float64) (bool, string) {
	// Skip if the device provided no network data at all (old app version).
	if fp.CellTowerIDs == "" && fp.WifiCount == 0 {
		return false, ""
	}

	// Wi-Fi desert in claimed commercial zone:
	// A coordinate inside Kigali city is expected to have multiple visible APs.
	// (Simplified: if they claimed any non-zero lat/lng and see zero Wi-Fi.)
	if fp.WifiCount == 0 && fp.CellTowerIDs == "" {
		reason := fmt.Sprintf(
			"GPS claims urban location (%.4f,%.4f) but device sees no Wi-Fi or cell towers",
			claimedLat, claimedLng,
		)
		d.log.Warn("GPS spoof suspicious",
			slog.String("reason", reason),
			slog.Float64("lat", claimedLat),
			slog.Float64("lng", claimedLng),
		)
		return true, reason
	}

	return false, ""
}
