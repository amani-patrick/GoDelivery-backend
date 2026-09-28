package matching

import (
	"context"
	"log/slog"
)

// MultiDropOptimiser computes the optimal delivery sequence for a driver
// carrying multiple stacked packages. It uses the OSRM Distance Matrix
// to solve the Traveling Salesperson problem for small N (≤8 drops)
// using brute-force permutation, which is tractable for real-world
// delivery stacks (typically 2-4 drops).
//
// For N > 8, it falls back to a nearest-neighbour greedy heuristic.

// DropPoint represents a single delivery destination in a multi-drop stack.
type DropPoint struct {
	DeliveryID string
	Lat        float64
	Lng        float64
}

// OptimisedSequence is the result of the TSP solver.
type OptimisedSequence struct {
	// Order is the optimised sequence of delivery IDs.
	Order []string `json:"order"`
	// TotalDistanceKm is the total road distance of the optimised route.
	TotalDistanceKm float64 `json:"total_distance_km"`
	// TotalETAMinutes is the estimated total time for the optimised route.
	TotalETAMinutes float64 `json:"total_eta_minutes"`
}

// OptimiseMultiDrop takes the driver's current position and a set of drop-off
// points, queries the OSRM Distance Matrix for all pairs, and returns the
// optimal delivery sequence.
//
// The driver's position is index 0; drop points are indices 1..N.
// The returned Order starts from the first drop and excludes the driver position.
func (e *Engine) OptimiseMultiDrop(ctx context.Context, driverLat, driverLng float64, drops []DropPoint) (*OptimisedSequence, error) {
	if len(drops) <= 1 {
		// Single drop — no optimisation needed.
		ids := make([]string, len(drops))
		for i, d := range drops {
			ids[i] = d.DeliveryID
		}
		return &OptimisedSequence{Order: ids}, nil
	}

	// Build coordinate array: [driver, drop0, drop1, ..., dropN-1]
	n := len(drops) + 1
	coords := make([][2]float64, n)
	coords[0] = [2]float64{driverLat, driverLng}
	for i, d := range drops {
		coords[i+1] = [2]float64{d.Lat, d.Lng}
	}

	// Query OSRM for the full NxN distance matrix.
	distMatrix := make([][]float64, n)
	etaMatrix := make([][]float64, n)

	for i := 0; i < n; i++ {
		// For each origin, compute distance to all other points.
		otherCoords := make([][2]float64, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				otherCoords = append(otherCoords, coords[j])
			}
		}

		// Use the first "other" point as destination for Table API.
		// For full matrix, we make individual route queries.
		distRow := make([]float64, n)
		etaRow := make([]float64, n)

		for j := 0; j < n; j++ {
			if j == i {
				distRow[j] = 0
				etaRow[j] = 0
				continue
			}
			src := [][2]float64{coords[i]}
			matrix, err := e.osrm.Table(ctx, src, coords[j][0], coords[j][1])
			if err != nil {
				// Haversine fallback
				matrix = e.haversineFallback(src, coords[j][0], coords[j][1])
			}
			distRow[j] = matrix.DistancesM[0] / 1000.0
			etaRow[j] = matrix.ETAsSeconds[0] / 60.0
		}
		distMatrix[i] = distRow
		etaMatrix[i] = etaRow
	}

	// Solve TSP starting from node 0 (driver position).
	dropIndices := make([]int, len(drops))
	for i := range drops {
		dropIndices[i] = i + 1
	}

	var bestPerm []int
	var bestDist float64

	if len(drops) <= 8 {
		// Brute-force permutation for small N.
		bestDist = 1e18
		permute(dropIndices, 0, func(perm []int) {
			dist := distMatrix[0][perm[0]]
			for k := 0; k < len(perm)-1; k++ {
				dist += distMatrix[perm[k]][perm[k+1]]
			}
			if dist < bestDist {
				bestDist = dist
				bestPerm = make([]int, len(perm))
				copy(bestPerm, perm)
			}
		})
	} else {
		// Nearest-neighbour greedy for large N.
		bestPerm, bestDist = nearestNeighbour(distMatrix, dropIndices)
	}

	// Build result.
	order := make([]string, len(bestPerm))
	totalETA := etaMatrix[0][bestPerm[0]]
	for i, idx := range bestPerm {
		order[i] = drops[idx-1].DeliveryID
		if i < len(bestPerm)-1 {
			totalETA += etaMatrix[bestPerm[i]][bestPerm[i+1]]
		}
	}

	e.log.Info("multi-drop sequence optimised",
		slog.Int("drops", len(drops)),
		slog.Float64("total_dist_km", bestDist),
		slog.Float64("total_eta_min", totalETA),
	)

	return &OptimisedSequence{
		Order:           order,
		TotalDistanceKm: bestDist,
		TotalETAMinutes: totalETA,
	}, nil
}

// permute generates all permutations of arr and calls fn for each.
func permute(arr []int, start int, fn func([]int)) {
	if start == len(arr)-1 {
		fn(arr)
		return
	}
	for i := start; i < len(arr); i++ {
		arr[start], arr[i] = arr[i], arr[start]
		permute(arr, start+1, fn)
		arr[start], arr[i] = arr[i], arr[start]
	}
}

// nearestNeighbour implements the greedy nearest-neighbour TSP heuristic.
// Returns the visit order and total distance.
func nearestNeighbour(distMatrix [][]float64, dropIndices []int) ([]int, float64) {
	visited := make(map[int]bool)
	order := make([]int, 0, len(dropIndices))
	totalDist := 0.0
	current := 0 // start at driver position

	for len(order) < len(dropIndices) {
		bestIdx := -1
		bestDist := 1e18
		for _, idx := range dropIndices {
			if visited[idx] {
				continue
			}
			if distMatrix[current][idx] < bestDist {
				bestDist = distMatrix[current][idx]
				bestIdx = idx
			}
		}
		if bestIdx == -1 {
			break
		}
		visited[bestIdx] = true
		order = append(order, bestIdx)
		totalDist += bestDist
		current = bestIdx
	}
	return order, totalDist
}
