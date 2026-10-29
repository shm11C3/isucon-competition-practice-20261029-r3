package main

import (
	"sort"
	"time"
)

const (
	urgentRideAge                = 8 * time.Second
	globalChairCandidatesPerRide = 4
)

type matchingPair struct {
	rideIndex  int
	chairIndex int
}

type matchingEdge struct {
	rideIndex  int
	chairIndex int
	eta        int
	// timePerScore is the chair time the pair takes per point it earns, scaled.
	timePerScore int64
}

// The benchmark scores a completed ride 500 + 100 per unit carried + 10 per unit
// driven to the pickup, and a chair is busy for the pickup, the carry and a fixed
// handover (accept, arrival, evaluation, payment, next match: about 1.5 s, 50
// ticks). Pickup distance alone ignores that a long ride earns twice as much per
// chair second as a short one.
const handoverETA = 50 * 1000

func edgeTimePerScore(ride Ride, chair freeChair, eta int) int64 {
	pickup := calculateDistance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)
	carry := calculateDistance(ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	busy := int64(eta) + int64(carry)*1000/int64(chair.Speed) + handoverETA
	score := int64(500 + 100*carry + 10*pickup)
	return busy * 1000 / score
}

func matchingValueLess(a, b matchingEdge) bool {
	if a.timePerScore != b.timePerScore {
		return a.timePerScore < b.timePerScore
	}
	return matchingEdgeLess(a, b)
}

func matchingEdgeLess(a, b matchingEdge) bool {
	if a.eta != b.eta {
		return a.eta < b.eta
	}
	if a.rideIndex != b.rideIndex {
		return a.rideIndex < b.rideIndex
	}
	return a.chairIndex < b.chairIndex
}

func approachETA(ride Ride, chair freeChair) (int, bool) {
	if chair.Speed <= 0 {
		return 0, false
	}
	distance := calculateDistance(chair.Latitude, chair.Longitude, ride.PickupLatitude, ride.PickupLongitude)
	return distance * 1000 / chair.Speed, true
}

func nearestAvailableChair(ride Ride, chairs []freeChair, usedChairs []bool) int {
	bestChair, bestETA := -1, 0
	for chairIndex, chair := range chairs {
		if usedChairs[chairIndex] {
			continue
		}
		eta, ok := approachETA(ride, chair)
		if ok && (bestChair < 0 || eta < bestETA) {
			bestChair, bestETA = chairIndex, eta
		}
	}
	return bestChair
}

// selectAgeAwareChairMatches keeps FIFO-nearest assignment for requests waiting
// at least eight seconds. It then greedily consumes, among the top four chairs per
// remaining ride by ETA, the edges with the least chair time per point earned. FIFO-nearest fallback preserves
// full utilization when the sparse edge set cannot match every ride/chair.
func selectAgeAwareChairMatches(rides []Ride, chairs []freeChair, now time.Time) []matchingPair {
	if len(rides) == 0 || len(chairs) == 0 {
		return nil
	}

	pairs := make([]matchingPair, 0, min(len(rides), len(chairs)))
	usedRides := make([]bool, len(rides))
	usedChairs := make([]bool, len(chairs))

	// The rides are already ordered by created_at. Match urgent rides in that
	// order, using the original nearest-chair ETA rule to protect Matching score.
	for rideIndex, ride := range rides {
		if now.Sub(ride.CreatedAt) < urgentRideAge {
			continue
		}
		chairIndex := nearestAvailableChair(ride, chairs, usedChairs)
		if chairIndex >= 0 {
			usedRides[rideIndex], usedChairs[chairIndex] = true, true
			pairs = append(pairs, matchingPair{rideIndex: rideIndex, chairIndex: chairIndex})
		}
	}

	// Keep distance work O(R*C), but bound global-edge storage and sorting to
	// O(R*K), with K=4. Reuse a fixed scratch array to avoid an allocation per ride.
	k := min(globalChairCandidatesPerRide, len(chairs))
	edges := make([]matchingEdge, 0, (len(rides)-len(pairs))*k)
	var topStorage [globalChairCandidatesPerRide]matchingEdge
	for rideIndex, ride := range rides {
		if usedRides[rideIndex] {
			continue
		}
		top := topStorage[:0]
		for chairIndex, chair := range chairs {
			if usedChairs[chairIndex] {
				continue
			}
			eta, ok := approachETA(ride, chair)
			if !ok {
				continue
			}
			edge := matchingEdge{rideIndex: rideIndex, chairIndex: chairIndex, eta: eta, timePerScore: edgeTimePerScore(ride, chair, eta)}
			if len(top) == k && !matchingEdgeLess(edge, top[k-1]) {
				continue
			}
			position := len(top)
			if position == k {
				position--
			} else {
				top = append(top, matchingEdge{})
			}
			for position > 0 && matchingEdgeLess(edge, top[position-1]) {
				top[position] = top[position-1]
				position--
			}
			top[position] = edge
		}
		edges = append(edges, top...)
	}
	// Each ride keeps its nearest chairs; among those pairs the ones that earn the
	// most per chair second go first.
	sort.Slice(edges, func(i, j int) bool { return matchingValueLess(edges[i], edges[j]) })
	for _, edge := range edges {
		if usedRides[edge.rideIndex] || usedChairs[edge.chairIndex] {
			continue
		}
		usedRides[edge.rideIndex], usedChairs[edge.chairIndex] = true, true
		pairs = append(pairs, matchingPair{rideIndex: edge.rideIndex, chairIndex: edge.chairIndex})
	}

	// A truncated edge list must not leave a usable chair idle. Fallback preserves
	// the existing FIFO-nearest behavior for every still-unmatched ride.
	for rideIndex, ride := range rides {
		if usedRides[rideIndex] {
			continue
		}
		chairIndex := nearestAvailableChair(ride, chairs, usedChairs)
		if chairIndex >= 0 {
			usedRides[rideIndex], usedChairs[chairIndex] = true, true
			pairs = append(pairs, matchingPair{rideIndex: rideIndex, chairIndex: chairIndex})
		}
	}

	return pairs
}
