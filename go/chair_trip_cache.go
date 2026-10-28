package main

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// chairTripSnapshot is the ride data needed to decide whether an interior
// coordinate can skip a query. Endpoints always verify the latest status in DB.
type chairTripSnapshot struct {
	RideID               string
	UserID               string
	PickupLatitude       int
	PickupLongitude      int
	DestinationLatitude  int
	DestinationLongitude int
	RideUpdatedAt        time.Time
	Status               string
}

type chairTripCacheEntry struct {
	streamGeneration uint64
	version          uint64
	live             bool
	trip             *chairTripSnapshot
}

type chairTripCacheToken struct {
	streamGeneration uint64
	version          uint64
	valid            bool
}

type chairTripCacheDecision uint8

const (
	chairTripCacheMiss chairTripCacheDecision = iota
	chairTripCacheInvalidGeneration
	chairTripCacheInactive
	chairTripCacheEndpoint
	chairTripCacheHit
)

var chairTripCache = struct {
	sync.Mutex
	nextGeneration uint64
	entries        map[string]chairTripCacheEntry
}{entries: make(map[string]chairTripCacheEntry)}

var chairTripCacheMetrics struct {
	hits, misses, invalidGenerations, inactive, endpoints atomic.Uint64
	decisions                                             atomic.Uint64
}

func beginChairTripStream(chairID string) uint64 {
	chairTripCache.Lock()
	defer chairTripCache.Unlock()
	chairTripCache.nextGeneration++
	generation := chairTripCache.nextGeneration
	old := chairTripCache.entries[chairID]
	chairTripCache.entries[chairID] = chairTripCacheEntry{
		streamGeneration: generation,
		version:          old.version + 1,
		live:             true,
	}
	return generation
}

func endChairTripStream(chairID string, generation uint64) {
	chairTripCache.Lock()
	defer chairTripCache.Unlock()
	entry, ok := chairTripCache.entries[chairID]
	if !ok || entry.streamGeneration != generation {
		return
	}
	entry.version++
	entry.live = false
	entry.trip = nil
	chairTripCache.entries[chairID] = entry
}

// clearChairTripCacheLocal invalidates every stream token as well as its
// snapshot. A read started before initialize cannot repopulate the new run.
func clearChairTripCacheLocal() {
	chairTripCache.Lock()
	chairTripCache.nextGeneration++
	chairTripCache.entries = make(map[string]chairTripCacheEntry)
	chairTripCache.Unlock()
	resetChairTripCacheMetrics()
}

func chairTripCacheReadToken(chairID string, generation uint64) (chairTripCacheToken, bool) {
	chairTripCache.Lock()
	defer chairTripCache.Unlock()
	entry, ok := chairTripCache.entries[chairID]
	if !ok || !entry.live || entry.streamGeneration != generation {
		return chairTripCacheToken{}, false
	}
	return chairTripCacheToken{streamGeneration: generation, version: entry.version, valid: true}, true
}

// storeChairTripFromRead is a compare-and-swap against both the stream owner
// and the version captured before the database read. A slower SSE/coordinate
// read cannot overwrite a newer status POST or stream snapshot.
func storeChairTripFromRead(chairID string, token chairTripCacheToken, trip *chairTripSnapshot) bool {
	if !token.valid {
		return false
	}
	chairTripCache.Lock()
	defer chairTripCache.Unlock()
	entry, ok := chairTripCache.entries[chairID]
	if !ok || !entry.live || entry.streamGeneration != token.streamGeneration || entry.version != token.version {
		return false
	}
	entry.version++
	entry.trip = cloneChairTrip(trip)
	chairTripCache.entries[chairID] = entry
	return true
}

func syncChairTripAfterStatus(chairID string, trip chairTripSnapshot) {
	chairTripCache.Lock()
	defer chairTripCache.Unlock()
	entry, ok := chairTripCache.entries[chairID]
	if !ok || !entry.live {
		return
	}
	current := entry.trip
	if current == nil {
		// The current stream has not established which ride is current yet.
		// Invalidate any read already in flight; keep falling back until a new
		// versioned DB read supplies the snapshot.
		entry.version++
		chairTripCache.entries[chairID] = entry
		return
	}
	if current.RideID != trip.RideID {
		// Do not let a late POST for an older ride replace newer stream state.
		if trip.RideUpdatedAt.Before(current.RideUpdatedAt) {
			entry.version++
			chairTripCache.entries[chairID] = entry
			return
		}
		if !trip.RideUpdatedAt.After(current.RideUpdatedAt) {
			entry.version++
			entry.trip = nil
			chairTripCache.entries[chairID] = entry
			return
		}
	}
	entry.version++
	entry.trip = cloneChairTrip(&trip)
	chairTripCache.entries[chairID] = entry
}

func lookupChairTripForCoordinate(chairID string, latitude, longitude int) (*chairTripSnapshot, chairTripCacheToken, chairTripCacheDecision) {
	chairTripCache.Lock()
	defer chairTripCache.Unlock()
	entry, ok := chairTripCache.entries[chairID]
	if !ok {
		return nil, chairTripCacheToken{}, chairTripCacheMiss
	}
	token := chairTripCacheToken{streamGeneration: entry.streamGeneration, version: entry.version, valid: entry.live}
	if !entry.live {
		return nil, token, chairTripCacheInvalidGeneration
	}
	trip := entry.trip
	if trip == nil {
		return nil, token, chairTripCacheMiss
	}
	if trip.Status != "ENROUTE" && trip.Status != "CARRYING" {
		return nil, token, chairTripCacheInactive
	}
	if (latitude == trip.PickupLatitude && longitude == trip.PickupLongitude) ||
		(latitude == trip.DestinationLatitude && longitude == trip.DestinationLongitude) {
		return cloneChairTrip(trip), token, chairTripCacheEndpoint
	}
	return cloneChairTrip(trip), token, chairTripCacheHit
}

func cloneChairTrip(trip *chairTripSnapshot) *chairTripSnapshot {
	if trip == nil {
		return nil
	}
	copy := *trip
	return &copy
}

func recordChairTripCacheDecision(decision chairTripCacheDecision) {
	switch decision {
	case chairTripCacheHit:
		chairTripCacheMetrics.hits.Add(1)
	case chairTripCacheMiss:
		chairTripCacheMetrics.misses.Add(1)
	case chairTripCacheInvalidGeneration:
		chairTripCacheMetrics.invalidGenerations.Add(1)
	case chairTripCacheInactive:
		chairTripCacheMetrics.inactive.Add(1)
	case chairTripCacheEndpoint:
		chairTripCacheMetrics.endpoints.Add(1)
	}
	if n := chairTripCacheMetrics.decisions.Add(1); n%10000 == 0 {
		logChairTripCacheMetrics()
	}
}

func logChairTripCacheMetrics() {
	slog.Info("chair trip cache",
		"hits", chairTripCacheMetrics.hits.Load(),
		"misses", chairTripCacheMetrics.misses.Load(),
		"invalid_generations", chairTripCacheMetrics.invalidGenerations.Load(),
		"inactive", chairTripCacheMetrics.inactive.Load(),
		"endpoints", chairTripCacheMetrics.endpoints.Load(),
	)
}

func resetChairTripCacheMetrics() {
	chairTripCacheMetrics.hits.Store(0)
	chairTripCacheMetrics.misses.Store(0)
	chairTripCacheMetrics.invalidGenerations.Store(0)
	chairTripCacheMetrics.inactive.Store(0)
	chairTripCacheMetrics.endpoints.Store(0)
	chairTripCacheMetrics.decisions.Store(0)
}
