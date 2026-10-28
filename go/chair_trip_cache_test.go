package main

import (
	"testing"
	"time"
)

func TestChairTripCacheRejectsSSEReadOlderThanStatusPost(t *testing.T) {
	clearChairTripCacheLocal()
	const chairID = "cache-version-chair"
	generation := beginChairTripStream(chairID)
	initial, ok := chairTripCacheReadToken(chairID, generation)
	if !ok || !storeChairTripFromRead(chairID, initial, &chairTripSnapshot{RideID: "ride-1", Status: "MATCHING"}) {
		t.Fatal("failed to seed stream snapshot")
	}
	staleRead, ok := chairTripCacheReadToken(chairID, generation)
	if !ok {
		t.Fatal("stream generation unexpectedly inactive")
	}
	syncChairTripAfterStatus(chairID, chairTripSnapshot{
		RideID: "ride-1", Status: "ENROUTE", RideUpdatedAt: time.Unix(1, 0),
	})
	if storeChairTripFromRead(chairID, staleRead, &chairTripSnapshot{RideID: "ride-1", Status: "MATCHING"}) {
		t.Fatal("stale SSE read overwrote a committed ENROUTE update")
	}
	trip, _, decision := lookupChairTripForCoordinate(chairID, 40, 41)
	if decision != chairTripCacheHit || trip == nil || trip.Status != "ENROUTE" {
		t.Fatalf("coordinate lookup = (%+v, %v), want current active snapshot", trip, decision)
	}
}

func TestChairTripCacheOldStreamCannotClearOrWriteNewGeneration(t *testing.T) {
	clearChairTripCacheLocal()
	const chairID = "cache-generation-chair"
	oldGeneration := beginChairTripStream(chairID)
	oldToken, _ := chairTripCacheReadToken(chairID, oldGeneration)
	storeChairTripFromRead(chairID, oldToken, &chairTripSnapshot{RideID: "ride-old", Status: "ENROUTE"})
	newGeneration := beginChairTripStream(chairID)
	endChairTripStream(chairID, oldGeneration)
	if storeChairTripFromRead(chairID, oldToken, &chairTripSnapshot{RideID: "ride-old", Status: "ENROUTE"}) {
		t.Fatal("old stream repopulated the new generation")
	}
	newToken, ok := chairTripCacheReadToken(chairID, newGeneration)
	if !ok {
		t.Fatal("old stream close cleared the current stream generation")
	}
	if !storeChairTripFromRead(chairID, newToken, &chairTripSnapshot{RideID: "ride-new", Status: "MATCHING"}) {
		t.Fatal("current stream could not store its snapshot")
	}
}

func TestChairTripCacheOnlyHitsInteriorActiveCoordinates(t *testing.T) {
	clearChairTripCacheLocal()
	const chairID = "cache-endpoint-chair"
	generation := beginChairTripStream(chairID)
	token, _ := chairTripCacheReadToken(chairID, generation)
	trip := &chairTripSnapshot{
		RideID: "ride-1", Status: "ENROUTE",
		PickupLatitude: 1, PickupLongitude: 2,
		DestinationLatitude: 8, DestinationLongitude: 9,
	}
	storeChairTripFromRead(chairID, token, trip)
	if _, _, decision := lookupChairTripForCoordinate(chairID, 1, 2); decision != chairTripCacheEndpoint {
		t.Fatalf("pickup lookup decision=%v, want endpoint fallback", decision)
	}
	if _, _, decision := lookupChairTripForCoordinate(chairID, 8, 9); decision != chairTripCacheEndpoint {
		t.Fatalf("destination lookup decision=%v, want endpoint fallback", decision)
	}
	if _, _, decision := lookupChairTripForCoordinate(chairID, 4, 5); decision != chairTripCacheHit {
		t.Fatalf("interior lookup decision=%v, want cache hit", decision)
	}
	endChairTripStream(chairID, generation)
	if _, _, decision := lookupChairTripForCoordinate(chairID, 4, 5); decision != chairTripCacheInvalidGeneration {
		t.Fatalf("closed stream lookup decision=%v, want DB fallback", decision)
	}
}

func TestChairTripCacheLateOlderRideStatusCannotReplaceNewerRide(t *testing.T) {
	clearChairTripCacheLocal()
	const chairID = "cache-ride-order-chair"
	generation := beginChairTripStream(chairID)
	token, _ := chairTripCacheReadToken(chairID, generation)
	storeChairTripFromRead(chairID, token, &chairTripSnapshot{
		RideID: "ride-new", Status: "MATCHING", RideUpdatedAt: time.Unix(20, 0),
	})
	staleRead, _ := chairTripCacheReadToken(chairID, generation)
	syncChairTripAfterStatus(chairID, chairTripSnapshot{
		RideID: "ride-old", Status: "ENROUTE", RideUpdatedAt: time.Unix(10, 0),
	})
	if storeChairTripFromRead(chairID, staleRead, &chairTripSnapshot{
		RideID: "ride-old", Status: "ENROUTE", RideUpdatedAt: time.Unix(10, 0),
	}) {
		t.Fatal("pre-status read overwrote the newer ride after an older POST")
	}
	trip, _, decision := lookupChairTripForCoordinate(chairID, 30, 31)
	if decision != chairTripCacheInactive || trip != nil {
		t.Fatalf("late old ride status became usable: trip=%+v decision=%v", trip, decision)
	}
}

func TestChairTripCacheAdvancesFromCompletedRideToNewerRide(t *testing.T) {
	clearChairTripCacheLocal()
	const chairID = "cache-consecutive-chair"
	generation := beginChairTripStream(chairID)
	token, _ := chairTripCacheReadToken(chairID, generation)
	storeChairTripFromRead(chairID, token, &chairTripSnapshot{
		RideID: "ride-old", Status: "COMPLETED", RideUpdatedAt: time.Unix(10, 0),
	})
	syncChairTripAfterStatus(chairID, chairTripSnapshot{
		RideID: "ride-new", Status: "ENROUTE", RideUpdatedAt: time.Unix(20, 0),
	})
	trip, _, decision := lookupChairTripForCoordinate(chairID, 30, 31)
	if decision != chairTripCacheHit || trip == nil || trip.RideID != "ride-new" {
		t.Fatalf("newer ride was not activated: trip=%+v decision=%v", trip, decision)
	}
}

func TestChairTripStatusPostDoesNotActivateUnseededStream(t *testing.T) {
	clearChairTripCacheLocal()
	const chairID = "cache-unseeded-chair"
	generation := beginChairTripStream(chairID)
	staleRead, _ := chairTripCacheReadToken(chairID, generation)
	syncChairTripAfterStatus(chairID, chairTripSnapshot{
		RideID: "ride-1", Status: "ENROUTE", RideUpdatedAt: time.Unix(10, 0),
	})
	if storeChairTripFromRead(chairID, staleRead, &chairTripSnapshot{RideID: "ride-old", Status: "MATCHING"}) {
		t.Fatal("status POST failed to invalidate an in-flight stream read")
	}
	if _, _, decision := lookupChairTripForCoordinate(chairID, 30, 31); decision != chairTripCacheMiss {
		t.Fatalf("unseeded status POST produced cache decision %v, want DB fallback", decision)
	}
}
