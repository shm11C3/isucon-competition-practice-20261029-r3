package main

import (
	"testing"
	"time"
)

func TestAgeAwareGlobalMatchingUsesGlobalETAAndUrgentFIFO(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	chairs := []freeChair{
		{ID: "fast", Speed: 10, Latitude: 0, Longitude: 0},
		{ID: "slow", Speed: 1, Latitude: 0, Longitude: 10},
	}
	rides := []Ride{
		{ID: "older-young", PickupLatitude: 0, PickupLongitude: 5, CreatedAt: now.Add(-time.Second)},
		{ID: "younger", PickupLatitude: 0, PickupLongitude: 1, CreatedAt: now.Add(-time.Second)},
	}
	pairs := selectAgeAwareChairMatches(rides, chairs, now)
	if len(pairs) != 2 || pairs[0] != (matchingPair{rideIndex: 1, chairIndex: 0}) || pairs[1] != (matchingPair{rideIndex: 0, chairIndex: 1}) {
		t.Fatalf("global ETA/fallback assignments = %#v, want younger ride on fast then older on slow", pairs)
	}

	urgentRides := []Ride{
		{ID: "urgent", PickupLatitude: 0, PickupLongitude: 5, CreatedAt: now.Add(-urgentRideAge)},
		rides[1],
	}
	urgentPairs := selectAgeAwareChairMatches(urgentRides, chairs, now)
	if len(urgentPairs) != 2 || urgentPairs[0] != (matchingPair{rideIndex: 0, chairIndex: 0}) || urgentPairs[1] != (matchingPair{rideIndex: 1, chairIndex: 1}) {
		t.Fatalf("urgent FIFO assignments = %#v, want oldest ride on nearest chair first", urgentPairs)
	}
}

func TestAgeAwareMatchingKeepsETAWindowUntilUrgency(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	chairs := []freeChair{
		{ID: "fast", Speed: 10, Latitude: 0, Longitude: 0},
		{ID: "slow", Speed: 1, Latitude: 0, Longitude: 10},
	}
	rides := []Ride{
		{ID: "older", PickupLatitude: 0, PickupLongitude: 5, CreatedAt: now.Add(-7 * time.Second)},
		{ID: "younger", PickupLatitude: 0, PickupLongitude: 1, CreatedAt: now.Add(-time.Second)},
	}
	pairs := selectAgeAwareChairMatches(rides, chairs, now)
	if len(pairs) != 2 || pairs[0] != (matchingPair{rideIndex: 1, chairIndex: 0}) || pairs[1] != (matchingPair{rideIndex: 0, chairIndex: 1}) {
		t.Fatalf("pre-urgency global ETA assignments = %#v, want younger ride on fast then older on slow", pairs)
	}
}

func TestAgeAwareMatchingUsesFIFOAfterUrgencyConsumesAllChairs(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	chairs := []freeChair{
		{ID: "fast", Speed: 10, Latitude: 0, Longitude: 0},
		{ID: "slow", Speed: 1, Latitude: 0, Longitude: 10},
	}
	rides := []Ride{
		{ID: "oldest", PickupLatitude: 0, PickupLongitude: 5, CreatedAt: now.Add(-9 * time.Second)},
		{ID: "next", PickupLatitude: 0, PickupLongitude: 1, CreatedAt: now.Add(-urgentRideAge)},
	}
	pairs := selectAgeAwareChairMatches(rides, chairs, now)
	if len(pairs) != 2 || pairs[0] != (matchingPair{rideIndex: 0, chairIndex: 0}) || pairs[1] != (matchingPair{rideIndex: 1, chairIndex: 1}) {
		t.Fatalf("urgent FIFO assignments = %#v, want oldest ride on nearest chair first and every chair used", pairs)
	}
}

func TestAgeAwareGlobalMatchingUniqueZeroSpeedEmptyAndFallback(t *testing.T) {
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	chairs := []freeChair{
		{ID: "c0", Speed: 1, Latitude: 0, Longitude: 0},
		{ID: "c1", Speed: 10, Latitude: 0, Longitude: 10},
		{ID: "invalid", Speed: 0, Latitude: 0, Longitude: 1},
	}
	rides := []Ride{
		{ID: "r0", PickupLatitude: 0, PickupLongitude: 1, CreatedAt: now.Add(-time.Second)},
		{ID: "r1", PickupLatitude: 0, PickupLongitude: 2, CreatedAt: now.Add(-time.Second)},
		{ID: "r2", PickupLatitude: 0, PickupLongitude: 3, CreatedAt: now.Add(-time.Second)},
	}
	pairs := selectAgeAwareChairMatches(rides, chairs, now)
	if len(pairs) != 2 {
		t.Fatalf("got %d matches, want two unique valid chairs", len(pairs))
	}
	usedRides, usedChairs := map[int]bool{}, map[int]bool{}
	for _, pair := range pairs {
		if usedRides[pair.rideIndex] || usedChairs[pair.chairIndex] || chairs[pair.chairIndex].Speed <= 0 {
			t.Fatalf("duplicate or invalid assignment: %#v", pairs)
		}
		usedRides[pair.rideIndex], usedChairs[pair.chairIndex] = true, true
	}
	if got := selectAgeAwareChairMatches(nil, chairs, now); len(got) != 0 {
		t.Fatalf("empty ride set returned matches: %#v", got)
	}
	if got := selectAgeAwareChairMatches(rides, nil, now); len(got) != 0 {
		t.Fatalf("empty chair set returned matches: %#v", got)
	}
}

func BenchmarkAgeAwareGlobalMatching700x500(b *testing.B) {
	const rideCount, chairCount = 700, 500
	now := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	rides := make([]Ride, rideCount)
	chairs := make([]freeChair, chairCount)
	for i := range rides {
		rides[i] = Ride{PickupLatitude: i % 100, PickupLongitude: (i * 37) % 100, CreatedAt: now.Add(-time.Second)}
	}
	for i := range chairs {
		chairs[i] = freeChair{Speed: 1 + i%10, Latitude: (i * 13) % 100, Longitude: (i * 29) % 100}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = selectAgeAwareChairMatches(rides, chairs, now)
	}
}
