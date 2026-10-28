package main

import (
	"context"
	"sync"
	"time"
)

type nearbyChairRow struct {
	ID        string `db:"id"`
	Name      string `db:"name"`
	Model     string `db:"model"`
	Latitude  int    `db:"latitude"`
	Longitude int    `db:"longitude"`
}

// Every nearby-chairs request needs the same list of free chairs with their
// latest positions, so each app host reads it at most once per interval and
// filters it by distance per request. The benchmark accepts positions up to
// 3 s old and chairs matched within the last 3 s, so a 200 ms old list is
// inside both tolerances; the response is stamped with the time of the read.
const freeChairSnapshotTTL = 200 * time.Millisecond

var (
	freeChairsMu sync.Mutex
	freeChairs   []nearbyChairRow
	freeChairsAt time.Time
)

func freeChairSnapshot(ctx context.Context) ([]nearbyChairRow, time.Time, error) {
	freeChairsMu.Lock()
	defer freeChairsMu.Unlock()
	if !freeChairsAt.IsZero() && time.Since(freeChairsAt) < freeChairSnapshotTTL {
		return freeChairs, freeChairsAt, nil
	}
	rows := []nearbyChairRow{}
	at := time.Now()
	// chair_distances holds each chair's latest coordinate; COMPLETED is written in the
	// same transaction that sets rides.evaluation, so an unfinished ride has none.
	if err := db.SelectContext(ctx, &rows, `SELECT c.id, c.name, c.model, cd.latitude, cd.longitude
		FROM chairs c
		JOIN chair_distances cd ON cd.chair_id = c.id
		WHERE c.is_active = TRUE
		  AND NOT EXISTS (
		    SELECT 1 FROM rides r
		    WHERE r.chair_id = c.id AND r.evaluation IS NULL
		  )
		ORDER BY c.id`); err != nil {
		return nil, time.Time{}, err
	}
	freeChairs, freeChairsAt = rows, at
	return rows, at, nil
}

// dropFreeChairSnapshot forgets the list of the previous run at initialize.
func dropFreeChairSnapshot() {
	freeChairsMu.Lock()
	freeChairs, freeChairsAt = nil, time.Time{}
	freeChairsMu.Unlock()
}
