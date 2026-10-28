package main

import (
	"context"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
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
	snapshotDB   *sqlx.DB
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
	at := time.Now()
	// A chair counts as busy from its accept (ENROUTE) on, as the benchmarker
	// counts it: a chair matched but not yet accepted is still listed, since the
	// benchmarker expects every chair it sees idle in nearby results (code 31)
	// and only flags a listed chair whose accepted ride is unfinished (code 30).
	// COMPLETED is written with rides.evaluation, so unfinished rides are the few
	// without one; three indexed reads keep this cheap (a single NOT EXISTS over
	// ride_statuses timed nearby requests out, R3 T-0036).
	active := []nearbyChairRow{}
	if err := snapshotDB.SelectContext(ctx, &active, `SELECT c.id, c.name, c.model, cd.latitude, cd.longitude
		FROM chairs c
		JOIN chair_distances cd ON cd.chair_id = c.id
		WHERE c.is_active = TRUE
		ORDER BY c.id`); err != nil {
		return nil, time.Time{}, err
	}
	unfinished := []struct {
		ID      string `db:"id"`
		ChairID string `db:"chair_id"`
	}{}
	if err := snapshotDB.SelectContext(ctx, &unfinished, `SELECT id, chair_id FROM rides WHERE evaluation IS NULL AND chair_id IS NOT NULL`); err != nil {
		return nil, time.Time{}, err
	}
	busy := map[string]bool{}
	if len(unfinished) > 0 {
		chairOfRide := make(map[string]string, len(unfinished))
		ids := make([]string, len(unfinished))
		for i, r := range unfinished {
			chairOfRide[r.ID] = r.ChairID
			ids[i] = r.ID
		}
		query, args, err := sqlx.In(`SELECT DISTINCT ride_id FROM ride_statuses WHERE ride_id IN (?) AND status = 'ENROUTE'`, ids)
		if err != nil {
			return nil, time.Time{}, err
		}
		accepted := []string{}
		if err := snapshotDB.SelectContext(ctx, &accepted, query, args...); err != nil {
			return nil, time.Time{}, err
		}
		for _, rideID := range accepted {
			busy[chairOfRide[rideID]] = true
		}
	}
	rows := make([]nearbyChairRow, 0, len(active))
	for _, c := range active {
		if !busy[c.ID] {
			rows = append(rows, c)
		}
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
