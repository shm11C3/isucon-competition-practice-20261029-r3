package main

import (
	"net/http"
)

type freeChair struct {
	ID        string `db:"id"`
	Speed     int    `db:"speed"`
	Latitude  int    `db:"latitude"`
	Longitude int    `db:"longitude"`
}

// このAPIをインスタンス内から一定間隔で叩かせることで、椅子とライドをマッチングさせる
// 待ち時間の長いライドから順に、到着が最も早い空き椅子を割り当てる
func internalGetMatching(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	rides := []Ride{}
	if err := db.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE chair_id IS NULL ORDER BY created_at`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if len(rides) == 0 {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 空き椅子: 割り当て済みのすべてのライドで6状態すべてが椅子へ通知済み。
	// A ride has all six statuses once it is evaluated (COMPLETED is written with the
	// evaluation), so a chair is busy while it has an unevaluated ride or an unsent
	// status; both sets are small, unlike counting sent statuses for every past ride.
	busyIDs := []string{}
	if err := db.SelectContext(ctx, &busyIDs, `
SELECT chair_id FROM rides WHERE evaluation IS NULL AND chair_id IS NOT NULL
UNION
SELECT r.chair_id FROM ride_statuses rs JOIN rides r ON r.id = rs.ride_id WHERE rs.chair_sent_at IS NULL AND r.chair_id IS NOT NULL`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	busy := make(map[string]bool, len(busyIDs))
	for _, id := range busyIDs {
		busy[id] = true
	}
	active := []freeChair{}
	if err := db.SelectContext(ctx, &active, `
SELECT c.id, cm.speed, cd.latitude, cd.longitude
FROM chairs c
JOIN chair_models cm ON cm.name = c.model
JOIN chair_distances cd ON cd.chair_id = c.id
WHERE c.is_active = TRUE`); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	chairs := active[:0]
	for _, c := range active {
		if !busy[c.ID] {
			chairs = append(chairs, c)
		}
	}

	used := make(map[string]bool, len(chairs))
	for _, ride := range rides {
		best := -1
		bestTime := 0
		for i, c := range chairs {
			if used[c.ID] || c.Speed <= 0 {
				continue
			}
			t := calculateDistance(c.Latitude, c.Longitude, ride.PickupLatitude, ride.PickupLongitude) * 1000 / c.Speed
			if best < 0 || t < bestTime {
				best, bestTime = i, t
			}
		}
		if best < 0 {
			break
		}
		res, err := db.ExecContext(ctx, "UPDATE rides SET chair_id = ? WHERE id = ? AND chair_id IS NULL", chairs[best].ID, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if n, _ := res.RowsAffected(); n == 1 {
			used[chairs[best].ID] = true
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
