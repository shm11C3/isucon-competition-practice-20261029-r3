package main

import (
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jmoiron/sqlx"
)

// Chair positions are written to chair_distances in batches. nginx sends all
// requests of one chair to the same app host, so its coordinates queue here in
// the order they were posted, and one multi-row upsert applies them in that
// order: each row adds its distance from the row before it, and the latest
// position and its time are stored together. Readers see a position at most
// one flush interval late.
type pendingCoordinate struct {
	chairID    string
	recordedAt time.Time
	latitude   int
	longitude  int
}

var (
	coordinatesMu sync.Mutex
	coordinates   []pendingCoordinate
	// flushMu keeps one flush in the database at a time so batches keep their order.
	flushMu sync.Mutex
	flushDB *sqlx.DB
)

const coordinateFlushInterval = 50 * time.Millisecond

func queueCoordinate(c pendingCoordinate) {
	coordinatesMu.Lock()
	coordinates = append(coordinates, c)
	coordinatesMu.Unlock()
}

// dropPendingCoordinates forgets queued positions of the previous run; initialize
// calls it on every host.
func dropPendingCoordinates() {
	flushMu.Lock()
	coordinatesMu.Lock()
	coordinates = nil
	coordinatesMu.Unlock()
	flushMu.Unlock()
	placedChairs.Range(func(k, _ any) bool { placedChairs.Delete(k); return true })
}

func flushCoordinates() {
	flushMu.Lock()
	defer flushMu.Unlock()
	flushLocked()
}

// placedChairs are the chairs this host has written a position for since start
// or initialize. A chair's coordinates all come to one host (see nginx), so the
// first one seen here is the chair's first since activation.
var placedChairs sync.Map

// placeCoordinate stores a chair's position. A new chair's first position is
// written before the chair is answered and added to every host's free-chair
// list: the benchmark counts a chair as waiting for a ride from the moment its
// activation returns, and a nearby-chairs answer that still lacks it (one
// buffer interval plus one list interval, about 250 ms) is a missing chair.
// Practice R3: 72 of 76 such errors in one run were chairs 0-300 ms after
// activation. Dropping the lists instead made every new chair cost each host a
// full re-read, and the late-run bursts of new chairs timed nearby-chairs out.
// Later positions are batched.
func placeCoordinate(c pendingCoordinate, chair *Chair) {
	if _, ok := placedChairs.Load(c.chairID); ok {
		queueCoordinate(c)
		return
	}
	flushMu.Lock()
	queueCoordinate(c)
	flushLocked()
	flushMu.Unlock()
	placedChairs.Store(c.chairID, struct{}{})
	addFreeChairEverywhere(nearbyChairRow{ID: chair.ID, Name: chair.Name, Model: chair.Model, Latitude: c.latitude, Longitude: c.longitude})
}

func flushLocked() {
	coordinatesMu.Lock()
	batch := coordinates
	coordinates = nil
	coordinatesMu.Unlock()
	if len(batch) == 0 {
		return
	}
	var sb strings.Builder
	sb.WriteString(`INSERT INTO chair_distances (chair_id, total_distance, total_distance_updated_at, latitude, longitude) VALUES `)
	args := make([]any, 0, len(batch)*4)
	for i, c := range batch {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString("(?, 0, ?, ?, ?)")
		args = append(args, c.chairID, c.recordedAt, c.latitude, c.longitude)
	}
	// MySQL evaluates the assignments left to right, so the distance uses the old position.
	sb.WriteString(` ON DUPLICATE KEY UPDATE
		total_distance = total_distance + ABS(VALUES(latitude) - latitude) + ABS(VALUES(longitude) - longitude),
		total_distance_updated_at = VALUES(total_distance_updated_at),
		latitude = VALUES(latitude),
		longitude = VALUES(longitude)`)
	for attempt := 0; ; attempt++ {
		_, err := flushDB.Exec(sb.String(), args...)
		if err == nil {
			return
		}
		if attempt >= 3 {
			// Chairs were already answered for these positions: put them back in
			// front of newer ones for the next flush instead of losing them.
			slog.Error("flush chair coordinates", "err", err, "rows", len(batch))
			coordinatesMu.Lock()
			coordinates = append(batch, coordinates...)
			coordinatesMu.Unlock()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func startCoordinateFlusher() {
	go func() {
		for range time.Tick(coordinateFlushInterval) {
			flushCoordinates()
		}
	}()
	// systemd stops the service with SIGTERM: write what is queued first.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-stop
		flushCoordinates()
		os.Exit(0)
	}()
}
