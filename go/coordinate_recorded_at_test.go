package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
)

// Run only against a disposable MySQL fixture. A delayed ride lookup must not
// backdate an externally visible coordinate by the time spent waiting for it.
func TestCoordinateRecordedAtAfterDelayedRideLookup(t *testing.T) {
	dsn := os.Getenv("ISUCON_TEST_DSN")
	if dsn == "" {
		t.Skip("ISUCON_TEST_DSN must select a disposable MySQL fixture")
	}
	fixture, err := sqlx.Connect("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer fixture.Close()
	previous := db
	db = fixture
	defer func() { db = previous }()
	const chairID = "repair-nearby-clock-test"
	defer fixture.Exec("DELETE FROM chair_locations WHERE chair_id = ?", chairID)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	blocker, err := fixture.Connx(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	if _, err := blocker.ExecContext(ctx, "LOCK TABLES rides WRITE"); err != nil {
		t.Fatal(err)
	}
	defer blocker.ExecContext(context.Background(), "UNLOCK TABLES")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/chair/coordinate", strings.NewReader(`{"latitude":123,"longitude":456}`))
	req = req.WithContext(context.WithValue(ctx, "chair", &Chair{ID: chairID}))
	done := make(chan struct{})
	go func() {
		defer close(done)
		chairPostCoordinate(recorder, req)
	}()
	// The official stale-position tolerance is three seconds. Block only the
	// ride lookup; inserts and the final commit retain their normal behavior.
	time.Sleep(3200 * time.Millisecond)
	unblockedAt := time.Now()
	if _, err := blocker.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response chairPostCoordinateResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.RecordedAt < unblockedAt.Add(-200*time.Millisecond).UnixMilli() {
		t.Fatalf("coordinate recorded_at predates its visibility by blocked lookup: recorded=%s lookup_released=%s", time.UnixMilli(response.RecordedAt), unblockedAt)
	}
}
