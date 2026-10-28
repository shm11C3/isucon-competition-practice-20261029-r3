package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Notification streams wait on these channels instead of polling the database.
// A status change is committed on whichever app host served it, so every write
// tells all hosts (ISURIDE_PEERS, this one included) which user and chair changed.
var (
	subsMu sync.Mutex
	subs   = map[string]map[chan struct{}]struct{}{}
)

func subscribe(key string) chan struct{} {
	ch := make(chan struct{}, 1)
	subsMu.Lock()
	if subs[key] == nil {
		subs[key] = map[chan struct{}]struct{}{}
	}
	subs[key][ch] = struct{}{}
	subsMu.Unlock()
	return ch
}

func unsubscribe(key string, ch chan struct{}) {
	subsMu.Lock()
	delete(subs[key], ch)
	if len(subs[key]) == 0 {
		delete(subs, key)
	}
	subsMu.Unlock()
}

func wakeLocal(keys ...string) {
	subsMu.Lock()
	for _, key := range keys {
		for ch := range subs[key] {
			select {
			case ch <- struct{}{}:
			default:
			}
		}
	}
	subsMu.Unlock()
}

var peerClient = &http.Client{Timeout: 2 * time.Second}

// notify wakes the streams of keys ("u:<user id>", "c:<chair id>") on every app host.
// Call it after the change is committed so the woken stream reads it.
func notify(keys ...string) {
	peers := strings.Split(os.Getenv("ISURIDE_PEERS"), ",")
	if len(peers) == 1 && peers[0] == "" {
		wakeLocal(keys...)
		return
	}
	body := strings.Join(keys, ",")
	for _, peer := range peers {
		go func(peer string) {
			res, err := peerClient.Post("http://"+peer+"/api/internal/notify", "text/plain", strings.NewReader(body))
			if err != nil {
				return
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}(peer)
	}
}

func internalPostNotify(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	wakeLocal(strings.Split(string(body), ",")...)
	w.WriteHeader(http.StatusNoContent)
}

// streamNotifications answers a notification endpoint as server-sent events.
// once returns the current payload (nil when there is nothing to show), a key
// that changes when the payload's ride or status changes, and whether it just
// marked an unsent status as sent; a marked status is followed by another read
// at once so a backlog drains in order. The stream ends well before the
// client's 60 s timeout; the client reconnects.
func streamNotifications(w http.ResponseWriter, r *http.Request, key string, once func() (any, string, bool, error)) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	wake := subscribe(key)
	defer unsubscribe(key, wake)

	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	deadline := time.Now().Add(25 * time.Second)
	last := ""
	first := true
	for {
		data, current, sent, err := once()
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("notification stream", "err", err)
			}
			return
		}
		if first && data == nil {
			// The frontend's first fetch waits for one chunk; a comment gives it one.
			// "data: null" would not do: the benchmarker fails to decode it.
			fmt.Fprint(w, ": waiting\n\n")
			flusher.Flush()
		}
		first = false
		if data != nil && (sent || current != last) {
			b, err := json.Marshal(data)
			if err != nil {
				return
			}
			if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
				return
			}
			flusher.Flush()
			last = current
		}
		if sent {
			continue
		}
		if time.Now().After(deadline) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-time.After(time.Second):
		}
	}
}
