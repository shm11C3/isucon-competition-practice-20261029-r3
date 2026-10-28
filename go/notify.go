package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// The frontend leaves its first fetch stream open when it switches to
// EventSource, and nginx may send the two to different hosts. Streams of one
// key share the database's sent cursor, so the older one could take a status
// the newer one never shows: a new stream ends every other stream of its key
// on all hosts and waits for them to exit before it reads anything, so its
// first event already reflects whatever the old stream consumed.
type streamHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var (
	streamsMu sync.Mutex
	streams   = map[string]map[string]streamHandle{}
)

func supersedeLocal(key, keep string) {
	streamsMu.Lock()
	var waits []chan struct{}
	for id, h := range streams[key] {
		if id != keep {
			h.cancel()
			waits = append(waits, h.done)
		}
	}
	streamsMu.Unlock()
	timeout := time.After(time.Second)
	for _, done := range waits {
		select {
		case <-done:
		case <-timeout:
			return
		}
	}
}

func supersede(key, keep string) {
	peers := strings.Split(os.Getenv("ISURIDE_PEERS"), ",")
	if len(peers) == 1 && peers[0] == "" {
		supersedeLocal(key, keep)
		return
	}
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			res, err := peerClient.Post("http://"+peer+"/api/internal/supersede", "text/plain", strings.NewReader(key+"\n"+keep))
			if err != nil {
				return
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}(peer)
	}
	wg.Wait()
}

func internalPostSupersede(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	key, keep, _ := strings.Cut(string(body), "\n")
	supersedeLocal(key, keep)
	w.WriteHeader(http.StatusNoContent)
}

// clearAuthCache drops cached access tokens on every app host and returns once
// they all answered, so no host authenticates a token of the previous run.
func clearAuthCache() {
	peers := strings.Split(os.Getenv("ISURIDE_PEERS"), ",")
	if len(peers) == 1 && peers[0] == "" {
		clearAuthCacheLocal()
		return
	}
	var wg sync.WaitGroup
	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			res, err := peerClient.Post("http://"+peer+"/api/internal/authclear", "text/plain", nil)
			if err != nil {
				return
			}
			io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}(peer)
	}
	wg.Wait()
	clearAuthCacheLocal()
}

func internalPostAuthClear(w http.ResponseWriter, r *http.Request) {
	clearAuthCacheLocal()
	w.WriteHeader(http.StatusNoContent)
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
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	idBytes := make([]byte, 8)
	rand.Read(idBytes)
	id := hex.EncodeToString(idBytes)
	done := make(chan struct{})
	streamsMu.Lock()
	if streams[key] == nil {
		streams[key] = map[string]streamHandle{}
	}
	streams[key][id] = streamHandle{cancel: cancel, done: done}
	streamsMu.Unlock()
	defer func() {
		close(done)
		streamsMu.Lock()
		delete(streams[key], id)
		if len(streams[key]) == 0 {
			delete(streams, key)
		}
		streamsMu.Unlock()
	}()
	supersede(key, id)

	wake := subscribe(key)
	defer unsubscribe(key, wake)

	header := w.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	deadline := time.Now().Add(25 * time.Second)
	last := ""
	first := true
	for {
		if ctx.Err() != nil {
			return
		}
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
		case <-time.After(2 * time.Second):
			// Every status change wakes its streams on all hosts; this only
			// recovers a wake lost to a failed peer call within the benchmark's
			// 3 s tolerance, and each read is a multi-query transaction.
		}
	}
}
