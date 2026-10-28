package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
)

// Access tokens never change once issued and handlers only read IDs from the
// authenticated row, so each token is looked up in MySQL once per generation.
// initialize swaps in an empty generation on every app host; a lookup stores
// into the generation it started in, so a read that races the swap cannot
// bring a token of the previous run back.
type authGeneration struct {
	user, owner, chair sync.Map
}

var authCache atomic.Pointer[authGeneration]

func init() { authCache.Store(&authGeneration{}) }

func clearAuthCacheLocal() { authCache.Store(&authGeneration{}) }

func appAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("app_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("app_session cookie is required"))
			return
		}
		accessToken := c.Value
		gen := authCache.Load()
		if v, ok := gen.user.Load(accessToken); ok {
			next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, "user", v.(*User))))
			return
		}
		user := &User{}
		err = db.GetContext(ctx, user, "SELECT * FROM users WHERE access_token = ?", accessToken)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		gen.user.Store(accessToken, user)
		ctx = context.WithValue(ctx, "user", user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func ownerAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("owner_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("owner_session cookie is required"))
			return
		}
		accessToken := c.Value
		gen := authCache.Load()
		if v, ok := gen.owner.Load(accessToken); ok {
			next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, "owner", v.(*Owner))))
			return
		}
		owner := &Owner{}
		if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE access_token = ?", accessToken); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		gen.owner.Store(accessToken, owner)
		ctx = context.WithValue(ctx, "owner", owner)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func chairAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("chair_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("chair_session cookie is required"))
			return
		}
		accessToken := c.Value
		gen := authCache.Load()
		if v, ok := gen.chair.Load(accessToken); ok {
			next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, "chair", v.(*Chair))))
			return
		}
		chair := &Chair{}
		err = db.GetContext(ctx, chair, "SELECT * FROM chairs WHERE access_token = ?", accessToken)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusUnauthorized, errors.New("invalid access token"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		gen.chair.Store(accessToken, chair)
		ctx = context.WithValue(ctx, "chair", chair)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
