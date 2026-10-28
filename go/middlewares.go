package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
)

// Access tokens never change once issued and handlers only read IDs from the
// authenticated row, so each token is looked up in MySQL once per process.
var (
	userByToken  sync.Map
	ownerByToken sync.Map
	chairByToken sync.Map
)

func appAuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		c, err := r.Cookie("app_session")
		if errors.Is(err, http.ErrNoCookie) || c.Value == "" {
			writeError(w, http.StatusUnauthorized, errors.New("app_session cookie is required"))
			return
		}
		accessToken := c.Value
		if v, ok := userByToken.Load(accessToken); ok {
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

		userByToken.Store(accessToken, user)
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
		if v, ok := ownerByToken.Load(accessToken); ok {
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

		ownerByToken.Store(accessToken, owner)
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
		if v, ok := chairByToken.Load(accessToken); ok {
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

		chairByToken.Store(accessToken, chair)
		ctx = context.WithValue(ctx, "chair", chair)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
