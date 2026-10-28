package main

import (
	"context"
	"sync"
)

// Rows that never change once they exist, cached per process to take reads off
// the notification streams. A ride's fare is fixed when it is created (its
// coupon is attached in the same transaction), and chairs and users keep their
// names. IDs are never reused, so entries stay valid across initialize; they
// are dropped there anyway to bound memory.
var (
	fareByRide   sync.Map // ride ID -> int
	chairByID    sync.Map // chair ID -> *Chair
	userNameByID sync.Map // user ID -> *User
)

func clearStaticCacheLocal() {
	for _, m := range []*sync.Map{&fareByRide, &chairByID, &userNameByID} {
		m.Range(func(k, _ any) bool {
			m.Delete(k)
			return true
		})
	}
}

func cachedChair(ctx context.Context, q executableGet, id string) (*Chair, error) {
	if v, ok := chairByID.Load(id); ok {
		return v.(*Chair), nil
	}
	chair := &Chair{}
	if err := q.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, id); err != nil {
		return nil, err
	}
	chairByID.Store(id, chair)
	return chair, nil
}

func cachedUser(ctx context.Context, q executableGet, id string) (*User, error) {
	if v, ok := userNameByID.Load(id); ok {
		return v.(*User), nil
	}
	user := &User{}
	if err := q.GetContext(ctx, user, `SELECT * FROM users WHERE id = ?`, id); err != nil {
		return nil, err
	}
	userNameByID.Store(id, user)
	return user, nil
}
