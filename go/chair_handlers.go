package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"
)

type chairPostChairsRequest struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	ChairRegisterToken string `json:"chair_register_token"`
}

type chairPostChairsResponse struct {
	ID      string `json:"id"`
	OwnerID string `json:"owner_id"`
}

func chairPostChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &chairPostChairsRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if req.Name == "" || req.Model == "" || req.ChairRegisterToken == "" {
		writeError(w, http.StatusBadRequest, errors.New("some of required fields(name, model, chair_register_token) are empty"))
		return
	}

	owner := &Owner{}
	if err := db.GetContext(ctx, owner, "SELECT * FROM owners WHERE chair_register_token = ?", req.ChairRegisterToken); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusUnauthorized, errors.New("invalid chair_register_token"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	chairID := ulid.Make().String()
	accessToken := secureRandomStr(32)

	_, err := db.ExecContext(
		ctx,
		"INSERT INTO chairs (id, owner_id, name, model, is_active, access_token) VALUES (?, ?, ?, ?, ?, ?)",
		chairID, owner.ID, req.Name, req.Model, false, accessToken,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "chair_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &chairPostChairsResponse{
		ID:      chairID,
		OwnerID: owner.ID,
	})
}

type postChairActivityRequest struct {
	IsActive bool `json:"is_active"`
}

func chairPostActivity(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)

	req := &postChairActivityRequest{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	_, err := db.ExecContext(ctx, "UPDATE chairs SET is_active = ? WHERE id = ?", req.IsActive, chair.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type chairPostCoordinateResponse struct {
	RecordedAt int64 `json:"recorded_at"`
}

func chairPostCoordinate(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &Coordinate{}
	if err := bindJSON(r, req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	chair := ctx.Value("chair").(*Chair)

	// A chair posts its coordinates one at a time, so the ride lookup, the
	// status insert and the position upsert need no shared transaction: each
	// statement commits on its own, which cuts the round trips of every tick.
	ride := struct {
		ID                   string         `db:"id"`
		UserID               string         `db:"user_id"`
		PickupLatitude       int            `db:"pickup_latitude"`
		PickupLongitude      int            `db:"pickup_longitude"`
		DestinationLatitude  int            `db:"destination_latitude"`
		DestinationLongitude int            `db:"destination_longitude"`
		Status               sql.NullString `db:"status"`
	}{}
	statusChanged := false
	if err := db.GetContext(ctx, &ride, `
SELECT r.id, r.user_id, r.pickup_latitude, r.pickup_longitude, r.destination_latitude, r.destination_longitude,
       (SELECT rs.status FROM ride_statuses rs WHERE rs.ride_id = r.id ORDER BY rs.created_at DESC LIMIT 1) AS status
FROM rides r WHERE r.chair_id = ? ORDER BY r.updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	} else {
		next := ""
		if req.Latitude == ride.PickupLatitude && req.Longitude == ride.PickupLongitude && ride.Status.String == "ENROUTE" {
			next = "PICKUP"
		} else if req.Latitude == ride.DestinationLatitude && req.Longitude == ride.DestinationLongitude && ride.Status.String == "CARRYING" {
			next = "ARRIVED"
		}
		if next != "" {
			if _, err := db.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, next); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
			statusChanged = true
		}
	}

	// Timestamp the coordinate after the ride lookup: a slow lookup must not
	// backdate a position. The value is stored and returned as is, so owners see
	// the same time the chair was told (see coordinate_buffer.go).
	recordedAt := time.Now().Truncate(time.Microsecond)

	queueCoordinate(pendingCoordinate{chairID: chair.ID, recordedAt: recordedAt, latitude: req.Latitude, longitude: req.Longitude})

	if statusChanged {
		notify("c:"+chair.ID, "u:"+ride.UserID)
	}

	writeJSON(w, http.StatusOK, &chairPostCoordinateResponse{
		RecordedAt: recordedAt.UnixMilli(),
	})
}

type simpleUser struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type chairGetNotificationResponse struct {
	Data         *chairGetNotificationResponseData `json:"data"`
	RetryAfterMs int                               `json:"retry_after_ms"`
}

type chairGetNotificationResponseData struct {
	RideID                string     `json:"ride_id"`
	User                  simpleUser `json:"user"`
	PickupCoordinate      Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate `json:"destination_coordinate"`
	Status                string     `json:"status"`
}

func chairNotificationOnce(ctx context.Context, chair *Chair) (*chairGetNotificationResponseData, bool, bool, error) {

	ride := &Ride{}
	yetSentRideStatus := RideStatus{}
	status := ""

	if err := db.GetContext(ctx, ride, `SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC LIMIT 1`, chair.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, false, nil
		}
		return nil, false, false, err
	}

	// 未送信のうち最古のステータス、なければ最新のステータスを 1 クエリで決める
	rideStatuses := []RideStatus{}
	if err := db.SelectContext(ctx, &rideStatuses, `SELECT * FROM ride_statuses WHERE ride_id = ? ORDER BY created_at ASC`, ride.ID); err != nil {
		return nil, false, false, err
	}
	if len(rideStatuses) == 0 {
		return nil, false, false, sql.ErrNoRows
	}
	more := false
	for i, rs := range rideStatuses {
		if rs.ChairSentAt == nil {
			yetSentRideStatus = rs
			for _, later := range rideStatuses[i+1:] {
				if later.ChairSentAt == nil {
					more = true
					break
				}
			}
			break
		}
	}
	if yetSentRideStatus.ID != "" {
		status = yetSentRideStatus.Status
	} else {
		status = rideStatuses[len(rideStatuses)-1].Status
	}

	user := &User{}
	err := db.GetContext(ctx, user, "SELECT * FROM users WHERE id = ?", ride.UserID)
	if err != nil {
		return nil, false, false, err
	}

	if yetSentRideStatus.ID != "" {
		_, err := db.ExecContext(ctx, `UPDATE ride_statuses SET chair_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, yetSentRideStatus.ID)
		if err != nil {
			return nil, false, false, err
		}
	}

	return &chairGetNotificationResponseData{
		RideID: ride.ID,
		User: simpleUser{
			ID:   user.ID,
			Name: fmt.Sprintf("%s %s", user.Firstname, user.Lastname),
		},
		PickupCoordinate: Coordinate{
			Latitude:  ride.PickupLatitude,
			Longitude: ride.PickupLongitude,
		},
		DestinationCoordinate: Coordinate{
			Latitude:  ride.DestinationLatitude,
			Longitude: ride.DestinationLongitude,
		},
		Status: status,
	}, yetSentRideStatus.ID != "", more, nil
}

func chairGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	chair := ctx.Value("chair").(*Chair)
	streamNotifications(w, r, "c:"+chair.ID, func() (any, string, bool, bool, error) {
		data, sent, more, err := chairNotificationOnce(ctx, chair)
		if err != nil || data == nil {
			return nil, "", sent, more, err
		}
		return data, data.RideID + data.Status, sent, more, nil
	})
}

type postChairRidesRideIDStatusRequest struct {
	Status string `json:"status"`
}

func chairPostRideStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	chair := ctx.Value("chair").(*Chair)

	req := &postChairRidesRideIDStatusRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, "SELECT * FROM rides WHERE id = ? FOR UPDATE", rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if ride.ChairID.String != chair.ID {
		writeError(w, http.StatusBadRequest, errors.New("not assigned to this ride"))
		return
	}

	switch req.Status {
	// Acknowledge the ride
	case "ENROUTE":
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "ENROUTE"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	// After Picking up user
	case "CARRYING":
		status, err := getLatestRideStatus(ctx, tx, ride.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if status != "PICKUP" {
			writeError(w, http.StatusBadRequest, errors.New("chair has not arrived yet"))
			return
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)", ulid.Make().String(), ride.ID, "CARRYING"); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	default:
		writeError(w, http.StatusBadRequest, errors.New("invalid status"))
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	notify("c:"+chair.ID, "u:"+ride.UserID)

	w.WriteHeader(http.StatusNoContent)
}
