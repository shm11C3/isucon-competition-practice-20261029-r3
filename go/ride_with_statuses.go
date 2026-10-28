package main

import (
	"context"
	"database/sql"
)

type rideStatusRow struct {
	Ride   Ride       `db:"ride"`
	Status RideStatus `db:"rs"`
}

type executableSelect interface {
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

// latestRideWithStatuses reads a notification stream's ride and all of its
// statuses (oldest first) in one statement, so both come from one snapshot.
// pick is a subquery returning that ride's id with one ? for arg.
func latestRideWithStatuses(ctx context.Context, q executableSelect, pick string, arg any) (*Ride, []RideStatus, error) {
	rows := []rideStatusRow{}
	if err := q.SelectContext(ctx, &rows, `SELECT
		r.id AS `+"`ride.id`"+`, r.user_id AS `+"`ride.user_id`"+`, r.chair_id AS `+"`ride.chair_id`"+`,
		r.pickup_latitude AS `+"`ride.pickup_latitude`"+`, r.pickup_longitude AS `+"`ride.pickup_longitude`"+`,
		r.destination_latitude AS `+"`ride.destination_latitude`"+`, r.destination_longitude AS `+"`ride.destination_longitude`"+`,
		r.evaluation AS `+"`ride.evaluation`"+`, r.created_at AS `+"`ride.created_at`"+`, r.updated_at AS `+"`ride.updated_at`"+`,
		rs.id AS `+"`rs.id`"+`, rs.ride_id AS `+"`rs.ride_id`"+`, rs.status AS `+"`rs.status`"+`,
		rs.created_at AS `+"`rs.created_at`"+`, rs.app_sent_at AS `+"`rs.app_sent_at`"+`, rs.chair_sent_at AS `+"`rs.chair_sent_at`"+`
		FROM rides r JOIN ride_statuses rs ON rs.ride_id = r.id
		WHERE r.id = (`+pick+`)
		ORDER BY rs.created_at ASC`, arg); err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, sql.ErrNoRows
	}
	ride := rows[0].Ride
	statuses := make([]RideStatus, len(rows))
	for i, row := range rows {
		statuses[i] = row.Status
	}
	return &ride, statuses, nil
}
