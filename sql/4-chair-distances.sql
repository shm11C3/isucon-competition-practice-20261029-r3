-- Same definition as the former per-request aggregation in ownerGetChairs:
-- consecutive Manhattan distances ordered by (created_at, id).
INSERT INTO chair_distances (chair_id, total_distance, total_distance_updated_at, latitude, longitude)
SELECT chair_id,
       SUM(IFNULL(distance, 0)),
       MAX(created_at),
       MAX(CASE WHEN rn = 1 THEN latitude END),
       MAX(CASE WHEN rn = 1 THEN longitude END)
FROM (SELECT chair_id,
             created_at,
             latitude,
             longitude,
             ABS(latitude - LAG(latitude) OVER (PARTITION BY chair_id ORDER BY created_at, id)) +
             ABS(longitude - LAG(longitude) OVER (PARTITION BY chair_id ORDER BY created_at, id)) AS distance,
             ROW_NUMBER() OVER (PARTITION BY chair_id ORDER BY created_at DESC, id DESC) AS rn
      FROM chair_locations) tmp
GROUP BY chair_id;
