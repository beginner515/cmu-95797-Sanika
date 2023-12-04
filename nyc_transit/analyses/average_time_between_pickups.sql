WITH PickupDiffs AS (
    SELECT
        tzt.Borough,
        tzt.Zone,
        ft.pickup_datetime,
        LAG(ft.pickup_datetime) OVER (
            PARTITION BY ft.PUlocationID
            ORDER BY ft.pickup_datetime
        ) AS prev_pickup_time
    FROM mart__fact_all_taxi_trips ft
    INNER JOIN taxi_zone_lookup tzt
        ON CAST(ft.PUlocationID AS INT) = tzt.LocationID  -- Cast as INT if necessary
),
TimeDiffs AS (
    SELECT
        Borough,
        Zone,
        pickup_datetime,
        EXTRACT(EPOCH FROM (pickup_datetime - prev_pickup_time)) / 60 AS time_diff_minutes -- Calculate the difference in minutes
    FROM PickupDiffs
    WHERE prev_pickup_time IS NOT NULL  -- Exclude the first trip which has no previous trip to compare to
)

SELECT
    Borough,
    Zone,
    AVG(time_diff_minutes) AS avg_time_between_pickups
FROM TimeDiffs
GROUP BY Borough, Zone
ORDER BY Borough, Zone;
