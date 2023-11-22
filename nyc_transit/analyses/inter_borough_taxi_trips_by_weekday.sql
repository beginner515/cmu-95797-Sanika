-- Calculate the total trips and inter-borough trips by weekday
WITH total_trips AS (
    SELECT
        WEEKDAY(pickup_datetime) AS weekday,
        COUNT(*) AS all_trips_count
    FROM mart__fact_all_taxi_trips tt
    GROUP BY all
),
inter_borough_trips AS (
    SELECT
        WEEKDAY(pickup_datetime) AS weekday,
        COUNT(*) AS inter_borough_count
    FROM mart__fact_all_taxi_trips tt
    JOIN mart__dim_locations pl ON tt.PUlocationID = pl.LocationID
    JOIN mart__dim_locations dl ON tt.DOlocationID = dl.LocationID
    WHERE pl.borough != dl.borough
    GROUP BY all
)
SELECT
    tt.weekday,
    tt.all_trips_count AS total_trips,
    it.inter_borough_count AS inter_borough_trips,
    CAST(it.inter_borough_count AS FLOAT) / tt.all_trips_count AS percent_inter_borough
FROM total_trips tt
JOIN inter_borough_trips it ON tt.weekday = it.weekday;