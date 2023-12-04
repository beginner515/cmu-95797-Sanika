-- mart__fact_trips_by_borough.sql

WITH trips_fact AS (
  SELECT
    l.borough,
    COUNT(*) AS trip_count
  FROM {{ ref('mart__fact_all_taxi_trips') }} t
    JOIN {{ ref('mart__dim_locations') }} l ON t.pulocationid = l.locationid
  GROUP BY
    l.borough
)

SELECT
  borough,
  trip_count
FROM
  trips_fact



















