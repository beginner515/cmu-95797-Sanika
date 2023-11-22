-- Calculate the total number of taxi trips ending in 'Airports' or 'EWR' service zones
SELECT COUNT(*) AS trips_ending_at_airport
FROM mart__fact_all_taxi_trips tt
JOIN mart__dim_locations dl ON tt.DOlocationID = dl.LocationID
WHERE dl.service_zone IN ('Airports', 'EWR')
GROUP BY all;