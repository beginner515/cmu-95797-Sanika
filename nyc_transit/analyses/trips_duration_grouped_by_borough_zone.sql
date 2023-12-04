-- trips_duration_grouped_by_borough_zone.sql

with extended_trip_data as (
    select
        cast(pulocationid as int) as location_id,  -- Ensure pulocationid is compatible with locationid
        duration_min
    from mart__fact_all_taxi_trips
    where pulocationid is not null and pulocationid = pulocationid::int
),

trip_data_with_location as (
    select
        etd.duration_min,
        dl.borough,
        dl.zone
    from extended_trip_data etd
    join mart__dim_locations dl
    on etd.location_id = dl.locationid
)

select
    borough,
    zone,
    count(*) as total_trips,
    avg(duration_min) as average_duration
from trip_data_with_location
group by borough, zone