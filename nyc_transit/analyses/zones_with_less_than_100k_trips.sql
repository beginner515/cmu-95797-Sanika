-- zones_with_less_than_100k_trips.sql

with combined_trips as (
    select
        cast(pulocationid as int) as location_id
    from mart__fact_all_taxi_trips
    where pulocationid is not null and pulocationid = pulocationid::int
)

select
    l.zone,
    count(*) as total_trips
from combined_trips ct
join mart__dim_locations l
on ct.location_id = l.locationid
group by l.zone
having count(*) < 100000