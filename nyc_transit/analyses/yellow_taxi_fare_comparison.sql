-- yellow_taxi_fare_comparison.sql

select
    yt.tpep_pickup_datetime,
    yt.fare_amount,
    loc.borough,
    loc.zone,
    avg(yt.fare_amount) over (partition by loc.borough) as avg_fare_borough,
    avg(yt.fare_amount) over (partition by loc.zone) as avg_fare_zone,
    avg(yt.fare_amount) over () as avg_fare_overall
from yellow_tripdata yt
join mart__dim_locations loc
on yt.pulocationid = loc.locationid;
