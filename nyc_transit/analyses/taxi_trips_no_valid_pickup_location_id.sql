-- taxi_trips_no_valid_pickup_location_id.sql

select
    tt.*
from mart__fact_all_taxi_trips tt
left join mart__dim_locations loc
on cast(tt.pulocationid as int) = loc.locationid
where loc.locationid is null;