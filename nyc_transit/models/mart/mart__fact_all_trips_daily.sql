select
    type,
    date_trunc('day', started_at_ts)::date as date,
    count(*) over () as total_trip_count,
    avg(duration_min) as average_trip_duration_min
from {{ ref('mart__fact_all_trips') }}
group by type, date