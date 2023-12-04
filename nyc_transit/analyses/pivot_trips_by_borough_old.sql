-- pivot_trips_by_borough.sql

select
    sum(case when borough = 'Brooklyn' then trip_count else 0 end) as trips_brooklyn,
    sum(case when borough = 'Queens' then trip_count else 0 end) as trips_queens,
    sum(case when borough = 'Unknown' then trip_count else 0 end) as trips_unknown,
    sum(case when borough = 'Bronx' then trip_count else 0 end) as trips_bronx,
    sum(case when borough = 'EWR' then trip_count else 0 end) as trips_ewr,
    sum(case when borough = 'Manhattan' then trip_count else 0 end) as trips_manhattan,
    sum(case when borough = 'Staten Island' then trip_count else 0 end) as trips_staten_island
from mart__fact_trips_by_borough