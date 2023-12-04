select
  {{ dbt_utils.star(ref('taxi_zone_lookup')) }}
from {{ ref('taxi_zone_lookup') }}