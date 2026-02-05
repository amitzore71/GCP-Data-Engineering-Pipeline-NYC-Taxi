select
  pu_location_id,
  count(*) as ride_count,
  avg(total_amount) as avg_total_amount,
  avg(trip_distance) as avg_distance
from {{ ref('stg_clean_trips') }}
group by 1
