select
  date(pickup_datetime) as ride_date,
  count(*) as ride_count,
  sum(total_amount) as total_revenue,
  avg(trip_distance) as avg_distance,
  avg(fare_amount) as avg_fare
from `taxi_analytics.clean_trips`
group by 1
