with source as (
  select *
  from {{ source('taxi', 'clean_trips') }}
)

select
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pu_location_id,
  do_location_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  airport_fee
from source
