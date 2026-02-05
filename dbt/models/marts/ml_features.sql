{{ config(materialized='table') }}

with base as (
  select
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    total_amount,
    payment_type,
    pu_location_id,
    do_location_id
  from {{ ref('stg_clean_trips') }}
  where total_amount is not null
    and trip_distance is not null
    and trip_distance >= 0
    and total_amount >= 0
),

features as (
  select
    total_amount,
    trip_distance,
    passenger_count,
    cast(payment_type as string) as payment_type,
    cast(pu_location_id as string) as pu_location_id,
    cast(do_location_id as string) as do_location_id,
    timestamp_diff(dropoff_datetime, pickup_datetime, minute) as trip_duration_minutes,
    extract(hour from pickup_datetime) as pickup_hour,
    extract(dayofweek from pickup_datetime) as pickup_dow
  from base
  where dropoff_datetime >= pickup_datetime
)

select *
from features
where trip_duration_minutes is not null
  and trip_duration_minutes >= 0
