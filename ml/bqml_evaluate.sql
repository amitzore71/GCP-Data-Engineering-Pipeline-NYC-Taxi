-- Evaluate the model
SELECT *
FROM ML.EVALUATE(
  MODEL `{{PROJECT_ID}}.{{BQ_DATASET}}.taxi_fare_model`,
  (
    SELECT
      total_amount,
      trip_distance,
      passenger_count,
      payment_type,
      pu_location_id,
      do_location_id,
      trip_duration_minutes,
      pickup_hour,
      pickup_dow
    FROM `{{PROJECT_ID}}.{{BQ_DATASET}}.ml_features`
  )
)
