-- Generate predictions and store them in a table
CREATE OR REPLACE TABLE `{{PROJECT_ID}}.{{BQ_DATASET}}.ml_predictions` AS
SELECT
  predicted_total_amount,
  total_amount,
  ABS(predicted_total_amount - total_amount) AS abs_error,
  trip_distance,
  passenger_count,
  payment_type,
  pu_location_id,
  do_location_id,
  trip_duration_minutes,
  pickup_hour,
  pickup_dow
FROM ML.PREDICT(
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
