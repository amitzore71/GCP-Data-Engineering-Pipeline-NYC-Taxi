-- Train a fare prediction model using BigQuery ML
CREATE OR REPLACE MODEL `{{PROJECT_ID}}.{{BQ_DATASET}}.taxi_fare_model`
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['total_amount'],
  l2_reg = 0.1,
  max_iterations = 20
) AS
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
WHERE total_amount IS NOT NULL
