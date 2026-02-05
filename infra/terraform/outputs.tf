output "raw_bucket" {
  value = google_storage_bucket.raw.name
}

output "processed_bucket" {
  value = google_storage_bucket.processed.name
}

output "curated_bucket" {
  value = google_storage_bucket.curated.name
}

output "dataflow_temp_bucket" {
  value = google_storage_bucket.dataflow_temp.name
}

output "dataflow_staging_bucket" {
  value = google_storage_bucket.dataflow_staging.name
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.taxi.dataset_id
}

output "dataflow_service_account" {
  value = google_service_account.dataflow_sa.email
}

output "composer_service_account" {
  value = google_service_account.composer_sa.email
}

output "composer_environment" {
  value = google_composer_environment.composer.name
}

output "composer_dag_bucket" {
  value = google_composer_environment.composer.config[0].dag_gcs_prefix
}
