provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  raw_bucket_name        = var.raw_bucket_name != "" ? var.raw_bucket_name : "${var.project_id}-raw-data"
  processed_bucket_name  = var.processed_bucket_name != "" ? var.processed_bucket_name : "${var.project_id}-processed-data"
  curated_bucket_name    = var.curated_bucket_name != "" ? var.curated_bucket_name : "${var.project_id}-curated-data"
  dataflow_temp_bucket   = var.dataflow_temp_bucket_name != "" ? var.dataflow_temp_bucket_name : "${var.project_id}-dataflow-temp"
  dataflow_staging_bucket = var.dataflow_staging_bucket_name != "" ? var.dataflow_staging_bucket_name : "${var.project_id}-dataflow-staging"
}

resource "google_storage_bucket" "raw" {
  name                        = local.raw_bucket_name
  location                    = var.location
  force_destroy               = var.bucket_force_destroy
  uniform_bucket_level_access = true
  labels                      = var.labels
}

resource "google_storage_bucket" "processed" {
  name                        = local.processed_bucket_name
  location                    = var.location
  force_destroy               = var.bucket_force_destroy
  uniform_bucket_level_access = true
  labels                      = var.labels
}

resource "google_storage_bucket" "curated" {
  name                        = local.curated_bucket_name
  location                    = var.location
  force_destroy               = var.bucket_force_destroy
  uniform_bucket_level_access = true
  labels                      = var.labels
}

resource "google_storage_bucket" "dataflow_temp" {
  name                        = local.dataflow_temp_bucket
  location                    = var.location
  force_destroy               = var.bucket_force_destroy
  uniform_bucket_level_access = true
  labels                      = var.labels
}

resource "google_storage_bucket" "dataflow_staging" {
  name                        = local.dataflow_staging_bucket
  location                    = var.location
  force_destroy               = var.bucket_force_destroy
  uniform_bucket_level_access = true
  labels                      = var.labels
}

resource "google_bigquery_dataset" "taxi" {
  dataset_id  = var.bq_dataset_id
  location    = var.bq_location
  description = "NYC taxi analytics dataset"
  labels      = var.labels
}

resource "google_service_account" "dataflow_sa" {
  account_id   = "dataflow-runner-sa"
  display_name = "Dataflow Runner Service Account"
}

resource "google_service_account" "composer_sa" {
  account_id   = "composer-sa"
  display_name = "Cloud Composer Service Account"
}

locals {
  composer_roles = toset([
    "roles/composer.worker",
    "roles/dataflow.developer",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
  ])

  dataflow_roles = toset([
    "roles/dataflow.worker",
    "roles/bigquery.dataEditor",
    "roles/storage.objectAdmin",
  ])
}

resource "google_project_iam_member" "composer_roles" {
  for_each = local.composer_roles
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "dataflow_roles" {
  for_each = local.dataflow_roles
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.dataflow_sa.email}"
}

resource "google_service_account_iam_member" "composer_impersonate_dataflow" {
  service_account_id = google_service_account.dataflow_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_composer_environment" "composer" {
  provider = google-beta
  name     = var.composer_env_name
  region   = var.region

  config {
    software_config {
      image_version = var.composer_image_version
      pypi_packages = var.composer_pypi_packages
      env_variables = {
        GCP_PROJECT      = var.project_id
        REGION           = var.region
        RAW_BUCKET       = local.raw_bucket_name
        PROCESSED_BUCKET = local.processed_bucket_name
        CURATED_BUCKET   = local.curated_bucket_name
        BQ_DATASET       = var.bq_dataset_id
        DATAFLOW_TEMP    = local.dataflow_temp_bucket
        DATAFLOW_STAGING = local.dataflow_staging_bucket
        DATAFLOW_SA      = google_service_account.dataflow_sa.email
        DATA_FILE_FORMAT = var.data_file_format
        DATA_MONTHS      = var.data_months
        DATA_BASE_URL    = var.data_base_url
        DBT_TARGET       = "composer"
      }
    }

    node_config {
      service_account = google_service_account.composer_sa.email
    }
  }

  depends_on = [
    google_project_iam_member.composer_roles,
    google_project_iam_member.dataflow_roles,
    google_service_account_iam_member.composer_impersonate_dataflow,
  ]
}
