variable "project_id" {
  type        = string
  description = "GCP project id"
}

variable "region" {
  type        = string
  description = "GCP region"
  default     = "us-central1"
}

variable "location" {
  type        = string
  description = "GCS bucket location"
  default     = "US"
}

variable "bq_location" {
  type        = string
  description = "BigQuery dataset location"
  default     = "US"
}

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery dataset id"
  default     = "taxi_analytics"
}

variable "raw_bucket_name" {
  type        = string
  description = "Override name for raw bucket"
  default     = ""
}

variable "processed_bucket_name" {
  type        = string
  description = "Override name for processed bucket"
  default     = ""
}

variable "curated_bucket_name" {
  type        = string
  description = "Override name for curated bucket"
  default     = ""
}

variable "dataflow_temp_bucket_name" {
  type        = string
  description = "Override name for Dataflow temp bucket"
  default     = ""
}

variable "dataflow_staging_bucket_name" {
  type        = string
  description = "Override name for Dataflow staging bucket"
  default     = ""
}

variable "composer_env_name" {
  type        = string
  description = "Cloud Composer environment name"
  default     = "taxi-composer"
}

variable "composer_image_version" {
  type        = string
  description = "Composer image version"
  default     = "composer-2.6.1-airflow-2.7.3"
}

variable "data_base_url" {
  type        = string
  description = "Base URL for taxi data downloads"
  default     = "https://d37ci6vzurychx.cloudfront.net/trip-data"
}

variable "data_months" {
  type        = string
  description = "Default months for ingestion"
  default     = "2023-01"
}

variable "data_file_format" {
  type        = string
  description = "Default file format for ingestion"
  default     = "csv"
}

variable "bucket_force_destroy" {
  type        = bool
  description = "Whether to force destroy buckets"
  default     = true
}

variable "labels" {
  type        = map(string)
  description = "Common labels"
  default     = {}
}

variable "composer_pypi_packages" {
  type        = map(string)
  description = "Extra PyPI packages for Composer"
  default = {
    "apache-beam[gcp]" = ">=2.50,<2.60"
    "pandas"           = ">=2.0,<3.0"
    "pandas-gbq"       = ">=0.20,<0.25"
    "great_expectations" = ">=0.17,<0.19"
    "dbt-bigquery"     = ">=1.7,<1.9"
    "pyarrow"          = ">=12,<16"
    "requests"         = ">=2.31,<3.0"
  }
}
