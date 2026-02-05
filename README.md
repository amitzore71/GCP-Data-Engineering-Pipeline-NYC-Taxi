# GCP Data Engineering Pipeline — NYC Taxi

This project is a full end-to-end data engineering pipeline built on GCP using real public NYC Taxi data. It covers everything from raw data ingestion to analytics and ML, with proper orchestration and data quality checks in between. The goal is to show how production-grade data systems are actually built, not just toy demos.

## What This Shows

This project demonstrates how to:

- Ingest raw files into Google Cloud Storage
- Process large batch data using Apache Beam on Dataflow
- Enforce data quality with Great Expectations
- Build analytics models using dbt
- Train and run ML models directly inside BigQuery using BigQuery ML
- Orchestrate the entire workflow with Cloud Composer (Airflow)

In short: raw data → clean data → analytics → ML → dashboards, all automated.

## Architecture (High Level)

Flow:

Data Source (NYC Taxi)  
→ GCS (raw layer)  
→ Dataflow (transform + load)  
→ BigQuery (raw + clean + marts)  
→ Great Expectations (data quality checks)  
→ dbt (analytics layer)  
→ BigQuery ML (model training + predictions)  
→ Looker Studio (dashboards)  
→ Cloud Composer (orchestration)

Refer to `docs/architecture.md` for the detailed architecture diagram.

## Why This Project Is Solid for a Portfolio

- Uses real-world scale data (not fake CSVs)
- Production-style pipeline (infra, orchestration, data quality)
- Covers batch ingestion, transformation, analytics, and ML
- Shows cloud-native tooling (GCP) used in real companies
- Clean separation of raw, clean, and analytics layers
- End-to-end ownership: infra → pipelines → models → dashboards

## Tech Stack

- Cloud Storage – raw data lake
- Apache Beam + Dataflow – batch processing
- BigQuery – data warehouse
- Great Expectations – data quality
- dbt – analytics modeling
- BigQuery ML – model training and inference
- Cloud Composer (Airflow) – orchestration
- Terraform – infrastructure
- Looker Studio – reporting

## What You Can Talk About in Interviews

You can confidently discuss:

- Designing bronze/silver/gold style layers in BigQuery
- Handling schema changes in upstream taxi datasets
- Choosing Dataflow over SQL-only transformations
- Enforcing data quality before analytics runs
- End-to-end orchestration in Airflow
- Using BigQuery ML to avoid unnecessary data movement
- Cost, performance, and scaling tradeoffs on GCP

## Real-World Extensions (Optional)

If you want to push this further:

- Add CDC or streaming ingestion with Pub/Sub
- Add data freshness SLAs and alerts in Airflow
- Add anomaly detection on fares and trip duration
- Add cost optimization using partitioning and clustering in BigQuery
- Add feature store style tables for ML workloads
