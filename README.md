# GCP Data Engineering Pipeline — NYC Taxi

A production-grade, end-to-end data engineering pipeline built on Google Cloud Platform using real NYC Taxi public data.

This project demonstrates how modern data platforms are actually designed in the real world — not toy examples. It covers ingestion, large-scale processing, data quality, analytics modeling, ML inside the warehouse, and full orchestration.

---

## What I Built

This project implements a complete data platform:

- Raw data ingestion into Google Cloud Storage
- Large-scale batch processing using Apache Beam on Dataflow
- Data quality validation using Great Expectations
- Analytics modeling using dbt
- ML model training and predictions using BigQuery ML
- End-to-end orchestration using Cloud Composer (Airflow)
- Automated infrastructure using Terraform

**End-to-end flow:**  
Raw data → Clean data → Analytics models → ML → Dashboards (fully automated)

---

## Architecture (High Level)

**Flow:**

NYC Taxi Data Source  
→ Google Cloud Storage (raw layer)  
→ Dataflow (transform)  
→ Google Cloud Storage (processed clean parquet)  
→ Great Expectations (data quality checks)  
→ BigQuery (clean, marts)  
→ dbt (analytics layer)  
→ BigQuery ML (model training + predictions)  
→ Looker Studio (dashboards)  
→ Cloud Composer (orchestration)

Detailed architecture is available in `docs/architecture.md`.

---

## Tech Stack

- Google Cloud Storage
- Apache Beam + Dataflow
- BigQuery
- Great Expectations
- dbt
- BigQuery ML
- Cloud Composer (Airflow)
- Terraform
- Looker Studio

---

## Key Engineering Highlights

- Designed multi-layer data architecture (raw, clean, marts)
- Implemented production-style batch processing on Dataflow
- Enforced data quality checks before analytics and ML
- Built analytics-ready models using dbt
- Trained ML models directly inside BigQuery (no data movement)
- Orchestrated the full pipeline using Airflow
- Provisioned cloud infrastructure using Terraform
- Optimized BigQuery tables using partitioning and clustering

---

## Future Enhancements

- Add streaming ingestion using Pub/Sub
- Implement data freshness SLAs and alerting
- Add anomaly detection on fares and trip duration
- Introduce feature-store style tables for ML workloads
- Add cost monitoring and automated budget alerts

---
