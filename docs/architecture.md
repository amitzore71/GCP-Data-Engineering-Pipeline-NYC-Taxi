# Architecture

```mermaid
graph LR
  A[NYC Taxi File Drops] -->|Download| B[Ingestion Script]
  B --> C[GCS Raw Bucket]
  C --> D[Dataflow / Beam Batch]
  D --> E[GCS Processed (clean parquet)]
  E --> F[Great Expectations]
  F --> G[BigQuery clean_trips]
  G --> H[dbt Models]
  H --> I[BigQuery marts]
  I --> J[BigQuery ML]
  J --> K[ml_predictions]
  I --> L[Looker Studio]
  K --> L
```

## Layers
- Raw: files stored in GCS.
- Clean: typed records in GCS, then loaded into BigQuery after validation.
- Marts: dbt aggregates for reporting.
- ML: BigQuery ML model + predictions.
