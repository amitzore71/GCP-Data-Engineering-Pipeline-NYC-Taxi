import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ID = os.environ.get("GCP_PROJECT") or os.environ.get("PROJECT_ID")
REGION = os.environ.get("REGION", "us-central1")
RAW_BUCKET = os.environ.get("RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("PROCESSED_BUCKET")
BQ_DATASET = os.environ.get("BQ_DATASET", "taxi_analytics")
DATAFLOW_TEMP = os.environ.get("DATAFLOW_TEMP")
DATAFLOW_STAGING = os.environ.get("DATAFLOW_STAGING")
DATAFLOW_SA = os.environ.get("DATAFLOW_SA", "")
DATA_FILE_FORMAT = os.environ.get("DATA_FILE_FORMAT", "csv")
DATA_MONTHS = os.environ.get("DATA_MONTHS", "2023-01")
DATA_BASE_URL = os.environ.get("DATA_BASE_URL", "")
DBT_TARGET = os.environ.get("DBT_TARGET", "composer")

DAGS_HOME = "/home/airflow/gcs/dags"

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def bq_command(sql_file: str) -> str:
    return (
        'sed "s/{{{{PROJECT_ID}}}}/{project}/g; s/{{{{BQ_DATASET}}}}/{dataset}/g" '
        "{dags}/ml/{sql_file} | bq query --use_legacy_sql=false --project_id {project}"
    ).format(
        project=PROJECT_ID,
        dataset=BQ_DATASET,
        dags=DAGS_HOME,
        sql_file=sql_file,
    )


with DAG(
    dag_id="taxi_batch_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["portfolio", "dataflow", "dbt", "ml"],
) as dag:
    ingest = BashOperator(
        task_id="ingest_to_gcs",
        bash_command=(
            "python {dags}/scripts/fetch_and_upload.py "
            "--bucket {raw_bucket} "
            "--prefix raw/nyc_taxi "
            "--months {months} "
            "--file-format {file_format} "
            "--base-url {base_url} "
            "--skip-existing"
        ).format(
            dags=DAGS_HOME,
            raw_bucket=RAW_BUCKET,
            months=DATA_MONTHS,
            file_format=DATA_FILE_FORMAT,
            base_url=DATA_BASE_URL,
        ),
    )

    dataflow = BashOperator(
        task_id="run_dataflow_batch",
        bash_command=(
            "python {dags}/pipelines/beam/batch_pipeline.py "
            "--input gs://{raw_bucket}/raw/nyc_taxi/yellow_tripdata_*.{file_format} "
            "--file_format {file_format} "
            "--output_dataset {dataset} "
            "--output_clean_path gs://{processed_bucket}/clean/{{{{ ds_nodash }}}}/clean_trips "
            "--runner DataflowRunner "
            "--project {project} "
            "--region {region} "
            "--temp_location gs://{temp_bucket}/temp "
            "--staging_location gs://{staging_bucket}/staging "
            "--bq_temp_location gs://{temp_bucket}/bq_temp "
            "--service_account_email {dataflow_sa} "
            "--requirements_file {dags}/pipelines/beam/requirements.txt"
        ).format(
            dags=DAGS_HOME,
            raw_bucket=RAW_BUCKET,
            file_format=DATA_FILE_FORMAT,
            dataset=BQ_DATASET,
            project=PROJECT_ID,
            region=REGION,
            temp_bucket=DATAFLOW_TEMP,
            staging_bucket=DATAFLOW_STAGING,
            dataflow_sa=DATAFLOW_SA,
            processed_bucket=PROCESSED_BUCKET,
        ),
    )

    ge_validate = BashOperator(
        task_id="great_expectations",
        bash_command=(
            "python {dags}/quality/run_ge.py "
            "--gcs-prefix gs://{processed_bucket}/clean/{{{{ ds_nodash }}}}/clean_trips "
            "--max-rows 50000 "
            "--max-files 5"
        ).format(dags=DAGS_HOME, processed_bucket=PROCESSED_BUCKET),
    )

    load_clean_to_bq = BashOperator(
        task_id="load_clean_to_bq",
        bash_command=(
            "bq load --source_format=PARQUET "
            "--project_id {project} "
            "{project}:{dataset}.clean_trips "
            "gs://{processed_bucket}/clean/{{{{ ds_nodash }}}}/clean_trips-*.parquet"
        ).format(project=PROJECT_ID, dataset=BQ_DATASET, processed_bucket=PROCESSED_BUCKET),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "cd {dags}/dbt && "
            "dbt deps --profiles-dir . && "
            "dbt run --profiles-dir . --target {target}"
        ).format(dags=DAGS_HOME, target=DBT_TARGET),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "cd {dags}/dbt && "
            "dbt deps --profiles-dir . && "
            "dbt test --profiles-dir . --target {target}"
        ).format(dags=DAGS_HOME, target=DBT_TARGET),
    )

    bqml_train = BashOperator(
        task_id="bqml_train",
        bash_command=bq_command("bqml_train.sql"),
    )

    bqml_evaluate = BashOperator(
        task_id="bqml_evaluate",
        bash_command=bq_command("bqml_evaluate.sql"),
    )

    bqml_predict = BashOperator(
        task_id="bqml_predict",
        bash_command=bq_command("bqml_predict.sql"),
    )

    (
        ingest
        >> dataflow
        >> ge_validate
        >> load_clean_to_bq
        >> dbt_run
        >> dbt_test
        >> bqml_train
        >> bqml_evaluate
        >> bqml_predict
    )
