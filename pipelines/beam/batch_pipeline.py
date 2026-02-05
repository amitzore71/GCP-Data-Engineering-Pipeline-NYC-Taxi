import argparse
import csv
import io
import logging

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.parquetio import ReadFromParquet
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp import bigquery as beam_bq

try:
    from pipelines.beam.transforms import build_clean_row
    from pipelines.beam.transforms import build_raw_row
    from pipelines.beam.transforms import is_valid_row
except ModuleNotFoundError:
    from transforms import build_clean_row
    from transforms import build_raw_row
    from transforms import is_valid_row

BQ_CLEAN_SCHEMA = (
    "vendor_id:INTEGER,"
    "pickup_datetime:TIMESTAMP,"
    "dropoff_datetime:TIMESTAMP,"
    "passenger_count:INTEGER,"
    "trip_distance:FLOAT,"
    "ratecode_id:INTEGER,"
    "store_and_fwd_flag:STRING,"
    "pu_location_id:INTEGER,"
    "do_location_id:INTEGER,"
    "payment_type:INTEGER,"
    "fare_amount:FLOAT,"
    "extra:FLOAT,"
    "mta_tax:FLOAT,"
    "tip_amount:FLOAT,"
    "tolls_amount:FLOAT,"
    "improvement_surcharge:FLOAT,"
    "total_amount:FLOAT,"
    "congestion_surcharge:FLOAT,"
    "airport_fee:FLOAT"
)


def read_csv_file(readable_file):
    with readable_file.open() as handle:
        text = io.TextIOWrapper(handle)
        reader = csv.DictReader(text)
        for row in reader:
            if row:
                yield row


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="GCS file pattern")
    parser.add_argument(
        "--file_format",
        default="csv",
        choices=["csv", "parquet"],
        help="Input file format",
    )
    parser.add_argument("--output_dataset", required=True, help="BigQuery dataset")
    parser.add_argument("--output_project", default=None, help="BigQuery project")
    parser.add_argument("--raw_table", default="raw_trips")
    parser.add_argument("--clean_table", default="clean_trips")
    parser.add_argument("--write_raw", action="store_true")
    parser.add_argument("--bq_temp_location", default=None)

    args, beam_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    gcp_options = pipeline_options.view_as(GoogleCloudOptions)
    project = args.output_project or gcp_options.project
    if not project:
        raise ValueError("GCP project is required via --project or --output_project")

    bq_temp_location = args.bq_temp_location or gcp_options.temp_location

    raw_table_spec = f"{project}:{args.output_dataset}.{args.raw_table}"
    clean_table_spec = f"{project}:{args.output_dataset}.{args.clean_table}"

    logging.info("Reading input: %s", args.input)
    logging.info("Output dataset: %s", args.output_dataset)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        if args.file_format == "parquet":
            rows = pipeline | "ReadParquet" >> ReadFromParquet(args.input)
        else:
            rows = (
                pipeline
                | "MatchFiles" >> fileio.MatchFiles(args.input)
                | "ReadMatches" >> fileio.ReadMatches()
                | "ParseCSV" >> beam.FlatMap(read_csv_file)
            )

        clean_rows = (
            rows
            | "BuildCleanRow" >> beam.Map(build_clean_row)
            | "FilterValid" >> beam.Filter(is_valid_row)
        )

        if args.write_raw:
            raw_rows = rows | "BuildRawRow" >> beam.Map(build_raw_row)
            _ = (
                raw_rows
                | "WriteRawToBQ"
                >> WriteToBigQuery(
                    raw_table_spec,
                    schema=beam_bq.SCHEMA_AUTODETECT,
                    method=WriteToBigQuery.Method.FILE_LOADS,
                    custom_gcs_temp_location=bq_temp_location,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                )
            )

        _ = (
            clean_rows
            | "WriteCleanToBQ"
            >> WriteToBigQuery(
                clean_table_spec,
                schema=BQ_CLEAN_SCHEMA,
                method=WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=bq_temp_location,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
