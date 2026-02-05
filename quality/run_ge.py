import argparse
import json
import os
import sys
import tempfile
from typing import Tuple

import great_expectations as ge
import pandas as pd
import pandas_gbq
import pyarrow.parquet as pq
from google.cloud import storage


def build_expectations(df):
    dataset = ge.from_pandas(df)
    results = []
    results.append(dataset.expect_column_values_to_not_be_null("pickup_datetime"))
    results.append(dataset.expect_column_values_to_not_be_null("dropoff_datetime"))
    results.append(dataset.expect_column_values_to_not_be_null("trip_distance"))
    results.append(dataset.expect_column_values_to_not_be_null("total_amount"))
    results.append(dataset.expect_column_values_to_be_between("trip_distance", min_value=0))
    results.append(dataset.expect_column_values_to_be_between("total_amount", min_value=0))
    results.append(dataset.expect_column_values_to_be_between("passenger_count", min_value=0))
    return results


def summarize(results):
    summary = {
        "success": all(result.get("success") for result in results),
        "checks": [],
    }
    for result in results:
        summary["checks"].append(
            {
                "expectation": result["expectation_config"]["expectation_type"],
                "success": result["success"],
            }
        )
    return summary


def parse_gcs_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError("GCS URI must start with gs://")
    path = uri[5:]
    if "/" not in path:
        return path, ""
    bucket, prefix = path.split("/", 1)
    return bucket, prefix


def load_parquet_from_gcs(prefix_uri: str, max_files: int, max_rows: int) -> pd.DataFrame:
    bucket_name, prefix = parse_gcs_uri(prefix_uri)
    client = storage.Client()
    parquet_blobs = []
    for blob in client.list_blobs(bucket_name, prefix=prefix):
        if blob.name.endswith(".parquet"):
            parquet_blobs.append(blob)
            if len(parquet_blobs) >= max_files:
                break

    if not parquet_blobs:
        raise RuntimeError(f"No parquet files found at {prefix_uri}")

    frames = []
    total_rows = 0

    for blob in parquet_blobs[:max_files]:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        tmp.close()
        try:
            blob.download_to_filename(tmp.name)
            table = pq.read_table(tmp.name)
            df_part = table.to_pandas()
        finally:
            if os.path.exists(tmp.name):
                os.remove(tmp.name)

        if df_part.empty:
            continue

        if max_rows:
            remaining = max_rows - total_rows
            if remaining <= 0:
                break
            if len(df_part) > remaining:
                df_part = df_part.head(remaining)

        frames.append(df_part)
        total_rows += len(df_part)

        if max_rows and total_rows >= max_rows:
            break

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Great Expectations checks on clean data")
    parser.add_argument("--project", required=False)
    parser.add_argument("--dataset", required=False)
    parser.add_argument("--table", default="clean_trips")
    parser.add_argument(
        "--gcs-prefix",
        default=None,
        help="GCS prefix for parquet files, e.g. gs://bucket/path/clean_trips",
    )
    parser.add_argument("--max-rows", type=int, default=50000)
    parser.add_argument("--max-files", type=int, default=5)

    args = parser.parse_args()

    if args.gcs_prefix:
        df = load_parquet_from_gcs(args.gcs_prefix, args.max_files, args.max_rows)
    else:
        if not args.project or not args.dataset:
            print("Missing --project/--dataset for BigQuery validation", file=sys.stderr)
            return 2
        query = (
            f"SELECT * FROM `{args.project}.{args.dataset}.{args.table}` "
            f"WHERE pickup_datetime IS NOT NULL LIMIT {args.max_rows}"
        )
        df = pandas_gbq.read_gbq(query, project_id=args.project, use_bqstorage_api=False)

    if df.empty:
        print("No rows returned for validation", file=sys.stderr)
        return 2

    results = build_expectations(df)
    summary = summarize(results)
    print(json.dumps(summary, indent=2, default=str))

    if not summary["success"]:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
