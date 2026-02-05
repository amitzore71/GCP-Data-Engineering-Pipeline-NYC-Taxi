import argparse
import os
import sys
import tempfile
from typing import List

import requests
from google.cloud import storage


def parse_months(months_value: str) -> List[str]:
    months = [m.strip() for m in months_value.split(",") if m.strip()]
    if not months:
        raise ValueError("--months must be a comma-separated list like 2023-01,2023-02")
    return months


def build_filename(month: str, file_format: str) -> str:
    return f"yellow_tripdata_{month}.{file_format}"


def download_file(url: str, output_path: str) -> None:
    with requests.get(url, stream=True, timeout=120) as response:
        response.raise_for_status()
        with open(output_path, "wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)


def upload_to_gcs(bucket_name: str, source_path: str, destination_blob: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_path)


def blob_exists(bucket_name: str, destination_blob: str) -> bool:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    return blob.exists()


def main() -> int:
    parser = argparse.ArgumentParser(description="Download NYC Taxi data and upload to GCS")
    parser.add_argument("--bucket", required=True, help="GCS bucket for raw data")
    parser.add_argument("--prefix", default="raw/nyc_taxi", help="GCS prefix for uploads")
    parser.add_argument("--months", required=True, help="Comma-separated months: 2023-01,2023-02")
    parser.add_argument(
        "--base-url",
        default="https://d37ci6vzurychx.cloudfront.net/trip-data",
        help="Base URL for source files",
    )
    parser.add_argument(
        "--file-format",
        default="csv",
        choices=["csv", "parquet"],
        help="File format to download",
    )
    parser.add_argument("--skip-existing", action="store_true")

    args = parser.parse_args()

    try:
        months = parse_months(args.months)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    base_url = args.base_url.rstrip("/")
    prefix = args.prefix.strip("/")

    for month in months:
        filename = build_filename(month, args.file_format)
        url = f"{base_url}/{filename}"
        destination = f"{prefix}/{filename}"

        if args.skip_existing and blob_exists(args.bucket, destination):
            print(f"Skipping existing file: gs://{args.bucket}/{destination}")
            continue

        print(f"Downloading {url}")
        with tempfile.TemporaryDirectory() as tempdir:
            local_path = os.path.join(tempdir, filename)
            download_file(url, local_path)
            print(f"Uploading to gs://{args.bucket}/{destination}")
            upload_to_gcs(args.bucket, local_path, destination)

    print("Done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
