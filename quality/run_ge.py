import argparse
import json
import sys

import great_expectations as ge
import pandas_gbq


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Great Expectations checks on BigQuery data")
    parser.add_argument("--project", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--table", default="clean_trips")
    parser.add_argument("--max-rows", type=int, default=50000)

    args = parser.parse_args()

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
