from datetime import datetime

from pipelines.beam.transforms import build_clean_row
from pipelines.beam.transforms import parse_timestamp
from pipelines.beam.transforms import to_float
from pipelines.beam.transforms import to_int


def test_parse_timestamp_basic():
    assert parse_timestamp("2023-01-01 10:00:00") == datetime(2023, 1, 1, 10, 0, 0)


def test_numeric_helpers():
    assert to_int("3") == 3
    assert to_int("3.0") == 3
    assert to_float("2.5") == 2.5


def test_build_clean_row():
    row = {
        "VendorID": "1",
        "tpep_pickup_datetime": "2023-01-01 10:00:00",
        "tpep_dropoff_datetime": "2023-01-01 10:10:00",
        "passenger_count": "2",
        "trip_distance": "3.5",
        "RatecodeID": "1",
        "PULocationID": "148",
        "DOLocationID": "230",
        "payment_type": "1",
        "fare_amount": "12.5",
        "total_amount": "15.0",
    }
    clean = build_clean_row(row)
    assert clean["vendor_id"] == 1
    assert clean["trip_distance"] == 3.5
    assert clean["pickup_datetime"] == datetime(2023, 1, 1, 10, 0, 0)
