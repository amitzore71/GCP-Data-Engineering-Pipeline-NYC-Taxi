from datetime import datetime

from pipelines.beam.transforms import is_valid_row


def test_invalid_distance():
    row = {
        "pickup_datetime": datetime(2023, 1, 1, 10, 0, 0),
        "dropoff_datetime": datetime(2023, 1, 1, 10, 10, 0),
        "trip_distance": -1.0,
    }
    assert not is_valid_row(row)


def test_invalid_time_order():
    row = {
        "pickup_datetime": datetime(2023, 1, 1, 10, 10, 0),
        "dropoff_datetime": datetime(2023, 1, 1, 10, 0, 0),
        "trip_distance": 1.0,
    }
    assert not is_valid_row(row)


def test_valid_row():
    row = {
        "pickup_datetime": datetime(2023, 1, 1, 10, 0, 0),
        "dropoff_datetime": datetime(2023, 1, 1, 10, 10, 0),
        "trip_distance": 1.0,
        "passenger_count": 1,
        "total_amount": 10.0,
    }
    assert is_valid_row(row)
