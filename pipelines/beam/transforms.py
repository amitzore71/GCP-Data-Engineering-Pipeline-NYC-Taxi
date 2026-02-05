from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

RAW_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
]

COLUMN_ALIASES = {
    "vendor_id": ["vendorid", "vendor_id"],
    "pickup_datetime": ["tpep_pickup_datetime", "pickup_datetime"],
    "dropoff_datetime": ["tpep_dropoff_datetime", "dropoff_datetime"],
    "passenger_count": ["passenger_count"],
    "trip_distance": ["trip_distance"],
    "ratecode_id": ["ratecodeid", "ratecode_id"],
    "store_and_fwd_flag": ["store_and_fwd_flag"],
    "pu_location_id": ["pulocationid", "pu_location_id"],
    "do_location_id": ["dolocationid", "do_location_id"],
    "payment_type": ["payment_type"],
    "fare_amount": ["fare_amount"],
    "extra": ["extra"],
    "mta_tax": ["mta_tax"],
    "tip_amount": ["tip_amount"],
    "tolls_amount": ["tolls_amount"],
    "improvement_surcharge": ["improvement_surcharge"],
    "total_amount": ["total_amount"],
    "congestion_surcharge": ["congestion_surcharge"],
    "airport_fee": ["airport_fee"],
}

RAW_ALIAS_MAP = {
    "VendorID": COLUMN_ALIASES["vendor_id"],
    "tpep_pickup_datetime": COLUMN_ALIASES["pickup_datetime"],
    "tpep_dropoff_datetime": COLUMN_ALIASES["dropoff_datetime"],
    "passenger_count": COLUMN_ALIASES["passenger_count"],
    "trip_distance": COLUMN_ALIASES["trip_distance"],
    "RatecodeID": COLUMN_ALIASES["ratecode_id"],
    "store_and_fwd_flag": COLUMN_ALIASES["store_and_fwd_flag"],
    "PULocationID": COLUMN_ALIASES["pu_location_id"],
    "DOLocationID": COLUMN_ALIASES["do_location_id"],
    "payment_type": COLUMN_ALIASES["payment_type"],
    "fare_amount": COLUMN_ALIASES["fare_amount"],
    "extra": COLUMN_ALIASES["extra"],
    "mta_tax": COLUMN_ALIASES["mta_tax"],
    "tip_amount": COLUMN_ALIASES["tip_amount"],
    "tolls_amount": COLUMN_ALIASES["tolls_amount"],
    "improvement_surcharge": COLUMN_ALIASES["improvement_surcharge"],
    "total_amount": COLUMN_ALIASES["total_amount"],
    "congestion_surcharge": COLUMN_ALIASES["congestion_surcharge"],
    "airport_fee": COLUMN_ALIASES["airport_fee"],
}

TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
]


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in row.items():
        if key is None:
            continue
        k = str(key).strip().lower()
        if isinstance(value, str):
            v = value.strip()
        else:
            v = value
        normalized[k] = v
    return normalized


def get_value(row: Dict[str, Any], aliases: list[str]) -> Optional[Any]:
    for alias in aliases:
        if alias in row and row[alias] not in (None, ""):
            return row[alias]
    return None


def parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip().replace("T", " ")
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def to_int(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None


def to_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def build_clean_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_row(row)
    pickup = parse_timestamp(get_value(normalized, COLUMN_ALIASES["pickup_datetime"]))
    dropoff = parse_timestamp(get_value(normalized, COLUMN_ALIASES["dropoff_datetime"]))

    return {
        "vendor_id": to_int(get_value(normalized, COLUMN_ALIASES["vendor_id"])),
        "pickup_datetime": pickup,
        "dropoff_datetime": dropoff,
        "passenger_count": to_int(get_value(normalized, COLUMN_ALIASES["passenger_count"])),
        "trip_distance": to_float(get_value(normalized, COLUMN_ALIASES["trip_distance"])),
        "ratecode_id": to_int(get_value(normalized, COLUMN_ALIASES["ratecode_id"])),
        "store_and_fwd_flag": get_value(normalized, COLUMN_ALIASES["store_and_fwd_flag"]),
        "pu_location_id": to_int(get_value(normalized, COLUMN_ALIASES["pu_location_id"])),
        "do_location_id": to_int(get_value(normalized, COLUMN_ALIASES["do_location_id"])),
        "payment_type": to_int(get_value(normalized, COLUMN_ALIASES["payment_type"])),
        "fare_amount": to_float(get_value(normalized, COLUMN_ALIASES["fare_amount"])),
        "extra": to_float(get_value(normalized, COLUMN_ALIASES["extra"])),
        "mta_tax": to_float(get_value(normalized, COLUMN_ALIASES["mta_tax"])),
        "tip_amount": to_float(get_value(normalized, COLUMN_ALIASES["tip_amount"])),
        "tolls_amount": to_float(get_value(normalized, COLUMN_ALIASES["tolls_amount"])),
        "improvement_surcharge": to_float(
            get_value(normalized, COLUMN_ALIASES["improvement_surcharge"])
        ),
        "total_amount": to_float(get_value(normalized, COLUMN_ALIASES["total_amount"])),
        "congestion_surcharge": to_float(
            get_value(normalized, COLUMN_ALIASES["congestion_surcharge"])
        ),
        "airport_fee": to_float(get_value(normalized, COLUMN_ALIASES["airport_fee"])),
    }


def build_raw_row(row: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_row(row)
    raw_row: Dict[str, Any] = {}
    for raw_col, aliases in RAW_ALIAS_MAP.items():
        raw_row[raw_col] = get_value(normalized, aliases)
    return raw_row


def is_valid_row(row: Dict[str, Any]) -> bool:
    if row.get("pickup_datetime") is None or row.get("dropoff_datetime") is None:
        return False
    if row["dropoff_datetime"] < row["pickup_datetime"]:
        return False
    trip_distance = row.get("trip_distance")
    if trip_distance is None or trip_distance < 0:
        return False
    passenger_count = row.get("passenger_count")
    if passenger_count is not None and passenger_count < 0:
        return False
    total_amount = row.get("total_amount")
    if total_amount is not None and total_amount < 0:
        return False
    return True
