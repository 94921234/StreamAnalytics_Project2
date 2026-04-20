import argparse
import json
from pathlib import Path
from typing import Any, Dict, List

from fastavro import parse_schema, writer


BASE_DIR = Path(__file__).resolve().parents[1]
SCHEMAS_DIR = BASE_DIR / "schemas"
DEFAULT_SAMPLE_DIR = BASE_DIR / "sample_data"


def load_schema(schema_name: str) -> Dict[str, Any]:
    with open(SCHEMAS_DIR / schema_name, "r", encoding="utf-8") as f:
        return parse_schema(json.load(f))


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in {path.name} at line {i}: {e}")
    return records


def _check_time(record: Dict[str, Any], file_name: str, idx: int) -> None:
    event_time = record.get("event_time")
    ingest_time = record.get("ingest_time")

    if event_time is None or ingest_time is None:
        raise ValueError(f"[{file_name} #{idx}] Missing event_time or ingest_time")

    if not isinstance(event_time, int) or not isinstance(ingest_time, int):
        raise ValueError(
            f"[{file_name} #{idx}] event_time and ingest_time must be integers (epoch milliseconds)"
        )

    if event_time > ingest_time:
        raise ValueError(
            f"[{file_name} #{idx}] event_time cannot be greater than ingest_time"
        )


def _check_lat_lon(record: Dict[str, Any], file_name: str, idx: int) -> None:
    lat = record.get("lat")
    lon = record.get("lon")

    if lat is not None:
        if not (-90.0 <= float(lat) <= 90.0):
            raise ValueError(f"[{file_name} #{idx}] Latitude out of range: {lat}")

    if lon is not None:
        if not (-180.0 <= float(lon) <= 180.0):
            raise ValueError(f"[{file_name} #{idx}] Longitude out of range: {lon}")


def _check_non_empty(record: Dict[str, Any], field: str, file_name: str, idx: int) -> None:
    value = record.get(field)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ValueError(f"[{file_name} #{idx}] Field '{field}' is null or empty")


def validate_records(
    records: List[Dict[str, Any]],
    file_name: str,
    feed_type: str,
    strict: bool,
) -> None:

    for idx, record in enumerate(records, start=1):
        _check_non_empty(record, "event_id", file_name, idx)
        _check_time(record, file_name, idx)

        if feed_type == "COURIER":
            _check_non_empty(record, "courier_id", file_name, idx)
            _check_non_empty(record, "zone_id", file_name, idx)
            _check_lat_lon(record, file_name, idx)

        if feed_type == "ORDER":
            _check_non_empty(record, "order_id", file_name, idx)
            _check_non_empty(record, "restaurant_id", file_name, idx)
            _check_non_empty(record, "zone_id", file_name, idx)

            if "order_value" in record and float(record["order_value"]) < 0:
                raise ValueError(
                    f"[{file_name} #{idx}] order_value cannot be negative: {record['order_value']}"
                )

    # Professional-level check: detect duplicate event_id values
    ids = [r.get("event_id") for r in records if r.get("event_id") is not None]
    duplicate_count = len(ids) - len(set(ids))

    if duplicate_count > 0:
        message = (
            f"[{file_name}] Found {duplicate_count} duplicate event_id values "
            f"(possible in at-least-once streaming delivery scenarios)."
        )

        if strict:
            raise ValueError(message)

        print(f"WARNING: {message}")


def write_avro(schema: Dict[str, Any], records: List[Dict[str, Any]], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "wb") as out:
        writer(out, schema, records)


def convert(
    feed_name: str,
    schema_file: str,
    jsonl_file: str,
    avro_file: str,
    outdir: Path,
    strict: bool,
) -> None:

    print(f"\nConverting {feed_name}...")

    schema = load_schema(schema_file)
    json_path = DEFAULT_SAMPLE_DIR / jsonl_file
    records = read_jsonl(json_path)

    feed_type = "COURIER" if "COURIER" in feed_name.upper() else "ORDER"

    validate_records(records, jsonl_file, feed_type, strict)

    out_path = outdir / avro_file
    write_avro(schema, records, out_path)

    print(f"OK -> {out_path} ({len(records)} records written)")


def main():
    parser = argparse.ArgumentParser(
        description="Generate AVRO sample files from JSONL inputs using AVSC schemas."
    )

    parser.add_argument(
        "--outdir",
        default=str(DEFAULT_SAMPLE_DIR),
        help="Output directory (default: sample_data/)",
    )

    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail if validation errors are detected.",
    )

    args = parser.parse_args()

    output_directory = Path(args.outdir)

    convert(
        "ORDER EVENTS",
        "order_events.avsc",
        "order_events_sample.jsonl",
        "order_events_sample.avro",
        output_directory,
        args.strict,
    )

    convert(
        "COURIER EVENTS",
        "courier_events.avsc",
        "courier_events_sample.jsonl",
        "courier_events_sample.avro",
        output_directory,
        args.strict,
    )


if __name__ == "__main__":
    main()
