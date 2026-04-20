"""
eventhub_producer.py - Milestone 2.1
Streams synthetic food-delivery events from the M1 generator to Azure Event Hubs.

Feeds:
  - group04_orders
  - group04_courierevents

Usage:
    pip install azure-eventhub python-dotenv
    python eventhub_producer.py
    python eventhub_producer.py --batch-size 20
    python eventhub_producer.py --from-file data.json

Environment variables:
    EVENTHUB_CONNECTION_STRING
    ORDERS_EVENTHUB_NAME      (default: group04_orders)
    COURIERS_EVENTHUB_NAME    (default: group04_courierevents)
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError
from dotenv import load_dotenv

load_dotenv()

CONNECTION_STRING = os.getenv("EVENTHUB_CONNECTION_STRING", "").strip()
ORDERS_HUB = os.getenv("ORDERS_EVENTHUB_NAME", "group_04_orders")
COURIERS_HUB = os.getenv("COURIERS_EVENTHUB_NAME", "group_04_courierevents")


def get_hub_name(event: dict) -> str:
    return COURIERS_HUB if "courier_status" in event else ORDERS_HUB


def get_partition_key(event: dict) -> str:
    return str(event.get("zone_id", ""))


def send_event(producers: dict, event: dict) -> None:
    hub = get_hub_name(event)
    batch = producers[hub].create_batch(
        partition_key=get_partition_key(event)
    )
    batch.add(EventData(json.dumps(event).encode("utf-8")))
    producers[hub].send_batch(batch)


def send_batch(producers: dict, events: list) -> None:
    grouped = {
        ORDERS_HUB: [],
        COURIERS_HUB: []
    }

    for event in events:
        grouped[get_hub_name(event)].append(event)

    for hub, hub_events in grouped.items():
        if not hub_events:
            continue

        producer = producers[hub]
        pkey = get_partition_key(hub_events[0])
        batch = producer.create_batch(partition_key=pkey)

        for event in hub_events:
            try:
                batch.add(
                    EventData(json.dumps(event).encode("utf-8"))
                )
            except ValueError:
                producer.send_batch(batch)
                batch = producer.create_batch(partition_key=pkey)
                batch.add(
                    EventData(json.dumps(event).encode("utf-8"))
                )

        producer.send_batch(batch)


def stream_live(producers: dict, interval: float = 1.0) -> None:
    gen_dir = Path(__file__).resolve().parent.parent.parent / "generator"
    sys.path.insert(0, str(gen_dir))

    from config import Config
    from generator import DataGenerator

    config = Config()
    generator = DataGenerator(config)

    print(f"[Producer] Streaming live | interval={interval}s")

    order_events, courier_events = generator.run()

    all_events = order_events + courier_events
    all_events.sort(key=lambda e: (e["event_time"], e["ingest_time"]))

    sent = 0

    try:
        for event in all_events:
            send_event(producers, event)
            sent += 1

            if sent % 50 == 0:
                print(f"Sent {sent} events...")

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"Stopped. Total sent: {sent}")

def stream_from_file(producers: dict, filepath: str, batch_size: int) -> None:
    path = Path(filepath)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    print(f"[Producer] Replaying {filepath} | batch_size={batch_size}")

    batch = []
    total = 0

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()

            if not line:
                continue

            batch.append(json.loads(line))

            if len(batch) >= batch_size:
                send_batch(producers, batch)
                total += len(batch)
                batch = []

    if batch:
        send_batch(producers, batch)
        total += len(batch)

    print(f"Done. Total sent: {total}")

print("ORDERS_HUB =", ORDERS_HUB)
print("COURIERS_HUB =", COURIERS_HUB)
def main() -> None:
    p = argparse.ArgumentParser(
        description="Group 04 - Azure Event Hubs Producer"
    )

    p.add_argument("--from-file", type=str, default=None)
    p.add_argument("--batch-size", type=int, default=10)
    p.add_argument("--interval", type=float, default=1.0)
    p.add_argument("--connection-string", type=str, default=None)

    args = p.parse_args()

    conn_str = (args.connection_string or CONNECTION_STRING).strip()
    
    print("DEBUG repr(conn_str):", repr(conn_str))
    print("DEBUG startswith Endpoint:", conn_str.startswith("Endpoint="))
    print("DEBUG contains key name:", "SharedAccessKeyName=" in conn_str)

    if not conn_str:
        raise ValueError(
            "EVENTHUB_CONNECTION_STRING is not set."
        )

    producers = {
        ORDERS_HUB: EventHubProducerClient.from_connection_string(
            conn_str,
            eventhub_name=ORDERS_HUB
        ),
        COURIERS_HUB: EventHubProducerClient.from_connection_string(
            conn_str,
            eventhub_name=COURIERS_HUB
        ),
    }

    try:
        if args.from_file:
            stream_from_file(
                producers,
                args.from_file,
                args.batch_size
            )
        else:
            stream_live(
                producers,
                interval=args.interval
            )

    except EventHubError as e:
        print(f"EventHubError: {e}")
        sys.exit(1)

    finally:
        for producer in producers.values():
            producer.close()


if __name__ == "__main__":
    main()
