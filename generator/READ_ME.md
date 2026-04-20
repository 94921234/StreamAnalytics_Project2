# 🍕 Food Delivery Synthetic Data Generator — Madrid

<<<<<<< HEAD
> Synthetic event stream generator for a food delivery platform set in Madrid. Built for stream analytics testing — watermarks, event-time processing, late data, and all the messy stuff that makes pipelines interesting.


---

## What is this?

This generates two realistic event streams that mimic how a food delivery app would behave in Madrid: **order events** (lifecycle of each delivery) and **courier events** (what couriers are doing and where they are). The data is intentionally imperfect — it ships with configurable edge cases like out-of-order events, duplicates, and missing FSM steps so you can actually test your pipeline handles them correctly.

Everything is tunable: number of restaurants, couriers, simulation duration, how dirty the data is, rush hour patterns, and more.

---

## Project Structure

```
.
├── main.py               # CLI entry point — start here
├── config.py             # Config dataclass + EdgeCaseProbabilities
├── demand_model.py       # Madrid traffic model + hourly demand curves
├── models.py             # Enums, MadridZone definitions, EntityPool
├── generator.py          # The main engine — event generation + edge case injection
├── serialize_samples.py  # Converts JSONL → Avro (needs fastavro)
├── config.json           # Example config file
└── output/               # Generated automatically on first run
    ├── order_events.jsonl
    ├── courier_events.jsonl
    ├── order_events.avro
    ├── courier_events.avro
    └── schemas/
        ├── order_event.avsc
        └── courier_event.avsc
```

---

## Getting Started

**Optional dependency** (only needed for Avro output):

```bash
pip install fastavro
```

Everything else runs on Python 3.10+ with no extra dependencies.

### Running it

```bash
# Default — 500 orders, 16 hours, starting Monday 08:00
python main.py

# Use a config file
python main.py --config config.json

# Override specific things from the CLI
python main.py --num-orders 2000 --sim-hours 24 --output-dir ./data

# Simulate a weekend (Saturday → +40% demand kicks in automatically)
python main.py --sim-start "2025-01-11 10:00:00"

# Scale up the fleet
python main.py --num-restaurants 30 --num-couriers 80 --num-orders 1000
```

**Config priority:** `dataclass defaults` < `config.json` < `CLI flags` — CLI always wins.

---

## Event Schemas

### Order Events

Tracks each order as it moves through the system.

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string (UUID) | Unique per event |
| `event_time` | ISO-8601 | When it actually happened |
| `ingest_time` | ISO-8601 | When it hit the broker — can be later than `event_time` |
| `order_id` | string | Ties events to the same order |
| `event_type` | enum | `ORDER_CREATED` → `ORDER_ACCEPTED` → `PREP_STARTED` → `READY_FOR_PICKUP` → `PICKED_UP` → `DELIVERED` \| `CANCELLED` |
| `order_status` | string | Current state of the order |
| `restaurant_id` | string | Which restaurant |
| `zone_id` | string | Madrid neighbourhood |
| `courier_id` | string? | `null` until the order is accepted |
| `order_value` | double | Amount in euros |
| `cancel_reason` | string? | Only present on `CANCELLED` events |

### Courier Events

Tracks courier movements and status changes.

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string (UUID) | Unique per event |
| `event_time` | ISO-8601 | When it happened |
| `ingest_time` | ISO-8601 | When it arrived at the broker |
| `courier_id` | string | Which courier |
| `event_type` | enum | `COURIER_ONLINE`, `COURIER_OFFLINE`, `COURIER_ASSIGNED`, `COURIER_ARRIVED_PICKUP`, `COURIER_ARRIVED_DROPOFF`, `COURIER_LOCATION` |
| `courier_status` | string | Current courier state |
| `zone_id` | string | Where they are in Madrid |
| `order_id` | string? | `null` if not on a delivery |
| `lat` / `lon` | double | GPS coordinates |
| `vehicle_type` | enum | `BIKE`, `SCOOTER`, or `CAR` |

> **Note:** Events with injected edge cases include a `_edge_case` field in the JSONL for debugging. This field is stripped from Avro output.

---

## Edge Cases

This is the main reason the generator exists. Each edge case is independently configurable and designed to test something specific in a stream processor.

| Code | What it does | Why it matters |
|------|-------------|----------------|
| **EC1** | **Out-of-order / late arrivals** — `ingest_time` is set artificially later than `event_time`. Three severities: mild (30s–2min), moderate (5–15min), severe (30min–2h) | Watermark logic, late data handling |
| **EC2** | **Duplicates** — same `event_id` emitted twice with `ingest_time` offset by +1–45s | Idempotency, deduplication |
| **EC3** | **Missing steps** — the FSM skips a state, e.g. `READY_FOR_PICKUP` → `DELIVERED` with no `PICKED_UP` in between | Gap detection in state machines |
| **EC4** | **Impossible durations** — transitions happen in 1–5 seconds, or with negative time deltas (clock drift) | Anomaly detection, SLA filtering |
| **EC5** | **Courier offline mid-delivery** — `COURIER_OFFLINE` fired while an order is still assigned | Orphaned order detection |

Default probabilities are low (2–5%) so the dataset is mostly clean. Crank them up in config to stress-test your pipeline.

---

## Madrid Traffic Model

`demand_model.py` applies real Madrid rush hour patterns to delivery times, so `event_time` values actually reflect what traffic does to a courier.

**Rush hours:**
- Morning: `08:00 – 10:00`
- Midday: `14:00 – 16:00`
- Evening: `18:00 – 21:00`

**Vehicle impact during rush:**

| Vehicle | Rush multiplier | Extra delay (rush only) |
|---------|----------------|--------------------------|
| `CAR` | ×2.8 | 65% chance of +5–20 min jam |
| `SCOOTER` | ×1.6 | 35% chance |
| `BIKE` | ×1.1 | 8% — basically immune |

Fleet mix: **50% bikes, 35% scooters, 15% cars** — roughly realistic for Madrid.

This enables queries like:

```sql
SELECT vehicle_type, zone_id,
       COUNT(*) FILTER (WHERE delivery_minutes > sla_minutes) AS late_deliveries
FROM order_metrics
GROUP BY vehicle_type, zone_id;
```

---

## Madrid Zones

Orders are distributed across 10 real neighbourhoods. Centro and Salamanca get the most demand; Vallecas and Carabanchel the least — which reflects how food delivery actually skews in the city.

| `zone_id` | Neighbourhood | Demand weight |
|-----------|--------------|---------------|
| `CENTER` | Centro | 3.5 ⬆ |
| `SALAMANCA` | Salamanca | 3.0 |
| `MALASANA` | Malasaña | 2.8 |
| `CHAMBERI` | Chamberí | 2.5 |
| `LAVAPIES` | Lavapiés | 2.0 |
| `RETIRO` | Retiro | 1.8 |
| `TETUAN` | Tetuán | 1.5 |
| `MONCLOA` | Moncloa | 1.3 |
| `VALLECAS` | Vallecas | 1.0 |
| `CARABANCHEL` | Carabanchel | 1.0 ⬇ |

---

## Configuration

Full `config.json` reference:

```json
{
  "num_restaurants": 20,
  "num_couriers": 50,
  "num_orders": 500,
  "demand_surge_multipliers": { "13": 2.5, "14": 3.0, "21": 3.5 },
  "weekend_demand_multiplier": 1.4,
  "cancellation_probability": 0.08,
  "promotion_periods": [
    { "start_hour": 12, "end_hour": 16, "demand_multiplier": 1.5, "name": "Lunch Deal" },
    { "start_hour": 20, "end_hour": 23, "demand_multiplier": 1.3, "name": "Dinner Special" }
  ],
  "simulation_start": "2025-01-13 08:00:00",
  "simulation_hours": 16,
  "output_dir": "output",
  "edge_cases": {
    "out_of_order": 0.05,
    "duplicate": 0.03,
    "missing_step": 0.04,
    "impossible_duration": 0.02,
    "courier_offline_mid_delivery": 0.03
  }
}
```

---

## Dependencies

| Package | Required | Purpose |
|---------|----------|---------|
| Python ≥ 3.10 | ✅ | — |
| `fastavro` | ❌ optional | Avro binary serialization (`pip install fastavro`) |
=======
## AVRO Sample Generation & Validation

This project includes JSONL sample data under `sample_data/` and corresponding AVSC schemas under `schemas/`.

The AVRO files can be generated using:

```bash
python generator/serialize_samples.py
>>>>>>> db5919e (Add AVRO sample generator script with validation)
