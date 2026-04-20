"""
generator.py — Main logic for generating synthetic events.

Contains:
  · EdgeCaseInjector — injection of the 5 required edge cases
  · OrderGenerator — complete FSM for an order (OrderEvents)
  · CourierEventGenerator — delivery cycles and GPS traces (CourierEvents)
  · OutputWriter — serialization to JSON Lines and AVRO binary
  · DataGenerator — main orchestrator of the simulation

REQUIRED EDGE CASES:
[EC1] Out-of-order / late arrivals
[EC2] Duplicate events (same event_id)
[EC3] Missing steps in the order FSM
[EC4] Impossible durations (absurd times)
[EC5] Courier offline mid-delivery
"""

import json
import os
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

from config import Config
from demand_model import DemandModel, TrafficModel
from models import (
    ZONE_MAP,
    CourierEventType,
    EntityPool,
    OrderEventType,
    VehicleType,
)

# ── importr fastavro ──────────────────
try:
    import fastavro
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False
    print("[WARNING] fastavro no instalado — output AVRO desactivado.")
    print("          Instalar con: pip install fastavro\n")


# =============================================================================
# AVRO SCHEMAS 
# =============================================================================

ORDER_EVENT_AVSC: Dict = {
    "type":      "record",
    "name":      "OrderEvent",
    "namespace": "com.fooddelivery.madrid",
    "doc":       "Food-delivery order lifecycle event — Stream Analytics M1",
    "fields": [
        {"name": "event_id",      "type": "string",
         "doc": "UUID único del evento"},
        {"name": "event_time",    "type": "string",
         "doc": "ISO-8601: instante en que ocurrió el evento (event-time)"},
        {"name": "ingest_time",   "type": "string",
         "doc": "ISO-8601: llegada al broker. Puede ser > event_time (out-of-order)"},
        {"name": "order_id",      "type": "string"},
        {"name": "event_type",    "type": {
            "type":    "enum",
            "name":    "OrderEventType",
            "symbols": [
                "ORDER_CREATED", "ORDER_ACCEPTED", "PREP_STARTED",
                "READY_FOR_PICKUP", "PICKED_UP", "DELIVERED", "CANCELLED",
            ],
        }},
        {"name": "order_status",  "type": "string"},
        {"name": "restaurant_id", "type": "string"},
        {"name": "zone_id",       "type": "string",
         "doc": "Zona de Madrid: CENTRO, SALAMANCA, CHAMBERI, etc."},
        {"name": "courier_id",    "type": ["null", "string"], "default": None},
        {"name": "order_value",   "type": "double",
         "doc": "Valor del pedido en EUR"},
        {"name": "cancel_reason", "type": ["null", "string"], "default": None},
    ],
}

COURIER_EVENT_AVSC: Dict = {
    "type":      "record",
    "name":      "CourierEvent",
    "namespace": "com.fooddelivery.madrid",
    "doc":       "Courier location and status event — Stream Analytics M1",
    "fields": [
        {"name": "event_id",       "type": "string"},
        {"name": "event_time",     "type": "string"},
        {"name": "ingest_time",    "type": "string"},
        {"name": "courier_id",     "type": "string"},
        {"name": "event_type",     "type": {
            "type":    "enum",
            "name":    "CourierEventType",
            "symbols": [
                "COURIER_ONLINE", "COURIER_OFFLINE", "COURIER_ASSIGNED",
                "COURIER_ARRIVED_PICKUP", "COURIER_ARRIVED_DROPOFF",
                "COURIER_LOCATION",
            ],
        }},
        {"name": "courier_status", "type": "string"},
        {"name": "zone_id",        "type": "string"},
        {"name": "order_id",       "type": ["null", "string"], "default": None},
        {"name": "lat",            "type": "double"},
        {"name": "lon",            "type": "double"},
        {"name": "vehicle_type",   "type": {
            "type":    "enum",
            "name":    "VehicleType",
            "symbols": ["BIKE", "SCOOTER", "CAR"],
        }},
    ],
}


# =============================================================================
# EDGE CASE INJECTOR 
# =============================================================================

class EdgeCaseInjector:
    """
    ╔══════════════════════════════════════════════════════════════════════╗
    ║  EDGE CASES OBLIGATORIOS                                             ║
    ║                                                                      ║
    ║  Deliberately inject the 5 types of anomalies required to            ║
    ║  test the robustness of the streaming pipeline                       ║
    ║                                                                      ║
    ║  [EC1] Out-of-order  → test watermarks / late data handling          ║
    ║  [EC2] Duplicates    → test idempotency / deduplication              ║
    ║  [EC3] Missing steps → test FSM robustness / gap detection           ║
    ║  [EC4] Impossible durations → test anomaly detection                 ║
    ║  [EC5] Courier offline mid-delivery → test mid-stream state errors   ║
    ╚══════════════════════════════════════════════════════════════════════╝
    """

    def __init__(self, config: Config):
        self.p = config.edge_cases

    # ── [EC1] Out-of-order events ─────────────────────────────────────────

    def maybe_inject_out_of_order(self, event: Dict) -> Dict:
        """
        [EDGE CASE 1 — OUT-OF-ORDER / LATE ARRIVAL]
        Artificially advances the ingest_time relative to the event_time,
        simulating a message arriving late to the message broker (Kafka).
        
        Severities:
          · Minor (30 s – 2 min): occasional network delay
          · Medium (5 – 15 min): temporary network partitioning
          · Severe (30 min – 2 h): retries, producer crash
              → directly challenges the watermark
        """
        if random.random() >= self.p.out_of_order:
            return event

        event = event.copy()
        severity = random.choices(
            ["minor", "medium", "severe"],
            weights=[0.6, 0.3, 0.1],
        )[0]

        delays = {"minor": (30, 120), "medium": (300, 900), "severe": (1800, 7200)}
        lo, hi   = delays[severity]
        delay_s  = random.randint(lo, hi)

        event_dt          = datetime.fromisoformat(event["event_time"])
        event["ingest_time"] = (event_dt + timedelta(seconds=delay_s)).isoformat()
        event["_edge_case"]  = f"OUT_OF_ORDER|delay={delay_s}s|severity={severity}"
        return event

    # ── [EC2] Duplicate events ────────────────────────────────────────────

    def maybe_duplicate_event(
        self, event: Dict, target_list: List[Dict]
    ) -> None:
        """
        [EDGE CASE 2 — DUPLICATE]
        Emits the same event (SAME event_id) a second time with a slightly later
        ingest_time, simulating at-least-once delivery.
        
        The downstream system must detect and discard the duplicate.
        """
        if random.random() >= self.p.duplicate:
            return

        dup = event.copy()
        ingest_dt        = datetime.fromisoformat(dup["ingest_time"])
        dup["ingest_time"] = (
            ingest_dt + timedelta(seconds=random.randint(1, 45))
        ).isoformat()
        dup["_edge_case"] = "DUPLICATE"
        target_list.append(dup)

    # ── [EC3] Missing steps ───────────────────────────────────────────────

    def should_skip_step(self) -> bool:
        """
        [EDGE CASE 3 — MISSING STEP]
        Returns True if the next FSM state should be skipped.
        Example: READY_FOR_PICKUP → DELIVERED (skipping PICKED_UP).
        """
        return random.random() < self.p.missing_step

    # ── [EC4] Impossible durations ────────────────────────────────────────

    def maybe_get_impossible_time(
        self, base_time: datetime
    ) -> Tuple[Optional[datetime], bool]:
        """
        [EDGE CASE 4 — IMPOSSIBLE DURATION]
        It generates an absurd timestamp for the following event:
          • instant (1–5 s): physically impossible delivery
          • suspicious (5–30 s): highly suspicious time
          • negative_clock (<0 s): clock out of sync (running backward)
        
        In the analytics layer, these records must be detected and filtered from the SLA calculation.
        """
        if random.random() >= self.p.impossible_duration:
            return None, False

        mode = random.choices(
            ["instant", "suspicious", "negative_clock"],
            weights=[0.5, 0.35, 0.15],
        )[0]

        if mode == "instant":
            delta_s = random.randint(1, 5)
        elif mode == "suspicious":
            delta_s = random.randint(5, 30)
        else:
            delta_s = -random.randint(1, 60)

        return base_time + timedelta(seconds=delta_s), True

    # ── [EC5] Courier offline mid-delivery ───────────────────────────────

    def should_go_offline(self) -> bool:
        """
        [EDGE CASE 5 — COURIER OFFLINE MID-DELIVERY]
        Returns True if the courier should issue COURIER_OFFLINE while
        it is in the ASSIGNED state (battery, coverage, app closure).
        
        The pipeline should detect the resulting "orphan" order.
        """
        return random.random() < self.p.courier_offline_mid_delivery


# =============================================================================
# ORDER GENERATOR
# =============================================================================

class OrderGenerator:
    """
    Generates the complete OrderEvents sequence for a single order
    
    Following the state FSM. Each transition can incorporate
    edge cases and applies Madrid traffic delays.

    FSM:
      ORDER_CREATED → ORDER_ACCEPTED → PREP_STARTED → READY_FOR_PICKUP
           → PICKED_UP → DELIVERED
           └→ CANCELLED (aat any point before DELIVERED)
    """

    def __init__(
        self,
        config:        Config,
        entity_pool:   EntityPool,
        edge_injector: EdgeCaseInjector,
        demand_model:  DemandModel,
    ):
        self.config  = config
        self.pool    = entity_pool
        self.edge    = edge_injector
        self.demand  = demand_model

    def generate(
        self, base_time: datetime
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        Generates all events for an order.
        Returns (event_list, courier_id | None).
        """
        order_id    = f"ORD_{uuid.uuid4().hex[:10].upper()}"
        zone_id     = self.demand.sample_zone()
        restaurant  = self.pool.get_random_restaurant()
        order_value = round(random.uniform(8.0, 65.0), 2)
        t           = base_time
        events: List[Dict] = []

        # ── ORDER_CREATED ─────────────────────────────────────────────────
        ev = self._build(
            order_id, OrderEventType.ORDER_CREATED, "PENDING",
            t, restaurant["restaurant_id"], zone_id, None, order_value, None,
        )
        ev = self.edge.maybe_inject_out_of_order(ev)        # [EC1]
        events.append(ev)
        self.edge.maybe_duplicate_event(ev, events)         # [EC2]

        # ── Early cancellation  ──────────────────────────────────────────
        if random.random() < self.config.cancellation_probability:
            t += timedelta(seconds=random.randint(10, 120))
            reason = random.choice([
                "CUSTOMER_REQUEST", "RESTAURANT_CLOSED",
                "NO_COURIERS_AVAILABLE", "PAYMENT_FAILED",
            ])
            ev = self._build(
                order_id, OrderEventType.CANCELLED, "CANCELLED",
                t, restaurant["restaurant_id"], zone_id,
                None, order_value, reason,
            )
            ev = self.edge.maybe_inject_out_of_order(ev)
            events.append(ev)
            return events, None

        # ── Courier assignation ─────────────────────────────────────────
        courier_id = self.pool.assign_courier()
        if courier_id is None:
            t += timedelta(seconds=random.randint(30, 180))
            ev = self._build(
                order_id, OrderEventType.CANCELLED, "CANCELLED",
                t, restaurant["restaurant_id"], zone_id,
                None, order_value, "NO_COURIERS_AVAILABLE",
            )
            events.append(ev)
            return events, None

        vehicle_type = self.pool.couriers[courier_id]["vehicle_type"]

        # ── ORDER_ACCEPTED ────────────────────────────────────────────────
        t += timedelta(seconds=random.randint(15, 90))
        ev = self._build(
            order_id, OrderEventType.ORDER_ACCEPTED, "ACCEPTED",
            t, restaurant["restaurant_id"], zone_id,
            courier_id, order_value, None,
        )
        ev = self.edge.maybe_inject_out_of_order(ev)
        events.append(ev)
        self.edge.maybe_duplicate_event(ev, events)

        # ── PREP_STARTED ──────────────────────────────────────────────────
        t += timedelta(seconds=random.randint(10, 60))
        ev = self._build(
            order_id, OrderEventType.PREP_STARTED, "PREPARING",
            t, restaurant["restaurant_id"], zone_id,
            courier_id, order_value, None,
        )
        ev = self.edge.maybe_inject_out_of_order(ev)
        events.append(ev)

        # ── READY_FOR_PICKUP (with a potentially impossible duration) ─────────────
        impossible_t, was_impossible = self.edge.maybe_get_impossible_time(t)  # [EC4]
        if was_impossible:
            t        = impossible_t
            edge_tag = "IMPOSSIBLE_PREP_DURATION"
        else:
            t        += timedelta(seconds=TrafficModel.get_prep_seconds())
            edge_tag  = None

        ev = self._build(
            order_id, OrderEventType.READY_FOR_PICKUP, "READY",
            t, restaurant["restaurant_id"], zone_id,
            courier_id, order_value, None,
        )
        if edge_tag:
            ev["_edge_case"] = edge_tag
        ev = self.edge.maybe_inject_out_of_order(ev)
        events.append(ev)

        # ── PICKED_UP (may be omitted) — [EC3] MISSING STEP ───────────
        if self.edge.should_skip_step():                                        # [EC3]
            # We marked the previous event for traceability
            events[-1]["_edge_case"] = (
                events[-1].get("_edge_case", "") + "|MISSING_NEXT:PICKED_UP"
            ).lstrip("|")
        else:
            # Transit to pickup: affected by Madrid traffic
            transit_s = TrafficModel.get_transit_seconds(t, vehicle_type)
            t        += timedelta(seconds=transit_s)
            ev = self._build(
                order_id, OrderEventType.PICKED_UP, "IN_TRANSIT",
                t, restaurant["restaurant_id"], zone_id,
                courier_id, order_value, None,
            )
            ev = self.edge.maybe_inject_out_of_order(ev)
            events.append(ev)
            self.edge.maybe_duplicate_event(ev, events)

        # ── DELIVERED (with a potentially impossible duration) ────────────────────
        impossible_t, was_impossible = self.edge.maybe_get_impossible_time(t)  # [EC4]
        if was_impossible:
            t        = impossible_t
            edge_del = "IMPOSSIBLE_DELIVERY_DURATION"
        else:
            # Transit to the customer: affected by Madrid traffic
            transit_s = TrafficModel.get_transit_seconds(t, vehicle_type)
            t        += timedelta(seconds=transit_s)
            edge_del  = None

        ev = self._build(
            order_id, OrderEventType.DELIVERED, "DELIVERED",
            t, restaurant["restaurant_id"], zone_id,
            courier_id, order_value, None,
        )
        if edge_del:
            ev["_edge_case"] = edge_del
        ev = self.edge.maybe_inject_out_of_order(ev)
        events.append(ev)

        return events, courier_id

    def _build(
        self,
        order_id:      str,
        event_type:    OrderEventType,
        order_status:  str,
        event_time:    datetime,
        restaurant_id: str,
        zone_id:       str,
        courier_id:    Optional[str],
        order_value:   float,
        cancel_reason: Optional[str],
    ) -> Dict:
        ts = event_time.isoformat()
        return {
            "event_id":      f"EVT_{uuid.uuid4().hex[:12].upper()}",
            "event_time":    ts,
            "ingest_time":   ts,
            "order_id":      order_id,
            "event_type":    event_type.value,
            "order_status":  order_status,
            "restaurant_id": restaurant_id,
            "zone_id":       zone_id,
            "courier_id":    courier_id,
            "order_value":   order_value,
            "cancel_reason": cancel_reason,
        }


# =============================================================================
# COURIER EVENT GENERATOR
# =============================================================================

class CourierEventGenerator:
    """
    Generates CourierEvents synchronized with OrderEvents.
    
    Includes:
      · COURIER_ONLINE on login
      · ASSIGNED → LOCATION × N → ARRIVED_PICKUP → LOCATION × N → ARRIVED_DROPOFF
      · [EC5] COURIER_OFFLINE mid-delivery with configured probability
    """

    def __init__(
        self,
        config:        Config,
        entity_pool:   EntityPool,
        edge_injector: EdgeCaseInjector,
        demand_model:  DemandModel,
    ):
        self.config = config
        self.pool   = entity_pool
        self.edge   = edge_injector
        self.demand = demand_model

    def generate_session_start(
        self, courier_id: str, session_time: datetime
    ) -> List[Dict]:
        """Generate the COURIER_ONLINE event at the start of the day's session."""
        courier  = self.pool.couriers[courier_id]
        zone_id  = courier["home_zone"]
        lat, lon = self.demand.sample_coords_in_zone(zone_id)
        ev = self._build(
            courier_id, CourierEventType.COURIER_ONLINE, "ONLINE",
            session_time, zone_id, None, lat, lon, courier["vehicle_type"],
        )
        ev = self.edge.maybe_inject_out_of_order(ev)
        return [ev]

    def generate_delivery_cycle(
        self,
        courier_id:    str,
        order_id:      str,
        assign_time:   datetime,
        pickup_time:   datetime,
        delivery_time: datetime,
        zone_id:       str,
    ) -> List[Dict]:
        """
        Generates the complete CourierEvents cycle for a delivery.
        Includes [EC5]: offline mid-delivery with configured probability.
        """
        courier      = self.pool.couriers.get(courier_id, {})
        vehicle_type = courier.get("vehicle_type", VehicleType.SCOOTER.value)
        lat_p, lon_p = self.demand.sample_coords_in_zone(zone_id)
        lat_d, lon_d = self.demand.sample_coords_in_zone(zone_id)
        events: List[Dict] = []

        # ── COURIER_ASSIGNED ──────────────────────────────────────────────
        ev = self._build(
            courier_id, CourierEventType.COURIER_ASSIGNED, "ASSIGNED",
            assign_time, zone_id, order_id, lat_p, lon_p, vehicle_type,
        )
        ev = self.edge.maybe_inject_out_of_order(ev)
        events.append(ev)

        # ── [EC5] COURIER OFFLINE MID-DELIVERY ───────────────────────────
        if self.edge.should_go_offline():                                      # [EC5]
            total_s     = max(1.0, (pickup_time - assign_time).total_seconds())
            offline_t   = assign_time + timedelta(
                seconds=random.uniform(30.0, total_s * 0.5)
            )
            ev_off = self._build(
                courier_id, CourierEventType.COURIER_OFFLINE, "OFFLINE",
                offline_t, zone_id, order_id,
                lat_p + random.uniform(-0.003, 0.003),
                lon_p + random.uniform(-0.003, 0.003),
                vehicle_type,
            )
            ev_off["_edge_case"] = "COURIER_OFFLINE_MID_DELIVERY"
            events.append(ev_off)
            return events  # The courier service is down; there are no more events.

        # ── LOCATION in transit to pickup ────────────────────────────────
        events.extend(self._location_trace(
            courier_id, vehicle_type, zone_id, order_id,
            assign_time, pickup_time, lat_p, lon_p,
            "IN_TRANSIT_TO_PICKUP", n_updates=random.randint(2, 5),
        ))

        # ── COURIER_ARRIVED_PICKUP ────────────────────────────────────────
        ev = self._build(
            courier_id, CourierEventType.COURIER_ARRIVED_PICKUP, "AT_PICKUP",
            pickup_time, zone_id, order_id, lat_p, lon_p, vehicle_type,
        )
        ev = self.edge.maybe_inject_out_of_order(ev)
        events.append(ev)

        # ── LOCATION in transit to client ───────────────────────────────
        events.extend(self._location_trace(
            courier_id, vehicle_type, zone_id, order_id,
            pickup_time, delivery_time, lat_d, lon_d,
            "IN_TRANSIT_TO_CUSTOMER", n_updates=random.randint(3, 7),
        ))

        # ── COURIER_ARRIVED_DROPOFF ───────────────────────────────────────
        ev = self._build(
            courier_id, CourierEventType.COURIER_ARRIVED_DROPOFF, "AT_DROPOFF",
            delivery_time, zone_id, order_id, lat_d, lon_d, vehicle_type,
        )
        ev = self.edge.maybe_inject_out_of_order(ev)
        events.append(ev)
        self.edge.maybe_duplicate_event(ev, events)

        return events

    def _location_trace(
        self,
        courier_id:   str,
        vehicle_type: str,
        zone_id:      str,
        order_id:     Optional[str],
        t_start:      datetime,
        t_end:        datetime,
        lat_dest:     float,
        lon_dest:     float,
        status:       str,
        n_updates:    int,
    ) -> List[Dict]:
        """Generates N time-interpolated COURIER_LOCATION updates."""
        total_s = max(1.0, (t_end - t_start).total_seconds())
        trace   = []
        for i in range(n_updates):
            frac = (i + 1) / (n_updates + 1)
            t    = t_start + timedelta(seconds=total_s * frac)
            ev   = self._build(
                courier_id, CourierEventType.COURIER_LOCATION, status,
                t, zone_id, order_id,
                lat_dest + random.uniform(-0.004, 0.004),
                lon_dest + random.uniform(-0.004, 0.004),
                vehicle_type,
            )
            ev = self.edge.maybe_inject_out_of_order(ev)
            trace.append(ev)
        return trace

    def _build(
        self,
        courier_id:     str,
        event_type:     CourierEventType,
        courier_status: str,
        event_time:     datetime,
        zone_id:        str,
        order_id:       Optional[str],
        lat:            float,
        lon:            float,
        vehicle_type:   str,
    ) -> Dict:
        ts = event_time.isoformat()
        return {
            "event_id":       f"EVT_{uuid.uuid4().hex[:12].upper()}",
            "event_time":     ts,
            "ingest_time":    ts,
            "courier_id":     courier_id,
            "event_type":     event_type.value,
            "courier_status": courier_status,
            "zone_id":        zone_id,
            "order_id":       order_id,
            "lat":            round(lat, 6),
            "lon":            round(lon, 6),
            "vehicle_type":   vehicle_type,
        }


# =============================================================================
# OUTPUT WRITER (OUTPUT FORMAT: JSON + AVRO)
# =============================================================================

class OutputWriter:
    """
    Write events in JSON Lines (JSONL) and AVRO binary format,
    including .avsc schema files in the output directory.
    """

    def __init__(self, config: Config):
        self.out_dir    = config.output_dir
        self.schema_dir = os.path.join(self.out_dir, "schemas")
        os.makedirs(self.out_dir,    exist_ok=True)
        os.makedirs(self.schema_dir, exist_ok=True)
        self._write_avsc()

    def _write_avsc(self) -> None:
        with open(os.path.join(self.schema_dir, "order_event.avsc"),   "w") as f:
            json.dump(ORDER_EVENT_AVSC,   f, indent=2)
        with open(os.path.join(self.schema_dir, "courier_event.avsc"), "w") as f:
            json.dump(COURIER_EVENT_AVSC, f, indent=2)
        print(f"  [Writer] Esquemas .avsc → {self.schema_dir}/")

    def write_json(
        self, order_events: List[Dict], courier_events: List[Dict]
    ) -> None:
        """Writes JSONL (one event per line). Keeps _edge_case for debugging."""
        def _dump(events: List[Dict], path: str) -> None:
            with open(path, "w", encoding="utf-8") as f:
                for ev in events:
                    f.write(json.dumps(ev, ensure_ascii=False) + "\n")

        o = os.path.join(self.out_dir, "order_events.jsonl")
        c = os.path.join(self.out_dir, "courier_events.jsonl")
        _dump(order_events,   o)
        _dump(courier_events, c)
        print(f"  [Writer] JSON → {o}  ({len(order_events)} eventos)")
        print(f"  [Writer] JSON → {c}  ({len(courier_events)} eventos)")

    def write_avro(
        self, order_events: List[Dict], courier_events: List[Dict]
    ) -> None:
        """Serializes in AVRO binary. Removes _edge_case fields (not in schema)."""
        if not AVRO_AVAILABLE:
            print("[Writer] AVRO skipped (fastavro not installed).")
            return

        def _clean(ev: Dict) -> Dict:
            return {k: v for k, v in ev.items() if not k.startswith("_")}

        parsed_order   = fastavro.parse_schema(ORDER_EVENT_AVSC)
        parsed_courier = fastavro.parse_schema(COURIER_EVENT_AVSC)

        o = os.path.join(self.out_dir, "order_events.avro")
        c = os.path.join(self.out_dir, "courier_events.avro")

        with open(o, "wb") as f:
            fastavro.writer(f, parsed_order,   [_clean(e) for e in order_events])
        with open(c, "wb") as f:
            fastavro.writer(f, parsed_courier, [_clean(e) for e in courier_events])

        print(f"  [Writer] AVRO → {o}  ({len(order_events)} events)")
        print(f"  [Writer] AVRO → {c}  ({len(courier_events)} events)")


# =============================================================================
# DATA GENERATOR — ORQUESTADOR PRINCIPAL
# =============================================================================

class DataGenerator:
    """
    Orchestrate full simulation:
      1. Initialize the entity pool
      2. Iterate hour by hour according to the demand model
      3. Generate coordinated OrderEvents and CourierEvents
      4. Write the output batches to JSON and AVRO
    """

    def __init__(self, config: Config):
        self.config      = config
        self.demand      = DemandModel(config)
        self.pool        = EntityPool(config)
        self.edge        = EdgeCaseInjector(config)
        self.order_gen   = OrderGenerator(config, self.pool, self.edge, self.demand)
        self.courier_gen = CourierEventGenerator(config, self.pool, self.edge, self.demand)
        self.writer      = OutputWriter(config)

    def run(self) -> Tuple[List[Dict], List[Dict]]:
        """Run the simulation and return (order_events, courier_events)."""
        sim_start = datetime.fromisoformat(self.config.simulation_start)
        self._print_header(sim_start)

        all_order_events:   List[Dict] = []
        all_courier_events: List[Dict] = []

        # Log in for all couriers at startup
        for cid in self.pool.couriers:
            t = sim_start + timedelta(minutes=random.randint(0, 30))
            all_courier_events.extend(
                self.courier_gen.generate_session_start(cid, t)
            )

        # Hourly loop
        orders_done = 0
        for hour_offset in range(self.config.simulation_hours):
            if orders_done >= self.config.num_orders:
                break
            hour_dt     = sim_start + timedelta(hours=hour_offset)
            n_this_hour = min(
                self.demand.get_orders_for_hour(hour_dt),
                self.config.num_orders - orders_done,
            )
            for i in range(n_this_hour):
                minute_offset = (i / max(n_this_hour, 1)) * 60.0
                order_time    = hour_dt + timedelta(minutes=minute_offset)

                order_evs, courier_id = self.order_gen.generate(order_time)
                all_order_events.extend(order_evs)

                if courier_id:
                    self._add_courier_cycle(
                        courier_id, order_evs, all_courier_events
                    )
                    self.pool.release_courier(courier_id)

                orders_done += 1

        self._print_stats(all_order_events, all_courier_events)
        print("\n  Writing output files...")
        self.writer.write_json(all_order_events, all_courier_events)
        self.writer.write_avro(all_order_events, all_courier_events)
        print(f"\n✅  Generation completed → {self.config.output_dir}/\n")
        return all_order_events, all_courier_events

    def _add_courier_cycle(
        self,
        courier_id:    str,
        order_evs:     List[Dict],
        courier_store: List[Dict],
    ) -> None:
        def _ts(evs: List[Dict], etype: str) -> Optional[datetime]:
            for e in evs:
                if e["event_type"] == etype:
                    return datetime.fromisoformat(e["event_time"])
            return None

        accepted_t  = _ts(order_evs, OrderEventType.ORDER_ACCEPTED.value)
        pickup_t    = _ts(order_evs, OrderEventType.PICKED_UP.value)
        delivered_t = _ts(order_evs, OrderEventType.DELIVERED.value)

        if not (accepted_t and delivered_t):
            return

        if pickup_t is None:
            # [EC3] PICKED_UP fue omitido → estimamos el tiempo
            pickup_t = delivered_t - timedelta(seconds=random.randint(60, 180))

        courier_store.extend(
            self.courier_gen.generate_delivery_cycle(
                courier_id    = courier_id,
                order_id      = order_evs[0]["order_id"],
                assign_time   = accepted_t,
                pickup_time   = pickup_t,
                delivery_time = delivered_t,
                zone_id       = order_evs[0]["zone_id"],
            )
        )

    # ── Presentation Helpers ───────────────────────────────────────────

    def _print_header(self, sim_start: datetime) -> None:
        is_wknd = sim_start.weekday() >= 5
        print(f"\n{'═' * 62}")
        print(f"  🍕  Food Delivery Data Generator — Madrid")
        print(f"{'═' * 62}")
        print(f"  Home simulation : {sim_start}")
        print(f"  Simulated hours   : {self.config.simulation_hours}h")
        print(f"  Target orders  : {self.config.num_orders}")
        print(f"  Restaurants      : {self.config.num_restaurants}")
        print(f"  Couriers          : {self.config.num_couriers}")
        print(f"  Type of day          : {'Weekend 🎉' if is_wknd else 'Business day'}")
        print(f"  Output directory : {self.config.output_dir}/")
        print(f"{'─' * 62}\n")

    def _print_stats(
        self,
        order_events:   List[Dict],
        courier_events: List[Dict],
    ) -> None:
        unique_orders = len({e["order_id"] for e in order_events})
        delivered     = sum(1 for e in order_events if e["event_type"] == "DELIVERED")
        cancelled     = sum(1 for e in order_events if e["event_type"] == "CANCELLED")

        def _count(evs: List[Dict], tag: str) -> int:
            return sum(1 for e in evs if tag in e.get("_edge_case", ""))

        print(f"\n{'─' * 62}")
        print(f"  📊  BATCH STATISTICS")
        print(f"{'─' * 62}")
        print(f"  Total order events : {len(order_events):>6}")
        print(f"  Total courier events : {len(courier_events):>6}")
        print(f"  Unique orders       : {unique_orders:>6}")
        print(f"  ├─ Delivered        : {delivered:>6}")
        print(f"  └─ Cancelled        : {cancelled:>6}")

        print(f"\n  🔴  INJECTED EDGE CASES")
        print(f"  ├─ [EC1] Out-of-order (order)    : {_count(order_events, 'OUT_OF_ORDER'):>5}  → test watermarks")
        print(f"  ├─ [EC1] Out-of-order (courier)  : {_count(courier_events, 'OUT_OF_ORDER'):>5}  → test watermarks")
        print(f"  ├─ [EC2] Duplicated (order)      : {_count(order_events, 'DUPLICATE'):>5}  → test dedup")
        print(f"  ├─ [EC2] Duplicated (courier)    : {_count(courier_events, 'DUPLICATE'):>5}  → test dedup")
        print(f"  ├─ [EC3] Missing steps           : {_count(order_events, 'MISSING'):>5}  → test FSM gaps")
        print(f"  ├─ [EC4] Impossible times      : {_count(order_events, 'IMPOSSIBLE'):>5}  → test anomaly")
        print(f"  └─ [EC5] Courier offline/delivery : {_count(courier_events, 'OFFLINE_MID'):>5}  → test mid-stream fail")

        zone_counts: Dict[str, int] = {}
        for e in order_events:
            if e["event_type"] == "ORDER_CREATED":
                z = e["zone_id"]
                zone_counts[z] = zone_counts.get(z, 0) + 1

        print(f"\n  📍  DISTRIBUTION BY ZONE")
        for zid, cnt in sorted(zone_counts.items(), key=lambda x: -x[1]):
            bar = "█" * (cnt * 20 // max(zone_counts.values(), default=1))
            print(f"  {ZONE_MAP[zid].display_name:<14} {cnt:>4}  {bar}")

        rush_counts: Dict[str, Dict[str, int]] = {}
        for e in order_events:
            if e["event_type"] == "DELIVERED" and e.get("courier_id"):
                vtype   = self.pool.couriers.get(e["courier_id"], {}).get("vehicle_type", "?")
                is_rush, _ = TrafficModel.is_rush_hour(datetime.fromisoformat(e["event_time"]))
                if vtype not in rush_counts:
                    rush_counts[vtype] = {"rush": 0, "total": 0}
                rush_counts[vtype]["total"] += 1
                if is_rush:
                    rush_counts[vtype]["rush"] += 1

        print(f"\n  🚗  TRAFFIC IMPACT IN MADRID DUE TO VEHICLE")
        for vtype, counts in sorted(rush_counts.items()):
            pct = (counts["rush"] / max(counts["total"], 1)) * 100
            print(
                f"  {vtype:<8} deliveries={counts['total']:>4}"
                f"  en_rush={counts['rush']:>4} ({pct:.1f}%)"
            )
        print(f"{'─' * 62}")
