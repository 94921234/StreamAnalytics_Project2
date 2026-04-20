# Feed Design

This document describes the two streaming feeds:

1. Order lifecycle events
2. Courier status / availability events

Detailed schema explanations will be added here.

# Uber Eats Streaming Feeds Specification

This document describes the structure and purpose of the two streaming feeds designed for Milestone 1.

---

# Order Events Feed 

## Overview

Represents the full lifecycle of an Uber Eats order, from creation to delivery or cancellation.

This feed enables:
- Windowed order KPIs
- Revenue analytics
- Cancellation analysis
- SLA monitoring
- Anomaly detection
- Stream–stream joins with courier feed

---

## Fields

| Field | Type (AVRO) | Description | Why It Is Important |
|-------|------------|------------|--------------------|
| event_id | string | Unique event ID (UUID) | Enables deduplication |
| event_time | long (epoch ms) | Real time when event occurred | Basis for windows & watermarks |
| ingest_time | long (epoch ms) | Time event was emitted | Allows simulation of late data |
| order_id | string | Unique order identifier | Primary key |
| event_type | enum | Type of lifecycle event | Defines state transition |
| order_status | enum | Current state of the order | Enables aggregations |
| restaurant_id | string | Associated restaurant | KPIs per restaurant |
| zone_id | string | Geographic zone | KPIs per zone |
| courier_id | ["null","string"] | Assigned courier (if exists) | Join with courier feed |
| order_value | double | Total order value | Revenue metrics |
| cancel_reason | ["null", enum] | Cancellation reason | Churn & fraud analysis |

---

# Courier Events Feed

## Overview

Represents courier availability, movement, and assignment activity.

This feed enables:
- Active courier windowed metrics
- Demand–supply health calculation
- Session window analytics
- Zone congestion detection
- Stream–stream joins with orders

---

## Fields

| Field | Type (AVRO) | Description | Why It Is Important |
|-------|------------|------------|--------------------|
| event_id | string | Unique event ID | Deduplication |
| event_time | long (epoch ms) | Real event time | Event-time processing |
| ingest_time | long (epoch ms) | Emission time | Late/out-of-order handling |
| courier_id | string | Courier identifier | Primary key |
| event_type | enum | Courier action type | Defines courier behavior |
| courier_status | enum | Current courier status | Availability metrics |
| zone_id | string | Current zone | Supply per zone |
| order_id | ["null","string"] | Associated order (if any) | Join with orders |
| lat | ["null","double"] | Latitude | Location analytics |
| lon | ["null","double"] | Longitude | Location analytics |
| vehicle_type | enum | Type of vehicle | Segmentation |

---

# Streaming Design Principles

Both feeds include:

- `event_time` → used for event-time windows and watermarks
- `ingest_time` → used to simulate late arrivals
- `event_id` → used for deduplication
- Join keys (`order_id`, `courier_id`, `zone_id`) → enable stream–stream joins

The generator will intentionally simulate:
- Late events
- Duplicates
- Missing lifecycle steps
- Impossible durations
- Courier offline mid-delivery
