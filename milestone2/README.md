# Milestone 2 — Stream Analytics Implementation

Real-time analytics pipeline for Group 04's food delivery platform.

## Architecture

```
Event Hubs (group_04_orders + group_04_courierevents)
  └─► Spark Structured Streaming  (milestone2/streaming/spark_streaming.py)
        └─► Azure Blob Storage — Parquet metrics
              └─► Streamlit Dashboard  (milestone2/dashboard/streamlit_app.py)
```

**Azure resources (provisioned by the professor — shared with all groups):**

| Resource | Value |
|---|---|
| Storage Account | `iesstsabbadbab` |
| Container | `group04` |
| Event Hubs namespace | `iesstsabbadbab-grp-01-05` |
| Resource Group | `SA_BBADBA2025NBDA_B` |

---

## Credentials

Two environment variables are required. **Never commit these to the repo.**

| Variable | Where to find it |
|---|---|
| `EVENTHUB_CONNECTION_STRING` | Azure Portal → Event Hubs namespace `iesstsabbadbab-grp-01-05` → Shared access policies → `RootManageSharedAccessKey` → Connection string–primary key |
| `STORAGE_ACCOUNT_KEY` | Azure Portal → Storage Account `iesstsabbadbab` → Access keys → `key1` |

Set them in your terminal before running anything:

```bash
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://iesstsabbadbab-grp-01-05.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>"
export STORAGE_ACCOUNT_KEY="<key1 from Azure Storage Account>"
```

> **Tip:** You can also place these in a `.env` file in the repo root (gitignored). The producer loads it automatically via `python-dotenv`.

---

## Option A — View the dashboard only (quickest)

The dashboard reads Parquet data already written to Azure Blob Storage. No Spark or producer needed.

```bash
cd milestone2/dashboard
pip install -r requirements.txt
export STORAGE_ACCOUNT_KEY="<your key>"
streamlit run streamlit_app.py
```

Open `http://localhost:8501`. Data auto-refreshes every 30 seconds.

---

## Option B — Run the full pipeline (producer + Spark + dashboard)

### 1. Install dependencies

```bash
# Producer
pip install -r milestone2/producer/requirements.txt

# Dashboard
pip install -r milestone2/dashboard/requirements.txt
```

Spark requires **Java 11+** and `spark-submit` on your PATH.
The `azure-eventhubs-spark` JAR is fetched automatically at runtime via `--packages`.

### 2. Set credentials

```bash
export EVENTHUB_CONNECTION_STRING="..."
export STORAGE_ACCOUNT_KEY="..."
```

### 3. Run the producer

The producer streams synthetic events from the M1 generator into Event Hubs.
**Run from the repo root** so it can import the `generator/` package:

```bash
# From repo root:
python milestone2/producer/eventhub_producer.py                   # live, 1 event/sec
python milestone2/producer/eventhub_producer.py --batch-size 20   # send in batches of 20
python milestone2/producer/eventhub_producer.py --from-file sample_data/order_events.jsonl  # replay a file
```

Press Ctrl+C to stop.

### 4. Run Spark Structured Streaming

```bash
cd milestone2/streaming
/workspaces/StreamAnalytics_Project/spark-3.5.8-bin-hadoop3/bin/spark-submit \
  --packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
  spark_streaming.py
```

Writes Parquet metrics continuously to:
- `wasbs://group04@iesstsabbadbab.blob.core.windows.net/metrics/uc1/`
- `wasbs://group04@iesstsabbadbab.blob.core.windows.net/metrics/uc2/`
- `wasbs://group04@iesstsabbadbab.blob.core.windows.net/metrics/uc2b/`
- `wasbs://group04@iesstsabbadbab.blob.core.windows.net/metrics/uc3/`
- `wasbs://group04@iesstsabbadbab.blob.core.windows.net/metrics/uc3b/`

Press Ctrl+C to stop.

### 5. Run the dashboard

```bash
cd milestone2/dashboard
streamlit run streamlit_app.py
```

Open `http://localhost:8501`.

---

## Option C — GitHub Codespace (same environment as the team)

1. Go to the repo on GitHub → **Code** → **Codespaces** → **New codespace**
2. In the Codespace terminal, set your credentials:

```bash
export EVENTHUB_CONNECTION_STRING="..."
export STORAGE_ACCOUNT_KEY="..."
```

3. Run any command from Options A or B — Python and the required tools are pre-installed.
4. When you run `streamlit run ...`, click **Open in Browser** on the port-forwarding notification to get a public URL you can share.

---

## Use Cases

| UC  | Type         | Description |
|-----|--------------|------------|
| UC1 | Basic        | Orders per zone per minute (1-min tumbling window) |
| UC2A | Intermediate | Demand vs supply imbalance per zone |
| UC2B | Intermediate | Courier idle inefficiency per zone |
| UC3A | Advanced     | Surge indicator: demand/supply ratio > 1.5 AND demand >= 3 |
| UC3B | Advanced     | Pricing anomaly proxy using order_value vs recent baseline |

### Notes

- All metrics are computed in real time using event-time windows and watermarks.  
- UC3B uses `order_value` as a proxy for pricing behaviour and compares current values to a recent baseline.  
- The pipeline operates in real time: data is continuously generated, streamed into Event Hubs, processed by Spark, and stored in Azure Blob Storage.  
- The dashboard reflects the current system state with a slight delay due to micro-batching.  
- The dashboard uses simulated event-time from the data generator, not wall-clock time. This is why displayed timestamps may not match real time.

---

## Repository structure (Milestone 2)

```
milestone2/
  docs/
    2.1_ingestion_design.md     # Event Hubs topic design doc
  producer/
    eventhub_producer.py        # Streams M1 generator output → Event Hubs
    requirements.txt            # azure-eventhub, python-dotenv, fastavro
  streaming/
    spark_streaming.py          # Spark Structured Streaming + UC1/UC2/UC3
  dashboard/
    streamlit_app.py            # Live Streamlit dashboard (reads from Blob Storage)
    requirements.txt            # streamlit, pandas, pyarrow, plotly, azure-storage-blob
  README.md                     # This file
```
