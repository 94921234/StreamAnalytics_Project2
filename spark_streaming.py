"""
spark_streaming.py - Milestone 2.2 + Use Cases 1, 2A, 2B, 3A, 3B
Spark Structured Streaming for Group 04 food delivery platform.

Reads from Azure Event Hubs:
  - group_04_orders
  - group_04_courierevents

Writes raw data and metrics to Azure Blob Storage (Parquet):
  wasbs://group04@iesstsabbadbab.blob.core.windows.net/

Use Cases:
  UC1  - Orders per zone per minute (Basic)
  UC2A - Demand-supply imbalance per zone (Intermediate)
  UC2B - Courier idle inefficiency per zone (Intermediate)
  UC3A - Surge indicator (Advanced)
  UC3B - Pricing anomaly proxy using order_value vs recent baseline (Advanced)

Run:
  spark-submit \
    --packages com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22,org.apache.hadoop:hadoop-azure:3.3.4,com.microsoft.azure:azure-storage:8.6.6 \
    spark_streaming.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

# ── Config ──────────────────────────────────────────────────────────────
ORDERS_HUB = "group_04_orders"
COURIERS_HUB = "group_04_courierevents"
STORAGE_ACCOUNT = "iesstsabbadbab"
CONTAINER = "group04"

EH_CONN_STR = os.getenv("EVENTHUB_CONNECTION_STRING", "").strip()
STORAGE_KEY = os.getenv("STORAGE_ACCOUNT_KEY", "").strip()

BASE = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"
CKPT = f"{BASE}/checkpoints"

WATERMARK_DELAY = "10 minutes"
WIN = "30 seconds"
BASELINE_WIN = "10 minutes"

SURGE_THRESHOLD = 1.2
IDLE_UTIL_THRESHOLD = 0.5
PRICING_ANOMALY_THRESHOLD = 1.25

# ── Spark session ───────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Group04_StreamAnalytics")
        .config(
            f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
            STORAGE_KEY,
        )
        .getOrCreate()
    )

def eh_conf(spark: SparkSession, hub: str) -> dict:
    conn = f"{EH_CONN_STR};EntityPath={hub}"
    encrypted = spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn)
    return {"eventhubs.connectionString": encrypted}

# ── Schemas ─────────────────────────────────────────────────────────────
ORDER_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("courier_id", StringType(), True),
    StructField("order_value", DoubleType(), True),
    StructField("cancel_reason", StringType(), True),
])

COURIER_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("courier_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("courier_status", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("vehicle_type", StringType(), True),
])

# ── Readers ─────────────────────────────────────────────────────────────
def read_orders(spark: SparkSession):
    raw = (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf(spark, ORDERS_HUB))
        .load()
    )

    return (
        raw
        .select(F.from_json(F.col("body").cast("string"), ORDER_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("event_id").isNotNull())
        .withColumn("event_time",F.to_timestamp("event_time"))
        .withColumn("ingest_time",F.to_timestamp("ingest_time"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

def read_couriers(spark: SparkSession):
    raw = (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf(spark, COURIERS_HUB))
        .load()
    )

    return (
        raw
        .select(F.from_json(F.col("body").cast("string"), COURIER_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("event_id").isNotNull())
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("ingest_time",F.to_timestamp("ingest_time"))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

# ── Raw sinks (data at rest) ────────────────────────────────────────────
def sink_parquet(df, name: str):
    return (
        df.writeStream
        .format("parquet")
        .option("path", f"{BASE}/{name}/")
        .option("checkpointLocation", f"{CKPT}/{name}/")
        .partitionBy("zone_id")
        .outputMode("append")
        .start()
    )

# ── UC1: Orders per zone per minute ────────────────────────────────────
def uc1(orders):
    agg = (
        orders
        .filter(F.col("event_type") == "ORDER_CREATED")
        .groupBy(F.window("event_time", WIN), "zone_id")
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("order_value").alias("total_value"),
            F.avg("order_value").alias("avg_value"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "order_count",
            "total_value",
            "avg_value",
        )
    )

    return (
        agg.writeStream
        .format("parquet")
        .option("path", f"{BASE}/metrics/uc1/")
        .option("checkpointLocation", f"{CKPT}/uc1/")
        .outputMode("append")
        .start()
    )

# ── UC2A: Demand-supply imbalance per zone ─────────────────────────────
def uc2(orders, couriers):
    demand_stream = (
        orders
        .filter(F.col("event_type").isin(["ORDER_CREATED", "ORDER_ACCEPTED"]))
        .select(
            "event_time",
            "zone_id",
            F.lit(1).alias("demand_value"),
            F.lit(0).alias("supply_value"),
        )
    )

    supply_stream = (
        couriers
        .filter(F.col("courier_status").isin(["ONLINE", "AVAILABLE"]))
        .select(
            "event_time",
            "zone_id",
            F.lit(0).alias("demand_value"),
            F.lit(1).alias("supply_value"),
        )
    )

    combined = demand_stream.unionByName(supply_stream)

    agg = (
        combined
        .groupBy(F.window("event_time", WIN), "zone_id")
        .agg(
            F.sum("demand_value").alias("demand"),
            F.sum("supply_value").alias("supply"),
        )
        .withColumn("imbalance", F.col("demand") - F.col("supply"))
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "demand",
            "supply",
            "imbalance",
        )
    )

    return (
        agg.writeStream
        .format("parquet")
        .option("path", f"{BASE}/metrics/uc2/")
        .option("checkpointLocation", f"{CKPT}/uc2/")
        .outputMode("append")
        .start()
    )

# ── UC2B: Courier idle inefficiency per zone ───────────────────────────
def uc2b(orders, couriers):
    demand_stream = (
        orders
        .filter(F.col("event_type").isin(["ORDER_CREATED", "ORDER_ACCEPTED"]))
        .select(
            "event_time",
            "zone_id",
            F.lit(1).alias("demand_value"),
            F.lit(0).alias("available_value"),
        )
    )

    available_stream = (
        couriers
        .filter(F.col("courier_status").isin(["ONLINE", "AVAILABLE"]))
        .select(
            "event_time",
            "zone_id",
            F.lit(0).alias("demand_value"),
            F.lit(1).alias("available_value"),
        )
    )

    combined = demand_stream.unionByName(available_stream)

    agg = (
        combined
        .groupBy(F.window("event_time", WIN), "zone_id")
        .agg(
            F.sum("demand_value").alias("demand"),
            F.sum("available_value").alias("available_couriers"),
        )
        .withColumn(
            "available_safe",
            F.when(F.col("available_couriers") == 0, F.lit(0.001))
             .otherwise(F.col("available_couriers"))
        )
        .withColumn(
            "utilization_ratio",
            F.round(F.col("demand") / F.col("available_safe"), 2)
        )
        .withColumn(
            "idle_inefficiency",
            F.when(
                F.col("available_couriers") > F.col("demand"),
                F.col("available_couriers") - F.col("demand")
            ).otherwise(F.lit(0))
        )
        .withColumn(
            "is_underutilised",
            (F.col("available_couriers") >= 2) &
            (F.col("utilization_ratio") < IDLE_UTIL_THRESHOLD)
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "demand",
            "available_couriers",
            "utilization_ratio",
            "idle_inefficiency",
            "is_underutilised",
        )
    )

    return (
        agg.writeStream
        .format("parquet")
        .option("path", f"{BASE}/metrics/uc2b/")
        .option("checkpointLocation", f"{CKPT}/uc2b/")
        .outputMode("append")
        .start()
    )

# ── UC3A: Surge indicator ───────────────────────────────────────────────
def uc3(orders, couriers):
    demand_stream = (
        orders
        .filter(F.col("event_type") == "ORDER_CREATED")
        .select(
            "event_time",
            "zone_id",
            F.lit(1).alias("demand_value"),
            F.lit(0).alias("supply_value"),
        )
    )

    supply_stream = (
        couriers
        .filter(F.col("courier_status").isin(["ONLINE", "AVAILABLE"]))
        .select(
            "event_time",
            "zone_id",
            F.lit(0).alias("demand_value"),
            F.lit(1).alias("supply_value"),
        )
    )

    combined = demand_stream.unionByName(supply_stream)

    surge = (
        combined
        .groupBy(F.window("event_time", WIN), "zone_id")
        .agg(
            F.sum("demand_value").alias("demand"),
            F.sum("supply_value").alias("supply"),
        )
        .withColumn(
            "supply_safe",
            F.when(F.col("supply") == 0, F.lit(0.001)).otherwise(F.col("supply"))
        )
        .withColumn("ratio", F.round(F.col("demand") / F.col("supply_safe"), 2))
        .withColumn(
            "is_surge",
            (F.col("ratio") > SURGE_THRESHOLD) & (F.col("demand") >= 3)
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "demand",
            "supply",
            "ratio",
            "is_surge",
        )
    )

    return (
        surge.writeStream
        .format("parquet")
        .option("path", f"{BASE}/metrics/uc3/")
        .option("checkpointLocation", f"{CKPT}/uc3/")
        .outputMode("append")
        .start()
    )

# ── UC3B: Pricing anomaly proxy using order_value baseline ─────────────
def uc3b(orders):
    current = (
        orders
        .filter(F.col("event_type") == "ORDER_CREATED")
        .groupBy(F.window("event_time", WIN), "zone_id")
        .agg(
            F.avg("order_value").alias("current_avg_order_value"),
            F.count("order_id").alias("order_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "zone_id",
            "current_avg_order_value",
            "order_count",
        )
    )

    baseline = (
        orders
        .filter(F.col("event_type") == "ORDER_CREATED")
        .groupBy(F.window("event_time", BASELINE_WIN, WIN), "zone_id")
        .agg(F.avg("order_value").alias("baseline_avg_order_value"))
        .select(
            F.col("window.end").alias("window_end"),
            "zone_id",
            "baseline_avg_order_value",
        )
    )

    anomalies = (
        current
        .join(baseline, on=["window_end", "zone_id"], how="inner")
        .withColumn(
            "baseline_safe",
            F.when(F.col("baseline_avg_order_value") == 0, F.lit(0.001))
             .otherwise(F.col("baseline_avg_order_value"))
        )
        .withColumn(
            "price_deviation_ratio",
            F.round(F.col("current_avg_order_value") / F.col("baseline_safe"), 2)
        )
        .withColumn(
            "is_pricing_anomaly",
            (F.col("price_deviation_ratio") > PRICING_ANOMALY_THRESHOLD) &
            (F.col("order_count") >= 3)
        )
        .select(
            "window_start",
            "window_end",
            "zone_id",
            "current_avg_order_value",
            "baseline_avg_order_value",
            "price_deviation_ratio",
            "order_count",
            "is_pricing_anomaly",
        )
    )

    return (
        anomalies.writeStream
        .format("parquet")
        .option("path", f"{BASE}/metrics/uc3b/")
        .option("checkpointLocation", f"{CKPT}/uc3b/")
        .outputMode("append")
        .start()
    )

# ── Main ────────────────────────────────────────────────────────────────
def main():
    if not EH_CONN_STR:
        raise ValueError("EVENTHUB_CONNECTION_STRING is not set")

    if not STORAGE_KEY:
        raise ValueError("STORAGE_ACCOUNT_KEY is not set")

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    orders = read_orders(spark)
    couriers = read_couriers(spark)

    sink_parquet(orders, "orders")
    sink_parquet(couriers, "couriers")

    uc1(orders)
    uc2(orders, couriers)
    uc2b(orders, couriers)
    uc3(orders, couriers)
    uc3b(orders)

    print("[Streaming] All queries running. Awaiting termination ...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
