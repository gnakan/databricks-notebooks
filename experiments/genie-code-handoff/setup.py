# Databricks notebook source
# MAGIC %md
# MAGIC # Setup: Genie Code — Bronze to Silver Experiment
# MAGIC
# MAGIC Creates the Unity Catalog schema and volume, then generates ~50,000
# MAGIC synthetic click event records as newline-delimited JSON in the volume.
# MAGIC
# MAGIC **Before running:** update the configuration block in the next cell to
# MAGIC match your catalog and schema.
# MAGIC
# MAGIC ### Data violations seeded in
# MAGIC
# MAGIC The synthetic data includes deliberate quality issues so the pipeline
# MAGIC expectations have something real to catch:
# MAGIC
# MAGIC - 5% of records are duplicate `event_id`s (full row duplicates)
# MAGIC - 2% of records have out-of-range `event_timestamp` values
# MAGIC   (older than 90 days or in the future)
# MAGIC - 2% null rate on each required field (`user_id`, `session_id`,
# MAGIC   `event_type`, `event_timestamp`)
# MAGIC - 30% null rate on the optional `device_type` field

# COMMAND ----------

import json
import random
import uuid
from datetime import datetime, timedelta, timezone

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration — update these values before running

# COMMAND ----------

CATALOG = "your_catalog"   # update to your Unity Catalog catalog
SCHEMA  = "your_schema"    # update to your schema
VOLUME  = "click_events_raw"
OUTPUT_FILENAME = "events.json"

BASE_RECORD_COUNT = 50_000
DUPLICATE_RATE = 0.05
OUT_OF_RANGE_TIMESTAMP_RATE = 0.02
REQUIRED_FIELD_NULL_RATE = 0.02
DEVICE_TYPE_NULL_RATE = 0.30

EVENT_TYPES = [
    "page_view",
    "click",
    "scroll",
    "hover",
    "submit",
    "login",
    "logout",
    "add_to_cart",
]
DEVICE_TYPES = ["mobile", "desktop", "tablet", "smart_tv"]

RANDOM_SEED = 20260430
random.seed(RANDOM_SEED)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create schema and volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
print(f"Schema ready: {CATALOG}.{SCHEMA}")
print(f"Volume ready: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate synthetic click event records

# COMMAND ----------


def generate_event_timestamp() -> str:
    """Return an ISO-8601 timestamp string.

    With probability OUT_OF_RANGE_TIMESTAMP_RATE the timestamp falls outside
    the valid 90-day lookback window (either older than 90 days or in the
    future). Otherwise the timestamp falls inside the window.
    """
    now = datetime.now(timezone.utc)
    if random.random() < OUT_OF_RANGE_TIMESTAMP_RATE:
        if random.random() < 0.5:
            offset = timedelta(days=random.randint(91, 365))
            ts = now - offset
        else:
            offset = timedelta(days=random.randint(1, 30))
            ts = now + offset
    else:
        offset_seconds = random.randint(0, 90 * 24 * 60 * 60)
        ts = now - timedelta(seconds=offset_seconds)
    return ts.isoformat()


def maybe_null(value, null_rate: float):
    """Return None with probability null_rate, otherwise return value."""
    if random.random() < null_rate:
        return None
    return value


def generate_record() -> dict:
    """Generate one click event record with deliberate violations seeded in."""
    event_id = str(uuid.uuid4())
    user_id = maybe_null(f"user_{random.randint(1, 5000):05d}", REQUIRED_FIELD_NULL_RATE)
    session_id = maybe_null(
        f"sess_{random.randint(1, 20000):06d}", REQUIRED_FIELD_NULL_RATE
    )
    event_timestamp = maybe_null(generate_event_timestamp(), REQUIRED_FIELD_NULL_RATE)
    event_type = maybe_null(random.choice(EVENT_TYPES), REQUIRED_FIELD_NULL_RATE)
    device_type = maybe_null(random.choice(DEVICE_TYPES), DEVICE_TYPE_NULL_RATE)
    return {
        "event_id": event_id,
        "user_id": user_id,
        "session_id": session_id,
        "event_timestamp": event_timestamp,
        "event_type": event_type,
        "device_type": device_type,
    }


# COMMAND ----------

base_records = [generate_record() for _ in range(BASE_RECORD_COUNT)]

duplicate_count = int(BASE_RECORD_COUNT * DUPLICATE_RATE)
duplicates = [dict(rec) for rec in random.sample(base_records, duplicate_count)]

all_records = base_records + duplicates
random.shuffle(all_records)

print(f"Base records: {len(base_records):,}")
print(f"Duplicate records appended: {len(duplicates):,}")
print(f"Total records: {len(all_records):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write records as newline-delimited JSON to the volume

# COMMAND ----------

output_path = f"{volume_path}/{OUTPUT_FILENAME}"

with open(output_path, "w") as fh:
    for record in all_records:
        fh.write(json.dumps(record) + "\n")

print(f"Wrote {len(all_records):,} records to {output_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

verify_df = spark.read.json(output_path)
verify_count = verify_df.count()
print(f"Spark read verification: {verify_count:,} records")
verify_df.printSchema()
display(verify_df.limit(10))
