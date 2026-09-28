# Databricks notebook source

# MAGIC %md
# MAGIC # Late Deletes After the Tombstone Expires: AUTO CDC, SCD Type 1 and Type 2
# MAGIC
# MAGIC In a recent Databricks Community thread about AUTO CDC and late-arriving deletes:
# MAGIC
# MAGIC > "If a delete arrives after its tombstone retention has passed, for example during a backfill of old data, what happens? Is the event silently ignored, applied as a new change, or does it cause an error?"
# MAGIC
# MAGIC The poster read the docs right: a deleted row is kept as a tombstone for a retention window, two days by default, and the docs don't say what happens once that window has closed. The replies split between "a silent no-op" and "old events can bring deleted rows back", so this is a test of the docs' own setting. Found this interesting, so I put together an experiment.
# MAGIC
# MAGIC The notebook builds one Lakeflow pipeline with four AUTO CDC targets (SCD Type 1 and Type 2, each with a 60-second and a default tombstone retention), waits out the short window, sends a batch of late events, and records what every target did with each one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on Databricks Free Edition in about 15 to 20 minutes. Most of that is four pipeline updates and one two-minute wait for the short retention window to pass.
# MAGIC
# MAGIC - **Compute:** serverless notebook compute, the default. The pipeline itself runs on [serverless Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/).
# MAGIC - **Libraries:** no pip installs. The notebook uses PySpark and the [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python), which serverless compute already has, to create and run the pipeline.
# MAGIC - **Identity:** your workspace user. The notebook writes the pipeline source file under your home folder and creates the pipeline as you, so you own its [event log](https://docs.databricks.com/aws/en/sql/language-manual/functions/event_log).
# MAGIC - **Objects created:** a schema `auto_cdc_late_deletes` in the `workspace` catalog, one Bronze Delta table, one pipeline and its four streaming tables. The cleanup cell at the end removes all of it.

# COMMAND ----------

import json
import os
import time

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `SHORT_RETENTION_SECONDS` is the tombstone retention on the two "short" targets. The docs set it with the `pipelines.cdc.tombstoneGCThresholdInSeconds` table property; the two "default" targets leave it unset, which means two days. `RETENTION_WAIT_SECONDS` is how long the notebook waits after the deletes land, so the short window has passed before any late event arrives.

# COMMAND ----------

CATALOG = "workspace"                 # Free Edition's writable catalog
SCHEMA = "auto_cdc_late_deletes"
SHORT_RETENTION_SECONDS = 60
RETENTION_WAIT_SECONDS = SHORT_RETENTION_SECONDS + 60

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
USER = ctx.userName().get()

BRONZE = f"{CATALOG}.{SCHEMA}.bronze_customer_changes"
PIPELINE_NAME = f"auto_cdc_late_deletes_{USER.split('@')[0]}"
SOURCE_DIR = f"/Workspace/Users/{USER}/auto_cdc_late_deletes"
SOURCE_PATH = f"{SOURCE_DIR}/cdc_pipeline.py"

TARGETS = {
    "scd1_short":   "customers_scd1_short",
    "scd1_default": "customers_scd1_default",
    "scd2_short":   "customers_scd2_short",
    "scd2_default": "customers_scd2_default",
}

w = WorkspaceClient()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"User: {USER}")
print(f"Schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: the change feed
# MAGIC
# MAGIC Six Stark Industries customers, one row per change event: `customer_id`, `name`, `city`, `operation` (`INSERT`, `UPDATE` or `DELETE`) and `seq`, the ordering column the pipeline's `SEQUENCE BY` reads. Each pipeline update below appends one batch to this table first, so the pipeline sees the events in the order a real feed would deliver them, late ones included.

# COMMAND ----------

CHANGE_SCHEMA = "customer_id INT, name STRING, city STRING, operation STRING, seq BIGINT"

def append_changes(rows):
    """Append one batch of change events to Bronze."""
    spark.createDataFrame(rows, CHANGE_SCHEMA).write.mode("append").saveAsTable(BRONZE)

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
spark.sql(f"CREATE TABLE {BRONZE} ({CHANGE_SCHEMA})")
print(f"Bronze table ready: {BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The pipeline source
# MAGIC
# MAGIC One [AUTO CDC flow](https://docs.databricks.com/aws/en/ldp/cdc) per target, all four reading the same Bronze feed through one view, with the same keys, the same `SEQUENCE BY seq` and the same delete rule. The only differences are the SCD type and the tombstone retention, set as a table property on [`create_streaming_table`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-streaming-table). The flow arguments follow the [`create_auto_cdc_flow` reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes).

# COMMAND ----------

PIPELINE_SOURCE = '''
from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

BRONZE = spark.conf.get("tl.bronze_table")
SHORT_RETENTION = spark.conf.get("tl.short_retention_seconds")

@dp.view
def customer_changes_feed():
    return spark.readStream.table(BRONZE)

RETENTION = {
    "short": {"pipelines.cdc.tombstoneGCThresholdInSeconds": SHORT_RETENTION},
    "default": {},
}

for scd in ("1", "2"):
    for arm, props in RETENTION.items():
        target = f"customers_scd{scd}_{arm}"
        dp.create_streaming_table(target, table_properties=props)
        dp.create_auto_cdc_flow(
            target=target,
            source="customer_changes_feed",
            keys=["customer_id"],
            sequence_by=col("seq"),
            apply_as_deletes=expr("operation = 'DELETE'"),
            except_column_list=["operation"],
            stored_as_scd_type=scd,
            name=f"cdc_{target}",
        )
'''

os.makedirs(SOURCE_DIR, exist_ok=True)
with open(SOURCE_PATH, "w") as f:
    f.write(PIPELINE_SOURCE)
print(f"Pipeline source written: {SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the pipeline
# MAGIC
# MAGIC A triggered, serverless pipeline publishing to `workspace.auto_cdc_late_deletes`. If an earlier run of this notebook left a pipeline with the same name, it is deleted first, which also drops its streaming tables, so every run starts clean.

# COMMAND ----------

def api(method, path, body=None, query=None):
    """Call the workspace REST API as the notebook user."""
    return w.api_client.do(method, path, body=body, query=query) or {}

for p in api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{PIPELINE_NAME}'"}).get("statuses", []):
    api("DELETE", f"/api/2.0/pipelines/{p['pipeline_id']}")
    print(f"Deleted earlier pipeline {p['pipeline_id']}")

PIPELINE_ID = api("POST", "/api/2.0/pipelines", body={
    "name": PIPELINE_NAME,
    "catalog": CATALOG,
    "schema": SCHEMA,
    "serverless": True,
    "continuous": False,
    "development": True,
    "libraries": [{"file": {"path": SOURCE_PATH}}],
    "configuration": {
        "tl.bronze_table": BRONZE,
        "tl.short_retention_seconds": str(SHORT_RETENTION_SECONDS),
    },
})["pipeline_id"]
print(f"Pipeline created: {PIPELINE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: run an update, read every target
# MAGIC
# MAGIC `run_update` starts an incremental update and waits for it to finish, returning its final state and, if it failed, the first error message from the event log. `snapshot` reads each target: for SCD Type 1 the row per key, for SCD Type 2 the current row (`__END_AT IS NULL`) and the count of history rows per key.

# COMMAND ----------

FINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}

def run_update(label):
    """Start an incremental update, wait for it, return (state, error message or None)."""
    update_id = api("POST", f"/api/2.0/pipelines/{PIPELINE_ID}/updates", body={"full_refresh": False})["update_id"]
    while True:
        state = api("GET", f"/api/2.0/pipelines/{PIPELINE_ID}/updates/{update_id}")["update"]["state"]
        if state in FINAL_STATES:
            break
        time.sleep(10)
    error = None
    if state != "COMPLETED":
        rows = spark.sql(f"""
            SELECT message FROM event_log('{PIPELINE_ID}')
            WHERE level = 'ERROR' AND origin.update_id = '{update_id}'
            ORDER BY timestamp LIMIT 1""").collect()
        error = rows[0]["message"] if rows else "no ERROR event recorded"
    print(f"{label}: update {update_id} -> {state}" + (f" ({error})" if error else ""))
    return state, error, update_id


def snapshot():
    """Current row (city, seq) and history row count per key, for every target."""
    snap = {}
    for arm, table in TARGETS.items():
        df = spark.table(f"{CATALOG}.{SCHEMA}.{table}")
        per_key = {}
        if arm.startswith("scd2"):
            history = {r["customer_id"]: r["n"] for r in df.groupBy("customer_id").agg(F.count("*").alias("n")).collect()}
            current = {r["customer_id"]: r for r in df.where("__END_AT IS NULL").collect()}
        else:
            history = {}
            current = {r["customer_id"]: r for r in df.collect()}
        for key in range(1, 7):
            row = current.get(key)
            per_key[key] = {
                "present": row is not None,
                "city": row["city"] if row else None,
                "history_rows": history.get(key, 1 if row else 0),
            }
        snap[arm] = per_key
    return snap

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the docs say
# MAGIC
# MAGIC From the [`create_auto_cdc_flow` reference](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes), on `apply_as_deletes`:
# MAGIC
# MAGIC > "To handle out-of-order data, the deleted row is temporarily retained as a tombstone in the underlying Delta table, and a view is created in the metastore that filters out these tombstones. The retention interval defaults to two days, and can be configured with the pipelines.cdc.tombstoneGCThresholdInSeconds table property."
# MAGIC
# MAGIC And on the setting itself: "This ensures that delete tombstones are retained long enough to correctly handle late-arriving or out-of-order deletion events." On `sequence_by`: "Lakeflow pipelines use this sequencing to handle change events that arrive out of order."
# MAGIC
# MAGIC The page doesn't say what a target does with a late event once the tombstone it needed is gone. The late batch below has four events, and each one asks a different version of that question.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 1: six customers arrive
# MAGIC
# MAGIC Every customer is inserted at `seq` 10.

# COMMAND ----------

CITIES = {1: "Seattle", 2: "Portland", 3: "Denver", 4: "Austin", 5: "Boston", 6: "Chicago"}
NAMES = {1: "Ana Ruiz", 2: "Ben Okafor", 3: "Cara Lind", 4: "Dev Patel", 5: "Eli Moss", 6: "Fay Chen"}

append_changes([(k, NAMES[k], CITIES[k], "INSERT", 10) for k in range(1, 7)])
update_states = {}
update_states["u1_inserts"], _, _ = run_update("Update 1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 2: two deletes and one update
# MAGIC
# MAGIC Customers 1 and 2 are deleted at `seq` 20. Customer 3 moves to Miami at `seq` 30. After this update, customers 1 and 2 exist only as tombstones on the SCD Type 1 targets and as closed history rows on the SCD Type 2 targets.

# COMMAND ----------

append_changes([
    (1, None, None, "DELETE", 20),
    (2, None, None, "DELETE", 20),
    (3, NAMES[3], "Miami", "UPDATE", 30),
])
update_states["u2_deletes"], _, _ = run_update("Update 2")
deletes_landed_at = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Let the short retention pass, then give the pipeline a MERGE to run
# MAGIC
# MAGIC The notebook waits until `RETENTION_WAIT_SECONDS` have passed since the deletes landed, then runs update 3, a single unrelated change (customer 6 moves to Dallas at `seq` 40). Update 3 exists only so every target runs one more MERGE after the short window has closed, before the late batch shows up.

# COMMAND ----------

remaining = RETENTION_WAIT_SECONDS - (time.time() - deletes_landed_at)
if remaining > 0:
    print(f"Waiting {remaining:.0f} more seconds for the short retention window to pass")
    time.sleep(remaining)

append_changes([(6, NAMES[6], "Dallas", "UPDATE", 40)])
update_states["u3_after_wait"], _, _ = run_update("Update 3")
before_late = snapshot()

HISTORY_COLUMNS = "customer_id, name, city, seq, __START_AT, __END_AT"

def scd2_history(stage):
    """Every SCD Type 2 row for customers 1 and 2, on both retention settings, tagged with a stage."""
    return spark.sql(f"""
        SELECT '{stage}' AS stage, 'scd2_short' AS target, {HISTORY_COLUMNS}
        FROM {CATALOG}.{SCHEMA}.customers_scd2_short WHERE customer_id IN (1, 2)
        UNION ALL
        SELECT '{stage}', 'scd2_default', {HISTORY_COLUMNS}
        FROM {CATALOG}.{SCHEMA}.customers_scd2_default WHERE customer_id IN (1, 2)
    """).collect()

history_before = scd2_history("1 before late batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 4: the late batch
# MAGIC
# MAGIC Four events, all arriving after the short retention window has closed:
# MAGIC
# MAGIC | Customer | Late event | Where it sits in `seq` order |
# MAGIC |---|---|---|
# MAGIC | 1 | UPDATE at `seq` 15 | older than its delete at 20 (the "old insert brings a deleted row back" case from the replies) |
# MAGIC | 2 | DELETE at `seq` 15 | older than the delete at 20 that already removed it (the backfill case the poster asked about) |
# MAGIC | 3 | DELETE at `seq` 25 | older than its update at 30, on a live row |
# MAGIC | 4 | DELETE at `seq` 25 | newer than its insert at 10, on a live row |
# MAGIC
# MAGIC Customer 5 gets no event; it is the control.

# COMMAND ----------

append_changes([
    (1, NAMES[1], "Tacoma", "UPDATE", 15),
    (2, None, None, "DELETE", 15),
    (3, None, None, "DELETE", 25),
    (4, None, None, "DELETE", 25),
])
late_state, late_error, late_update_id = run_update("Update 4 (late batch)")
update_states["u4_late_batch"] = late_state
after_late = snapshot()

# COMMAND ----------

# MAGIC %md
# MAGIC ## What each target did with each late event
# MAGIC
# MAGIC For every target and customer, the notebook compares the current row before and after the late batch and labels the change: `came back` (absent before, present after), `deleted` (present before, absent after), `updated` (present both times, different city), or `no change`. For SCD Type 2 it also records how many history rows each customer gained.

# COMMAND ----------

CASES = {
    1: "key1_late_update_after_delete",
    2: "key2_late_delete_of_deleted",
    3: "key3_late_delete_older_than_update",
    4: "key4_late_delete_newer_than_row",
    5: "key5_control",
}

def label(before, after):
    """Name the visible change to one key's current row."""
    if not before["present"] and after["present"]:
        return "came back"
    if before["present"] and not after["present"]:
        return "deleted"
    if before["present"] and before["city"] != after["city"]:
        return "updated"
    return "no change"

outcomes = {}
table_rows = []
for arm in TARGETS:
    outcomes[arm] = {}
    for key, case in CASES.items():
        b, a = before_late[arm][key], after_late[arm][key]
        outcomes[arm][case] = label(b, a)
        outcomes[arm][f"{case}_history_rows_added"] = a["history_rows"] - b["history_rows"]
        table_rows.append((arm, key, case, outcomes[arm][case], a["city"], a["history_rows"] - b["history_rows"]))
    outcomes[arm]["active_rows"] = sum(1 for k in range(1, 7) if after_late[arm][k]["present"])
    outcomes[arm]["history_rows"] = sum(after_late[arm][k]["history_rows"] for k in range(1, 7))

print("Current rows after the late batch, per target and customer")
display(spark.createDataFrame(
    table_rows,
    "target STRING, customer_id INT, late_event STRING, outcome STRING, current_city STRING, history_rows_added INT",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customers 1 and 2 on the two SCD Type 2 targets, before and after the late batch
# MAGIC
# MAGIC Both customers were deleted at `seq` 20 before the late batch arrived. Customer 1's late event is an update at `seq` 15; customer 2's is a delete at `seq` 15. The table shows every history row for both, on the short and default retention targets, as it stood after update 3 and again after the late batch. A row with a null `__END_AT` is a current row.

# COMMAND ----------

history_after = scd2_history("2 after late batch")
history_rows = [tuple(r) for r in history_before + history_after]

print("Customers 1 and 2 history, SCD Type 2, before and after the late batch")
display(
    spark.createDataFrame(
        history_rows,
        "stage STRING, target STRING, customer_id INT, name STRING, city STRING, seq BIGINT, __START_AT BIGINT, __END_AT BIGINT",
    ).orderBy("customer_id", "target", "stage", "__START_AT")
)

def history_signature(rows, target, key):
    """Compact (city, start, end) list for one customer on one target, ordered by start."""
    picked = sorted(
        [r for r in rows if r["target"] == target and r["customer_id"] == key],
        key=lambda r: (r["__START_AT"] is None, r["__START_AT"]),
    )
    return [[r["city"], r["__START_AT"], r["__END_AT"]] for r in picked]

scd2_histories = {
    target: {
        f"customer{key}_{when}": history_signature(rows, target, key)
        for key in (1, 2)
        for when, rows in (("before", history_before), ("after", history_after))
    }
    for target in ("scd2_short", "scd2_default")
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rows the late batch upserted and deleted, from the event log
# MAGIC
# MAGIC The [CDC metrics](https://docs.databricks.com/aws/en/ldp/cdc-advanced) the pipeline records per flow: `num_upserted_rows` and `num_deleted_rows` for the late-batch update, read from the pipeline [event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs).

# COMMAND ----------

flow_metrics = {}
metric_keys_seen = set()
try:
    for r in spark.sql(f"""
        SELECT origin.flow_name AS flow,
               get_json_object(details, '$.flow_progress.metrics') AS metrics
        FROM event_log('{PIPELINE_ID}')
        WHERE event_type = 'flow_progress'
          AND origin.update_id = '{late_update_id}'
    """).collect():
        metrics = json.loads(r["metrics"]) if r["metrics"] else {}
        metric_keys_seen.update(metrics)
        current = flow_metrics.setdefault(r["flow"], {})
        for name in ("num_upserted_rows", "num_deleted_rows"):
            if metrics.get(name) is not None:
                current[name] = max(current.get(name, 0), int(metrics[name]))
    metrics_error = None
except Exception as e:  # the late batch is still measured by the snapshots above
    metrics_error = type(e).__name__
    print(f"Event log metrics unavailable: {metrics_error}")

print(f"Metric names the late-batch flow_progress events carried: {sorted(metric_keys_seen)}")
for arm, table in TARGETS.items():
    m = flow_metrics.get(f"cdc_{table}", {})
    outcomes[arm]["late_batch_upserted_rows"] = m.get("num_upserted_rows")
    outcomes[arm]["late_batch_deleted_rows"] = m.get("num_deleted_rows")
    print(f"{arm}: upserted {m.get('num_upserted_rows')}, deleted {m.get('num_deleted_rows')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

results = {
    "short_retention_setting": SHORT_RETENTION_SECONDS,
    "retention_wait_setting": RETENTION_WAIT_SECONDS,
    "customers_seeded": 6,
    "late_events": 4,
    "updates": update_states,
    "late_batch_state": late_state,
    "late_batch_error": late_error,
    "metrics_error": metrics_error,
    "metric_names_seen": sorted(metric_keys_seen),
    "arms": outcomes,
    "scd2_histories": scd2_histories,
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC No deleted customer came back. Customer 1's late update arrived after its delete and after the 60-second retention had passed, and it left customer 1 deleted on all four targets. All four pipeline updates completed, so no late event made an update fail.
# MAGIC
# MAGIC The sequence order held for live rows too. The late delete at `seq` 25 for customer 3, whose latest update was at 30, was ignored and the row still read Miami. The late delete at 25 for customer 4, last changed at 10, was applied everywhere. Customer 2's backfilled delete at 15 changed nothing visible on the SCD Type 1 targets.
# MAGIC
# MAGIC The SCD Type 2 targets are where the late batch left a mark, and it's the part to plan for:
# MAGIC
# MAGIC - **Customer 1:** the history went from Seattle 10 to 20 to Seattle 10 to 15, then Tacoma 15 to 20. The late update was placed in sequence order ahead of the delete, and the customer stayed deleted.
# MAGIC - **Customer 2:** the history went from Portland 10 to 20 to two rows, Portland 10 to 15 and a second Portland row with a null `__START_AT` that still ends at 20. Neither is current.
# MAGIC
# MAGIC So a query for who is active today (`__END_AT IS NULL`) comes out the same before and after. A query that counts versions per customer, or picks the row valid at a point in time with `__START_AT <= x AND __END_AT > x`, now has a row with no start to handle.
# MAGIC
# MAGIC The 60-second and two-day targets recorded identical outcomes for every event. This run can't tell whether the short-retention tombstones had been cleared by the time the late batch arrived, 120 seconds after the deletes, so what it shows is that nothing came back at that setting, not that expiry is safe in general. The event log carried the `num_upserted_rows` and `num_deleted_rows` metric names for the late batch, but the notebook's per-flow lookup came back empty, so the counts above come from reading the tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [The AUTO CDC APIs](https://docs.databricks.com/aws/en/ldp/cdc): how AUTO CDC processes SCD Type 1 and Type 2 changes, with a worked example of out-of-order updates.
# MAGIC - [`create_auto_cdc_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes): every flow argument, including `apply_as_deletes` and the tombstone retention setting.
# MAGIC - [Advanced AUTO CDC topics](https://docs.databricks.com/aws/en/ldp/cdc-advanced): the CDC metrics in the event log, DML on AUTO CDC targets, and partial updates.
# MAGIC - [The `event_log` table-valued function](https://docs.databricks.com/aws/en/sql/language-manual/functions/event_log): reading a pipeline's event log from SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Deletes the pipeline (which drops its four streaming tables), the Bronze table, the schema and the pipeline source file. Set `CLEANUP = False` to keep them for a look around.

# COMMAND ----------

CLEANUP = True

if CLEANUP:
    api("DELETE", f"/api/2.0/pipelines/{PIPELINE_ID}")
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    os.remove(SOURCE_PATH)
    print(f"Removed pipeline {PIPELINE_ID}, schema {CATALOG}.{SCHEMA} and {SOURCE_PATH}")
