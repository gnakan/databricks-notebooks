# Databricks notebook source

# MAGIC %md
# MAGIC # Where Failed Rows Go: Three Quarantine Designs for Lakeflow Expectations
# MAGIC
# MAGIC In a recent Databricks Community thread about quarantining records that fail pipeline expectations:
# MAGIC
# MAGIC > "Are you creating a separate "invalid" DLT table for every Silver table, or have you found a way to centralize all "expectation failures" into a single governed view?"
# MAGIC
# MAGIC The poster also asked how a data steward's corrected record gets back in without a full refresh, and whether a long list of expectations slows a pipeline down. The one reply laid out a clear design: one Silver table that keeps every row with an `is_quarantined` flag and a `failed_rules` array, a central quarantine table with a fixed envelope schema when there are several Silver tables, and a second append flow for corrections. That's a design worth testing on a real pipeline, so I put together an experiment.
# MAGIC
# MAGIC The notebook builds the three designs side by side in one Lakeflow pipeline over the same two Stark Industries Bronze tables, each with a documented list of bad rows. It checks which rows every design caught, reads from the pipeline event log how many times each design reads Bronze, sends a steward's corrections back through a second append flow, tries a quarantine view that knows about fixes, and times a second pipeline with zero, 4 and 20 expectations on the same data.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on Databricks Free Edition in about 25 to 30 minutes. Almost all of that is six pipeline updates; the notebook waits for each one.
# MAGIC
# MAGIC - **Free Edition:** serverless notebook compute, the default, and [serverless Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/), which the notebook creates for you.
# MAGIC - **Paid workspace:** the same notebook runs unchanged; set `CATALOG` to a catalog you can create schemas in.
# MAGIC - **Libraries:** no pip installs. The notebook uses PySpark and the [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python), which serverless compute already has, to create and run the pipelines.
# MAGIC - **Identity:** your workspace user. The pipeline source files go under your home folder and the pipelines run as you, so you can read their [event logs](https://docs.databricks.com/aws/en/sql/language-manual/functions/event_log).
# MAGIC - **Objects created:** a schema `ldp_quarantine` in the `workspace` catalog holding two Bronze tables, a ground-truth table, a corrections table and eight views; two pipelines and their streaming tables; two pipeline source files. The cleanup cell at the end removes all of it.

# COMMAND ----------

import json
import os
import statistics
import time

from databricks.sdk import WorkspaceClient
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `ORDER_ROWS` and `CUSTOMER_ROWS` size the two Bronze tables. `TIMED_UPDATES` is how many full refreshes of the timing pipeline are measured after one untimed warm-up; the results report the median.

# COMMAND ----------

CATALOG = "workspace"                 # Free Edition's writable catalog
SCHEMA = "ldp_quarantine"
ORDER_ROWS = 1_000_000
CUSTOMER_ROWS = 100_000
TIMED_UPDATES = 3                     # measured full refreshes, after one warm-up
CORRECTIONS_TO_SEND = 500             # quarantined orders the steward fixes

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
USER = ctx.userName().get()
USER_TAG = USER.split("@")[0].replace(".", "_")

ORDERS_BRONZE = f"{CATALOG}.{SCHEMA}.orders_bronze"
CUSTOMERS_BRONZE = f"{CATALOG}.{SCHEMA}.customers_bronze"
KNOWN_BAD = f"{CATALOG}.{SCHEMA}.known_bad_rows"
ORDER_CORRECTIONS = f"{CATALOG}.{SCHEMA}.order_corrections"

SOURCE_DIR = f"/Workspace/Users/{USER}/ldp_quarantine"
DESIGNS_SOURCE_PATH = f"{SOURCE_DIR}/quarantine_designs.py"
TIMING_SOURCE_PATH = f"{SOURCE_DIR}/expectation_timing.py"
DESIGNS_PIPELINE = f"ldp_quarantine_designs_{USER_TAG}"
TIMING_PIPELINE = f"ldp_quarantine_timing_{USER_TAG}"

w = WorkspaceClient()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"User: {USER}")
print(f"Schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: two Stark Industries feeds with known bad rows
# MAGIC
# MAGIC Orders and customers, each with bad rows placed by a fixed rule on the row number, so the list of bad rows is known before any pipeline runs:
# MAGIC
# MAGIC | Table | Row number | What's wrong | Rule it breaks |
# MAGIC |---|---|---|---|
# MAGIC | orders | ends in 11 (mod 100) | no customer | `valid_customer` |
# MAGIC | orders | ends in 22 | negative amount | `valid_amount` |
# MAGIC | orders | ends in 33 | country `XX` | `valid_country` |
# MAGIC | orders | ends in 44 | status `lost` | `valid_status` |
# MAGIC | orders | ends in 55 | negative amount and status `lost` | `valid_amount`, `valid_status` |
# MAGIC | customers | 7 (mod 50) | no email | `valid_email` |
# MAGIC | customers | 17 | email with no `@` | `valid_email` |
# MAGIC | customers | 27 | tier `platinum` | `valid_tier` |
# MAGIC | customers | 37 | no email and country `XX` | `valid_email`, `valid_country` |
# MAGIC
# MAGIC `known_bad_rows` holds that list: one row per bad record with the rules it should fail. Every design is scored against it.

# COMMAND ----------

o = spark.range(ORDER_ROWS).withColumnRenamed("id", "n")
m = F.col("n") % 100
orders = o.select(
    F.format_string("SO-%07d", "n").alias("order_id"),
    F.when(m == 11, F.lit(None)).otherwise(F.format_string("CUST-%06d", F.col("n") % CUSTOMER_ROWS)).alias("customer_id"),
    F.format_string("SKU-%04d", F.col("n") % 700).alias("sku"),
    (F.col("n") % 5 + 1).cast("int").alias("quantity"),
    F.when(m.isin(22, 55), F.lit(-5.0)).otherwise(F.round(F.col("n") % 997 + 19.99, 2)).alias("amount"),
    F.when(m == 33, F.lit("XX")).otherwise(F.element_at(F.array(*[F.lit(c) for c in ["US", "CA", "MX", "UK", "DE", "FR", "JP"]]), (F.col("n") % 7 + 1).cast("int"))).alias("country"),
    F.when(m.isin(44, 55), F.lit("lost")).otherwise(F.element_at(F.array(*[F.lit(s) for s in ["placed", "shipped", "delivered", "returned"]]), (F.col("n") % 4 + 1).cast("int"))).alias("status"),
    F.timestamp_seconds(F.lit(1788220800) + F.col("n") % 2_592_000).alias("event_time"),
)

c = spark.range(CUSTOMER_ROWS).withColumnRenamed("id", "n")
k = F.col("n") % 50
customers = c.select(
    F.format_string("CUST-%06d", "n").alias("customer_id"),
    F.format_string("Stark Customer %06d", "n").alias("name"),
    F.when(k.isin(7, 37), F.lit(None))
     .when(k == 17, F.format_string("customer%06d.starkindustries.com", "n"))
     .otherwise(F.format_string("customer%06d@starkindustries.com", "n")).alias("email"),
    F.when(k == 37, F.lit("XX")).otherwise(F.element_at(F.array(*[F.lit(c) for c in ["US", "CA", "MX", "UK", "DE", "FR", "JP"]]), (F.col("n") % 7 + 1).cast("int"))).alias("country"),
    F.when(k == 27, F.lit("platinum")).otherwise(F.element_at(F.array(*[F.lit(t) for t in ["bronze", "silver", "gold"]]), (F.col("n") % 3 + 1).cast("int"))).alias("tier"),
    F.timestamp_seconds(F.lit(1767225600) + F.col("n") * 60).alias("event_time"),
)

known_bad = (
    o.where(m.isin(11, 22, 33, 44, 55)).select(
        F.lit("orders").alias("source"),
        F.format_string("SO-%07d", "n").alias("record_id"),
        F.when(m == 11, F.array(F.lit("valid_customer")))
         .when(m == 22, F.array(F.lit("valid_amount")))
         .when(m == 33, F.array(F.lit("valid_country")))
         .when(m == 44, F.array(F.lit("valid_status")))
         .otherwise(F.array(F.lit("valid_amount"), F.lit("valid_status"))).alias("expected_rules"),
    ).unionByName(
    c.where(k.isin(7, 17, 27, 37)).select(
        F.lit("customers").alias("source"),
        F.format_string("CUST-%06d", "n").alias("record_id"),
        F.when(k.isin(7, 17), F.array(F.lit("valid_email")))
         .when(k == 27, F.array(F.lit("valid_tier")))
         .otherwise(F.array(F.lit("valid_email"), F.lit("valid_country"))).alias("expected_rules"),
    ))
)

orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ORDERS_BRONZE)
customers.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CUSTOMERS_BRONZE)
known_bad.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(KNOWN_BAD)

# The steward's corrections table starts empty, with the orders schema, so the append flow has a source from the first update.
spark.sql(f"DROP TABLE IF EXISTS {ORDER_CORRECTIONS}")
spark.table(ORDERS_BRONZE).limit(0).write.saveAsTable(ORDER_CORRECTIONS)

display(spark.table(KNOWN_BAD).groupBy("source").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## The three designs, as one pipeline
# MAGIC
# MAGIC All three designs cover both Bronze tables and use the same rules. The rules are written once and shared.
# MAGIC
# MAGIC - **Design A, an invalid table per Silver table.** Silver keeps the valid rows with [`expect_all_or_drop`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-expectations); a second table reads Bronze again with the rules inverted and keeps the rest. Two Bronze tables means four tables.
# MAGIC - **Design B, one Silver table with the verdict as data.** Silver keeps every row and adds `failed_rules` (the names of the rules the row broke) and `is_quarantined`. The rules also run as warn-mode expectations, so the event log records per-rule counts.
# MAGIC - **Design C, a central quarantine table.** Silver is built as in B. One more streaming table, `c_quarantine`, takes the quarantined rows of both Silver tables through two append flows, in the envelope schema from the thread: `source_table`, `event_time`, `failed_rules` and the original row as a JSON `payload`.
# MAGIC
# MAGIC Every design also gets the steward's corrections the same way: a second [append flow](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow) per design reads `order_corrections` and writes into that design's orders Silver table. The table and flow names carry the design letter (`a_`, `b_`, `c_`) so every flow in the event log can be traced to its design.

# COMMAND ----------

RULES_SOURCE = '''
ORDER_RULES = {
    "valid_customer": "customer_id IS NOT NULL",
    "valid_amount": "amount > 0",
    "valid_country": "country IN ('US', 'CA', 'MX', 'UK', 'DE', 'FR', 'JP')",
    "valid_status": "status IN ('placed', 'shipped', 'delivered', 'returned')",
}
CUSTOMER_RULES = {
    "valid_email": "email IS NOT NULL AND email RLIKE '^[^@]+@[^@]+[.][a-z]+$'",
    "valid_country": "country IN ('US', 'CA', 'MX', 'UK', 'DE', 'FR', 'JP')",
    "valid_tier": "tier IN ('bronze', 'silver', 'gold')",
}
'''

DESIGNS_SOURCE = '''
from pyspark import pipelines as dp
from pyspark.sql import functions as F
''' + RULES_SOURCE + '''
SOURCES = {
    "orders": (spark.conf.get("tl.orders_bronze"), ORDER_RULES),
    "customers": (spark.conf.get("tl.customers_bronze"), CUSTOMER_RULES),
}
CORRECTIONS = spark.conf.get("tl.order_corrections")


def all_rules(rules):
    return " AND ".join(f"({r})" for r in rules.values())


def with_verdict(df, rules):
    """Add failed_rules (names of broken rules) and is_quarantined to every row."""
    broken = F.array(*[F.when(~F.coalesce(F.expr(r), F.lit(False)), F.lit(name)) for name, r in rules.items()])
    return (df.withColumn("failed_rules", F.filter(broken, lambda x: x.isNotNull()))
              .withColumn("is_quarantined", F.size("failed_rules") > 0))


def design_a(name, bronze, rules):
    @dp.table(name=f"a_{name}_silver")
    @dp.expect_all_or_drop(rules)
    def silver():
        return spark.readStream.table(bronze)

    @dp.table(name=f"a_{name}_invalid")
    def invalid():
        return spark.readStream.table(bronze).where(f"NOT ({all_rules(rules)})")


def design_b(name, bronze, rules):
    @dp.table(name=f"b_{name}_silver")
    @dp.expect_all(rules)
    def silver():
        return with_verdict(spark.readStream.table(bronze), rules)


def design_c(name, bronze, rules):
    @dp.table(name=f"c_{name}_silver")
    @dp.expect_all(rules)
    def silver():
        return with_verdict(spark.readStream.table(bronze), rules)

    @dp.append_flow(target="c_quarantine", name=f"c_quarantine_from_{name}")
    def to_quarantine():
        df = spark.readStream.table(f"c_{name}_silver").where("is_quarantined")
        row = [col for col in df.columns if col not in ("failed_rules", "is_quarantined")]
        return df.select(
            F.lit(f"c_{name}_silver").alias("source_table"),
            F.col("event_time"),
            F.col("failed_rules"),
            F.to_json(F.struct(*row)).alias("payload"),
        )


dp.create_streaming_table("c_quarantine")

for name, (bronze, rules) in SOURCES.items():
    design_a(name, bronze, rules)
    design_b(name, bronze, rules)
    design_c(name, bronze, rules)


@dp.append_flow(target="a_orders_silver", name="a_orders_corrections")
def a_corrections():
    return spark.readStream.table(CORRECTIONS)


@dp.append_flow(target="b_orders_silver", name="b_orders_corrections")
def b_corrections():
    return with_verdict(spark.readStream.table(CORRECTIONS), ORDER_RULES)


@dp.append_flow(target="c_orders_silver", name="c_orders_corrections")
def c_corrections():
    return with_verdict(spark.readStream.table(CORRECTIONS), ORDER_RULES)
'''

os.makedirs(SOURCE_DIR, exist_ok=True)
with open(DESIGNS_SOURCE_PATH, "w") as f:
    f.write(DESIGNS_SOURCE)
print(f"Pipeline source written: {DESIGNS_SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The timing pipeline
# MAGIC
# MAGIC A second pipeline answers the performance question on its own, so the three designs don't compete with it for compute. Three streaming tables read the same orders Bronze table: one with no expectations, one with the 4 order rules, one with those 4 plus 16 more checks a team might add (lengths, ranges, formats). All three use warn mode, so every table writes the same rows and the only difference is the rules.

# COMMAND ----------

TIMING_SOURCE = '''
from pyspark import pipelines as dp
''' + RULES_SOURCE + '''
BRONZE = spark.conf.get("tl.orders_bronze")

EXTRA_RULES = {
    "order_id_format": "order_id RLIKE '^SO-[0-9]{7}$'",
    "order_id_length": "length(order_id) = 10",
    "sku_format": "sku RLIKE '^SKU-[0-9]{4}$'",
    "sku_present": "sku IS NOT NULL",
    "quantity_min": "quantity >= 1",
    "quantity_max": "quantity <= 1000",
    "amount_max": "amount < 1000000",
    "amount_scale": "round(amount, 2) = amount",
    "country_length": "length(country) = 2",
    "country_upper": "upper(country) = country",
    "status_lower": "lower(status) = status",
    "event_time_present": "event_time IS NOT NULL",
    "event_time_min": "event_time >= '2020-01-01'",
    "event_time_max": "event_time < '2030-01-01'",
    "customer_format": "customer_id IS NULL OR customer_id RLIKE '^CUST-[0-9]{6}$'",
    "line_total": "quantity * amount < 100000000",
}


@dp.table(name="t_rules_0")
def rules_0():
    return spark.readStream.table(BRONZE)


@dp.table(name="t_rules_4")
@dp.expect_all(ORDER_RULES)
def rules_4():
    return spark.readStream.table(BRONZE)


@dp.table(name="t_rules_20")
@dp.expect_all({**ORDER_RULES, **EXTRA_RULES})
def rules_20():
    return spark.readStream.table(BRONZE)
'''

with open(TIMING_SOURCE_PATH, "w") as f:
    f.write(TIMING_SOURCE)
print(f"Pipeline source written: {TIMING_SOURCE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create both pipelines
# MAGIC
# MAGIC Triggered, serverless pipelines publishing to `workspace.ldp_quarantine`. If an earlier run of this notebook left pipelines with the same names, they're deleted first, which also drops their streaming tables, so every run starts clean.

# COMMAND ----------

def api(method, path, body=None, query=None):
    """Call the workspace REST API as the notebook user."""
    return w.api_client.do(method, path, body=body, query=query) or {}


def create_pipeline(name, source_path):
    """Delete any pipeline with this name, then create a fresh serverless one."""
    for p in api("GET", "/api/2.0/pipelines", query={"filter": f"name LIKE '{name}'"}).get("statuses", []):
        api("DELETE", f"/api/2.0/pipelines/{p['pipeline_id']}")
        print(f"Deleted earlier pipeline {p['pipeline_id']}")
    pipeline_id = api("POST", "/api/2.0/pipelines", body={
        "name": name,
        "catalog": CATALOG,
        "schema": SCHEMA,
        "serverless": True,
        "continuous": False,
        "development": True,
        "libraries": [{"file": {"path": source_path}}],
        "configuration": {
            "tl.orders_bronze": ORDERS_BRONZE,
            "tl.customers_bronze": CUSTOMERS_BRONZE,
            "tl.order_corrections": ORDER_CORRECTIONS,
        },
    })["pipeline_id"]
    print(f"Pipeline {name}: {pipeline_id}")
    return pipeline_id


DESIGNS_ID = create_pipeline(DESIGNS_PIPELINE, DESIGNS_SOURCE_PATH)
TIMING_ID = create_pipeline(TIMING_PIPELINE, TIMING_SOURCE_PATH)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers: run an update, read the event log
# MAGIC
# MAGIC `run_update` starts an update and waits for it. What it returns comes from the updates API, so a full refresh can't hide. `flow_report` reads one update's `flow_definition` and `flow_progress` events from the [event log](https://docs.databricks.com/aws/en/ldp/monitor-event-logs), which is where the pipeline records what each flow read and wrote and how much executor time it used. A flow's `input_datasets` names only datasets inside the pipeline, so the tables a flow reads from outside it, like Bronze, come from the query plan in the same event.

# COMMAND ----------

FINAL_STATES = {"COMPLETED", "FAILED", "CANCELED"}
WATCHED_TABLES = {ORDERS_BRONZE.split(".")[-1], CUSTOMERS_BRONZE.split(".")[-1], ORDER_CORRECTIONS.split(".")[-1]}


def run_update(pipeline_id, label, full_refresh=False):
    """Start an update, wait for it, return a summary dict."""
    update_id = api("POST", f"/api/2.0/pipelines/{pipeline_id}/updates", body={"full_refresh": full_refresh})["update_id"]
    while True:
        update = api("GET", f"/api/2.0/pipelines/{pipeline_id}/updates/{update_id}")["update"]
        if update["state"] in FINAL_STATES:
            break
        time.sleep(10)
    error = None
    if update["state"] != "COMPLETED":
        rows = spark.sql(f"""
            SELECT message FROM event_log('{pipeline_id}')
            WHERE level = 'ERROR' AND origin.update_id = '{update_id}'
            ORDER BY timestamp LIMIT 1""").collect()
        error = rows[0]["message"] if rows else "no ERROR event recorded"
    print(f"{label}: update {update_id} -> {update['state']} (full_refresh={update.get('full_refresh')})" + (f" ({error})" if error else ""))
    return {"update_id": update_id, "state": update["state"], "full_refresh": update.get("full_refresh"), "error": error}


def short_name(flow):
    """Flow names can come back qualified with catalog and schema; keep the last part."""
    return (flow or "").split(".")[-1].strip("`")


def flow_report(pipeline_id, update_id):
    """Per-flow inputs, rows written and dropped, executor time and run time for one update."""
    events = spark.sql(f"""
        SELECT origin.flow_name AS flow, event_type, timestamp, details
        FROM event_log('{pipeline_id}')
        WHERE origin.update_id = '{update_id}'
          AND event_type IN ('flow_definition', 'flow_progress')
        ORDER BY timestamp""").collect()
    report = {}
    for e in events:
        if not e["flow"] or short_name(e["flow"]) == "missingFlowName":
            continue
        r = report.setdefault(short_name(e["flow"]), {
            "inputs": [], "reads": [], "rows_written": 0, "rows_dropped": 0,
            "executor_ms": 0, "running_at": None, "completed_at": None,
        })
        details = json.loads(e["details"])
        if e["event_type"] == "flow_definition":
            definition = details.get("flow_definition", {})
            r["inputs"] = [short_name(d.get("name") if isinstance(d, dict) else d)
                           for d in definition.get("input_datasets", [])]
            # input_datasets lists only datasets inside the pipeline; the query plan names the tables it scans.
            plan = json.dumps(definition)
            r["reads"] = sorted({t for t in WATCHED_TABLES if t in plan} | set(r["inputs"]))
            continue
        progress = details.get("flow_progress", {})
        metrics = progress.get("metrics") or {}
        r["rows_written"] += int(metrics.get("num_output_rows") or 0)
        r["executor_ms"] += int(metrics.get("executor_time_ms") or 0)
        r["rows_dropped"] += int((progress.get("data_quality") or {}).get("dropped_records") or 0)
        if progress.get("status") == "RUNNING" and r["running_at"] is None:
            r["running_at"] = e["timestamp"]
        if progress.get("status") == "COMPLETED":
            r["completed_at"] = e["timestamp"]
    for r in report.values():
        r["run_seconds"] = (round((r["completed_at"] - r["running_at"]).total_seconds(), 1)
                            if r["running_at"] and r["completed_at"] else None)
    return report

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 1: build all three designs
# MAGIC
# MAGIC The first update creates every table from Bronze. The cell prints each flow's inputs and output as the event log recorded them.

# COMMAND ----------

updates = {}
updates["designs_first"] = run_update(DESIGNS_ID, "Designs, update 1")
first_flows = flow_report(DESIGNS_ID, updates["designs_first"]["update_id"])

print("Flows in update 1, from the event log")
display(spark.createDataFrame(
    [(f, ", ".join(r["reads"]), r["rows_written"], r["rows_dropped"]) for f, r in sorted(first_flows.items())],
    "flow STRING, reads STRING, rows_written LONG, rows_dropped LONG",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the docs say
# MAGIC
# MAGIC From [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations), on warn mode, the default: "Invalid records are written to the target." On drop: "Invalid records are dropped before data is written to the target. The count of dropped records is logged alongside other dataset metrics."
# MAGIC
# MAGIC From [Use flows in Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/flow-examples): "Using append flow queries instead of `UNION` allows you to append to a streaming table from multiple sources without running a full refresh." And: "Flows are identified by a _flow name_, and this name is used to identify streaming checkpoints."
# MAGIC
# MAGIC The [quarantine pattern in the docs](https://docs.databricks.com/aws/en/ldp/expectation-patterns) describes the goal as a way to "enable separate processing paths for valid and invalid records in downstream operations." The cells below check each design against the known bad rows, count its Bronze reads, and then test the corrections flow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which bad rows each design caught
# MAGIC
# MAGIC For every design and source table, the quarantined record IDs are compared with `known_bad_rows`: caught (on the list and quarantined), missed (on the list, not quarantined), and flagged by mistake (quarantined, not on the list). For B and C, the recorded `failed_rules` is also compared with the rules each row should have failed. Design A's invalid table carries no rule names, so it records zero there. Leaked rows are known bad rows that made it into the clean side: Design A's Silver table, or the rows B and C mark as not quarantined.

# COMMAND ----------

def t(name):
    return f"{CATALOG}.{SCHEMA}.{name}"

ID_COLUMN = {"orders": "order_id", "customers": "customer_id"}


def quarantined(design, source):
    """(record_id, failed_rules or None) for every row the design quarantined."""
    idc = ID_COLUMN[source]
    if design == "A":
        return spark.table(t(f"a_{source}_invalid")).select(F.col(idc).alias("record_id"), F.lit(None).cast("array<string>").alias("failed_rules"))
    if design == "B":
        return spark.table(t(f"b_{source}_silver")).where("is_quarantined").select(F.col(idc).alias("record_id"), "failed_rules")
    return (spark.table(t("c_quarantine")).where(F.col("source_table") == f"c_{source}_silver")
            .select(F.get_json_object("payload", f"$.{idc}").alias("record_id"), "failed_rules"))


def clean_side(design, source):
    """Record IDs on the valid side of each design."""
    idc = ID_COLUMN[source]
    if design == "A":
        return spark.table(t(f"a_{source}_silver")).select(F.col(idc).alias("record_id"))
    return spark.table(t(f"{design.lower()}_{source}_silver")).where("NOT is_quarantined").select(F.col(idc).alias("record_id"))


def score(design, source):
    """Caught, missed, flagged by mistake, rule names right, leaked, for one design and source."""
    truth = spark.table(KNOWN_BAD).where(F.col("source") == source).select("record_id", "expected_rules")
    got = quarantined(design, source)
    joined = truth.join(got, "record_id", "full_outer")
    row = joined.agg(
        F.sum(F.when(F.col("expected_rules").isNotNull() & F.col("failed_rules").isNotNull(), 1).otherwise(0)).alias("rules_named"),
        F.sum(F.when(F.col("expected_rules").isNotNull() & F.array_sort("failed_rules").eqNullSafe(F.array_sort("expected_rules")), 1).otherwise(0)).alias("rules_exact"),
    ).first()
    got_ids = got.select("record_id").distinct()
    truth_ids = truth.select("record_id")
    return {
        "known_bad": truth_ids.count(),
        "quarantined": got_ids.count(),
        "caught": truth_ids.join(got_ids, "record_id").count(),
        "missed": truth_ids.join(got_ids, "record_id", "left_anti").count(),
        "flagged_by_mistake": got_ids.join(truth_ids, "record_id", "left_anti").count(),
        "rules_named": int(row["rules_named"] or 0),
        "rules_exact": int(row["rules_exact"] or 0),
        "leaked_to_clean_side": clean_side(design, source).join(truth_ids, "record_id").count(),
    }


designs = {d: {s: score(d, s) for s in ("orders", "customers")} for d in ("A", "B", "C")}

print("Known bad rows caught, per design and source table")
display(spark.createDataFrame(
    [(d, s, v["known_bad"], v["caught"], v["missed"], v["flagged_by_mistake"], v["rules_exact"], v["leaked_to_clean_side"])
     for d, per in designs.items() for s, v in per.items()],
    "design STRING, source STRING, known_bad LONG, caught LONG, missed LONG, flagged_by_mistake LONG, rule_names_exact LONG, leaked_to_clean_side LONG",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## How many times each design reads Bronze
# MAGIC
# MAGIC From the `flow_definition` events of update 1: the flows whose query plan scans a Bronze table, grouped by design. The same cell counts the tables each design leaves behind.

# COMMAND ----------

BRONZE_NAMES = {short_name(ORDERS_BRONZE), short_name(CUSTOMERS_BRONZE)}

for d in designs:
    prefix = f"{d.lower()}_"
    flows = {f: r for f, r in first_flows.items() if f.startswith(prefix)}
    designs[d]["flows"] = len(flows)
    designs[d]["flows_reading_bronze"] = sum(1 for r in flows.values() if BRONZE_NAMES & set(r["reads"]))
    designs[d]["tables"] = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").where(F.col("tableName").startswith(prefix)).count()
    print(f"Design {d}: {designs[d]['flows_reading_bronze']} of {designs[d]['flows']} flows read Bronze, "
          f"{designs[d]['tables']} tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Design C's payload: can the original row come back out?
# MAGIC
# MAGIC The envelope stores each quarantined row as JSON. This cell parses every order payload back with the Bronze schema and compares it, column for column, with the Bronze row it came from.

# COMMAND ----------

orders_schema = spark.table(ORDERS_BRONZE).schema
parsed = (spark.table(t("c_quarantine")).where("source_table = 'c_orders_silver'")
          .select(F.from_json("payload", orders_schema).alias("r")).select("r.*"))
payload_rows = parsed.count()
bronze = spark.table(ORDERS_BRONZE)
payload_identical = parsed.join(
    bronze, [parsed[c.name].eqNullSafe(bronze[c.name]) for c in orders_schema], "left_semi").count()
print(f"Order payloads: {payload_rows:,}; identical to their Bronze row: {payload_identical:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Clean and quarantine views for Design B
# MAGIC
# MAGIC Design B's two views live outside the pipeline as ordinary Unity Catalog views, so a steward and a downstream job can each query their side by name.

# COMMAND ----------

for source in ("orders", "customers"):
    spark.sql(f"CREATE OR REPLACE VIEW {t(f'b_{source}_clean')} AS SELECT * FROM {t(f'b_{source}_silver')} WHERE NOT is_quarantined")
    spark.sql(f"CREATE OR REPLACE VIEW {t(f'b_{source}_quarantine')} AS SELECT * FROM {t(f'b_{source}_silver')} WHERE is_quarantined")
print("Views created: b_orders_clean, b_orders_quarantine, b_customers_clean, b_customers_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## A steward sends corrections
# MAGIC
# MAGIC The steward takes `CORRECTIONS_TO_SEND` orders from `b_orders_quarantine` that failed only `valid_amount`, sets the amount to `125.00`, and writes them to `order_corrections` once. All three designs' corrections flows read that one table. Before the next update, the notebook records the row count and Delta version of every table in the schema, so it can see which tables that update wrote to.

# COMMAND ----------

fixes = (spark.table(t("b_orders_quarantine"))
         .where(F.col("failed_rules") == F.array(F.lit("valid_amount")))
         .orderBy("order_id").limit(CORRECTIONS_TO_SEND)
         .drop("failed_rules", "is_quarantined")
         .withColumn("amount", F.lit(125.0)))
fixes.write.mode("append").saveAsTable(ORDER_CORRECTIONS)
corrected_ids = [r["order_id"] for r in spark.table(ORDER_CORRECTIONS).select("order_id").collect()]
print(f"Corrections written: {len(corrected_ids)}")


B_VIEWS = {f"b_{s}_{side}" for s in ("orders", "customers") for side in ("clean", "quarantine")}


def table_state():
    """Row count and latest Delta version for every table the designs pipeline publishes."""
    state = {}
    for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect():
        name = r["tableName"]
        if name[:2] not in ("a_", "b_", "c_") or name in B_VIEWS:
            continue
        try:
            last = spark.sql(f"DESCRIBE HISTORY {t(name)} LIMIT 1").first()
            version, operation = last["version"], last["operation"]
        except Exception as e:  # row counts still show what changed
            version, operation = None, f"history unavailable: {type(e).__name__}"
        state[name] = {"rows": spark.table(t(name)).count(), "version": version, "operation": operation}
    return state

before_fix = table_state()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update 2: the corrections flow
# MAGIC
# MAGIC An ordinary update, no full refresh requested. Bronze hasn't changed; the only new data is in `order_corrections`.

# COMMAND ----------

updates["designs_corrections"] = run_update(DESIGNS_ID, "Designs, update 2 (corrections)")
fix_flows = flow_report(DESIGNS_ID, updates["designs_corrections"]["update_id"])
after_fix = table_state()

print("What the correction update wrote")
display(spark.createDataFrame(
    [(name, before_fix[name]["rows"], after_fix[name]["rows"], after_fix[name]["version"] != before_fix[name]["version"],
      after_fix[name]["operation"], fix_flows.get(name, {}).get("rows_written"))
     for name in sorted(after_fix) if name in before_fix],
    "table STRING, rows_before LONG, rows_after LONG, new_version BOOLEAN, last_operation STRING, rows_written_by_main_flow LONG",
))

ORDERS_SILVER = {"A": "a_orders_silver", "B": "b_orders_silver", "C": "c_orders_silver"}
per_design = {}
for d, table in ORDERS_SILVER.items():
    fixed = spark.table(t(table)).where(F.col("order_id").isin(corrected_ids))
    per_design[d] = {
        "flow_rows_written": fix_flows.get(f"{d.lower()}_orders_corrections", {}).get("rows_written"),
        "silver_rows_before": before_fix[table]["rows"],
        "silver_rows_after": after_fix[table]["rows"],
        "fixed_orders_valid": fixed.count() if d == "A" else fixed.where("NOT is_quarantined").count(),
    }
correction = {
    "update_state": updates["designs_corrections"]["state"],
    "full_refresh": updates["designs_corrections"]["full_refresh"],
    "corrections_sent": len(corrected_ids),
    "designs": per_design,
    "tables_with_new_version": sorted(n for n in after_fix if n in before_fix and after_fix[n]["version"] != before_fix[n]["version"]),
    "flows_writing_rows": {f: r["rows_written"] for f, r in fix_flows.items() if r["rows_written"]},
}
print(json.dumps(correction, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Teaching each quarantine queue about fixes
# MAGIC
# MAGIC An append flow adds rows to its target and leaves the rows already there alone. So after the corrections update, each design's queue may still hold the original bad order next to a fixed copy that now sits on the clean side. Each design's steward works from a different queue:
# MAGIC
# MAGIC | Design | The steward's queue | Where the fixed order lands |
# MAGIC |---|---|---|
# MAGIC | A | `a_orders_invalid` | `a_orders_silver` |
# MAGIC | B | `b_orders_quarantine`, the flag filter on `b_orders_silver` | `b_orders_silver`, unflagged |
# MAGIC | C | `c_quarantine`, the central table | `c_orders_silver`, unflagged |
# MAGIC
# MAGIC Each queue gets an "open" version with one extra condition: a record stays in it only while its Silver table has no row for it that passes the rules. For C, whose queue holds orders and customers, the view first pulls each record's ID back out of the JSON `payload`. The cell counts the orders in every queue, checks that every bad order the steward hasn't fixed is still in the open version, and times a count on each queue, taking the median of `VIEW_QUERY_REPEATS` after one warm-up.

# COMMAND ----------

VIEW_QUERY_REPEATS = 5

spark.sql(f"""
    CREATE OR REPLACE VIEW {t('a_orders_open_invalid')} AS
    SELECT q.*
    FROM {t('a_orders_invalid')} AS q
    WHERE NOT EXISTS (
        SELECT 1 FROM {t('a_orders_silver')} AS f
        WHERE f.order_id = q.order_id)
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW {t('b_orders_open_quarantine')} AS
    SELECT q.*
    FROM {t('b_orders_silver')} AS q
    WHERE q.is_quarantined
      AND NOT EXISTS (
          SELECT 1 FROM {t('b_orders_silver')} AS f
          WHERE f.order_id = q.order_id AND NOT f.is_quarantined)
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW {t('c_quarantine_keyed')} AS
    SELECT *, coalesce(get_json_object(payload, '$.order_id'),
                       get_json_object(payload, '$.customer_id')) AS record_id
    FROM {t('c_quarantine')}
""")
spark.sql(f"""
    CREATE OR REPLACE VIEW {t('c_open_quarantine')} AS
    SELECT q.*
    FROM {t('c_quarantine_keyed')} AS q
    WHERE NOT EXISTS (
        SELECT 1 FROM (
            SELECT 'c_orders_silver' AS source_table, order_id AS record_id
            FROM {t('c_orders_silver')} WHERE NOT is_quarantined
            UNION ALL
            SELECT 'c_customers_silver', customer_id
            FROM {t('c_customers_silver')} WHERE NOT is_quarantined) AS f
        WHERE f.source_table = q.source_table AND f.record_id = q.record_id)
""")


def median_count_ms(view):
    """Warm up once, then return the median time of VIEW_QUERY_REPEATS counts, in ms."""
    spark.table(view).count()
    samples = []
    for _ in range(VIEW_QUERY_REPEATS):
        start = time.perf_counter()
        spark.table(view).count()
        samples.append((time.perf_counter() - start) * 1000)
    return round(statistics.median(samples))


def orders_in(view, design):
    """The order IDs in a queue view, as one column named order_id."""
    q = spark.table(t(view))
    if design == "C":
        return q.where("source_table = 'c_orders_silver'").select(F.col("record_id").alias("order_id"))
    return q.select("order_id")


QUEUES = {
    "A": {"flag_only": "a_orders_invalid", "open_only": "a_orders_open_invalid"},
    "B": {"flag_only": "b_orders_quarantine", "open_only": "b_orders_open_quarantine"},
    "C": {"flag_only": "c_quarantine_keyed", "open_only": "c_open_quarantine"},
}
unfixed_bad = (spark.table(KNOWN_BAD).where("source = 'orders'")
               .where(~F.col("record_id").isin(corrected_ids)).select(F.col("record_id").alias("order_id")))
queues = {}
for d, views in QUEUES.items():
    queues[d] = {}
    for label, view in views.items():
        ids = orders_in(view, d)
        queues[d][label] = {
            "view": view,
            "orders": ids.count(),
            "fixed_orders_listed": ids.where(F.col("order_id").isin(corrected_ids)).count(),
            "unfixed_bad_orders_missing": unfixed_bad.join(ids, "order_id", "left_anti").count(),
            "median_count_ms": median_count_ms(t(view)),
        }

print("Quarantine queues after the fixes, flag only and open only, per design")
display(spark.createDataFrame(
    [(d, label, v["view"], v["orders"], v["fixed_orders_listed"], v["unfixed_bad_orders_missing"], v["median_count_ms"])
     for d, per in queues.items() for label, v in per.items()],
    "design STRING, queue STRING, view STRING, orders LONG, fixed_orders_listed LONG, unfixed_bad_orders_missing LONG, median_count_ms LONG",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expectation overhead: 0, 4 and 20 rules
# MAGIC
# MAGIC The timing pipeline runs one untimed full refresh as a warm-up, then `TIMED_UPDATES` more. For each table the cell reads the flow's executor time and run time from the event log, and reports the median across the timed updates. The three flows run in the same update on the same compute, so they see the same conditions.

# COMMAND ----------

TIMING_ARMS = ["t_rules_0", "t_rules_4", "t_rules_20"]

updates["timing_warmup"] = run_update(TIMING_ID, "Timing, warm-up", full_refresh=True)
samples = {arm: {"executor_ms": [], "run_seconds": []} for arm in TIMING_ARMS}
timing_states = []
for i in range(TIMED_UPDATES):
    u = run_update(TIMING_ID, f"Timing, update {i + 1}", full_refresh=True)
    timing_states.append(u["state"])
    report = flow_report(TIMING_ID, u["update_id"])
    for arm in TIMING_ARMS:
        r = report.get(arm, {})
        if r.get("executor_ms"):
            samples[arm]["executor_ms"].append(r["executor_ms"])
        if r.get("run_seconds") is not None:
            samples[arm]["run_seconds"].append(r["run_seconds"])

timing = {}
for arm in TIMING_ARMS:
    ex, rs = samples[arm]["executor_ms"], samples[arm]["run_seconds"]
    timing[arm] = {
        "median_executor_ms": int(statistics.median(ex)) if ex else None,
        "median_run_seconds": round(statistics.median(rs), 1) if rs else None,
        "executor_ms_samples": ex,
        "run_seconds_samples": rs,
    }
rules_per_arm = {"t_rules_0": 0, "t_rules_4": 4, "t_rules_20": 20}

print("Expectation overhead: median over the timed full refreshes")
display(spark.createDataFrame(
    [(arm, rules_per_arm[arm], v["median_executor_ms"], v["median_run_seconds"]) for arm, v in timing.items()],
    "table STRING, rules INT, median_executor_ms LONG, median_run_seconds DOUBLE",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC
# MAGIC Every number the findings quote comes from this one line.

# COMMAND ----------

results = {
    "order_rows": ORDER_ROWS,
    "customer_rows": CUSTOMER_ROWS,
    "known_bad_orders": designs["A"]["orders"]["known_bad"],
    "known_bad_customers": designs["A"]["customers"]["known_bad"],
    "updates": {k: {"state": v["state"], "full_refresh": v["full_refresh"], "error": v["error"]} for k, v in updates.items()},
    "designs": designs,
    "c_payload_rows": payload_rows,
    "c_payload_identical": payload_identical,
    "correction": correction,
    "queues": queues,
    "view_query_repeats": VIEW_QUERY_REPEATS,
    "timing": timing,
    "timing_update_states": timing_states,
    "update_repeats": TIMED_UPDATES,
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC All three designs caught every bad row: 50,000 of 50,000 orders and 8,000 of 8,000 customers, with none missed, none flagged by mistake and none on the clean side. What separates them is how often you read Bronze and how many tables you look after. Design A read Bronze in 4 flows and left 4 tables. Designs B and C each read it in 2, and both wrote down exactly which rules every bad row broke, which A's invalid tables don't. C's central table held the original row well enough that all 50,000 order payloads parsed back to their exact Bronze row.
# MAGIC
# MAGIC The corrections flows did what the thread said they would. The update after the steward's fixes was an ordinary one, not a full refresh. In every design the append flow wrote exactly the 500 fixed orders into its orders Silver table, and all 500 read as valid there.
# MAGIC
# MAGIC And in every design, the steward's queue kept all 500. An append flow adds rows and leaves the existing ones where they are, so `a_orders_invalid`, `b_orders_quarantine` and `c_quarantine` each still listed every order the steward had already fixed. You can get a fixed record back in without a full refresh. Your quarantine queue simply won't know it's been fixed.
# MAGIC
# MAGIC One extra condition fixes that for each design: a record stays in the queue only while its Silver table has no row for it that passes the rules. Each open queue listed 49,500 orders, none of the 500 fixed ones, and missed no bad order the steward hadn't touched. The queues got slower to read, from 465 to 990 ms for A, 730 to 1,037 ms for B and 506 to 1,247 ms for C, where the ID has to come back out of the JSON payload first. Only the steward's query pays for that.
# MAGIC
# MAGIC On the performance question: with 20 expectations the flow used a median 2,483 ms of executor time, against 1,604 ms with none and 1,320 ms with 4, while run time only went from 3.0 to 3.5 seconds. Four rules landing under zero tells you how much these small timings move from run to run, so read the 20-rule number as roughly a second of extra work at a million rows, not as a ratio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Manage data quality with pipeline expectations](https://docs.databricks.com/aws/en/ldp/expectations): the warn, drop and fail actions and where their metrics land.
# MAGIC - [Expectation recommendations and advanced patterns](https://docs.databricks.com/aws/en/ldp/expectation-patterns): the docs' own quarantine pattern with an `is_quarantined` column.
# MAGIC - [Use flows in Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/flow-examples): several flows writing to one streaming table, and how flow names map to checkpoints.
# MAGIC - [`append_flow`](https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-append-flow): every argument of the decorator the corrections flow uses.
# MAGIC - [Pipeline event log schema](https://docs.databricks.com/aws/en/ldp/monitor-event-log-schema): the `flow_definition` and `flow_progress` fields this notebook reads.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Deletes both pipelines (which drops their streaming tables), the schema with everything else in it, and the two pipeline source files. Set `CLEANUP = False` to keep them for a look around.

# COMMAND ----------

CLEANUP = True

if CLEANUP:
    for pid in (DESIGNS_ID, TIMING_ID):
        api("DELETE", f"/api/2.0/pipelines/{pid}")
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    for path in (DESIGNS_SOURCE_PATH, TIMING_SOURCE_PATH):
        os.remove(path)
    print(f"Removed pipelines {DESIGNS_ID} and {TIMING_ID}, schema {CATALOG}.{SCHEMA} and the pipeline source files")
