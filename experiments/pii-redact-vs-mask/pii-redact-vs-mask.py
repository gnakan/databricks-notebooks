# Databricks notebook source

# MAGIC %md
# MAGIC # Two PII Patterns, One Catalog: Redact Silver or Mask It
# MAGIC
# MAGIC > "Redact/Hash PII at the Silver layer (permanent change), or...Keep PII in Silver and use Dynamic Data Masking at the Gold/View layer."
# MAGIC > Source: Databricks Community, Data Engineering
# MAGIC
# MAGIC That's the question from a recent community thread, and it's one I hear a lot. The raw PII lands in
# MAGIC Bronze either way. The choice is what Silver, the table everyone downstream reads, looks like: a copy
# MAGIC with the PII stripped out, or the full record behind a
# MAGIC [Unity Catalog column mask](https://docs.databricks.com/aws/en/data-governance/unity-catalog/row-and-column-filters)
# MAGIC so non-privileged users see a placeholder at query time.
# MAGIC
# MAGIC Both patterns are natively supported on Databricks. So I built both from the same Bronze table and
# MAGIC measured what each one costs you, including what it takes to get the personally identifiable
# MAGIC information (PII) back when a new use case needs it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC **What you need:**
# MAGIC - A Databricks Free Edition workspace (Unity Catalog is included)
# MAGIC - Serverless notebook compute (the default on Free Edition)
# MAGIC - The ability to create tables and SQL UDFs in a catalog you own
# MAGIC
# MAGIC **No pip installs required.** This experiment uses PySpark and the standard library only.
# MAGIC
# MAGIC **Runtime:** a few minutes end-to-end, mostly Delta writes and the repeated timed queries.
# MAGIC
# MAGIC **Cleanup:** the notebook creates a schema `pii_experiment` in a catalog you specify below.
# MAGIC All objects are under that schema and can be dropped with a single `DROP SCHEMA ... CASCADE`.

# COMMAND ----------

import json
import statistics
import time
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# --- Edit these ---
CATALOG   = "workspace"     # a catalog you can write to; Free Edition ships with `workspace`
SCHEMA    = "pii_experiment"
ROW_COUNT = 100_000         # synthetic PII records; scales the storage and latency numbers
REPEATS   = 5               # timed runs per write and query; results report the median

# Derived names
BRONZE_TABLE      = f"{CATALOG}.{SCHEMA}.customers_bronze"           # raw PII as it lands, shared by both patterns
SILVER_REDACTED   = f"{CATALOG}.{SCHEMA}.customers_silver_redacted"  # Pattern A: PII stripped from Silver
SILVER_MASKED     = f"{CATALOG}.{SCHEMA}.customers_silver_masked"    # Pattern B: PII kept in Silver, masked at read
SILVER_RESTORED   = f"{CATALOG}.{SCHEMA}.customers_silver_restored"  # Pattern A rebuilt with the PII back
MASK_SSN          = f"{CATALOG}.{SCHEMA}.mask_ssn"                   # column mask UDF for ssn
MASK_EMAIL        = f"{CATALOG}.{SCHEMA}.mask_email"                 # column mask UDF for email

# COMMAND ----------

# Create the schema if it doesn't exist; DROP it at the end when you're done
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers
# MAGIC
# MAGIC Every write and every query runs once to warm up, then `REPEATS` more times, and I keep the median.
# MAGIC Whichever pattern goes first would otherwise pay the compute cold start and look slower than it is.

# COMMAND ----------

def time_write(df, table: str) -> int:
    """Overwrite the table once to warm up, then REPEATS timed overwrites; return the median ms."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    samples = []
    for _ in range(REPEATS):
        t0 = time.monotonic()
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        samples.append((time.monotonic() - t0) * 1000)
    return int(statistics.median(samples))


def time_query(sql: str) -> tuple:
    """Run once to warm up, then REPEATS timed runs; return (median ms, last result row)."""
    row = spark.sql(sql).collect()[0]
    samples = []
    for _ in range(REPEATS):
        t0 = time.monotonic()
        row = spark.sql(sql).collect()[0]
        samples.append((time.monotonic() - t0) * 1000)
    return int(statistics.median(samples)), row


def size_bytes(table: str) -> int:
    """Delta table size from DESCRIBE DETAIL, after OPTIMIZE.

    Compacting first means the number reflects the data, not how many small files the write left behind,
    so tables written different ways still compare.
    """
    spark.sql(f"OPTIMIZE {table}")
    return spark.sql(f"DESCRIBE DETAIL {table}").select("sizeInBytes").collect()[0][0]


def set_masks(table: str) -> None:
    """Bind the ssn and email masks to a table's PII columns."""
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN ssn SET MASK {MASK_SSN}")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN email SET MASK {MASK_EMAIL}")


def drop_masks(table: str) -> None:
    """Remove the ssn and email masks from a table."""
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN ssn DROP MASK")
    spark.sql(f"ALTER TABLE {table} ALTER COLUMN email DROP MASK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: the raw PII lands
# MAGIC
# MAGIC The synthetic data has the shape of a customer record. Each row carries a name and a region, plus a
# MAGIC Social Security number (SSN) and an email, which are the PII columns. Both patterns start from this same Bronze table.

# COMMAND ----------

raw_df = (
    spark.range(ROW_COUNT)
    .select(
        F.col("id").cast(IntegerType()).alias("customer_id"),
        F.concat(F.lit("Customer_"), F.col("id").cast(StringType())).alias("first_name"),
        F.concat(F.lit("Stark_"),    F.col("id").cast(StringType())).alias("last_name"),
        F.concat(
            (F.col("id") % 900 + 100).cast(StringType()), F.lit("-"),
            (F.col("id") % 90  + 10 ).cast(StringType()), F.lit("-"),
            (F.col("id") % 9000 + 1000).cast(StringType()),
        ).alias("ssn"),
        F.concat(
            F.lit("customer_"), F.col("id").cast(StringType()), F.lit("@stark-industries.com")
        ).alias("email"),
        F.element_at(F.array(F.lit("US"), F.lit("EU"), F.lit("APAC")),
                     (F.col("id") % 3 + 1).cast(IntegerType())).alias("region"),
    )
)

# A re-run finds Bronze masked from last time; lift the masks so it starts as raw data again
if spark.catalog.tableExists(BRONZE_TABLE):
    try:
        drop_masks(BRONZE_TABLE)
    except Exception as exc:
        print(f"No Bronze masks to drop: {type(exc).__name__}")

raw_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(BRONZE_TABLE)
bronze_storage_bytes = size_bytes(BRONZE_TABLE)

# What does an ordinary read of Bronze return right now?
bronze_sample = spark.sql(f"SELECT ssn, email FROM {BRONZE_TABLE} LIMIT 1").collect()[0]
bronze_pii_visible_bool = bronze_sample["ssn"] != "***-**-****"

print(f"Bronze: {ROW_COUNT:,} rows, {bronze_storage_bytes:,} bytes")
print(f"Bronze read returns : ssn='{bronze_sample['ssn']}', email='{bronze_sample['email']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern A: Redact Silver
# MAGIC
# MAGIC The write-time approach: Silver is built from Bronze with the PII columns dropped. This is what a
# MAGIC [Lakeflow pipeline](https://docs.databricks.com/aws/en/ldp/transform) transform step does when it
# MAGIC drops or hashes PII before Silver materializes. I'm doing the same thing with a plain Spark write.
# MAGIC
# MAGIC Anyone reading Silver never sees the PII, because it isn't there. It still exists one layer up, in Bronze.

# COMMAND ----------

bronze_df = spark.table(BRONZE_TABLE)

redact_write_time_ms = time_write(bronze_df.drop("ssn", "email"), SILVER_REDACTED)
redact_storage_bytes = size_bytes(SILVER_REDACTED)

print(f"Pattern A Silver write time (median of {REPEATS}) : {redact_write_time_ms:,} ms")
print(f"Pattern A Silver table size : {redact_storage_bytes:,} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pattern B: Keep PII in Silver, Mask It
# MAGIC
# MAGIC The query-time approach: Silver keeps the full record, and a
# MAGIC [column mask](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply)
# MAGIC decides per query what each user sees. A column mask is a SQL UDF bound to a column with
# MAGIC `ALTER TABLE ... ALTER COLUMN ... SET MASK`.
# MAGIC
# MAGIC The UDFs below check whether the current user is a member of the group `pii_full_access`. If yes,
# MAGIC they return the real value. If no, SSN comes back as `'***-**-****'` and email as `'***@***'`.
# MAGIC Your workspace has no such group yet, so every read in this notebook takes the non-privileged path,
# MAGIC which is the one most of your users would take.
# MAGIC
# MAGIC To add a mask to an existing table you need to own it (you do, you just wrote it) or hold both
# MAGIC `MANAGE` and `SELECT` on it.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE FUNCTION {MASK_SSN}(ssn STRING)
  RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii_full_access') THEN ssn
    ELSE '***-**-****'
  END
""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {MASK_EMAIL}(email STRING)
  RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pii_full_access') THEN email
    ELSE '***@***'
  END
""")

# A re-run finds Silver masked from last time; the overwrite needs the masks off first
if spark.catalog.tableExists(SILVER_MASKED):
    try:
        drop_masks(SILVER_MASKED)
    except Exception as exc:
        print(f"No Silver masks to drop: {type(exc).__name__}")

raw_write_time_ms = time_write(bronze_df, SILVER_MASKED)
raw_storage_bytes = size_bytes(SILVER_MASKED)
set_masks(SILVER_MASKED)

# Check the table itself, not just the UDF: what does a read return now?
sample = spark.sql(f"SELECT ssn, email FROM {SILVER_MASKED} LIMIT 1").collect()[0]
mask_applied_bool = (sample["ssn"] == "***-**-****" and sample["email"] == "***@***")

print(f"Pattern B Silver write time (median of {REPEATS}) : {raw_write_time_ms:,} ms")
print(f"Pattern B Silver table size : {raw_storage_bytes:,} bytes")
print(f"Masks applied on Pattern B Silver: {mask_applied_bool}")
print(f"A read of one row returns : ssn='{sample['ssn']}', email='{sample['email']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 1: What reading Silver costs
# MAGIC
# MAGIC Three full scans, each reading every column its table has, wrapped in `MAX(LENGTH(...))` so the
# MAGIC engine has to evaluate each value (and, with the masks on, run each mask) for every row:
# MAGIC
# MAGIC - **Pattern A:** the redacted Silver table, with nothing to mask.
# MAGIC - **Pattern B, masks on:** the non-privileged path.
# MAGIC - **Pattern B, masks off:** the same scan with the masks dropped for a moment, standing in for the
# MAGIC   privileged path.

# COMMAND ----------

REDACT_SQL = f"""
SELECT COUNT(*) AS n,
       MAX(LENGTH(first_name)) AS a, MAX(LENGTH(last_name)) AS b, MAX(LENGTH(region)) AS c
FROM {SILVER_REDACTED}
"""
MASKED_SQL = f"""
SELECT COUNT(*) AS n,
       MAX(LENGTH(first_name)) AS a, MAX(LENGTH(last_name)) AS b, MAX(LENGTH(region)) AS c,
       MAX(ssn) AS max_ssn, MAX(email) AS max_email
FROM {SILVER_MASKED}
"""

redact_query_time_ms, redact_result = time_query(REDACT_SQL)
print(f"Pattern A query time (median of {REPEATS}) : {redact_query_time_ms:,} ms  (row count: {redact_result['n']:,})")

# COMMAND ----------

mask_query_time_ms, mask_result = time_query(MASKED_SQL)
print(f"Pattern B mask-on query time (median of {REPEATS}) : {mask_query_time_ms:,} ms")
print(f"  MAX(ssn), MAX(email) under mask : '{mask_result['max_ssn']}', '{mask_result['max_email']}'")

# COMMAND ----------

# Drop the masks, measure, then re-apply. The raw PII is readable here.
drop_masks(SILVER_MASKED)
try:
    privileged_query_time_ms, priv_result = time_query(MASKED_SQL)
finally:
    # Put the masks back even if the query failed
    set_masks(SILVER_MASKED)

pii_recovered_unmasked_bool = priv_result["max_ssn"] != "***-**-****"

print(f"Pattern B mask-off query time (median of {REPEATS}) : {privileged_query_time_ms:,} ms")
print(f"  MAX(ssn), MAX(email) without mask : '{priv_result['max_ssn']}', '{priv_result['max_email']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 2: A new use case needs the SSN back
# MAGIC
# MAGIC Say a fraud team now needs SSN in Silver. In Pattern A, Silver doesn't have it, so you rebuild
# MAGIC Silver from Bronze with the column restored and point consumers at the result. I time that rebuild
# MAGIC and count the rows it rewrites. (It only works while Bronze still holds the data. Past Bronze
# MAGIC retention, you'd be re-ingesting from the source system.)
# MAGIC
# MAGIC In Pattern B, the SSN never left Silver. Access comes from membership in the group the mask checks,
# MAGIC so the change is adding the fraud team to `pii_full_access`. No rows move. Account groups are managed
# MAGIC at the account level, so the notebook leaves that step to you; the masks-off read in Probe 1 shows
# MAGIC what the privileged path returns.

# COMMAND ----------

restore_rebuild_time_ms = time_write(bronze_df, SILVER_RESTORED)
restore_rows_rewritten = spark.table(SILVER_RESTORED).count()
restored_has_ssn_bool = "ssn" in spark.table(SILVER_RESTORED).columns

print(f"Pattern A rebuild with SSN restored (median of {REPEATS}) : {restore_rebuild_time_ms:,} ms")
print(f"  Rows rewritten : {restore_rows_rewritten:,}   ssn column back: {restored_has_ssn_bool}")
print(f"Pattern B rows rewritten to grant SSN access : 0")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe 3: Bronze still holds the PII
# MAGIC
# MAGIC Redacting Silver doesn't remove the PII; Bronze still has it. The same thread points out that data
# MAGIC science teams sometimes take a cut straight from Bronze, so Bronze needs governing in both patterns.
# MAGIC The same two masks work there: one `ALTER TABLE` per column.

# COMMAND ----------

set_masks(BRONZE_TABLE)
bronze_after = spark.sql(f"SELECT ssn, email FROM {BRONZE_TABLE} LIMIT 1").collect()[0]
bronze_masked_bool = (bronze_after["ssn"] == "***-**-****" and bronze_after["email"] == "***@***")

print(f"Bronze read before its masks : ssn='{bronze_sample['ssn']}'")
print(f"Bronze read after its masks  : ssn='{bronze_after['ssn']}', email='{bronze_after['email']}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# Silver-only and Bronze-plus-Silver storage for each pattern
storage_delta_pct = round((raw_storage_bytes - redact_storage_bytes) / redact_storage_bytes * 100, 1)
pattern_a_total_bytes = bronze_storage_bytes + redact_storage_bytes
pattern_b_total_bytes = bronze_storage_bytes + raw_storage_bytes
total_storage_delta_pct = round((pattern_b_total_bytes - pattern_a_total_bytes) / pattern_a_total_bytes * 100, 1)

def tables_holding_pii(tables: list) -> int:
    """How many of a pattern's tables store an ssn or email column, masked or not."""
    return sum(1 for t in tables if {"ssn", "email"} & set(spark.table(t).columns))

results = {
    "row_count":                    ROW_COUNT,
    "query_repeats":                REPEATS,
    "bronze_storage_bytes":         bronze_storage_bytes,
    "bronze_pii_visible_bool":      bronze_pii_visible_bool,
    "redact_write_time_ms":         redact_write_time_ms,
    "raw_write_time_ms":            raw_write_time_ms,
    "redact_storage_bytes":         redact_storage_bytes,
    "raw_storage_bytes":            raw_storage_bytes,
    "storage_delta_pct":            storage_delta_pct,
    "pattern_a_total_bytes":        pattern_a_total_bytes,
    "pattern_b_total_bytes":        pattern_b_total_bytes,
    "total_storage_delta_pct":      total_storage_delta_pct,
    "redact_query_time_ms":         redact_query_time_ms,
    "mask_query_time_ms":           mask_query_time_ms,
    "privileged_query_time_ms":     privileged_query_time_ms,
    "mask_overhead_ms":             mask_query_time_ms - redact_query_time_ms,
    "mask_vs_unmasked_ms":          mask_query_time_ms - privileged_query_time_ms,
    "mask_applied_bool":            mask_applied_bool,
    "pii_recovered_unmasked_bool":  pii_recovered_unmasked_bool,
    "pii_columns_in_redacted":      len({"ssn", "email"} & set(spark.table(SILVER_REDACTED).columns)),
    "restore_rebuild_time_ms":      restore_rebuild_time_ms,
    "restore_rows_rewritten":       restore_rows_rewritten,
    "restored_has_ssn_bool":        restored_has_ssn_bool,
    "mask_grant_rows_rewritten":    0,
    "bronze_masked_bool":           bronze_masked_bool,
    "tables_holding_pii_a":         tables_holding_pii([BRONZE_TABLE, SILVER_REDACTED]),
    "tables_holding_pii_b":         tables_holding_pii([BRONZE_TABLE, SILVER_MASKED]),
}

print("RESULTS_JSON", json.dumps(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I learned
# MAGIC
# MAGIC Redacting Silver doesn't get rid of your PII. With Silver redacted, a plain read of Bronze still
# MAGIC returned the real SSN and email, and it kept doing so until Bronze got the same two masks. So you're
# MAGIC masking Bronze under either pattern.
# MAGIC
# MAGIC Where the two patterns differ, on my run of 100,000 rows:
# MAGIC
# MAGIC | | Redact Silver | Mask Silver |
# MAGIC |---|---|---|
# MAGIC | Silver write (median of 5) | 2,085 ms | 2,113 ms |
# MAGIC | Silver size | 103,805 bytes | 181,829 bytes (75.2% larger) |
# MAGIC | Full scan of Silver (median of 5) | 636 ms | 812 ms masks on, 720 ms masks off |
# MAGIC | Getting SSN back into Silver | rebuild: 100,000 rows rewritten | group membership change: 0 rows |
# MAGIC | Tables holding PII | 1 (Bronze) | 2 (Bronze and Silver) |
# MAGIC
# MAGIC The write cost the same either way. Redacting gets you a smaller Silver table and one less table
# MAGIC holding PII. Masking costs you some storage and a little scan time, and in return the SSN is one
# MAGIC group change away when a new use case needs it, instead of a rebuild. At 100,000 rows that rebuild
# MAGIC took about 2 seconds; at production volume it's a backfill, and once Bronze ages out it's a
# MAGIC re-ingest from the source system.
# MAGIC
# MAGIC Scan timings move around from run to run, so read the scan row as "masks add a
# MAGIC fraction of a second to a full scan at this size", not as a fixed overhead.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Row filters and column masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/row-and-column-filters):
# MAGIC   overview of both Unity Catalog table-level controls, including where they don't apply
# MAGIC - [Manually apply row filters and column masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/manually-apply):
# MAGIC   the full `ALTER TABLE ... SET MASK` syntax and the privileges it needs
# MAGIC - [Attribute-based access control in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies):
# MAGIC   the scale-up path: manage masking policies through governed tags rather than per-table `ALTER TABLE`,
# MAGIC   so Bronze and Silver pick up the same rule
# MAGIC - [Transform data with Lakeflow pipelines](https://docs.databricks.com/aws/en/ldp/transform):
# MAGIC   how to put the write-time redaction step into a declarative pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Uncomment to remove everything this notebook created
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
# print(f"Dropped schema: {CATALOG}.{SCHEMA}")
