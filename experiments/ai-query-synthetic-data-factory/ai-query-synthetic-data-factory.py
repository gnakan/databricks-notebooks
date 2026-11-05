# Databricks notebook source

# MAGIC %md
# MAGIC In a recent Databricks [community technical blog post](https://community.databricks.com/t5/technical-blog/ai-functions-for-synthetic-test-data-generate-realistic-datasets/ba-p/162951) about generating synthetic test data with AI Functions:
# MAGIC
# MAGIC > "Today, you can do the same thing in pure SQL using Databricks AI Functions."
# MAGIC
# MAGIC Found "in pure SQL" interesting, so I put together an experiment.
# MAGIC
# MAGIC A pressing plant doesn't need you to cut every record. You hand it the stamper and it presses the copies. Databricks' [`ai_query` function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query) does that for test data: you describe the schema once in SQL and it writes the rows. The notebook builds three synthetic tables at 100 and 1,000 rows each, then counts what landed in every column. You end up with a populated schema you can drop into a data engineering test suite.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) serverless compute. There are no `%pip install` steps: the `ai_query` function is one of the built-in [AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions), and `json`, `math`, `time`, and `matplotlib` already ship with the serverless Python environment.
# MAGIC
# MAGIC The `ai_query` function invokes an existing [Model Serving](https://docs.databricks.com/aws/en/machine-learning/model-serving/index) endpoint. Foundation Model API endpoints are ready to call on Free Edition, so there's nothing to deploy. The next cell checks that the endpoint answers before the main generation begins.
# MAGIC
# MAGIC Each row is one model call, so the tokens grow with the row count. `ROWS_SMALL` and `ROWS_LARGE` in the config cell control scale. Start with the defaults (100 and 1,000) and raise from there.

# COMMAND ----------

import json
import math
import time

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

CATALOG    = "workspace"       # the writable catalog on Free Edition
SCHEMA     = "ai_query_synth"
MODEL      = "databricks-meta-llama-3-3-70b-instruct"

ROWS_SMALL = 100
ROWS_LARGE = 1_000

# Databricks Extended Brand Guidelines palette
OAT_LIGHT, OAT, WHITE   = "#F9F7F4", "#EEEDE9", "#FFFFFF"
NAVY, NAVY_600           = "#1B3139", "#1B5162"
GRAY_TEXT, GRAY_LINES    = "#5A6F77", "#DCE0E2"
LAVA                     = "#FF3621"
BLUE_600, BLUE_400       = "#2272B4", "#8ACAFF"
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"
FONTS = '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Schema ready: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: does the ai_query function answer here?
# MAGIC
# MAGIC I run one row first, before the main tables. Without it, a misconfigured endpoint or an older runtime returns null for every row, and the filter at the end of each query drops the whole table with no error. The Databricks docs say `responseFormat` needs [Databricks Runtime 15.4 LTS or above](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query), and one row shows whether this workspace has it before 1,000 rows disappear.
# MAGIC
# MAGIC With `failOnError => false`, the `ai_query` function returns a struct holding `result` and `errorMessage`, where `result` is the model's JSON as a string, or null when the row failed. I ask for a JSON object with `responseFormat => '{"type": "json_object"}'` and parse `result` with `from_json` against the table's schema, so every field's name and type live in one place per table. This cell checks that the parsed `rec.name` comes back on a single known-good prompt. If this cell fails, stop here and verify the endpoint.

# COMMAND ----------

probe_df = spark.sql(f"""
SELECT from_json(ai_query(
  '{MODEL}',
  'Return a JSON object with exactly two fields: name (a full name STRING) and city (a US city STRING).',
  responseFormat  => '{{"type": "json_object"}}',
  modelParameters => named_struct('temperature', 1.0),
  failOnError     => false
).result, 'STRUCT<name: STRING, city: STRING>') AS rec
""")

probe_row = probe_df.collect()[0]["rec"]
assert probe_row is not None and probe_row["name"] is not None, \
    f"Probe failed -- rec={probe_row}"
print(f"Probe: name={probe_row['name']!r}, city={probe_row['city']!r}")
print("ai_query callable with a JSON response format. Continuing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer dimension
# MAGIC
# MAGIC The customer table has three model-written text fields (`name`, `email`, `city`) and two that SQL fills in (`plan_tier`, `signup_date`). The source article recommends cycling categories with modulo and using `DATE_ADD` with `RAND` for dates. The modulo cycle is what keeps `plan_tier` balanced: row 1 gets one tier, row 2 the next, whatever the model writes in the other columns. The variety column for this table is `city`, which the model writes freely.
# MAGIC
# MAGIC With `failOnError => false`, a failed row's `result` is null, `from_json` returns null for it, and the filter at the end of the query drops it. The row count in the result table is the actual yield, which may be less than `ROWS_SMALL` or `ROWS_LARGE` on a rate-limited or overloaded endpoint.

# COMMAND ----------

def _customer_sql(rows: int, table: str) -> str:
    return """
CREATE OR REPLACE TABLE {catalog}.{schema}.{table} AS
SELECT
  r.id                                                          AS customer_id,
  r.rec.name                                           AS name,
  r.rec.email                                          AS email,
  r.rec.city                                           AS city,
  CASE (r.id % 4)
    WHEN 0 THEN 'free'
    WHEN 1 THEN 'starter'
    WHEN 2 THEN 'professional'
    ELSE        'enterprise'
  END                                                           AS plan_tier,
  DATE_ADD(DATE('2023-01-01'), CAST(RAND() * 730 AS INT))      AS signup_date
FROM (
  SELECT
    id,
    from_json(ai_query(
      '{model}',
      CONCAT(
        'Return a JSON object with exactly these string fields: ',
        'name (full name), email (corporate email address), city (US city). ',
        'Do not include plan_tier or signup_date. ',
        'Row: ', CAST(id AS STRING)
      ),
      responseFormat  => '{{"type": "json_object"}}',
      modelParameters => named_struct('temperature', 1.0),
      failOnError     => false
    ).result, 'STRUCT<name: STRING, email: STRING, city: STRING>') AS rec
  FROM (SELECT EXPLODE(SEQUENCE(1, {rows})) AS id)
) r
WHERE r.rec IS NOT NULL
""".format(catalog=CATALOG, schema=SCHEMA, table=table, model=MODEL, rows=rows)

t0 = time.perf_counter()
spark.sql(_customer_sql(ROWS_SMALL, "customers_100"))
t_cust_100 = time.perf_counter() - t0
n_cust_100 = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.customers_100").collect()[0]["n"]
print(f"customers_100: {n_cust_100} rows in {t_cust_100:.1f}s ({t_cust_100/max(n_cust_100,1):.2f}s/row)")

t0 = time.perf_counter()
spark.sql(_customer_sql(ROWS_LARGE, "customers_1000"))
t_cust_1000 = time.perf_counter() - t0
n_cust_1000 = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.customers_1000").collect()[0]["n"]
print(f"customers_1000: {n_cust_1000} rows in {t_cust_1000:.1f}s ({t_cust_1000/max(n_cust_1000,1):.2f}s/row)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Order history
# MAGIC
# MAGIC The orders table has two model-written fields (`product_sku`, `status`), and SQL fills in the quantity, the order date and the customer. The source article warns that Databricks' `ai_gen` function, which doesn't take `modelParameters`, "may produce some repeated rows", and that without a temperature of at least 1.0 "the LLM generates near-identical rows". So every call to the `ai_query` function here sets `temperature` to 1.0, the floor it recommends.
# MAGIC
# MAGIC Customer ID is a random integer in the range of the customer table's actual row count (which may be less than the target after any dropped rows). This is test data, not a relational database with enforced foreign keys.

# COMMAND ----------

def _orders_sql(rows: int, table: str, cust_n: int) -> str:
    return """
CREATE OR REPLACE TABLE {catalog}.{schema}.{table} AS
SELECT
  r.id                                                          AS order_id,
  CAST(RAND() * {cust_n} AS INT) + 1                           AS customer_id,
  r.rec.product_sku                                    AS product_sku,
  CAST(RAND() * 10 AS INT) + 1                                 AS quantity,
  r.rec.status                                         AS status,
  DATE_ADD(DATE('2023-01-01'), CAST(RAND() * 730 AS INT))      AS order_date
FROM (
  SELECT
    id,
    from_json(ai_query(
      '{model}',
      CONCAT(
        'Return a JSON object with exactly these string fields: ',
        'product_sku (e.g. SOFT-WIDGET-XL), ',
        'status (one of: pending|processing|shipped|delivered|cancelled). ',
        'Row: ', CAST(id AS STRING)
      ),
      responseFormat  => '{{"type": "json_object"}}',
      modelParameters => named_struct('temperature', 1.0),
      failOnError     => false
    ).result, 'STRUCT<product_sku: STRING, status: STRING>') AS rec
  FROM (SELECT EXPLODE(SEQUENCE(1, {rows})) AS id)
) r
WHERE r.rec IS NOT NULL
""".format(catalog=CATALOG, schema=SCHEMA, table=table, model=MODEL, rows=rows, cust_n=cust_n)

t0 = time.perf_counter()
spark.sql(_orders_sql(ROWS_SMALL, "orders_100", n_cust_100))
t_ord_100 = time.perf_counter() - t0
n_ord_100 = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.orders_100").collect()[0]["n"]
print(f"orders_100: {n_ord_100} rows in {t_ord_100:.1f}s ({t_ord_100/max(n_ord_100,1):.2f}s/row)")

t0 = time.perf_counter()
spark.sql(_orders_sql(ROWS_LARGE, "orders_1000", n_cust_1000))
t_ord_1000 = time.perf_counter() - t0
n_ord_1000 = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.orders_1000").collect()[0]["n"]
print(f"orders_1000: {n_ord_1000} rows in {t_ord_1000:.1f}s ({t_ord_1000/max(n_ord_1000,1):.2f}s/row)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Support tickets
# MAGIC
# MAGIC I gave the tickets table the most model-written content: `issue_type` and `priority`, each picked from a list, plus `resolution_notes` as free text. The source article's own ticket example cycles the issue type with modulo and lets the model write the text. Here the model picks the type and the priority too, which is the case the article steers around, so the counts below show what that choice does.
# MAGIC
# MAGIC The response format holds the field names and types, not the length of `resolution_notes`.

# COMMAND ----------

def _tickets_sql(rows: int, table: str, cust_n: int, temperature: float = 1.0) -> str:
    return """
CREATE OR REPLACE TABLE {catalog}.{schema}.{table} AS
SELECT
  r.id                                                          AS ticket_id,
  CAST(RAND() * {cust_n} AS INT) + 1                           AS customer_id,
  r.rec.issue_type                                     AS issue_type,
  r.rec.priority                                       AS priority,
  r.rec.resolution_notes                               AS resolution_notes,
  DATE_ADD(DATE('2023-01-01'), CAST(RAND() * 730 AS INT))      AS created_at
FROM (
  SELECT
    id,
    from_json(ai_query(
      '{model}',
      CONCAT(
        'Return a JSON object with exactly these string fields: ',
        'issue_type (one of: billing|technical|account|feature-request|other), ',
        'priority (one of: low|medium|high|critical), ',
        'resolution_notes (1 to 3 sentences describing how the issue was resolved). ',
        'Row: ', CAST(id AS STRING)
      ),
      responseFormat  => '{{"type": "json_object"}}',
      modelParameters => named_struct('temperature', {temperature}),
      failOnError     => false
    ).result, 'STRUCT<issue_type: STRING, priority: STRING, resolution_notes: STRING>') AS rec
  FROM (SELECT EXPLODE(SEQUENCE(1, {rows})) AS id)
) r
WHERE r.rec IS NOT NULL
""".format(catalog=CATALOG, schema=SCHEMA, table=table, model=MODEL, rows=rows, cust_n=cust_n,
           temperature=temperature)

t0 = time.perf_counter()
spark.sql(_tickets_sql(ROWS_SMALL, "tickets_100", n_cust_100))
t_tick_100 = time.perf_counter() - t0
n_tick_100 = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.tickets_100").collect()[0]["n"]
print(f"tickets_100: {n_tick_100} rows in {t_tick_100:.1f}s ({t_tick_100/max(n_tick_100,1):.2f}s/row)")

t0 = time.perf_counter()
spark.sql(_tickets_sql(ROWS_LARGE, "tickets_1000", n_cust_1000))
t_tick_1000 = time.perf_counter() - t0
n_tick_1000 = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.tickets_1000").collect()[0]["n"]
print(f"tickets_1000: {n_tick_1000} rows in {t_tick_1000:.1f}s ({t_tick_1000/max(n_tick_1000,1):.2f}s/row)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tickets again, hotter
# MAGIC
# MAGIC The source article's examples run hotter than 1.0, at 1.2 to 1.5. So I build the 1,000-row ticket table a second time with the same prompt at `temperature` 1.5, the top of that range, to see whether more heat spreads the picks across the list.

# COMMAND ----------

TEMP_HOT = 1.5

t0 = time.perf_counter()
spark.sql(_tickets_sql(ROWS_LARGE, "tickets_1000_hot", n_cust_1000, temperature=TEMP_HOT))
t_tick_hot = time.perf_counter() - t0
n_tick_hot = spark.sql(f"SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.tickets_1000_hot").collect()[0]["n"]
print(f"tickets_1000_hot (temperature {TEMP_HOT}): {n_tick_hot} rows in {t_tick_hot:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Measure: schema consistency and diversity
# MAGIC
# MAGIC Two numbers matter for judging whether the output is usable as test data. First, the null-field rate: what fraction of rows have any required LLM-generated field as null, which happens when the model returns a malformed JSON or the row times out and `failOnError => false` returns a NULL `result`. Second, variety: does a column spread across its values, or pile up on one? I score it with Shannon entropy, where a column split evenly over K values scores ln(K) and a column holding one value scores 0.
# MAGIC
# MAGIC The variety column is `city` for customers (free text), `status` for orders and `issue_type` for tickets (each a pick from a list of five). Then I count every allowed value in the 1,000-row tables, for the three columns the model picks from a list and for `plan_tier`, which SQL deals by cycling through four tiers. The source article recommends that cycling for categories; the counts show what happens when you hand the choice to the model instead.

# COMMAND ----------

# Schema consistency: null required-field rate per table and scale
NULL_CHECKS = {
    "cust_100":  (f"{CATALOG}.{SCHEMA}.customers_100", ["name", "email", "city"]),
    "cust_1000": (f"{CATALOG}.{SCHEMA}.customers_1000", ["name", "email", "city"]),
    "ord_100":   (f"{CATALOG}.{SCHEMA}.orders_100",    ["product_sku", "status"]),
    "ord_1000":  (f"{CATALOG}.{SCHEMA}.orders_1000",   ["product_sku", "status"]),
    "tick_100":  (f"{CATALOG}.{SCHEMA}.tickets_100",   ["issue_type", "priority", "resolution_notes"]),
    "tick_1000": (f"{CATALOG}.{SCHEMA}.tickets_1000",  ["issue_type", "priority", "resolution_notes"]),
    "tick_hot":  (f"{CATALOG}.{SCHEMA}.tickets_1000_hot", ["issue_type", "priority", "resolution_notes"]),
}

null_rates = {}
for key, (table, cols) in NULL_CHECKS.items():
    null_pred = " OR ".join(f"{c} IS NULL" for c in cols)
    total    = spark.sql(f"SELECT COUNT(*) AS n FROM {table}").collect()[0]["n"]
    bad_rows = spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE {null_pred}").collect()[0]["n"]
    null_rates[key] = bad_rows / total if total > 0 else 1.0
    print(f"{key}: {bad_rows}/{total} rows with at least one null required field ({null_rates[key]:.1%})")

# COMMAND ----------

# Categorical diversity: Shannon entropy over the diversity column
DIV_COLS = {
    "cust_100":  (f"{CATALOG}.{SCHEMA}.customers_100", "city"),
    "cust_1000": (f"{CATALOG}.{SCHEMA}.customers_1000", "city"),
    "ord_100":   (f"{CATALOG}.{SCHEMA}.orders_100",   "status"),
    "ord_1000":  (f"{CATALOG}.{SCHEMA}.orders_1000",  "status"),
    "tick_100":  (f"{CATALOG}.{SCHEMA}.tickets_100",  "issue_type"),
    "tick_1000": (f"{CATALOG}.{SCHEMA}.tickets_1000", "issue_type"),
}

def shannon_entropy(counts):
    total = sum(counts)
    if total == 0:
        return 0.0
    return max(0.0, -sum((c / total) * math.log(c / total) for c in counts if c > 0))

entropies = {}
for key, (table, col) in DIV_COLS.items():
    rows = spark.sql(
        f"SELECT {col}, COUNT(*) AS cnt FROM {table} GROUP BY {col} ORDER BY cnt DESC"
    ).collect()
    counts = [r["cnt"] for r in rows]
    entropies[key] = shannon_entropy(counts)
    n_unique = len(counts)
    max_ent = math.log(n_unique) if n_unique > 1 else 0.0
    print(f"{key}: {n_unique} unique '{col}' values, entropy {entropies[key]:.3f} (max {max_ent:.3f})")

n_cities_1000 = spark.sql(
    f"SELECT COUNT(DISTINCT city) AS n FROM {CATALOG}.{SCHEMA}.customers_1000"
).collect()[0]["n"]

# COMMAND ----------

# What each list column actually holds at 1,000 rows: a count for every allowed value.
# A value outside the list (a different spelling, a new label) is counted as "something else".
CATEGORICALS = [
    # (key, label, table, column, allowed values, who chooses)
    ("plan_tier",  "Plan tier",   "customers_1000", "plan_tier",
     ["free", "starter", "professional", "enterprise"], "SQL"),
    ("status",     "Order status", "orders_1000",   "status",
     ["pending", "processing", "shipped", "delivered", "cancelled"], "model"),
    ("issue_type", "Ticket type",  "tickets_1000",  "issue_type",
     ["billing", "technical", "account", "feature-request", "other"], "model"),
    ("priority",   "Ticket priority", "tickets_1000", "priority",
     ["low", "medium", "high", "critical"], "model"),
    ("issue_type_hot", "Ticket type at 1.5", "tickets_1000_hot", "issue_type",
     ["billing", "technical", "account", "feature-request", "other"], "model"),
]

value_counts = {}
for key, label, table, col, allowed, chooser in CATEGORICALS:
    rows = spark.sql(
        f"SELECT LOWER(TRIM({col})) AS v, COUNT(*) AS cnt FROM {CATALOG}.{SCHEMA}.{table} GROUP BY 1"
    ).collect()
    counts = {v: 0 for v in allowed}
    counts["something else"] = 0
    for r in rows:
        counts[r["v"] if r["v"] in allowed else "something else"] += r["cnt"]
    value_counts[key] = counts
    used = sum(1 for v in allowed if counts[v] > 0)
    print(f"{label} ({chooser}): {used} of {len(allowed)} values used -> " +
          ", ".join(f"{v} {n}" for v, n in counts.items()))

# COMMAND ----------

# DBTITLE 1,What the model picked, value by value
# hide-code
fig, axes = plt.subplots(1, len(CATEGORICALS), figsize=(17, 4.2), sharex=True)
fig.patch.set_facecolor(OAT_LIGHT)

for ax, (key, label, table, col, allowed, chooser) in zip(axes, CATEGORICALS):
    counts = value_counts[key]
    names = allowed + (["something else"] if counts["something else"] else [])
    total = sum(counts.values()) or 1
    shares = [100 * counts[v] / total for v in names]
    colors = [NAVY_600 if chooser == "SQL" else NAVY for _ in allowed] + \
             ([GRAY_TEXT] if counts["something else"] else [])
    y = np.arange(len(names))[::-1]
    ax.barh(y, shares, color=colors, height=0.62)
    for yi, share, v in zip(y, shares, names):
        ax.text(share + 2, yi, f"{counts[v]:,}", va="center", fontsize=10, color=GRAY_TEXT)
    ax.set_yticks(y)
    ax.set_yticklabels(names, color=NAVY, fontsize=10)
    ax.set_xlim(0, 115)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f"{v:.0f}%" if v <= 100 else ""))
    ax.set_facecolor(OAT_LIGHT)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRAY_LINES)
    ax.tick_params(colors=GRAY_TEXT, labelsize=10)
    who = "dealt by SQL" if chooser == "SQL" else (
        f"model, temp {TEMP_HOT}" if key.endswith("_hot") else "model, temp 1.0")
    ax.set_title(f"{label.split(' at ')[0]}\n{who}", color=NAVY, fontsize=12, fontweight="bold", loc="left")

fig.supxlabel("Share of the 1,000-row table (count beside each bar)", color=GRAY_TEXT, fontsize=10)
fig.tight_layout(pad=1.6)
display(fig)
plt.close(fig)

# COMMAND ----------

# DBTITLE 1,Run time by table and scale
# hide-code
fig, ax = plt.subplots(figsize=(7.5, 3.6))
fig.patch.set_facecolor(OAT_LIGHT)
ax.set_facecolor(OAT_LIGHT)
x = np.arange(3)
w = 0.36
ax.bar(x - w/2, [t_cust_100, t_ord_100, t_tick_100],    w, color=BLUE_600, label="100 rows asked")
ax.bar(x + w/2, [t_cust_1000, t_ord_1000, t_tick_1000], w, color=NAVY,     label="1,000 rows asked")
ax.set_xticks(x)
ax.set_xticklabels(["Customers", "Orders", "Tickets"], color=NAVY)
ax.set_ylabel("Seconds, one run each", color=GRAY_TEXT)
ax.set_title("Run time by table and scale", color=NAVY, fontsize=13, fontweight="bold", loc="left")
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
for side in ("left", "bottom"):
    ax.spines[side].set_color(GRAY_LINES)
ax.tick_params(colors=GRAY_TEXT, labelsize=10)
ax.legend(facecolor=OAT, edgecolor=GRAY_LINES, labelcolor=NAVY, fontsize=10)
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
# Built from _lib/notebook-kit/results_panel_starter.py: same tokens, same helpers.
import html


def esc(value) -> str:
    """Escape any run value before it goes into the HTML."""
    return html.escape(str(value))


def stat(number, words: str, out_of=None) -> str:
    """A scoreboard figure: a big number, an optional 'out of', and a plain-language caption."""
    of = f"<span style='color:{GRAY_TEXT};font-size:18px'> / {esc(out_of)}</span>" if out_of is not None else ""
    return (f"<div style='min-width:150px'><div style='font-size:30px;font-weight:700;color:{NAVY}'>{esc(number)}{of}</div>"
            f"<div style='font-size:13px;color:{GRAY_TEXT};line-height:1.35'>{esc(words)}</div></div>")


def stats(*figures: str) -> str:
    """A row of stat() figures under the headline."""
    return f"<div style='display:flex;gap:34px;flex-wrap:wrap;padding-bottom:26px'>{''.join(figures)}</div>"


def measured_rule(value: float, largest: float, pop: bool = False) -> str:
    """A rule whose length carries a value. pop=True draws it in lava."""
    width = max(1, 100 * value / largest) if largest and value else 0
    return f"<div style='height:6px;background:{LAVA if pop else NAVY};width:{width:.0f}%'></div>"


def row(name: str, count=None, tag: str = "", tag_pop: bool = False, note: str = "", body: str = "") -> str:
    """One group: a hairline, its name, a tag, a note, a count, then its body."""
    tag_html = f"<span style='font-size:12px;font-weight:700;color:{LAVA if tag_pop else NAVY_600}'>{esc(tag)}</span>" if tag else ""
    note_html = f"<span style='font-size:12px;color:{GRAY_TEXT}'>{esc(note)}</span>" if note else ""
    count_html = f"<span style='margin-left:auto;font-size:30px;font-weight:700;color:{NAVY}'>{esc(count)}</span>" if count is not None else ""
    return (f"<div style='padding:22px 0 18px;border-top:1px solid {GRAY_LINES}'>"
            f"<div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap'>"
            f"<span style='font-size:26px;font-weight:700;color:{NAVY}'>{esc(name)}</span>{tag_html}{note_html}{count_html}</div>"
            f"{body}</div>")


def share_line(value: str, n: int, total: int, pop: bool = False, muted: bool = False) -> str:
    """One allowed value: its name, a rule as long as its share of the table, and its count."""
    color = GRAY_TEXT if muted or n == 0 else NAVY
    return (f"<div style='display:flex;align-items:center;gap:12px;padding:3px 0;font-size:14px'>"
            f"<span style='flex:0 0 140px;color:{color}'>{esc(value)}</span>"
            f"<div style='flex:1 1 auto;background:{OAT};height:6px'>{measured_rule(n, total, pop=pop)}</div>"
            f"<span style='flex:0 0 60px;text-align:right;color:{color};font-variant-numeric:tabular-nums'>{n:,}</span></div>")


def callout(text: str) -> str:
    """The one lava-edged line the reader should not miss."""
    return (f"<div style='padding:14px 18px;margin-bottom:22px;background:{OAT};border-left:4px solid {LAVA};"
            f"font-size:15px;color:{NAVY}'>{text}</div>")


def panel(headline: str, dek: str, body: str) -> str:
    """The whole display: oat ground, navy top edge, DM Sans, a headline, a dek, then the body."""
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 30px;font-family:{SANS};"
            f"border-top:6px solid {NAVY}'>"
            f"<div style='font-size:40px;font-weight:700;line-height:1.1;max-width:760px'>{esc(headline)}</div>"
            f"<div style='font-size:17px;color:{GRAY_TEXT};line-height:1.45;max-width:680px;margin:12px 0 26px'>{dek}</div>"
            f"{body}</div>")


WORDS = {0: "none", 1: "one", 2: "two", 3: "three", 4: "four", 5: "five"}
NULL_KEY = {"plan_tier": "cust_1000", "status": "ord_1000", "issue_type": "tick_1000", "priority": "tick_1000",
            "issue_type_hot": "tick_hot"}


def used_of(key, allowed):
    return sum(1 for v in allowed if value_counts[key][v] > 0)


# The model-picked column that used the smallest share of its list leads the panel.
model_cols = [c for c in CATEGORICALS if c[5] == "model" and not c[0].endswith("_hot")]
lead = min(model_cols, key=lambda c: (used_of(c[0], c[4]) / len(c[4]), entropies.get(NULL_KEY[c[0]], 0)))
l_key, l_label, _, _, l_allowed, _ = lead
l_counts = value_counts[l_key]
l_total = sum(l_counts.values())
l_used = used_of(l_key, l_allowed)
l_top = max(l_allowed, key=lambda v: l_counts[v])

sql_counts = value_counts["plan_tier"]
sql_total = sum(sql_counts.values())
sql_shares = [sql_counts[v] / sql_total for v in CATEGORICALS[0][4]] if sql_total else [0]

rows_asked = 3 * (ROWS_SMALL + ROWS_LARGE) + ROWS_LARGE
rows_kept = n_cust_100 + n_cust_1000 + n_ord_100 + n_ord_1000 + n_tick_100 + n_tick_1000 + n_tick_hot

headline = (f"Asked to pick from {WORDS.get(len(l_allowed), len(l_allowed))} {l_label.lower()}s, "
            f"the model used {WORDS.get(l_used, l_used)}")
dek = (f"Each rule is one allowed value's share of a 1,000-row table. Plan tier is dealt by SQL; "
       f"the next three columns are picked by {esc(MODEL)} at temperature 1.0, and the last repeats "
       f"ticket type at {TEMP_HOT}.")
hot_counts = value_counts["issue_type_hot"]
hot_total = sum(hot_counts.values())
hot_allowed = CATEGORICALS[-1][4]
hot_used = used_of("issue_type_hot", hot_allowed)
hot_top = max(hot_allowed, key=lambda v: hot_counts[v])

body = stats(
    stat(f"{rows_kept:,}", "rows kept", out_of=f"{rows_asked:,}"),
    stat(f"{n_cities_1000:,}", "different cities in 1,000 customers"),
    stat(l_used, f"{l_label.lower()}s the model used", out_of=len(l_allowed)),
    stat(f"{t_tick_1000:.0f}s", "to generate 1,000 tickets, one run"),
)
sql_spread = (f"{min(sql_shares):.0%} each" if round(min(sql_shares), 2) == round(max(sql_shares), 2)
              else f"between {min(sql_shares):.0%} and {max(sql_shares):.0%} each")
body += callout(
    f"The model chose <b>{esc(l_top)}</b> for {l_counts[l_top]:,} of {l_total:,} rows. "
    f"At temperature {TEMP_HOT}, ticket type used {WORDS.get(hot_used, hot_used)} of five, "
    f"<b>{esc(hot_top)}</b> on {hot_counts[hot_top]:,} of {hot_total:,}. "
    f"SQL dealt the four plan tiers {sql_spread}."
)
for key, label, _, _, allowed, chooser in CATEGORICALS:
    counts = value_counts[key]
    total = sum(counts.values())
    lines = "".join(
        share_line(v, counts[v], total, pop=(chooser == "model" and total and counts[v] / total > 0.5))
        for v in allowed
    )
    if counts["something else"]:
        lines += share_line("something else", counts["something else"], total, muted=True)
    null_rate = null_rates.get(NULL_KEY[key], 0)
    note = "" if chooser == "SQL" else f"{null_rate:.1%} of rows in this table missing a field"
    if key.endswith("_hot"):
        note = f"temperature {TEMP_HOT}; " + note
    body += row(label, count=f"{used_of(key, allowed)} of {len(allowed)}",
                tag="dealt by SQL" if chooser == "SQL" else "picked by the model",
                tag_pop=(chooser == "model"), note=note,
                body=f"<div style='padding-top:12px'>{lines}</div>")

displayHTML(panel(headline, dek, body))

# COMMAND ----------

print("RESULTS_JSON", json.dumps({
    "cust_100_seconds":   round(t_cust_100, 2),
    "cust_100_rows":      n_cust_100,
    "cust_100_null_rate": round(null_rates.get("cust_100", 0), 4),
    "cust_100_entropy":   round(entropies.get("cust_100", 0), 4),
    "cust_1000_seconds":   round(t_cust_1000, 2),
    "cust_1000_rows":      n_cust_1000,
    "cust_1000_null_rate": round(null_rates.get("cust_1000", 0), 4),
    "cust_1000_entropy":   round(entropies.get("cust_1000", 0), 4),
    "cust_1000_distinct_cities": n_cities_1000,
    "ord_100_seconds":   round(t_ord_100, 2),
    "ord_100_rows":      n_ord_100,
    "ord_100_null_rate": round(null_rates.get("ord_100", 0), 4),
    "ord_100_entropy":   round(entropies.get("ord_100", 0), 4),
    "ord_1000_seconds":   round(t_ord_1000, 2),
    "ord_1000_rows":      n_ord_1000,
    "ord_1000_null_rate": round(null_rates.get("ord_1000", 0), 4),
    "ord_1000_entropy":   round(entropies.get("ord_1000", 0), 4),
    "tick_100_seconds":   round(t_tick_100, 2),
    "tick_100_rows":      n_tick_100,
    "tick_100_null_rate": round(null_rates.get("tick_100", 0), 4),
    "tick_100_entropy":   round(entropies.get("tick_100", 0), 4),
    "tick_1000_seconds":   round(t_tick_1000, 2),
    "tick_1000_rows":      n_tick_1000,
    "tick_1000_null_rate": round(null_rates.get("tick_1000", 0), 4),
    "tick_1000_entropy":   round(entropies.get("tick_1000", 0), 4),
    "tick_hot_temperature": TEMP_HOT,
    "tick_hot_seconds":   round(t_tick_hot, 2),
    "tick_hot_rows":      n_tick_hot,
    "tick_hot_null_rate": round(null_rates.get("tick_hot", 0), 4),
    "value_counts_1000": value_counts,
    "rows_asked": rows_asked,
    "rows_kept":  rows_kept,
    "runs": 1,
}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run shows
# MAGIC
# MAGIC The experiment results suggest letting the model write the free text and letting SQL deal the categories, the split the source article recommends. The model wrote 114 different cities across the 978 customers it kept. Given a list of five ticket types, it filed 971 of 973 tickets as technical and never picked billing, account, feature-request or other. Order status leaned the same way, with delivered on 969 of 995 orders, and priority came back as medium on 672 tickets and high on 299, with low and critical never used. SQL, cycling through four plan tiers, dealt them 247, 242, 245 and 244.
# MAGIC
# MAGIC Turning the temperature up to 1.5 didn't spread the ticket types. The model still filed 865 of the 886 tickets it kept as technical, 21 came back with a type that isn't on the list, and 2.5% of the kept rows were missing a field, against 0.2% at 1.0.
# MAGIC
# MAGIC The structure held up well everywhere else: 4,124 of the 4,300 rows I asked for came back, and no kept customer or order row at 1,000 was missing a field. Each table ran once, so read the timings as one run rather than a benchmark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC The cell below drops the schema and all tables created by this notebook. Run it when you no longer need the generated data. Skip it if you want to keep the tables for downstream testing.

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
print(f"Cleaned up: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [**ai_query function reference**](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query): the full parameter list, `responseFormat` STRUCT schema syntax, `modelParameters` keys, `failOnError` return type, and runtime version requirements.
# MAGIC - [**SQL AI Functions overview**](https://docs.databricks.com/aws/en/large-language-models/ai-functions): every built-in AI Function, with a table of which one fits which job.
# MAGIC - [**Foundation Model APIs**](https://docs.databricks.com/aws/en/machine-learning/foundation-models/index): the models the `ai_query` function can call, with their limits and parameters.
# MAGIC - [**Databricks Free Edition**](https://docs.databricks.com/aws/en/getting-started/free-edition): the free workspace this notebook runs on.
# MAGIC - [**serverless SQL warehouses**](https://docs.databricks.com/aws/en/compute/sql-warehouse/serverless): how to size a warehouse when you run the same SQL as a batch job.
