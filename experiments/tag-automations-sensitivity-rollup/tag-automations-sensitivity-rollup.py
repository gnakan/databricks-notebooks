# Databricks notebook source
# MAGIC %md
# MAGIC In a recent Databricks docs page about [Tag Automations](https://docs.databricks.com/aws/en/admin/governed-tags/automate-tag-assignment) (Beta):
# MAGIC
# MAGIC > "To keep table-level classification in sync with sensitive columns found by Data Classification, create an automation that tags a table when any of its columns carries a sensitivity tag"
# MAGIC
# MAGIC The page gives that roll-up as a worked example: describe the rule in plain English, let Genie build it, dry-run it, turn it on. I wanted to see what I could do with it, so I put together an experiment that runs the recipe end to end and adds the part the page stops short of: an [attribute-based access control](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/) policy on the other side of the tag. The notebook builds a Stark Industries schema with classified columns, an automation rolls those column tags up to a table-level `sensitivity_tier` [governed tag](https://docs.databricks.com/aws/en/admin/governed-tags/), and a column mask policy keyed on that tag hides the classified columns the moment it lands. Then a new table shows up and gets the same treatment without anyone touching it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) in about 15 minutes, most of it waiting on two automation runs. Tag Automations is a Beta feature, and on the Free Edition workspace I used it was already switched on under **Settings > Previews**. The first cell checks that for you rather than assuming it.
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **A workspace where you can create governed tags and policies.** Tag Automations needs `USE CATALOG`, `USE SCHEMA` and `APPLY TAG` on the catalog, `MANAGE` on the catalog in scope, and `ASSIGN` on every governed tag it assigns. On Free Edition you are the workspace admin, so all of that is already yours. On a shared workspace, ask for those [privileges](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges/privileges) on one catalog you can play in.
# MAGIC - **One step in the UI.** There is no API for creating an automation today; it lives in Catalog Explorer. The notebook pauses at that step, tells you exactly what to type into Genie, and then polls Unity Catalog until the tag shows up.
# MAGIC - **Libraries: none.** The notebook uses `requests`, which is preinstalled, for the [Tag Policy API](https://docs.databricks.com/api/workspace/tagpolicies), and Spark SQL for everything else. No pip install, no restart.
# MAGIC - **Compute:** default serverless. The SQL runs through the notebook's Spark session.
# MAGIC - **Identity:** workspace user plus the per-session token resolved from notebook context. Nothing hardcoded.

# COMMAND ----------

import json
import time
import uuid

import pandas as pd
import requests

# Dynamic resolution: user, workspace URL, per-session token. Never hardcode these.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
CURRENT_USER    = ctx.userName().get()
WORKSPACE_URL   = ctx.apiUrl().get()
WORKSPACE_TOKEN = ctx.apiToken().get()
HEADERS         = {"Authorization": f"Bearer {WORKSPACE_TOKEN}"}

# Where the Stark Industries tables land. `workspace` is the default writable
# catalog on Free Edition; change CATALOG if you are on a shared workspace.
CATALOG = "workspace"
SCHEMA  = "stark_tag_automation"
FQ      = f"{CATALOG}.{SCHEMA}"

# The governed tag the automation assigns. Custom governed tag keys cannot
# contain a dot (the `class.*` and `system.*` namespaces are reserved), which
# I found out by trying `stark.sensitivity` first.
TAG_KEY    = "sensitivity_tier"
TAG_VALUES = ["restricted", "internal"]

# The built-in classification tags the roll-up keys on. Data Classification
# ships these as governed tags in every workspace.
CLASS_TAGS = ["class.email_address", "class.credit_card", "class.us_ssn", "class.phone_number"]

RUN_ID = uuid.uuid4().hex[:8]

def api(method, path, **kwargs):
    """Small REST helper against the current workspace using the session token."""
    r = requests.request(method, f"{WORKSPACE_URL}{path}", headers=HEADERS, timeout=60, **kwargs)
    return r.status_code, (r.json() if r.content else {})

print(f"User      : {CURRENT_USER}")
print(f"Workspace : {WORKSPACE_URL}")
print(f"Schema    : {FQ}")
print(f"Tag       : {TAG_KEY} in {TAG_VALUES}")
print(f"Run ID    : {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check what this workspace can reach
# MAGIC
# MAGIC Four gates, each reported on its own line. The first reads the `metadata_automations` entry from the [workspace settings API](https://docs.databricks.com/aws/en/admin/workspace-settings/manage-previews), which is the same toggle you see on the Previews page. The second lists governed tags through the [Tag Policy API](https://docs.databricks.com/api/workspace/tagpolicies) and confirms the four `class.*` tags the roll-up depends on are there. The third and fourth read the `information_schema` [table_tags](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/table_tags) and [column_tags](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/column_tags) views, which is how the notebook watches for the automation's result later. If a gate fails, you find out here instead of six cells down.

# COMMAND ----------

gates_ok = True

# [gate 1] the Tag Automations preview is on for this workspace.
code, body = api("GET", "/api/2.1/settings/metadata_automations")
enabled = body.get("effective_boolean_val", {}).get("value") if code == 200 else None
if enabled is True:
    print("[gate 1] Previews: Tag Automations (metadata_automations) -> OK (on)")
else:
    gates_ok = False
    print(f"[gate 1] Previews: Tag Automations (metadata_automations) -> FAIL (http {code}, effective={enabled}). "
          "A workspace admin turns it on under Settings > Previews.")

# [gate 2] the Tag Policy API answers and the class.* governed tags exist.
code, body = api("GET", "/api/2.1/tag-policies")
if code == 200:
    keys = {t["tag_key"] for t in body.get("tag_policies", body if isinstance(body, list) else [])}
    missing = [t for t in CLASS_TAGS if t not in keys]
    if missing:
        gates_ok = False
        print(f"[gate 2] Governed tags via Tag Policy API -> FAIL (missing {missing})")
    else:
        print(f"[gate 2] Governed tags via Tag Policy API -> OK ({len(keys)} governed tags, all four class.* present)")
else:
    gates_ok = False
    print(f"[gate 2] Governed tags via Tag Policy API -> FAIL (http {code})")

# [gate 3] and [gate 4] the information_schema tag views are readable.
for n, view in ((3, "table_tags"), (4, "column_tags")):
    try:
        spark.sql(f"SELECT tag_name FROM {CATALOG}.information_schema.{view} LIMIT 0")
        print(f"[gate {n}] {CATALOG}.information_schema.{view} readable -> OK")
    except Exception as e:
        gates_ok = False
        print(f"[gate {n}] {CATALOG}.information_schema.{view} readable -> FAIL ({type(e).__name__})")

print()
print("All gates open, carry on." if gates_ok else "At least one gate is closed. The cells below assume all four are open.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build the Stark Industries schema and classify the columns
# MAGIC
# MAGIC Five small tables. Three carry columns you would not want an analyst to see in the clear (an email, a card number, a social security number, a phone number) and two are plain operational data with nothing sensitive in them. The interesting part is the tagging. In a workspace with [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) enabled on the catalog, the `class.*` tags land on their own, typically within a day of a table showing up. I set them by hand with [`ALTER TABLE ... SET TAGS`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-alter-table) so the notebook does not wait on a scan, and so you can see exactly which columns carry which tag before the automation ever runs. The tags are the same governed keys Data Classification would assign, so nothing downstream can tell the difference.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")

TABLES = {
    # table: (CTAS body, {column: class tag})
    "customers": (
        """SELECT 1 AS customer_id, 'Pepper Potts' AS full_name, 'pepper@starkindustries.com' AS email, '4111111111111111' AS card_number
           UNION ALL SELECT 2, 'Happy Hogan', 'happy@starkindustries.com', '5500000000000004'
           UNION ALL SELECT 3, 'James Rhodes', 'rhodey@starkindustries.com', '340000000000009'""",
        {"email": "class.email_address", "card_number": "class.credit_card"},
    ),
    "employees": (
        """SELECT 1001 AS employee_id, 'Pepper Potts' AS full_name, '078-05-1120' AS ssn, '+1-310-555-0142' AS phone
           UNION ALL SELECT 1002, 'Happy Hogan', '219-09-9999', '+1-310-555-0177'""",
        {"ssn": "class.us_ssn", "phone": "class.phone_number"},
    ),
    "support_tickets": (
        """SELECT 5001 AS ticket_id, 'pepper@starkindustries.com' AS contact_email, 'Arc reactor firmware' AS subject, 'open' AS status
           UNION ALL SELECT 5002, 'happy@starkindustries.com', 'Badge access', 'closed'""",
        {"contact_email": "class.email_address"},
    ),
    "orders": (
        """SELECT 100 AS order_id, 1 AS customer_id, 42.5 AS amount, DATE'2026-09-01' AS ordered_on
           UNION ALL SELECT 101, 2, 13.0, DATE'2026-09-02'""",
        {},
    ),
    "products": (
        """SELECT 'MK-85' AS sku, 'Mark 85 chest plate' AS product_name, 'armor' AS category
           UNION ALL SELECT 'AR-01', 'Arc reactor, palladium', 'power'""",
        {},
    ),
}

for name, (body, col_tags) in TABLES.items():
    spark.sql(f"CREATE OR REPLACE TABLE {FQ}.{name} AS {body}")
    for col, tag in col_tags.items():
        spark.sql(f"ALTER TABLE {FQ}.{name} ALTER COLUMN {col} SET TAGS ('{tag}' = '')")
    print(f"{name:<16} {len(col_tags)} classified column(s): {', '.join(f'{c} -> {t}' for c, t in col_tags.items()) or 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create the governed tag the automation assigns
# MAGIC
# MAGIC An automation can only assign a [governed tag](https://docs.databricks.com/aws/en/admin/tag-policies/) with defined allowed values, so `sensitivity_tier` has to exist before the automation does. The Tag Policy API creates it in one call. The cell is idempotent: if the tag is already there from an earlier run, it says so and moves on.
# MAGIC
# MAGIC One thing to know that I found the slow way: a freshly created governed tag takes a minute or two to become visible to the policy compiler. When I created the tag and wrote the mask policy in the same breath, `CREATE POLICY` came back with `Unknown tag policy key` even though the tag was already listed by the API. Thirty seconds later the same statement compiled. The tag gets created here, four cells before the policy needs it, so the lag is absorbed by the time you get there.

# COMMAND ----------

code, body = api("GET", f"/api/2.1/tag-policies/{TAG_KEY}")
if code == 200:
    print(f"Governed tag {TAG_KEY} already exists (id {body.get('id')}), values {[v['name'] for v in body.get('values', [])]}")
else:
    code, body = api("POST", "/api/2.1/tag-policies", json={
        "tag_key": TAG_KEY,
        "description": "Table-level sensitivity tier rolled up from column classification tags.",
        "values": [{"name": v} for v in TAG_VALUES],
    })
    if code == 200:
        print(f"Created governed tag {TAG_KEY} (id {body.get('id')}) with values {TAG_VALUES}")
    else:
        raise RuntimeError(f"Tag Policy API returned {code}: {body}")

TAG_CREATED_AT = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Snapshot the tags before the automation runs
# MAGIC
# MAGIC Two queries against `information_schema`, one for table-level tags and one for column-level tags, scoped to the schema. This is the "before" picture: five classified columns across three tables, and zero `sensitivity_tier` tags anywhere. The same two queries run again after the automation, so the difference is the whole result.

# COMMAND ----------

def tag_snapshot(label):
    """Return (table_tags_df, column_tags_df) for the experiment schema and print a one-line summary."""
    tt = spark.sql(f"""
        SELECT table_name, tag_name, tag_value
        FROM {CATALOG}.information_schema.table_tags
        WHERE schema_name = '{SCHEMA}'
        ORDER BY table_name, tag_name
    """).toPandas()
    ct = spark.sql(f"""
        SELECT table_name, column_name, tag_name
        FROM {CATALOG}.information_schema.column_tags
        WHERE schema_name = '{SCHEMA}'
        ORDER BY table_name, column_name
    """).toPandas()
    tiered = sorted(tt.loc[tt.tag_name == TAG_KEY, "table_name"].unique())
    print(f"[{label}] {len(ct)} classified columns across {ct.table_name.nunique()} tables; "
          f"{len(tiered)} table(s) carry {TAG_KEY}: {tiered or 'none'}")
    return tt, ct

before_tt, before_ct = tag_snapshot("before")
display(before_ct)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create the automation in Catalog Explorer
# MAGIC
# MAGIC This is the one step that happens outside the notebook. Automations are created in the UI, and Databricks recommends describing the rule to Genie rather than filling in the form. Here is the exact path and the exact prompt I used:
# MAGIC
# MAGIC 1. Open **Catalog** in the left sidebar, then **Governed Tags**, then the **Automations** tab.
# MAGIC 2. Click **Create automation**.
# MAGIC 3. Paste this into the Genie box:
# MAGIC
# MAGIC    > Tag every table in the `workspace.stark_tag_automation` schema that has at least one column tagged `class.email_address`, `class.credit_card`, `class.us_ssn` or `class.phone_number` with `sensitivity_tier` = `restricted`. Run it manually, not on a schedule.
# MAGIC
# MAGIC 4. Check what Genie built: scope is the `workspace` catalog narrowed to the `stark_tag_automation` schema, target is tables, condition is **Match any** over four **Column tag** entries, action is **Assign** `sensitivity_tier: restricted`, schedule is **Manual**.
# MAGIC 5. Save. Saving starts a dry run automatically. It should match three tables: `customers`, `employees` and `support_tickets`. If it matches something else, fix the conditions and dry-run again before enabling anything.
# MAGIC 6. Click **Enable**, then **Run**.
# MAGIC
# MAGIC Then come back here and run the next cell. It polls `information_schema.table_tags` every ten seconds until all three tables carry the tag, and reports how long it waited. That number starts when you run the cell, not when you clicked Run, so it includes however long you took to get back here; the automation's own run time is the **Duration** column in its run history.

# COMMAND ----------

def wait_for_tag(expected_tables, timeout_s=900, every_s=10):
    """Poll information_schema.table_tags until every expected table carries TAG_KEY or the timeout passes."""
    t0 = time.time()
    seen = set()
    while time.time() - t0 < timeout_s:
        rows = spark.sql(f"""
            SELECT table_name FROM {CATALOG}.information_schema.table_tags
            WHERE schema_name = '{SCHEMA}' AND tag_name = '{TAG_KEY}' AND tag_value = 'restricted'
        """).collect()
        seen = {r.table_name for r in rows}
        if set(expected_tables) <= seen:
            elapsed = time.time() - t0
            print(f"{TAG_KEY}=restricted is on {sorted(seen)} after {elapsed:.0f}s of polling.")
            return elapsed, seen
        time.sleep(every_s)
    print(f"Timed out after {timeout_s}s. Tagged so far: {sorted(seen) or 'none'}. "
          "Check the automation's run history in Catalog Explorer, then rerun this cell.")
    return None, seen

EXPECTED_RUN_1 = ["customers", "employees", "support_tickets"]
run_1_seconds, run_1_tagged = wait_for_tag(EXPECTED_RUN_1)

after_tt, after_ct = tag_snapshot("after run 1")
display(after_tt)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Put a mask policy on the other side of the tag
# MAGIC
# MAGIC The automation is only half the story. A table tagged `restricted` is still wide open until something acts on the tag, and that something is an [ABAC policy](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies). This one is defined once at the schema level with [`CREATE POLICY`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-policy): it applies to any table in the schema where `has_tag_value('sensitivity_tier', 'restricted')` is true, and inside those tables it masks every column carrying one of the four `class.*` tags. The mask itself is a plain [SQL function](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function) that keeps the last four characters.
# MAGIC
# MAGIC The policy is granted to `account users`, which includes you, so you see the mask in your own session. In a real deployment you would add `EXCEPT` for the group that owns the data. If `CREATE POLICY` complains about an unknown tag policy key, that is the propagation lag from Step 3; wait a minute and rerun the cell.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE FUNCTION {FQ}.mask_last_four(v STRING)
    RETURNS STRING
    RETURN CONCAT('***', RIGHT(v, 4))
""")

match_columns = " OR ".join(f"has_tag('{t}')" for t in CLASS_TAGS)
spark.sql(f"""
    CREATE OR REPLACE POLICY stark_restricted_mask
    ON SCHEMA {FQ}
    COMMENT 'Mask classified columns on any table the automation tagged restricted'
    COLUMN MASK {FQ}.mask_last_four
    TO `account users`
    FOR TABLES
    WHEN has_tag_value('{TAG_KEY}', 'restricted')
    MATCH COLUMNS {match_columns} AS sensitive
    ON COLUMN sensitive
""")
print(f"Policy stark_restricted_mask created on {FQ}, keyed on {TAG_KEY}=restricted, masking columns tagged {CLASS_TAGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Read the tables back
# MAGIC
# MAGIC One query per table, so you can see the policy doing exactly what the tag told it to. On the three tagged tables the classified columns come back masked and everything else comes back in the clear. On `orders` and `products`, which the automation never touched, nothing changes. Nobody wrote a grant, a view or a per-table rule against any of the five.

# COMMAND ----------

masked_columns = 0
for name in TABLES:
    df = spark.table(f"{FQ}.{name}").toPandas()
    masked = [c for c in df.columns if df[c].astype(str).str.startswith("***").all()]
    masked_columns += len(masked)
    print(f"{name:<16} masked: {masked or 'none'}")
    display(df)

print()
print(f"{masked_columns} columns masked across {len(run_1_tagged)} tagged tables, 0 per-table grants written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: A new table shows up
# MAGIC
# MAGIC This is the part I actually cared about. A marketing team lands a `prospects` table with an email column in it. Data Classification would tag that column within a day; here I tag it by hand the moment the table exists. Then run the automation again (**Run** on the same automation in Catalog Explorer, no changes to the rule) and watch what happens to the new table. The policy already exists, so the only thing that has to move is the tag.

# COMMAND ----------

spark.sql(f"""
    CREATE OR REPLACE TABLE {FQ}.prospects AS
    SELECT 9001 AS prospect_id, 'Stark Expo 2026' AS campaign, 'guest@stark.expo' AS email
    UNION ALL SELECT 9002, 'Stark Expo 2026', 'press@stark.expo'
""")
spark.sql(f"ALTER TABLE {FQ}.prospects ALTER COLUMN email SET TAGS ('class.email_address' = '')")

print("prospects created with email tagged class.email_address. In the clear right now:")
display(spark.table(f"{FQ}.prospects").toPandas())
print()
print("Now click Run on the automation in Catalog Explorer, then run the next cell.")

# COMMAND ----------

run_2_seconds, run_2_tagged = wait_for_tag(EXPECTED_RUN_1 + ["prospects"])

df = spark.table(f"{FQ}.prospects").toPandas()
display(df)
newly_masked = [c for c in df.columns if df[c].astype(str).str.startswith("***").all()]
print(f"prospects masked columns after run 2: {newly_masked or 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: The numbers in one table
# MAGIC
# MAGIC Every value the write-up cites, read back from `information_schema` rather than remembered, and printed as one `RESULTS_JSON` line so the numbers trace back to this run. Tables in scope, tables the automation tagged on each run, columns the policy masked, and how long the notebook waited for each run's tag to show up, counted from when the waiting cell started.

# COMMAND ----------

final_tt, final_ct = tag_snapshot("final")
tiered = sorted(final_tt.loc[final_tt.tag_name == TAG_KEY, "table_name"].unique())
all_tables = [t.tableName for t in spark.sql(f"SHOW TABLES IN {FQ}").collect()]

summary = pd.DataFrame([
    {"Metric": "Tables in scope",                         "Value": len(all_tables)},
    {"Metric": "Tables tagged restricted after run 1",    "Value": len(run_1_tagged)},
    {"Metric": "Tables tagged restricted after run 2",    "Value": len(run_2_tagged)},
    {"Metric": "Classified columns in the schema",        "Value": len(final_ct)},
    {"Metric": "Columns masked by one policy",            "Value": masked_columns + len(newly_masked)},
    {"Metric": "Per-table grants or views written",       "Value": 0},
    {"Metric": "Seconds waited for tag (run 1)",     "Value": None if run_1_seconds is None else round(run_1_seconds)},
    {"Metric": "Seconds waited for tag (run 2)",     "Value": None if run_2_seconds is None else round(run_2_seconds)},
])
display(summary)

results = {
    "run_id": RUN_ID,
    "gates_ok": gates_ok,
    "tables_in_scope": len(all_tables),
    "run_1_tagged": sorted(run_1_tagged),
    "run_2_tagged": sorted(run_2_tagged),
    "tiered_tables_final": tiered,
    "classified_columns": len(final_ct),
    "columns_masked_run_1": masked_columns,
    "prospects_masked_columns": newly_masked,
    "columns_masked_total": masked_columns + len(newly_masked),
    "per_table_grants_written": 0,
    "run_1_seconds_waited": None if run_1_seconds is None else round(run_1_seconds),
    "run_2_seconds_waited": None if run_2_seconds is None else round(run_2_seconds),
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC The automation tagged exactly the tables it should have. Run 1 put `sensitivity_tier = restricted` on `customers`, `employees` and `support_tickets`, the three tables with a classified column, and left `orders` and `products` alone. The one policy then masked 5 columns across those three tables, and nobody wrote a grant, a view or a per-table rule for any of them.
# MAGIC
# MAGIC The new table is the part worth your time. `prospects` landed with its email column tagged, the automation ran again with no change to the rule, and the table came back tagged and its `email` column masked. That's 6 columns masked by one policy, every classified column in the schema, and the only thing that moved was a tag.
# MAGIC
# MAGIC A few things to know before you build this yourself:
# MAGIC
# MAGIC - **Create the governed tag early.** When I first built this, a brand-new governed tag took about a minute to reach the policy compiler, which is why Step 3 runs four cells before the policy needs it.
# MAGIC - **Keep dots out of your own tag keys.** The [tag key rules](https://docs.databricks.com/aws/en/admin/governed-tags/manage-governed-tags) rule out `.` along with a few other characters, and prefixes like `class.` and `system.` are Databricks' own. `sensitivity_tier` works; `stark.sensitivity` doesn't.
# MAGIC - **Automations live in Catalog Explorer today.** There's no API or SQL for them yet, so for a platform team that keeps its rules in version control, that's where the results point next.
# MAGIC - **Tags are the contract.** In a real workspace, [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification) writes the `class.*` column tags; here they were set by hand so the notebook didn't wait on a scan. The automation and the policy can't tell the difference.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Automate tag assignment](https://docs.databricks.com/aws/en/admin/governed-tags/automate-tag-assignment): the Beta feature this notebook is built on, including every condition type and the 500-asset and 5-tag limits per automation.
# MAGIC - [Governed tags](https://docs.databricks.com/aws/en/admin/governed-tags/): how governed tags differ from free-form tags and why an automation can only assign the governed kind.
# MAGIC - [Attribute-based access control](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/): the policy model that turns a tag into a row filter or column mask.
# MAGIC - [Data Classification](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification): the scanner that assigns the `class.*` column tags this notebook set by hand.
# MAGIC - [Tag Policy API](https://docs.databricks.com/api/workspace/tagpolicies): the REST calls the notebook uses to create and delete the governed tag.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Set `RUN_CLEANUP = True` and run the cell to drop the policy, the schema and the governed tag. Delete the automation in Catalog Explorer first: it references `sensitivity_tier`, and there is no API to remove it from here. Leave `RUN_CLEANUP` at `False` if you want to keep poking at the tables.

# COMMAND ----------

RUN_CLEANUP = False

if RUN_CLEANUP:
    # DROP POLICY has no IF EXISTS form, so a missing policy is caught rather than assumed.
    try:
        spark.sql(f"DROP POLICY stark_restricted_mask ON SCHEMA {FQ}")
    except Exception as e:
        print(f"Policy drop skipped: {type(e).__name__}")
    spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
    code, body = api("DELETE", f"/api/2.1/tag-policies/{TAG_KEY}")
    print(f"Dropped policy and schema {FQ}; governed tag {TAG_KEY} delete -> http {code}")
    if code != 200:
        print("If the tag delete failed, the automation that references it probably still exists. Delete it in Catalog Explorer and rerun.")
else:
    print(f"Skipped. Set RUN_CLEANUP = True to drop {FQ}, the policy and the {TAG_KEY} governed tag.")
