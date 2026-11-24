# Databricks notebook source

# MAGIC %md
# MAGIC > "Support for AI Function tasks in Declarative Automation Bundles is now in Beta."
# MAGIC > Source: [Databricks November 2026 release notes](https://docs.databricks.com/aws/en/release-notes/product/2026/november#ai-function-task-support-in-declarative-automation-bundles-beta)
# MAGIC
# MAGIC If you already call an [AI Function](https://docs.databricks.com/aws/en/large-language-models/ai-functions.html) from a SQL cell, the obvious question is whether an AI Function task gives you anything new, or whether it's the same call in a different shell.
# MAGIC
# MAGIC So I ran the same classification both ways on the same table. One side is Databricks' `ai_classify` function in a SQL cell. The other is an AI Function task running the same `ai_classify` function with the same version and the same labels, created as a job from this notebook. Then I changed the table (two new documents, one edited, one deleted) and ran both again. What I'm watching is what each side does on the second run: how many rows go back to the model, whether the results stay in step with the table, and what the platform keeps a record of.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC **Runs on Free Edition.** Both sides run on [serverless compute](https://docs.databricks.com/aws/en/compute/serverless/dependencies), the default on Free Edition.
# MAGIC
# MAGIC The AI Function task is in Beta. If the job create cell fails, ask a workspace admin to turn on **Lakeflow AI Function Task** on the Previews page (username menu > Previews), per the [AI Function tasks docs](https://docs.databricks.com/aws/en/jobs/tasks/ai-functions/).
# MAGIC
# MAGIC The task finds what changed by reading the table's [change data feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed), so the documents table is created with `delta.enableChangeDataFeed = true`. There's nothing to install: the job is created with the Databricks SDK that ships with serverless, and it runs as you, so you need permission to create tables in the schema.
# MAGIC
# MAGIC Everything this notebook creates lives in one schema, `workspace.dab_ai_function_task`, and the last cell removes the job and the schema.

# COMMAND ----------

# Configuration
CATALOG  = "workspace"                  # Free Edition default writable catalog
SCHEMA   = "dab_ai_function_task"       # everything this notebook creates lives here
SOURCE   = f"{CATALOG}.{SCHEMA}.stark_documents"
CELL_OUT = f"{CATALOG}.{SCHEMA}.cell_classified"
TASK_OUT = "task_classified"            # the task creates and owns this table
JOB_NAME = "dab-ai-function-task-classify"
CLEANUP  = True                         # drop the job and schema at the end

LABELS = {
    "INVOICE":  "A bill requesting payment for goods or services",
    "RECEIPT":  "Acknowledges a payment already made or items received",
    "CONTRACT": "A binding agreement between parties",
}

# Start clean: the task refuses an output table it didn't create, so a rerun begins with an empty schema
spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
spark.sql(f"CREATE SCHEMA {CATALOG}.{SCHEMA}")
print(f"Schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the documents table
# MAGIC
# MAGIC Twenty Stark Industries documents with a known label each, written to a Delta table with change data feed on.

# COMMAND ----------

docs = [
    (1,  "INVOICE",  "Stark Industries Invoice #SI-2026-0041. To: Pepper Potts. Advanced repulsor array maintenance Q3. Amount due: 142,000. Due 2026-12-01."),
    (2,  "RECEIPT",  "Payment received. Stark Industries ref #REC-2026-0019. From: S.H.I.E.L.D. For: Mark L fabrication materials. Paid in full 2026-10-15."),
    (3,  "CONTRACT", "Services Agreement between Stark Industries and Rand Corporation. Term: 12 months from 2027-01-01. Exclusive consulting on arc reactor safety. Signed by both parties."),
    (4,  "INVOICE",  "Stark Industries Invoice #SI-2026-0055. To: Avengers Initiative. Vibranium stress testing, 3 sessions. Total: 67,200. Net 30."),
    (5,  "RECEIPT",  "Acknowledgment of receipt. Item: 400 units Mark II gauntlet components. Received from Hammer Industries on 2026-09-28. Condition: acceptable."),
    (6,  "CONTRACT", "Non-disclosure agreement between Stark Industries and Strategic Homeland Intervention. Subject: next-generation propulsion. Duration: perpetual. Governing law: California."),
    (7,  "INVOICE",  "Stark Industries Invoice #SI-2026-0062. To: Tony Stark personal account. R&D prototype expenses October 2026. Total: 312,400. Terms: immediate."),
    (8,  "RECEIPT",  "Payment confirmation. Stark Industries legal dept. Contract execution fee, Rand Corporation ref RANDC-0022. Received 15,000 on 2026-11-01."),
    (9,  "INVOICE",  "Invoice #SI-2026-0070 from Stark Industries to Oscorp. Clean-room rental, 14 days. Balance due within 45 days of this notice."),
    (10, "RECEIPT",  "Thank you for your payment. Stark Expo booth deposit, order #EXPO-311. Amount received: 25,000. No balance remaining."),
    (11, "CONTRACT", "Master Supply Agreement. Stark Industries will purchase palladium cores from Roxxon Energy for 36 months at the rates in Schedule A. Either party may terminate with 90 days notice."),
    (12, "INVOICE",  "Billing statement #SI-2026-0074. Customer: Damage Control. Debris removal support, Midtown site. Please remit 58,750 by 2026-12-15."),
    (13, "RECEIPT",  "Goods received note. 12 crates of arc reactor housings delivered to Stark Tower dock 3. Signed for by H. Hogan, 2026-11-03."),
    (14, "CONTRACT", "Employment agreement between Stark Industries and Dr. Bruce Banner. Position: visiting researcher. Start date 2027-02-01. Compensation and IP terms attached."),
    (15, "INVOICE",  "Stark Industries Invoice #SI-2026-0081. To: Wakandan Design Group. Joint test flights, November. Amount due: 204,000 upon receipt."),
    (16, "RECEIPT",  "Payment acknowledged for invoice #SI-2026-0041. Received from Pepper Potts office, 142,000, wire ref 88231. Account settled."),
    (17, "CONTRACT", "Lease agreement. Stark Industries leases hangar 7 at Malibu airfield from Coastal Properties LLC for five years. Rent escalates 3% annually."),
    (18, "INVOICE",  "Invoice #SI-2026-0088. Bill to: Pym Technologies. Quantum tunnel calibration services, 3 site visits. Total payable: 91,300."),
    (19, "RECEIPT",  "Cash receipt. Stark Industries cafeteria catering for board meeting, paid by corporate card. 1,240 settled on 2026-11-07."),
    (20, "CONTRACT", "Memorandum of agreement between Stark Industries and the City of New York for the Stark Tower energy grid pilot. Term: 24 months. Signed and witnessed."),
]

spark.sql(f"DROP TABLE IF EXISTS {SOURCE}")
spark.sql(f"""
    CREATE TABLE {SOURCE} (id INT, expected_label STRING, text STRING)
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")
spark.createDataFrame(docs, "id INT, expected_label STRING, text STRING").write.mode("append").saveAsTable(SOURCE)

print(f"{SOURCE}: {spark.table(SOURCE).count()} documents")
display(spark.sql(f"SELECT id, expected_label, left(text, 70) AS text_preview FROM {SOURCE} ORDER BY id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The cell side: the ai_classify function in SQL
# MAGIC
# MAGIC Databricks' `ai_classify` function, version 2.1, with the labels and descriptions passed as JSON. This is the way most people run an AI step today: a query that classifies whatever is in the table when you run it.

# COMMAND ----------

import json, time

LABELS_JSON = json.dumps(LABELS).replace("'", "\\'")

def run_cell_side():
    """Classify every row in the source table with SQL ai_classify; return the rows sent to the model."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {CELL_OUT} AS
        SELECT id, ai_classify(text, '{LABELS_JSON}', MAP('version', '2.1')) AS response
        FROM {SOURCE}
    """)
    return spark.table(SOURCE).count()   # a query classifies every row it reads

cell_r1_rows = run_cell_side()
print(f"Cell side, run 1: {cell_r1_rows} rows sent to the model")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The task side: an AI Function task
# MAGIC
# MAGIC The same step declared as a job task. The cell below creates the job through the Jobs API with the task spec as a Python dict. In a bundle, the same spec sits under `resources.jobs.<job>.tasks` in `databricks.yml`; the field names are identical:
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     classify_documents:
# MAGIC       name: dab-ai-function-task-classify
# MAGIC       max_concurrent_runs: 1
# MAGIC       tasks:
# MAGIC         - task_key: classify
# MAGIC           ai_function_task:
# MAGIC             delta_table:
# MAGIC               table_name: workspace.dab_ai_function_task.stark_documents
# MAGIC               input_column: text
# MAGIC               merge_key_columns: [id]
# MAGIC             output:
# MAGIC               catalog: workspace
# MAGIC               schema: dab_ai_function_task
# MAGIC               table: task_classified
# MAGIC             ai_classify:
# MAGIC               version: "2.1"
# MAGIC               inline_labels:
# MAGIC                 labels:
# MAGIC                   - name: INVOICE
# MAGIC                     description: A bill requesting payment for goods or services
# MAGIC                   - name: RECEIPT
# MAGIC                     description: Acknowledges a payment already made or items received
# MAGIC                   - name: CONTRACT
# MAGIC                     description: A binding agreement between parties
# MAGIC ```
# MAGIC
# MAGIC Bundle support for this task type arrived in Databricks CLI 1.24.0, per the [bundle task types reference](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types.html). The docs ask for `max_concurrent_runs: 1` on these jobs.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone

w = WorkspaceClient()

job_spec = {
    "name": JOB_NAME,
    "max_concurrent_runs": 1,
    "tasks": [{
        "task_key": "classify",
        "ai_function_task": {
            "delta_table": {"table_name": SOURCE, "input_column": "text", "merge_key_columns": ["id"]},
            "output": {"catalog": CATALOG, "schema": SCHEMA, "table": TASK_OUT},
            "ai_classify": {
                "version": "2.1",
                "inline_labels": {"labels": [{"name": k, "description": v} for k, v in LABELS.items()]},
            },
        },
    }],
}

# Remove a job left over from an earlier run of this notebook
for j in w.jobs.list(name=JOB_NAME):
    w.jobs.delete(j.job_id)

job_id = w.api_client.do("POST", "/api/2.2/jobs/create", body=job_spec)["job_id"]
print(f"Created job {job_id}: {JOB_NAME}")

# COMMAND ----------

def run_task_side():
    """Run the job, wait for it, return the run record."""
    run_id = w.api_client.do("POST", "/api/2.2/jobs/run-now", body={"job_id": job_id})["run_id"]
    while True:
        run = w.api_client.do("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        if run["status"]["state"] == "TERMINATED":
            return run
        time.sleep(15)

def pipeline_id_of(table):
    """The pipeline behind the task's output table, from its table properties."""
    props = spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]["properties"]
    return props.get("pipelines.pipelineId")

def model_rows_for(pipeline_id, since_ms):
    """Sum the pipeline's flow metrics for updates after since_ms: rows routed to the model, upserted, deleted."""
    events = w.api_client.do("GET", f"/api/2.0/pipelines/{pipeline_id}/events", query={"max_results": 250})["events"]
    routed = upserted = deleted = 0
    for e in events:
        if e.get("event_type") != "flow_progress":
            continue
        ts = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00")).timestamp() * 1000
        if ts < since_ms:
            continue
        m = e["details"]["flow_progress"].get("metrics") or {}
        flow = e["origin"].get("flow_name", "")
        if flow.endswith(".route_changes"):
            routed += m.get("num_output_rows", 0)
        elif flow.endswith(f".{TASK_OUT}"):
            upserted += m.get("num_upserted_rows", 0)
            deleted  += m.get("num_deleted_rows", 0)
    return routed, upserted, deleted

run1 = run_task_side()
print(f"Task side, run 1: {run1['status']['termination_details']['code']}")

# COMMAND ----------

TASK_TABLE  = f"{CATALOG}.{SCHEMA}.{TASK_OUT}"
PIPELINE_ID = pipeline_id_of(TASK_TABLE)
task_r1_routed, task_r1_upserted, task_r1_deleted = model_rows_for(PIPELINE_ID, run1["start_time"])

print(f"Output table:  {TASK_TABLE}")
print(f"Pipeline:      {PIPELINE_ID}")
print(f"Run 1: {task_r1_routed} rows routed to the model, {task_r1_upserted} upserted, {task_r1_deleted} deleted")
display(spark.sql(f"SELECT id, response, error FROM {TASK_TABLE} ORDER BY id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change the table
# MAGIC
# MAGIC Four changes, the kind a real documents table sees between runs. Two documents arrive, one gets corrected and one is removed. Document 2 was a receipt; its corrected text is a payment reminder, so its right label changes to INVOICE.

# COMMAND ----------

spark.sql(f"""
    INSERT INTO {SOURCE} VALUES
      (21, 'INVOICE', 'Stark Industries Invoice #SI-2026-0093. To: Parker Industries. Web-shooter fluid analysis. Amount due: 12,800, net 30.'),
      (22, 'RECEIPT', 'Received with thanks: 67,200 from the Avengers Initiative for invoice #SI-2026-0055. Paid in full.')
""")
spark.sql(f"""
    UPDATE {SOURCE}
    SET expected_label = 'INVOICE',
        text = 'Payment reminder: Stark Industries invoice #SI-2026-0019 for Mark L fabrication materials is overdue. Balance due immediately.'
    WHERE id = 2
""")
spark.sql(f"DELETE FROM {SOURCE} WHERE id = 7")

CHANGED_ROWS = 4
print(f"{SOURCE}: {spark.table(SOURCE).count()} documents after {CHANGED_ROWS} changes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run both sides again

# COMMAND ----------

cell_r2_rows = run_cell_side()
print(f"Cell side, run 2: {cell_r2_rows} rows sent to the model")

run2 = run_task_side()
task_r2_routed, task_r2_upserted, task_r2_deleted = model_rows_for(PIPELINE_ID, run2["start_time"])
print(f"Task side, run 2: {run2['status']['termination_details']['code']}")
print(f"                  {task_r2_routed} rows routed to the model, {task_r2_upserted} upserted, {task_r2_deleted} deleted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Do the two sides agree with the table?
# MAGIC
# MAGIC Both outputs joined to the current table on `id`: each side's label, the expected label, and any row an output holds that the table no longer has.

# COMMAND ----------

compare = spark.sql(f"""
    WITH src AS (SELECT id, expected_label FROM {SOURCE}),
    cell AS (SELECT id, response:response[0].value::STRING AS cell_label FROM {CELL_OUT}),
    task AS (SELECT id, response:response[0].value::STRING AS task_label FROM {TASK_TABLE})
    SELECT
        COALESCE(s.id, c.id, t.id)       AS id,
        s.expected_label,
        c.cell_label,
        t.task_label,
        s.id IS NULL                     AS not_in_table
    FROM src s
    FULL OUTER JOIN cell c ON c.id = s.id
    FULL OUTER JOIN task t ON t.id = COALESCE(s.id, c.id)
    ORDER BY id
""")
display(compare)

cmp = compare.toPandas()
in_table     = cmp[~cmp["not_in_table"]]
table_rows   = len(in_table)
cell_correct = int((in_table["cell_label"] == in_table["expected_label"]).sum())
task_correct = int((in_table["task_label"] == in_table["expected_label"]).sum())
sides_agree  = int((in_table["cell_label"] == in_table["task_label"]).sum())
cell_stale   = int((cmp["not_in_table"] & cmp["cell_label"].notna()).sum())
task_stale   = int((cmp["not_in_table"] & cmp["task_label"].notna()).sum())
doc2_task    = in_table.loc[in_table["id"] == 2, "task_label"].iloc[0]
task_errors  = spark.sql(f"SELECT count(*) AS n FROM {TASK_TABLE} WHERE error IS NOT NULL").collect()[0]["n"]

print(f"Rows in table:           {table_rows}")
print(f"Cell side correct:       {cell_correct} / {table_rows}")
print(f"Task side correct:       {task_correct} / {table_rows}")
print(f"Sides agree:             {sides_agree} / {table_rows}")
print(f"Stale rows (cell, task): {cell_stale}, {task_stale}")
print(f"Doc 2 after edit (task): {doc2_task}")
print(f"Task rows with an error: {task_errors}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the platform keeps
# MAGIC
# MAGIC The task's output is not a plain table. Its properties name the pipeline that maintains it, and that pipeline's event log is where the per-run row counts above came from. The cell side's output is a table the query replaced.

# COMMAND ----------

def props_of(table):
    return spark.sql(f"DESCRIBE DETAIL {table}").collect()[0]["properties"]

def kind_of(table):
    row = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").filter("col_name = 'Type'").collect()
    return row[0]["data_type"] if row else "unknown"

task_props = props_of(TASK_TABLE)
cell_props = props_of(CELL_OUT)
task_kind  = kind_of(TASK_TABLE)
cell_kind  = kind_of(CELL_OUT)

keep = ["pipelines.pipelineId", "spark.internal.pipelines.table_artifact_type", "delta.enableChangeDataFeed", "delta.enableRowTracking"]
display(spark.createDataFrame(
    [(k, str(cell_props.get(k)), str(task_props.get(k))) for k in keep] + [("table type", cell_kind, task_kind)],
    "property STRING, cell_side STRING, task_side STRING",
))

# COMMAND ----------

# DBTITLE 1,Rows sent to the model per run
# hide-code
import matplotlib.pyplot as plt
import numpy as np

OAT_LIGHT, NAVY, GRAY_TEXT, LAVA = "#F9F7F4", "#1B3139", "#5A6F77", "#FF3621"

fig, ax = plt.subplots(figsize=(7, 3.2), facecolor=OAT_LIGHT)
ax.set_facecolor(OAT_LIGHT)
x = np.arange(2)
cell_vals = [cell_r1_rows, cell_r2_rows]
task_vals = [task_r1_routed, task_r2_routed]
b1 = ax.bar(x - 0.18, cell_vals, 0.34, color=NAVY, label="SQL cell")
b2 = ax.bar(x + 0.18, task_vals, 0.34, color=LAVA, label="AI Function task")
for bars in (b1, b2):
    for b in bars:
        ax.text(b.get_x() + b.get_width() / 2, b.get_height() + 0.3, f"{int(b.get_height())}",
                ha="center", va="bottom", color=NAVY, fontsize=10)
ax.set_xticks(x, ["Run 1: new table", f"Run 2: after {CHANGED_ROWS} changes"])
ax.set_ylabel("rows sent to the model", color=GRAY_TEXT, fontsize=10)
ax.set_title("Rows sent to the model per run", color=NAVY, fontsize=12, fontweight="bold", pad=10)
ax.tick_params(colors=GRAY_TEXT)
ax.spines[["top", "right"]].set_visible(False)
ax.spines[["left", "bottom"]].set_color(GRAY_TEXT)
ax.legend(fontsize=9, frameon=False, labelcolor=GRAY_TEXT)
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
import html

OAT_LIGHT, NAVY, GRAY_TEXT, GRAY_LINES = "#F9F7F4", "#1B3139", "#5A6F77", "#DCE0E2"
SANS  = "DM Sans, Helvetica Neue, Arial, sans-serif"
FONTS = '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'

def esc(v): return html.escape(str(v))

def stat(number, words):
    return (f"<div style='min-width:150px'>"
            f"<div style='font-size:30px;font-weight:700;color:{NAVY}'>{esc(number)}</div>"
            f"<div style='font-size:13px;color:{GRAY_TEXT};line-height:1.35;white-space:pre-line'>{esc(words)}</div></div>")

def row(label, cell, task):
    return (f"<tr><td style='padding:4px 18px 4px 0;color:{GRAY_TEXT}'>{esc(label)}</td>"
            f"<td style='padding:4px 18px 4px 0;color:{NAVY}'>{esc(cell)}</td>"
            f"<td style='padding:4px 0;color:{NAVY}'>{esc(task)}</td></tr>")

NL = "\n"
tiles = "".join([
    stat(CHANGED_ROWS, "rows changed" + NL + "between runs"),
    stat(cell_r2_rows, "rows the cell" + NL + "sent on run 2"),
    stat(task_r2_routed, "rows the task" + NL + "sent on run 2"),
    stat(f"{sides_agree}/{table_rows}", "labels where" + NL + "the two agree"),
])
rows_html = "".join([
    row("Rows sent, run 1", cell_r1_rows, task_r1_routed),
    row("Rows sent, run 2", cell_r2_rows, task_r2_routed),
    row("Correct labels", f"{cell_correct}/{table_rows}", f"{task_correct}/{table_rows}"),
    row("Rows kept for deleted docs", cell_stale, task_stale),
    row("Output table", cell_kind, task_kind),
])

displayHTML(f"""
{FONTS}
<div style='font-family:{SANS};background:{OAT_LIGHT};padding:28px 32px;border-radius:8px;max-width:720px'>
  <div style='font-size:19px;font-weight:700;color:{NAVY};margin-bottom:20px'>SQL cell vs AI Function task: Free Edition run</div>
  <div style='display:flex;gap:34px;flex-wrap:wrap;padding-bottom:22px'>
    {tiles}
  </div>
  <table style='font-size:13px;border-top:1px solid {GRAY_LINES};padding-top:10px;border-collapse:collapse'>
    <tr><td></td><td style='padding:8px 18px 4px 0;font-weight:700;color:{NAVY}'>SQL cell</td><td style='padding:8px 0 4px 0;font-weight:700;color:{NAVY}'>AI Function task</td></tr>
    {rows_html}
  </table>
  <div style='font-size:13px;color:{GRAY_TEXT};margin-top:12px'>Surface: databricks-free-serverless &nbsp;·&nbsp; ai_classify version 2.1</div>
</div>
""")

# COMMAND ----------

results = {
    "table_rows_run1":        len(docs),
    "changed_rows":           CHANGED_ROWS,
    "table_rows_run2":        table_rows,
    "cell_r1_rows_sent":      cell_r1_rows,
    "cell_r2_rows_sent":      cell_r2_rows,
    "task_r1_rows_routed":    task_r1_routed,
    "task_r1_upserted":       task_r1_upserted,
    "task_r1_deleted":        task_r1_deleted,
    "task_r2_rows_routed":    task_r2_routed,
    "task_r2_upserted":       task_r2_upserted,
    "task_r2_deleted":        task_r2_deleted,
    "task_r1_result":         run1["status"]["termination_details"]["code"],
    "task_r2_result":         run2["status"]["termination_details"]["code"],
    "cell_correct":           cell_correct,
    "task_correct":           task_correct,
    "sides_agree":            sides_agree,
    "cell_stale_rows":        cell_stale,
    "task_stale_rows":        task_stale,
    "doc2_task_label_after_edit": doc2_task,
    "task_error_rows":        int(task_errors),
    "task_table_type":        task_kind,
    "cell_table_type":        cell_kind,
    "task_has_pipeline":      PIPELINE_ID is not None,
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# Cleanup: remove the job and everything in the schema
if CLEANUP:
    w.jobs.delete(job_id)
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    print(f"Removed job {job_id} and schema {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC On the first run there's no difference to see. The table was new, so the SQL cell and the AI Function task each sent all 20 documents to the model.
# MAGIC
# MAGIC The second run is where they split. After four changes, the SQL cell sent the whole table back to the model, 21 documents. The task sent 3: the two new documents and the one I corrected. The deleted document's result came out of the task's table without a model call at all.
# MAGIC
# MAGIC The answers didn't move. Both sides labelled all 21 documents correctly, they agreed on every one, and document 2 came back as INVOICE once its text turned into a payment reminder. Neither output kept a result for the document I removed: the cell because it rewrites its table every time, the task because it deleted that row.
# MAGIC
# MAGIC What changes is what you're left with. The cell's output is a managed table the query replaces on each run. The task's output is a streaming table, and its properties name the pipeline that keeps it in step with the source.
# MAGIC
# MAGIC In plain words: the experiment results suggest the AI Function task isn't a new way to call the model; it's a way to stop calling it twice. If your table changes a little between runs, the task only sends the model what changed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Add tasks to jobs in Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/job-task-types.html): the `ai_function_task` reference with YAML examples for `ai_classify`, `ai_extract`, and `ai_parse_document`.
# MAGIC - [AI Function tasks for jobs](https://docs.databricks.com/aws/en/jobs/tasks/ai-functions/): the job editor side, plus the requirements, privileges, and limits for the task type.
# MAGIC - [ai_classify function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify.html): the SQL function the cell side calls.
# MAGIC - [Change data feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed): what the task reads to find the rows that changed.
# MAGIC - [What are Declarative Automation Bundles?](https://docs.databricks.com/aws/en/dev-tools/bundles/index.html): how bundles package a job and everything around it as one YAML-defined artifact.
