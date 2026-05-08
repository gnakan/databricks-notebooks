# Databricks notebook source

# MAGIC %md
# MAGIC > "MLflow trace storage in Unity Catalog tables, previously in Beta, is now in Public Preview. Traces are stored in OpenTelemetry (OTel) format, access is governed through Unity Catalog schema and table permissions, and you can query traces from Databricks SQL or the MLflow Python SDK."
# MAGIC
# MAGIC That release note is the headline. The practitioner question underneath it: what's actually reachable for MLflow tracing on Databricks Free Edition today, when the UC-backed trace table path is gated on externally-stored UC catalogs and workspace admin Previews?
# MAGIC
# MAGIC Here's what this notebook tests: capture a simulated LLM call as an MLflow span, retrieve it from Python via `search_traces`, and view it in the MLflow UI — entirely on Free Edition. It also includes an availability probe at the end that tells you, on any workspace, whether the UC trace storage Public Preview is reachable for you. What you walk away with: a working agent observability pattern that runs everywhere Databricks runs, plus a clear read on where the UC governance layer kicks in.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **Databricks Free Edition** — [databricks.com/learn/free-edition](https://www.databricks.com/learn/free-edition)
# MAGIC - **Compute:** the default Free Edition Serverless runtime
# MAGIC - **Libraries:** `mlflow[databricks]>=3.11.0` — Free Edition's pre-installed MLflow predates the modern tracing API, so the next cell installs a current version and restarts the Python kernel
# MAGIC - **Unity Catalog:** the default workspace catalog is fine for the tracing path; the optional probe at the end tests whether your environment can also reach UC-backed trace storage

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.11.0" "openai>=1.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow

# Point MLflow at the Databricks workspace tracking server and the UC-aware
# model registry. On Free Edition / Serverless, set_registry_uri must be called
# explicitly — Serverless Spark Connect does not expose spark.mlflow.modelRegistryUri,
# so MLflow's default lookup raises CONFIG_NOT_AVAILABLE without it.
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

# Create (or reuse) the experiment
# Databricks stores this in your workspace under /Users/<your-email>/
current_user = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
)
experiment_name = f"/Users/{current_user}/mlflow-traces-uc-demo"
mlflow.set_experiment(experiment_name)

experiment = mlflow.get_experiment_by_name(experiment_name)
print(f"Experiment ID : {experiment.experiment_id}")
print(f"Artifact root : {experiment.artifact_location}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What we're about to run
# MAGIC
# MAGIC We're going to make a real Foundation Model API call — a question routed to a Databricks-hosted Llama 4 endpoint, a response coming back — and wrap it in an MLflow span. The span captures the inputs, outputs, token usage, and latency in OpenTelemetry format. Then we'll retrieve the same span from Python and inspect it.
# MAGIC
# MAGIC **What do you think will happen?**
# MAGIC
# MAGIC Will `mlflow.search_traces` come back with the span fully populated — trace_id, status, the prompt and response we set — or will it return a row with the structural fields filled in but the inputs and outputs missing because the span context didn't flush to the tracking server in time?
# MAGIC
# MAGIC Make your prediction before you run the next cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging the LLM call
# MAGIC
# MAGIC We use `mlflow.start_span()` as a context manager. Inside the span, we record:
# MAGIC - The inputs going into the model
# MAGIC - The outputs it returned
# MAGIC - Metadata to attach to the span (model name, token counts, finish reason)
# MAGIC
# MAGIC The call below is a real Foundation Model API request. Free Edition is no-cost
# MAGIC and includes Foundation Model APIs governed by quota (active endpoints, no
# MAGIC provisioned throughput) rather than per-token billing. The workspace's
# MAGIC authenticated session provides auth — no API key is needed.

# COMMAND ----------

from openai import OpenAI

# Auth comes from the Databricks notebook context — no API key to manage.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
client = OpenAI(
    api_key=ctx.apiToken().get(),
    base_url=f"{ctx.apiUrl().get()}/serving-endpoints",
)

MODEL = "databricks-llama-4-maverick"  # any READY chat endpoint listed in the workspace works
question = "What are the top three reasons a Bronze-to-Silver DLT pipeline might fail data quality checks?"

with mlflow.start_span(name="llm_call", span_type="LLM") as span:
    span.set_inputs({"prompt": question, "model": MODEL})

    completion = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": question}],
        max_tokens=256,
    )

    response_text = completion.choices[0].message.content
    span.set_outputs({"response": response_text})
    span.set_attributes({
        "model": MODEL,
        "input_tokens": completion.usage.prompt_tokens,
        "output_tokens": completion.usage.completion_tokens,
        "finish_reason": completion.choices[0].finish_reason,
    })

print("Span logged.")
print(f"Response preview: {response_text[:120]}...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieving the trace
# MAGIC
# MAGIC `mlflow.search_traces` returns a Pandas DataFrame, one row per trace, with the
# MAGIC inputs, outputs, status, and timing for each. This is the programmatic surface
# MAGIC for agent observability — the same data you can browse interactively in the
# MAGIC MLflow UI under the experiment page in your workspace.

# COMMAND ----------

import pandas as pd

traces_df = mlflow.search_traces(
    experiment_ids=[experiment.experiment_id],
    max_results=5,
)

# search_traces returns a Pandas DataFrame
print(f"Traces found: {len(traces_df)}")
if len(traces_df) > 0:
    print("\nColumns:", list(traces_df.columns))
    print("\nMost recent trace:")
    row = traces_df.iloc[0]
    print(f"  trace_id  : {row.get('trace_id', row.get('request_id', 'n/a'))}")
    print(f"  status    : {row.get('status', 'n/a')}")
    print(f"  timestamp : {row.get('timestamp_ms', 'n/a')}")
display(traces_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The reveal
# MAGIC
# MAGIC The span you just logged came back as a structured row — trace_id, status, timing, and
# MAGIC the prompt and response you set as inputs and outputs. You can also browse it
# MAGIC interactively in the MLflow UI: open the experiment page in your workspace and you'll
# MAGIC see the trace timeline, span tree, and attributes rendered as a UI.
# MAGIC
# MAGIC That's the agent observability surface today on Free Edition. Every span you log is a
# MAGIC structured, searchable, inspectable record of an LLM decision your code made. You can
# MAGIC pull traces in code for evaluation harnesses. You can browse them in the UI for
# MAGIC debugging. The OpenTelemetry format means they travel cleanly into any observability
# MAGIC stack you're already running.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's not reachable on Free Edition (and how to test for yourself)
# MAGIC
# MAGIC The April 30, 2026 release note announced traces as Delta tables in Unity Catalog —
# MAGIC `SELECT *` on agent decision history, governed alongside your production data. That
# MAGIC path requires:
# MAGIC
# MAGIC - Workspace admin enabling the "OpenTelemetry on Databricks" Preview
# MAGIC - MLflow `>= 3.11`
# MAGIC - A SQL warehouse with `CAN USE`
# MAGIC - A UC catalog backed by **external storage** — UC tables in default storage are not
# MAGIC   supported as trace destinations
# MAGIC
# MAGIC Free Edition's metastore root is default storage, so any catalog created there
# MAGIC inherits default storage too. That puts the SQL-queryable trace path out of reach
# MAGIC on Free Edition today.
# MAGIC
# MAGIC The probe below creates a dedicated `tl_mlflow_probe` catalog so cleanup is a single
# MAGIC `DROP CATALOG ... CASCADE` regardless of outcome. Run it on a paid workspace with an
# MAGIC externally-stored metastore root and you should see all three gates pass.

# COMMAND ----------

# Availability probe: is the UC trace storage runtime open on this workspace?
# We create a dedicated catalog AND a fresh experiment for the probe. Both isolate
# the test from the rest of the notebook: cleanup is one DROP CATALOG ... CASCADE
# plus deleting the probe experiment from the MLflow UI.
#
# Why a fresh experiment: MLflow rejects setting a UC trace_location on any
# experiment that already contains traces ("A UC Trace Destination can only be
# linked to an Experiment that does not already contain any traces"). The main
# experiment above already has the span we logged, so we need a clean one to
# test the storage-type gate on its own.

import os
import uuid

PROBE_CATALOG = "tl_mlflow_probe"
PROBE_SCHEMA  = "default"
PROBE_PREFIX  = "trace"
PROBE_EXPERIMENT = f"/Users/{current_user}/mlflow-uc-probe-{uuid.uuid4().hex[:8]}"

# Create the dedicated probe catalog and schema (idempotent).
# On a paid workspace with an externally-stored metastore root, the new catalog
# inherits external storage and gate 3 should pass.
# On Free Edition, the new catalog inherits Free Edition's default-storage
# metastore root, so gate 3 will still fail — but cleanup is one statement.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {PROBE_CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {PROBE_CATALOG}.{PROBE_SCHEMA}")
print(f"Probe catalog ready:    {PROBE_CATALOG}.{PROBE_SCHEMA}")
print(f"Probe experiment:       {PROBE_EXPERIMENT}")

# Gate 1: MLflow version must be >= 3.11 for the trace_location parameter
mlflow_parts = tuple(int(p) for p in mlflow.__version__.split(".")[:2])
gate_version = mlflow_parts >= (3, 11)
print(f"[gate 1] MLflow version: {mlflow.__version__}  -> {'OK' if gate_version else 'FAIL (need >= 3.11)'}")

# Gate 2: UnityCatalog import path must resolve on this MLflow build
try:
    from mlflow.entities.trace_location import UnityCatalog
    gate_import = True
    print("[gate 2] mlflow.entities.trace_location.UnityCatalog import: OK")
except ImportError as exc:
    gate_import = False
    print(f"[gate 2] UnityCatalog import: FAIL ({exc})")

# Gate 3: API call against a fresh experiment in the dedicated probe catalog.
# If your workspace requires an explicit SQL warehouse for trace ingestion, set
# MLFLOW_TRACING_SQL_WAREHOUSE_ID to the warehouse ID before this runs (auto-
# discovery handles most paid workspaces and Free Edition).
gate3_ok = False
if gate_version and gate_import:
    try:
        os.environ.setdefault("MLFLOW_TRACING_SQL_WAREHOUSE_ID", "")
        mlflow.set_experiment(
            experiment_name=PROBE_EXPERIMENT,
            trace_location=UnityCatalog(
                catalog_name=PROBE_CATALOG,
                schema_name=PROBE_SCHEMA,
                table_prefix=PROBE_PREFIX,
            ),
        )
        gate3_ok = True
        print(f"[gate 3] set_experiment with trace_location: OK ({PROBE_CATALOG}.{PROBE_SCHEMA}.{PROBE_PREFIX}_otel_*)")
        print("\nRESULT: UC trace storage is reachable on this workspace.")
    except Exception as exc:
        print(f"[gate 3] set_experiment with trace_location: FAIL")
        print(f"         {type(exc).__name__}: {exc}")
        print("\nRESULT: UC trace storage runtime is gated. The error class/message above identifies which gate.")
else:
    print("\nRESULT: Cannot probe gate 3 — earlier gate failed.")

print(f"\nCleanup (run when you're done):")
print(f"  spark.sql(\"DROP CATALOG {PROBE_CATALOG} CASCADE\")")
print(f"  # then delete the probe experiment from the MLflow UI: {PROBE_EXPERIMENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closing the loop (only runs if gate 3 passed)
# MAGIC
# MAGIC If gate 3 passed, your workspace can write traces to a UC Delta table — and we
# MAGIC have the destination provisioned. The cell below logs one real span to the probe
# MAGIC experiment (linked to the UC trace destination), waits a few seconds for the
# MAGIC async trace flush, and runs `SELECT *` against the OTel spans table. That is the
# MAGIC `from LLM call to SQL row` reveal the original release note promised.
# MAGIC
# MAGIC If gate 3 didn't pass, this cell prints a skip message — there's no UC trace
# MAGIC table to query, which is itself the architectural finding for that workspace.

# COMMAND ----------

if gate3_ok:
    import time

    # The probe experiment is currently active (set_experiment was called above).
    # Log a real span to confirm the path: span -> UC table -> SQL.
    with mlflow.start_span(name="probe_test_call", span_type="LLM") as span:
        span.set_inputs({"prompt": "Probe test: confirm trace lands in UC.", "model": MODEL})
        completion = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": "What does ETL stand for? One sentence."}],
            max_tokens=64,
        )
        probe_response = completion.choices[0].message.content
        span.set_outputs({"response": probe_response})
        span.set_attributes({
            "model": MODEL,
            "input_tokens": completion.usage.prompt_tokens,
            "output_tokens": completion.usage.completion_tokens,
        })

    print("Probe span logged. Waiting briefly for the async trace flush...")
    time.sleep(5)

    table_fqn = f"{PROBE_CATALOG}.{PROBE_SCHEMA}.{PROBE_PREFIX}_otel_spans"

    print(f"\nSchema of {table_fqn}:")
    display(spark.sql(f"DESCRIBE TABLE {table_fqn}"))

    print(f"\nRow count after the probe span:")
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {table_fqn}").collect()[0]["n"]
    print(f"  {n} row(s)")
    if n == 0:
        print("  (If 0, the async flush may not have caught up yet — re-run this cell.)")

    print(f"\nFor full row inspection: the OTel spans table includes VARIANT columns")
    print(f"that the Spark Connect client cannot render directly. Open the workspace")
    print(f"SQL editor and run:")
    print(f"  SELECT * FROM {table_fqn} LIMIT 10")
else:
    print("Skipped — gate 3 did not pass on this workspace, so there is no UC trace destination to query.")
    print("This skip is itself the architectural finding: trace ingestion requires externally-stored UC.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this means for teams building agents today
# MAGIC
# MAGIC On any Databricks workspace, including Free Edition, the MLflow tracing API is enough
# MAGIC to validate the agent observability pattern end to end: log spans, search them in code,
# MAGIC inspect them in a UI, ship them to OTel-compatible tooling. That's a real surface a
# MAGIC team can build on today.
# MAGIC
# MAGIC The next step — `SELECT *` on traces from Databricks SQL with full Unity Catalog
# MAGIC governance — is a paid-workspace surface. The architectural gate is externally-stored
# MAGIC UC, not the API itself. If you're running in a paid workspace with admin access and a
# MAGIC catalog backed by external storage, the same notebook (with `trace_location` set on
# MAGIC `set_experiment`) gets you the SQL path. The probe above tells you when your
# MAGIC environment crosses that line.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC **[MLflow Tracing Overview](https://mlflow.org/docs/latest/genai/tracing/)** — The full picture on spans, traces, and the OTel data model. Start here to instrument a real agent.
# MAGIC
# MAGIC **[Store OpenTelemetry traces in Unity Catalog (Databricks docs)](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog)** — The official enablement path: Previews, MLflow version, SQL warehouse, `trace_location=UnityCatalog(...)`.
# MAGIC
# MAGIC **[Query OpenTelemetry traces stored in Unity Catalog (Databricks docs)](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/observe-with-traces/query-dbsql)** — SQL patterns once the UC path is enabled.
# MAGIC
# MAGIC **[MLflow Public Preview release note (April 30, 2026)](https://docs.databricks.com/aws/en/release-notes/product/2026/april#mlflow-trace-storage-in-unity-catalog-is-now-in-public-preview)** — The announcement this notebook reacts to.
# MAGIC
# MAGIC **[Databricks Free Edition](https://www.databricks.com/learn/free-edition)** — If you don't have a workspace yet, this notebook runs end-to-end on Free Edition (everything except the optional UC trace storage probe).
