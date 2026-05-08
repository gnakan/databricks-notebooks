# Databricks notebook source

# MAGIC %md
# MAGIC > "MLflow trace storage in Unity Catalog tables, previously in Beta, is now in Public Preview. Traces are stored in OpenTelemetry (OTel) format, access is governed through Unity Catalog schema and table permissions, and you can query traces from Databricks SQL or the MLflow Python SDK."
# MAGIC
# MAGIC The release note is the headline. The question I had underneath it: where is that SQL-queryable path actually reachable today, given that the UC-backed trace destination is gated on externally-stored UC catalogs and workspace admin Previews?
# MAGIC
# MAGIC I built an availability probe to find out. The notebook captures a real Llama 4 call as an MLflow span, retrieves it via `search_traces`, and then runs three gates against a dedicated probe catalog to test whether the UC trace storage runtime is open on this workspace. The probe is the artifact. Run it on the workspace in front of you and the gate output is the answer: which gates pass, which gate fails, and what the failure says.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **A Databricks workspace.** I demonstrated this notebook on Databricks Free Edition and on a Premium Azure workspace with an externally-stored UC metastore. Either runs the tracing path. The probe at the end is what tells the two environments apart.
# MAGIC - **Compute:** the default Serverless runtime.
# MAGIC - **Libraries:** `mlflow[databricks]>=3.11.0`. The pre-installed MLflow on most workspaces predates the modern tracing API, so the next cell installs a current version and restarts the Python kernel.
# MAGIC - **Unity Catalog:** the default workspace catalog is fine for the tracing path. The probe creates a dedicated `tl_mlflow_probe` catalog so cleanup is one statement.

# COMMAND ----------

# MAGIC %pip install "mlflow[databricks]>=3.11.0" "openai>=1.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow

# Point MLflow at the Databricks workspace tracking server and the UC-aware
# model registry. On Serverless compute, set_registry_uri must be called
# explicitly. Serverless Spark Connect does not expose spark.mlflow.modelRegistryUri,
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
# MAGIC ## Logging the LLM call
# MAGIC
# MAGIC I wrapped the call in `mlflow.start_span()` as a context manager. Inside the span, I recorded the prompt, the response, and the token/finish-reason metadata.
# MAGIC
# MAGIC The call is a real Foundation Model API request to a Databricks-hosted Llama 4 endpoint. The workspace's authenticated session provides auth, so there is no API key to manage.

# COMMAND ----------

from openai import OpenAI

# Auth comes from the Databricks notebook context. No API key to manage.
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
# MAGIC `mlflow.search_traces` returns a Pandas DataFrame, one row per trace, with the inputs, outputs, status, and timing for each. The same data renders interactively in the MLflow UI under the experiment page in the workspace.

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
# MAGIC ## Probing the SQL-queryable path
# MAGIC
# MAGIC The April 30, 2026 release note announced traces as Delta tables in Unity Catalog. `SELECT *` on agent decision history, governed alongside production data. The path requires:
# MAGIC
# MAGIC - Workspace admin enabling the "OpenTelemetry on Databricks" Preview
# MAGIC - MLflow `>= 3.11`
# MAGIC - A SQL warehouse with `CAN USE`
# MAGIC - A UC catalog backed by **external storage**. UC tables in default storage are not supported as trace destinations.
# MAGIC
# MAGIC The probe below tests the runtime side of those requirements. It creates a dedicated `tl_mlflow_probe` catalog (so cleanup is one `DROP CATALOG ... CASCADE` regardless of outcome) and runs three gates: MLflow version, the `UnityCatalog` import path, and an actual `set_experiment(..., trace_location=UnityCatalog(...))` call. The third gate is where storage type matters. A workspace whose metastore root is externally-stored UC will see all three gates pass. A workspace whose metastore root is default storage will see gate 3 fail with a verbatim error that names the constraint. Both outcomes are evidence.

# COMMAND ----------

# Availability probe: is the UC trace storage runtime open on this workspace?
# I created a dedicated catalog AND a fresh experiment for the probe. Both isolate
# the test from the rest of the notebook: cleanup is one DROP CATALOG ... CASCADE
# plus deleting the probe experiment from the MLflow UI.
#
# Why a fresh experiment: MLflow rejects setting a UC trace_location on any
# experiment that already contains traces ("A UC Trace Destination can only be
# linked to an Experiment that does not already contain any traces"). The main
# experiment above already has the span I logged, so I needed a clean one to
# test the storage-type gate on its own.

import uuid

PROBE_CATALOG = "tl_mlflow_probe"
PROBE_SCHEMA  = "default"
PROBE_PREFIX  = "trace"
PROBE_EXPERIMENT = f"/Users/{current_user}/mlflow-uc-probe-{uuid.uuid4().hex[:8]}"

# Create the dedicated probe catalog and schema (idempotent).
# The new catalog inherits the metastore root's storage type. If the root is
# externally-stored UC, gate 3 should pass. If the root is default storage,
# gate 3 will fail with the verbatim error message naming the constraint.
# Cleanup is one statement regardless of outcome.
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
# If the workspace requires an explicit SQL warehouse for trace ingestion, set
# MLFLOW_TRACING_SQL_WAREHOUSE_ID to the warehouse ID before this runs.
# Auto-discovery handles most workspaces.
gate3_ok = False
if gate_version and gate_import:
    try:
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
    print("\nRESULT: Cannot probe gate 3. An earlier gate failed.")

print(f"\nCleanup (run when you're done):")
print(f"  spark.sql(\"DROP CATALOG {PROBE_CATALOG} CASCADE\")")
print(f"  # then delete the probe experiment from the MLflow UI: {PROBE_EXPERIMENT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closing the loop (only runs if gate 3 passed)
# MAGIC
# MAGIC If gate 3 passed, the workspace can write traces to a UC Delta table and the destination is provisioned. The cell below logs one real span to the probe experiment (linked to the UC trace destination), waits a few seconds for the async trace flush, and inspects the OTel spans table the release note promised. That closes the loop from LLM call to SQL row.
# MAGIC
# MAGIC If gate 3 did not pass, the cell prints a skip message. The skip is itself the architectural finding: trace ingestion requires externally-stored UC, and this workspace does not have it.

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
        print("  (If 0, the async flush may not have caught up yet. Re-run this cell.)")

    print(f"\nFor full row inspection: the OTel spans table includes VARIANT columns")
    print(f"that the Spark Connect client cannot render directly. Open the workspace")
    print(f"SQL editor and run:")
    print(f"  SELECT * FROM {table_fqn} LIMIT 10")
else:
    print("Skipped. Gate 3 did not pass on this workspace, so there is no UC trace destination to query.")
    print("This skip is itself the architectural finding: trace ingestion requires externally-stored UC.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I learned
# MAGIC
# MAGIC The MLflow tracing API was reachable on every workspace I ran this on. Spans logged, traces searched, the OTel data model rendered cleanly in the UI and in `search_traces`. That part is portable. A team can build the agent observability pattern on any Databricks workspace today and have the traces travel into Datadog, Grafana, or whatever else is on the stack.
# MAGIC
# MAGIC The SQL-queryable destination is the part that bifurcates. The gate is metastore root storage type, not workspace tier. On a workspace with an externally-stored UC metastore, gate 3 passed and the close-the-loop cell produced a queryable Delta row within seconds. On a workspace whose metastore root was default storage, gate 3 stopped with `Tables created in default storage are not supported`. Same notebook, same MLflow version. The variable was the storage architecture, nothing else.
# MAGIC
# MAGIC The probe is the artifact. Run it on the workspace in front of you and the gate output is the answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC **[MLflow Tracing Overview](https://mlflow.org/docs/latest/genai/tracing/).** The full picture on spans, traces, and the OTel data model. Start here to instrument a real agent.
# MAGIC
# MAGIC **[Store OpenTelemetry traces in Unity Catalog (Databricks docs)](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog).** The official enablement path: Previews, MLflow version, SQL warehouse, `trace_location=UnityCatalog(...)`.
# MAGIC
# MAGIC **[Query OpenTelemetry traces stored in Unity Catalog (Databricks docs)](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/observe-with-traces/query-dbsql).** SQL patterns once the UC path is enabled.
# MAGIC
# MAGIC **[MLflow Public Preview release note (April 30, 2026)](https://docs.databricks.com/aws/en/release-notes/product/2026/april#mlflow-trace-storage-in-unity-catalog-is-now-in-public-preview).** The announcement this notebook reacts to.
# MAGIC
# MAGIC **[Databricks Free Edition](https://www.databricks.com/learn/free-edition).** If you don't have a workspace yet, this notebook runs end-to-end on Free Edition. The probe at the end will fail gate 3 with the verbatim default-storage error, which is itself the architectural finding for that environment.
