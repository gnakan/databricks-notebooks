# Databricks notebook source
# MAGIC %md
# MAGIC In a recent Databricks blog post about the [Genie One MCP server](https://docs.databricks.com/aws/en/agents/mcp-tools/genie-mcp) going generally available:
# MAGIC
# MAGIC > "It gives any agent a single interface to retrieve structured and unstructured data, insights, and answers from Genie One, grounded in governed business context from Genie Ontology."
# MAGIC
# MAGIC The post pitches the Model Context Protocol (MCP) server as the way to stop every agent in a company from carrying its own private idea of what "revenue" means, and it names coding agents as one of the places it belongs. I use Claude Code regularly, and the docs page makes a claim I can test: that Genie's governed context produces more accurate answers than an agent writing SQL straight against the tables. So, I put together an experiment. The notebook builds a small Stark Industries sales schema whose business rules a schema reader can easily get wrong, then hands the same eight business questions to the same tool-calling model twice: once with plain SQL tools, once with the Genie One MCP server as its only tool. It grades both against ground truth computed in the notebook and records right answers, tool calls, tokens and seconds for each.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) in about 15 minutes, most of it waiting on Genie. The Genie One MCP server is generally available, and on the Free Edition workspace I used the `genie_one_mcp` workspace setting already read as on. The first code cell checks everything the comparison needs before it creates anything.
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **A SQL warehouse you can use.** Free Edition gives you exactly one, and both arms of the experiment run their SQL on it. [Chat in Genie One](https://docs.databricks.com/aws/en/genie-one/chat) needs `CAN USE` on at least one warehouse too.
# MAGIC - **A Foundation Model API chat model that supports [function calling](https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling).** The default is `databricks-qwen3-next-80b-a3b-instruct`. It's on the function-calling list, and it's the lightest instruct model on the Free Edition roster I'd trust with a multi-step SQL loop. `MODEL` in the config cell swaps it.
# MAGIC - **Access to the Genie One MCP Service.** The service lives at a [Unity Gateway](https://docs.databricks.com/aws/en/agents/mcp-tools/mcp-services) URL, `/ai-gateway/mcp-services/system.ai.genie_one_mcp`, and the notebook calls it as you, with the notebook's own session token. The docs say account users hold `EXECUTE` on `system.ai` by default, and that on-behalf-of OAuth needs the `ai-gateway` scope. If the service answers with JSON remote procedure call (JSON-RPC) error `-32007`, "Not authorized to invoke MCP service," the first cell reports it and stops.
# MAGIC - **Workspace admin, for one optional step.** Genie One reads [workspace instructions](https://docs.databricks.com/aws/en/genie-one/chat) from `/Workspace/.genie_workspace_instructions.md`. The notebook writes the Stark Industries rules there if no such file exists, and deletes it at the end. If your workspace already has one, the notebook leaves it alone and says so. On Free Edition you're the admin.
# MAGIC - **Libraries: none.** Everything goes through `requests`, which is preinstalled: the [Statement Execution API](https://docs.databricks.com/aws/en/dev-tools/sql-execution-tutorial) for SQL, the OpenAI-compatible [chat endpoint](https://docs.databricks.com/aws/en/machine-learning/model-serving/query-chat-models) for the model, and plain JSON-RPC over HTTP for MCP. No pip install, no restart.
# MAGIC - **Compute:** default serverless for the notebook, plus the SQL warehouse.
# MAGIC - **Identity:** your workspace user and the per-session token from notebook context. Nothing hardcoded.
# MAGIC
# MAGIC Two things to know going in. Genie One [remembers your past conversations](https://docs.databricks.com/aws/en/genie-one/chat) and can cite them, so a second run of this notebook isn't a clean room for the Genie arm. And the token counts recorded here are the calling agent's tokens only. Whatever Genie spends on its side isn't part of them.

# COMMAND ----------

import base64
import json
import random
import re
import time
import uuid
import datetime as dt

import matplotlib.pyplot as plt
import pandas as pd
import requests
from pyspark.sql.types import DateType, DoubleType, LongType, StringType, StructField, StructType

# Dynamic resolution: user, workspace URL, per-session token. Never hardcode these.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
CURRENT_USER    = ctx.userName().get()
WORKSPACE_URL   = ctx.apiUrl().get().rstrip("/")
WORKSPACE_TOKEN = ctx.apiToken().get()

# Where the Stark Industries tables land. `workspace` is the default writable
# catalog on Free Edition; change CATALOG on a shared workspace.
CATALOG = "workspace"
SCHEMA  = "stark_genie_mcp"
FQ      = f"{CATALOG}.{SCHEMA}"
METRIC_VIEW = f"{FQ}.revenue_metrics"

# The calling agent's model. Same model, same system prompt, same turn budget for both arms.
MODEL      = "databricks-qwen3-next-80b-a3b-instruct"
MAX_TURNS  = 16      # model calls per question, per arm
QUESTION_TIMEOUT_S = 240

# The Genie One MCP Service. Endpoint and credential live here and nowhere else,
# so swapping either is a one-line change.
MCP_URL = f"{WORKSPACE_URL}/ai-gateway/mcp-services/system.ai.genie_one_mcp"
MCP_TOKEN = WORKSPACE_TOKEN          # the notebook runs as whoever runs it
MCP_AUTH  = "notebook session token"
PASS_WAREHOUSE_META = True   # send warehouse_id in _meta on genie_ask, per the docs
POLL_SLEEP_S = 3             # pause before each genie_poll_response, so polls don't overlap

# Workspace instructions for Genie One chat. Written only when the file doesn't exist.
WRITE_WORKSPACE_INSTRUCTIONS = True
INSTRUCTIONS_API_PATH = "/.genie_workspace_instructions.md"   # /Workspace/.genie_workspace_instructions.md

AS_OF = dt.date(2026, 6, 30)   # the date "active customer" questions are asked as of
SEED  = 39                     # the synthetic data is deterministic for this seed
RUN_ID = uuid.uuid4().hex[:8]

HEADERS = {"Authorization": f"Bearer {WORKSPACE_TOKEN}"}

def api(method: str, path: str, **kwargs) -> tuple:
    """Call a workspace REST endpoint with the session token and return (status, json body)."""
    r = requests.request(method, f"{WORKSPACE_URL}{path}", headers=HEADERS, timeout=120, **kwargs)
    try:
        body = r.json() if r.content else {}
    except ValueError:
        body = {"raw": r.text[:500]}
    return r.status_code, body

print(f"User      : {CURRENT_USER}")
print(f"Workspace : {WORKSPACE_URL}")
print(f"Schema    : {FQ}")
print(f"Model     : {MODEL}")
print(f"MCP URL   : {MCP_URL}")
print(f"MCP auth  : {MCP_AUTH}")
print(f"Run ID    : {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check what this workspace can reach
# MAGIC
# MAGIC Four gates, one line each, before anything gets created. The first finds a SQL warehouse. The second sends the model one tiny request with one tool attached and checks that it answers with a tool call. The third reads the `genie_one_mcp` entry from the [workspace settings API](https://docs.databricks.com/aws/en/admin/workspace-settings/manage-previews). The fourth opens an MCP session with the Genie One service and calls `tools/list`, which is how the docs say to find a service's tools rather than hardcoding them.
# MAGIC
# MAGIC If any gate fails, the cell stops the notebook with the reason, and nothing has been created yet. There's deliberately no fallback to the older Beta endpoint at `/api/2.0/mcp/genie`: the docs say it's deprecated and sunsets on October 31, 2026.

# COMMAND ----------

class MCPError(Exception):
    """A JSON-RPC error returned by the MCP service."""

class MCPClient:
    """Minimal MCP client over streamable HTTP: initialize, tools/list, tools/call."""

    def __init__(self, url: str, token: str):
        """Store the service URL and bearer token; the session starts on initialize()."""
        self.url = url
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        self.session_id = None
        self._id = 0

    def _post(self, payload: dict) -> dict:
        """POST one JSON-RPC message and return the matching response, parsing JSON or SSE bodies."""
        headers = dict(self.headers)
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        r = requests.post(self.url, headers=headers, json=payload, timeout=180)
        self.session_id = r.headers.get("Mcp-Session-Id", self.session_id)
        if "id" not in payload:
            return {}
        text = r.text
        if r.headers.get("Content-Type", "").startswith("text/event-stream"):
            messages = [json.loads(line[5:].strip()) for line in text.splitlines()
                        if line.startswith("data:") and line[5:].strip()]
            matching = [m for m in messages if m.get("id") == payload["id"]]
            body = matching[-1] if matching else (messages[-1] if messages else {})
        else:
            try:
                body = json.loads(text) if text else {}
            except ValueError:
                raise MCPError(f"http {r.status_code}, non-JSON body: {text[:200]}")
        if "error" in body:
            err = body["error"]
            raise MCPError(f"http {r.status_code}, code {err.get('code')}: {err.get('message')}")
        if r.status_code >= 400:
            raise MCPError(f"http {r.status_code}: {text[:200]}")
        return body.get("result", {})

    def request(self, method: str, params: dict = None) -> dict:
        """Send a JSON-RPC request and return its result."""
        self._id += 1
        return self._post({"jsonrpc": "2.0", "id": self._id, "method": method, "params": params or {}})

    def initialize(self) -> dict:
        """Open the MCP session. No MCP Apps capability is declared, so text tools are offered."""
        result = self.request("initialize", {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "stark-genie-mcp-notebook", "version": RUN_ID},
        })
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return result

    def list_tools(self) -> list:
        """Return the service's tools with their input schemas."""
        return self.request("tools/list").get("tools", [])

    def call_tool(self, name: str, arguments: dict, meta: dict = None) -> dict:
        """Call one tool; `meta` goes in the request's _meta field."""
        params = {"name": name, "arguments": arguments}
        if meta:
            params["_meta"] = meta
        return self.request("tools/call", params)


def chat(messages: list, tools: list = None, max_tokens: int = 1024) -> dict:
    """Call the Foundation Model API chat endpoint (OpenAI-compatible), retrying on rate limits."""
    payload = {"messages": messages, "max_tokens": max_tokens, "temperature": 0}
    if tools:
        payload["tools"] = tools
    for attempt in range(5):
        code, body = api("POST", f"/serving-endpoints/{MODEL}/invocations", json=payload)
        if code == 200:
            return body
        if code in (429, 503):
            time.sleep(2 ** attempt * 3)
            continue
        raise RuntimeError(f"chat endpoint returned http {code}: {str(body)[:300]}")
    raise RuntimeError("chat endpoint kept returning 429/503 after 5 attempts")


gates_ok = True
fail_reasons = []
probe = {"warehouse_ok": False, "model_ok": False, "genie_one_mcp_setting": None,
         "mcp_ok": False, "mcp_tools": [], "mcp_auth": MCP_AUTH, "mcp_error": None}

# [gate 1] a SQL warehouse this user can run statements on.
code, body = api("GET", "/api/2.0/sql/warehouses")
warehouses = body.get("warehouses", []) if code == 200 else []
if warehouses:
    running = [w for w in warehouses if w.get("state") == "RUNNING"]
    WAREHOUSE = (running or warehouses)[0]
    WAREHOUSE_ID = WAREHOUSE["id"]
    probe["warehouse_ok"] = True
    print(f"[gate 1] SQL warehouse -> OK ({WAREHOUSE['name']}, {WAREHOUSE.get('cluster_size')}, {WAREHOUSE.get('state')})")
else:
    WAREHOUSE_ID = None
    gates_ok = False
    fail_reasons.append("no SQL warehouse visible to this user")
    print(f"[gate 1] SQL warehouse -> FAIL (http {code}, none visible). Ask for CAN USE on a warehouse.")

# [gate 2] the model answers and calls a tool when given one.
ping_tool = [{"type": "function", "function": {
    "name": "ping", "description": "Reply to a connectivity check.",
    "parameters": {"type": "object", "properties": {"note": {"type": "string"}}, "required": ["note"]}}}]
try:
    resp = chat([{"role": "user", "content": "Call the ping tool with note set to 'ok'."}], tools=ping_tool, max_tokens=64)
    called = resp["choices"][0]["message"].get("tool_calls") or []
    if called:
        probe["model_ok"] = True
        print(f"[gate 2] {MODEL} function calling -> OK (called {called[0]['function']['name']})")
    else:
        gates_ok = False
        fail_reasons.append(f"{MODEL} answered without calling the tool")
        print(f"[gate 2] {MODEL} function calling -> FAIL (answered without a tool call). Try another model in MODEL.")
except Exception as e:
    gates_ok = False
    fail_reasons.append(f"{MODEL} not reachable: {e}")
    print(f"[gate 2] {MODEL} function calling -> FAIL ({e})")

# [gate 3] the workspace setting behind the Genie One MCP server.
code, body = api("GET", "/api/2.1/settings/genie_one_mcp")
probe["genie_one_mcp_setting"] = body.get("effective_boolean_val", {}).get("value") if code == 200 else None
if probe["genie_one_mcp_setting"] is True:
    print("[gate 3] Workspace setting genie_one_mcp -> OK (on)")
else:
    gates_ok = False
    fail_reasons.append(f"genie_one_mcp setting reads {probe['genie_one_mcp_setting']} (http {code})")
    print(f"[gate 3] Workspace setting genie_one_mcp -> FAIL (http {code}, effective={probe['genie_one_mcp_setting']}). "
          "A workspace admin checks it under Settings > Previews.")

# [gate 4] the Genie One MCP Service answers initialize and tools/list with this notebook's credential.
try:
    mcp = MCPClient(MCP_URL, MCP_TOKEN)
    mcp.initialize()
    MCP_TOOLS = mcp.list_tools()
    probe["mcp_tools"] = sorted(t["name"] for t in MCP_TOOLS)
    probe["mcp_ok"] = "genie_ask" in probe["mcp_tools"]
    if probe["mcp_ok"]:
        print(f"[gate 4] Genie One MCP Service tools/list -> OK ({', '.join(probe['mcp_tools'])})")
    else:
        gates_ok = False
        fail_reasons.append(f"tools/list answered but genie_ask is missing: {probe['mcp_tools']}")
        print(f"[gate 4] Genie One MCP Service tools/list -> FAIL (no genie_ask in {probe['mcp_tools']})")
except Exception as e:
    MCP_TOOLS = []
    probe["mcp_error"] = str(e)
    gates_ok = False
    if "-32007" in str(e):
        reason = (f"-32007, the {MCP_AUTH} isn't authorized to invoke the MCP Service. Check that "
                  "system.ai.genie_one_mcp appears under Unity Gateway > MCPs in this workspace, and that "
                  "you hold EXECUTE on it plus USE CATALOG on system and USE SCHEMA on system.ai")
    else:
        reason = str(e)
    fail_reasons.append(reason)
    print(f"[gate 4] Genie One MCP Service tools/list -> FAIL ({reason})")

print()
if not gates_ok:
    raise RuntimeError("Stopping before anything is created. Closed gates: " + "; ".join(fail_reasons))
print("All four gates open, carry on.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build the Stark Industries sales schema
# MAGIC
# MAGIC Three tables: `customers`, `orders` and `returns`, generated from a fixed seed so every run gets the same rows. Five business rules are baked into the data, each one the kind of thing a schema reader gets wrong without being told:
# MAGIC
# MAGIC - **Net revenue** is order value minus refunds from `returns`, and only `completed` orders count.
# MAGIC - **The fiscal year runs February 1 to January 31** and is named for the calendar year it ends in, so fiscal year (FY) 2026 is Feb 1, 2025 through Jan 31, 2026.
# MAGIC - **Three internal test accounts** (`account_type = 'internal_test'`) place big orders and must be excluded from every metric.
# MAGIC - **An active customer** has at least one completed order in the 90 days ending on the as-of date.
# MAGIC - **A new customer** in a fiscal year is one whose first completed order falls in that fiscal year.
# MAGIC
# MAGIC The next cell writes the tables with Spark. The one after it writes those rules into [table and column comments](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment), which both arms can read.

# COMMAND ----------

def generate(seed: int) -> tuple:
    """Return (customers, orders, returns) rows for the Stark Industries schema, deterministic per seed."""
    rng = random.Random(seed)
    regions = ["West", "Central", "East", "International"]
    segments = ["Enterprise", "Mid-Market", "SMB"]
    customers = []
    for cid in range(1, 61):
        customers.append((cid, f"Stark Industries Account {cid:03d}", rng.choices(segments, [2, 3, 5])[0],
                          rng.choices(regions, [3, 2, 4, 1])[0], "standard" if cid % 9 else "partner"))
    for cid, name in ((901, "Stark QA Test Account"), (902, "Stark Internal Demo"), (903, "Stark Load Test")):
        customers.append((cid, name, "Mid-Market", "West", "internal_test"))
    start, days = dt.date(2025, 1, 1), 546   # Jan 1, 2025 through Jun 30, 2026
    orders, returns = [], []
    oid = 10000
    for cid, _, seg, _, typ in customers:
        n = rng.randint(25, 40) if typ == "internal_test" else rng.randint(2, 16)
        base = {"Enterprise": 4200, "Mid-Market": 1800, "SMB": 650}[seg] * (3 if typ == "internal_test" else 1)
        first = rng.randint(0, days - 30)
        for _ in range(n):
            oid += 1
            d = start + dt.timedelta(days=rng.randint(first, days - 1))
            amt = round(base * rng.uniform(0.4, 1.8), 2)
            status = "cancelled" if rng.random() < 0.12 else "completed"
            orders.append((oid, cid, d, amt, status))
            if status == "completed" and rng.random() < 0.14:
                returns.append((oid, d + dt.timedelta(days=rng.randint(3, 40)), round(amt * rng.uniform(0.2, 1.0), 2)))
    return customers, orders, returns


customers_rows, orders_rows, returns_rows = generate(SEED)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
tables = {
    "customers": (customers_rows, StructType([
        StructField("customer_id", LongType()), StructField("customer_name", StringType()),
        StructField("segment", StringType()), StructField("region", StringType()),
        StructField("account_type", StringType())])),
    "orders": (orders_rows, StructType([
        StructField("order_id", LongType()), StructField("customer_id", LongType()),
        StructField("order_date", DateType()), StructField("gross_amount", DoubleType()),
        StructField("status", StringType())])),
    "returns": (returns_rows, StructType([
        StructField("order_id", LongType()), StructField("return_date", DateType()),
        StructField("refund_amount", DoubleType())])),
}
for name, (rows, schema) in tables.items():
    spark.createDataFrame(rows, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.{name}")
    print(f"{FQ}.{name:<10} {len(rows):>4} rows")

# COMMAND ----------

TABLE_COMMENTS = {
    "customers": ("Stark Industries customer accounts. account_type = 'internal_test' marks QA, demo and load-test "
                  "accounts: exclude them from every business metric. An active customer has at least one completed "
                  "order in the 90 days ending on the as-of date (order_date > as_of_date - 90 days and "
                  "order_date <= as_of_date). A new customer in a fiscal year is one whose first completed order "
                  "falls in that fiscal year."),
    "orders": ("One row per Stark Industries order. Only status = 'completed' counts toward revenue. The fiscal year "
               "runs Feb 1 to Jan 31 and is named for the calendar year it ends in: FY2026 is Feb 1, 2025 to "
               "Jan 31, 2026. Fiscal Q1 is Feb-Apr, Q2 May-Jul, Q3 Aug-Oct, Q4 Nov-Jan. Net revenue = gross_amount "
               "minus returns.refund_amount, attributed to the order's fiscal period. Average order value = net "
               "revenue / number of completed orders."),
    "returns": ("Refunds against completed Stark Industries orders, at most one per order. Return rate = total "
                "refund_amount / total gross_amount of completed orders in the same fiscal period, as a percent."),
}
COLUMN_COMMENTS = {
    ("customers", "account_type"): "standard, partner or internal_test. internal_test accounts are excluded from all metrics.",
    ("customers", "region"): "Sales region: West, Central, East or International.",
    ("customers", "segment"): "Customer segment: Enterprise, Mid-Market or SMB.",
    ("orders", "gross_amount"): "Order value in USD before returns. Not revenue on its own: subtract refunds for net revenue.",
    ("orders", "status"): "completed or cancelled. Cancelled orders are never revenue.",
    ("orders", "order_date"): "Order date. Stark Industries reports on a Feb-Jan fiscal year, not the calendar year.",
    ("returns", "refund_amount"): "USD refunded on the order. Subtract from gross_amount for net revenue.",
}
def sql_str(text: str) -> str:
    """Quote text as a SQL string literal, escaping backslashes and single quotes."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"

for table, comment in TABLE_COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {FQ}.{table} IS {sql_str(comment)}")
for (table, column), comment in COLUMN_COMMENTS.items():
    spark.sql(f"ALTER TABLE {FQ}.{table} ALTER COLUMN {column} COMMENT {sql_str(comment)}")
print(f"Commented {len(TABLE_COMMENTS)} tables and {len(COLUMN_COMMENTS)} columns in {FQ}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Give Genie the same rules, in the places Genie reads
# MAGIC
# MAGIC The [Genie Ontology](https://docs.databricks.com/aws/en/genie/genie-ontology) page names two kinds of context. One is modeled: "Modeled context that you define, govern, and certify through features like metric views, domains, and Pages." The other is inferred: "A map of snippets that Genie automatically extracts and maintains from your existing assets and usage." A fresh schema has no usage to infer from, so the notebook supplies the modeled half.
# MAGIC
# MAGIC First, a [metric view](https://docs.databricks.com/aws/en/uc-semantics/metric-views/create), created in SQL the way the docs describe: "wrap the YAML definition in a CREATE VIEW statement with the WITH METRICS clause." It encodes net revenue, the fiscal calendar and the test-account exclusion once. It's a Unity Catalog object in the same schema, so the SQL arm can find it and read its definition too. That's on purpose: both arms see the same Unity Catalog metadata.
# MAGIC
# MAGIC Second, the workspace instructions file. The Genie One chat docs say to "create a Markdown file at the following path in your workspace," and that "Chat reads this file automatically with no additional configuration required." The MCP docs say "The server honors your Genie One configuration in Databricks." This file is the one piece of context only the Genie arm gets, because it's Genie One configuration rather than Unity Catalog metadata. It restates the same rules the comments carry.
# MAGIC
# MAGIC The SQL for the metric view runs on the warehouse through the Statement Execution API, which is also what the SQL arm's tools use.

# COMMAND ----------

def run_sql(statement: str, max_rows: int = 50, timeout_s: int = 120) -> dict:
    """Run one statement on the warehouse via the Statement Execution API; return columns, rows or an error."""
    code, body = api("POST", "/api/2.0/sql/statements", json={
        "warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "30s",
        "disposition": "INLINE", "format": "JSON_ARRAY", "row_limit": max_rows})
    if code != 200:
        return {"error": f"http {code}: {str(body)[:300]}"}
    deadline = time.time() + timeout_s
    while body.get("status", {}).get("state") in ("PENDING", "RUNNING") and time.time() < deadline:
        time.sleep(2)
        code, body = api("GET", f"/api/2.0/sql/statements/{body['statement_id']}")
    state = body.get("status", {}).get("state")
    if state != "SUCCEEDED":
        return {"error": f"{state}: {body.get('status', {}).get('error', {}).get('message', '')[:400]}"}
    cols = [c["name"] for c in body.get("manifest", {}).get("schema", {}).get("columns", [])]
    return {"columns": cols, "rows": body.get("result", {}).get("data_array", []) or []}


FISCAL_YEAR_SQL    = "YEAR(order_date) + CASE WHEN MONTH(order_date) >= 2 THEN 1 ELSE 0 END"
FISCAL_QUARTER_SQL = "CAST(FLOOR(((MONTH(order_date) + 10) % 12) / 3) AS INT) + 1"

metric_view_sql = f"""
CREATE OR REPLACE VIEW {METRIC_VIEW}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "Stark Industries revenue KPIs. Completed orders only, internal_test accounts excluded, Feb-Jan fiscal year."
source: {FQ}.orders
joins:
  - name: customer
    source: {FQ}.customers
    'on': source.customer_id = customer.customer_id
  - name: ret
    source: {FQ}.returns
    'on': source.order_id = ret.order_id
filter: source.status = 'completed' AND customer.account_type <> 'internal_test'
fields:
  - name: Order Date
    expr: source.order_date
  - name: Fiscal Year
    expr: {FISCAL_YEAR_SQL.replace('order_date', 'source.order_date')}
    comment: "Feb 1 to Jan 31, named for the calendar year it ends in. FY2026 = Feb 1, 2025 to Jan 31, 2026."
  - name: Fiscal Quarter
    expr: {FISCAL_QUARTER_SQL.replace('order_date', 'source.order_date')}
    comment: "Q1 Feb-Apr, Q2 May-Jul, Q3 Aug-Oct, Q4 Nov-Jan."
  - name: Region
    expr: customer.region
  - name: Segment
    expr: customer.segment
  - name: Customer ID
    expr: source.customer_id
measures:
  - name: Net Revenue
    expr: SUM(source.gross_amount) - SUM(COALESCE(ret.refund_amount, 0))
    comment: "Order value minus refunds, completed orders only, test accounts excluded."
    synonyms: ["revenue", "net sales"]
  - name: Gross Revenue
    expr: SUM(source.gross_amount)
  - name: Refunds
    expr: SUM(COALESCE(ret.refund_amount, 0))
  - name: Completed Orders
    expr: COUNT(1)
  - name: Average Order Value
    expr: (SUM(source.gross_amount) - SUM(COALESCE(ret.refund_amount, 0))) / COUNT(1)
    comment: "Net revenue divided by completed orders."
  - name: Return Rate Percent
    expr: 100 * SUM(COALESCE(ret.refund_amount, 0)) / SUM(source.gross_amount)
    comment: "Refunds as a percent of gross revenue."
  - name: Customers
    expr: COUNT(DISTINCT source.customer_id)
$$
"""
mv = run_sql(metric_view_sql)
metric_view_created = "error" not in mv
print(f"Metric view {METRIC_VIEW} -> {'created' if metric_view_created else 'FAILED: ' + mv['error']}")

INSTRUCTIONS = f"""# Stark Industries data conventions

Stark Industries sales data lives in `{FQ}` (customers, orders, returns).
For revenue questions use the metric view `{METRIC_VIEW}`.

- Revenue means net revenue: completed orders only, minus refunds from `returns`, attributed to the order's fiscal period.
- Cancelled orders are never revenue.
- Exclude customers with account_type = 'internal_test' from every metric.
- The fiscal year runs Feb 1 to Jan 31 and is named for the calendar year it ends in. FY2026 = Feb 1, 2025 to Jan 31, 2026. Q1 = Feb-Apr.
- An active customer has at least one completed order in the 90 days ending on the as-of date.
- A new customer in a fiscal year is one whose first completed order falls in that fiscal year.
- Average order value = net revenue / completed orders. Return rate = refunds / gross revenue of completed orders, as a percent.
"""
code, status = api("GET", "/api/2.0/workspace/get-status", params={"path": INSTRUCTIONS_API_PATH})
if code == 200:
    instructions_file = "existing"
    print(f"/Workspace{INSTRUCTIONS_API_PATH} already exists. Leaving it alone; the Genie arm runs with your instructions, not these.")
elif not WRITE_WORKSPACE_INSTRUCTIONS:
    instructions_file = "skipped"
    print("WRITE_WORKSPACE_INSTRUCTIONS is False; the Genie arm runs on the metric view and comments only.")
else:
    code, body = api("POST", "/api/2.0/workspace/import", json={
        "path": INSTRUCTIONS_API_PATH, "format": "AUTO", "overwrite": False,
        "content": base64.b64encode(INSTRUCTIONS.encode()).decode()})
    code2, status = api("GET", "/api/2.0/workspace/get-status", params={"path": INSTRUCTIONS_API_PATH})
    instructions_file = "written" if code == 200 and status.get("object_type") == "FILE" else f"failed (http {code}, {status.get('object_type')})"
    print(f"/Workspace{INSTRUCTIONS_API_PATH} -> {instructions_file}")

CONTEXT_READY_AT = time.time()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Compute the ground truth
# MAGIC
# MAGIC Eight questions, each answered here with explicit SQL that applies every rule. The same cell also computes the answer a reader would get by ignoring the rules (calendar year, gross amounts, every status, every account), just to confirm the traps change the answer. A trap that doesn't change anything isn't testing anything.
# MAGIC
# MAGIC The last line checks the metric view against the ground truth for the first question. If the metric view and the SQL disagree, the Genie arm is working from a bad definition and the comparison isn't fair.

# COMMAND ----------

GOOD_CTE = f"""
WITH good AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.gross_amount,
           COALESCE(r.refund_amount, 0) AS refund_amount,
           c.region, c.segment,
           {FISCAL_YEAR_SQL.replace('order_date', 'o.order_date')} AS fiscal_year,
           {FISCAL_QUARTER_SQL.replace('order_date', 'o.order_date')} AS fiscal_quarter
    FROM {FQ}.orders AS o
    JOIN {FQ}.customers AS c ON o.customer_id = c.customer_id
    LEFT JOIN {FQ}.returns AS r ON o.order_id = r.order_id
    WHERE o.status = 'completed' AND c.account_type <> 'internal_test'
)"""
NAIVE_CTE = f"""
WITH naive AS (
    SELECT o.order_id, o.customer_id, o.order_date, o.gross_amount, c.region, c.segment
    FROM {FQ}.orders AS o
    JOIN {FQ}.customers AS c ON o.customer_id = c.customer_id
)"""
ACTIVE = f"order_date > DATE_SUB(DATE'{AS_OF}', 90) AND order_date <= DATE'{AS_OF}'"

QUESTIONS = [
    {"id": "q1", "kind": "money", "text": "What was net revenue in fiscal year 2026?",
     "truth": f"{GOOD_CTE} SELECT ROUND(SUM(gross_amount - refund_amount), 2) FROM good WHERE fiscal_year = 2026",
     "naive": f"{NAIVE_CTE} SELECT ROUND(SUM(gross_amount), 2) FROM naive WHERE YEAR(order_date) = 2025"},
    {"id": "q2", "kind": "money", "text": "What was net revenue in the first quarter of fiscal year 2027?",
     "truth": f"{GOOD_CTE} SELECT ROUND(SUM(gross_amount - refund_amount), 2) FROM good WHERE fiscal_year = 2027 AND fiscal_quarter = 1",
     "naive": f"{NAIVE_CTE} SELECT ROUND(SUM(gross_amount), 2) FROM naive WHERE order_date BETWEEN DATE'2026-01-01' AND DATE'2026-03-31'"},
    {"id": "q3", "kind": "count", "text": f"How many active customers were there as of {AS_OF:%B %-d, %Y}?",
     "truth": f"{GOOD_CTE} SELECT COUNT(DISTINCT customer_id) FROM good WHERE {ACTIVE}",
     "naive": f"{NAIVE_CTE} SELECT COUNT(DISTINCT customer_id) FROM naive WHERE {ACTIVE}"},
    {"id": "q4", "kind": "name", "options": ["West", "Central", "East", "International"],
     "text": "Which region had the highest net revenue in fiscal year 2026? Answer with the region name.",
     "truth": f"{GOOD_CTE} SELECT region FROM good WHERE fiscal_year = 2026 GROUP BY region ORDER BY SUM(gross_amount - refund_amount) DESC LIMIT 1",
     "naive": f"{NAIVE_CTE} SELECT region FROM naive WHERE YEAR(order_date) = 2025 GROUP BY region ORDER BY SUM(gross_amount) DESC LIMIT 1"},
    {"id": "q5", "kind": "pct", "text": "What was the return rate in fiscal year 2026, as a percent?",
     "truth": f"{GOOD_CTE} SELECT ROUND(100 * SUM(refund_amount) / SUM(gross_amount), 2) FROM good WHERE fiscal_year = 2026",
     "naive": None},
    {"id": "q6", "kind": "count", "text": "How many new customers did Stark Industries gain in fiscal year 2026?",
     "truth": f"""{GOOD_CTE}, firsts AS (SELECT customer_id, MIN(order_date) AS first_order FROM good GROUP BY customer_id)
                 SELECT COUNT(customer_id) FROM firsts WHERE {FISCAL_YEAR_SQL.replace('order_date', 'first_order')} = 2026""",
     "naive": f"""{NAIVE_CTE}, firsts AS (SELECT customer_id, MIN(order_date) AS first_order FROM naive GROUP BY customer_id)
                 SELECT COUNT(customer_id) FROM firsts WHERE YEAR(first_order) = 2025"""},
    {"id": "q7", "kind": "money", "text": "What was the average order value in fiscal year 2026?",
     "truth": f"{GOOD_CTE} SELECT ROUND(SUM(gross_amount - refund_amount) / COUNT(order_id), 2) FROM good WHERE fiscal_year = 2026",
     "naive": f"{NAIVE_CTE} SELECT ROUND(AVG(gross_amount), 2) FROM naive WHERE YEAR(order_date) = 2025"},
    {"id": "q8", "kind": "name", "options": ["Enterprise", "Mid-Market", "SMB"],
     "text": f"Which customer segment had the most active customers as of {AS_OF:%B %-d, %Y}? Answer with the segment name.",
     "truth": f"{GOOD_CTE} SELECT segment FROM good WHERE {ACTIVE} GROUP BY segment ORDER BY COUNT(DISTINCT customer_id) DESC LIMIT 1",
     "naive": f"{NAIVE_CTE} SELECT segment FROM naive WHERE {ACTIVE} GROUP BY segment ORDER BY COUNT(DISTINCT customer_id) DESC LIMIT 1"},
]

for q in QUESTIONS:
    q["truth_value"] = spark.sql(q["truth"]).collect()[0][0]
    q["naive_value"] = spark.sql(q["naive"]).collect()[0][0] if q["naive"] else None

traps_that_change_answer = sum(1 for q in QUESTIONS if q["naive_value"] is not None and q["naive_value"] != q["truth_value"])
display(pd.DataFrame([{"Question": q["id"], "Asked": q["text"], "Ground truth": str(q["truth_value"]),
                       "Ignoring the rules": str(q["naive_value"])} for q in QUESTIONS]))

mv_check = run_sql(f"SELECT ROUND(MEASURE(`Net Revenue`), 2) FROM {METRIC_VIEW} WHERE `Fiscal Year` = 2026") if metric_view_created else {"error": "no metric view"}
mv_q1 = float(mv_check["rows"][0][0]) if "rows" in mv_check and mv_check["rows"] else None
metric_view_matches_truth = mv_q1 is not None and abs(mv_q1 - float(QUESTIONS[0]["truth_value"])) < 0.01
print(f"Traps that change the answer: {traps_that_change_answer} of {sum(1 for q in QUESTIONS if q['naive'])}")
print(f"Metric view FY2026 net revenue: {mv_q1}  ground truth: {QUESTIONS[0]['truth_value']}  match: {metric_view_matches_truth}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: One agent loop, two tool sets
# MAGIC
# MAGIC Both arms run through the same loop: the same model, the same system prompt, the same 16-call budget and the same instruction to finish on a `FINAL:` line so the grader can read the answer. The only thing that changes is the tools.
# MAGIC
# MAGIC **SQL arm.** Three tools: `list_tables` (names, types and table comments from [`information_schema`](https://docs.databricks.com/aws/en/sql/language-manual/information-schema/tables)), `describe_table` (`DESCRIBE TABLE EXTENDED`, which carries column comments and, for the metric view, its full YAML) and `run_sql`, which accepts read-only statements only. This is roughly what a coding agent with a SQL connection has.
# MAGIC
# MAGIC **Genie arm.** Whatever `tools/list` returned in Step 1, passed to the model with their own names, descriptions and input schemas. The docs list `genie_ask`, `genie_poll_response`, `genie_get_query_result`, `genie_cancel_response` and `view_ask`; the notebook drops the last two, since cancelling isn't part of answering and `view_ask` is "Offered instead of genie_ask to MCP Apps clients, and preferred when available." The docs are clear that "Because Genie runs an agent that searches data and executes SQL, answers are asynchronous," so the model has to poll, and the notebook counts those polls separately. It also passes the warehouse ID in `_meta` on `genie_ask`, as the docs describe.
# MAGIC
# MAGIC Grading is mechanical. Money answers count within 0.5% of the truth, counts must match exactly, the return rate counts within 0.1 points (as a percent or a fraction), and name answers count when the right name appears and no other option does.

# COMMAND ----------

SYSTEM_PROMPT = (
    "You are a data analyst agent answering business questions about Stark Industries. "
    "Use only the tools you are given to find the answer; don't guess. "
    "If a tool reports that work is still in progress, keep checking with the tools provided until you have a result. "
    "When you have the answer, end your reply with one line in the form 'FINAL: <answer>', where <answer> is a plain "
    "number with no currency symbol, commas or units, or a single name."
)

def question_prompt(q: dict) -> str:
    """The user message both arms receive for a question."""
    return f"Stark Industries sales data is in the Unity Catalog schema {FQ}. {q['text']}"

def as_text(content) -> str:
    """Extract text from a chat message's content, whether it's a string or a list of typed parts."""
    if isinstance(content, list):
        return "".join(p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text")
    return content or ""

def run_agent(q: dict, tools: list, execute) -> dict:
    """Run the tool-calling loop for one question and return the answer text plus counters."""
    messages = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": question_prompt(q)}]
    out = {"answer": "", "tool_calls": 0, "genie_polls": 0, "prompt_tokens": 0, "completion_tokens": 0,
           "turns": 0, "error": None, "tools_used": []}
    t0 = time.time()
    try:
        for _ in range(MAX_TURNS):
            if time.time() - t0 > QUESTION_TIMEOUT_S:
                out["error"] = "question timeout"
                break
            resp = chat(messages, tools=tools)
            out["turns"] += 1
            usage = resp.get("usage", {})
            out["prompt_tokens"] += usage.get("prompt_tokens", 0)
            out["completion_tokens"] += usage.get("completion_tokens", 0)
            msg = resp["choices"][0]["message"]
            calls = msg.get("tool_calls") or []
            if not calls:
                out["answer"] = as_text(msg.get("content"))
                break
            messages.append({"role": "assistant", "content": as_text(msg.get("content")), "tool_calls": calls})
            for call in calls:
                name = call["function"]["name"]
                try:
                    args = json.loads(call["function"].get("arguments") or "{}")
                except ValueError:
                    args = {}
                out["tool_calls"] += 1
                out["genie_polls"] += name == "genie_poll_response"
                out["tools_used"].append(name)
                try:
                    result = execute(name, args)
                except Exception as e:
                    result = f"ERROR: {type(e).__name__}: {e}"
                messages.append({"role": "tool", "tool_call_id": call["id"], "content": str(result)[:6000]})
        else:
            out["error"] = "turn budget exhausted"
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
    out["seconds"] = round(time.time() - t0, 1)
    out["tokens"] = out["prompt_tokens"] + out["completion_tokens"]
    return out


def to_number(s: str):
    """Parse the first number in s, honoring a trailing K/M/B or %; None if there isn't one."""
    m = re.search(r"-?\d[\d,]*\.?\d*\s*([kKmMbB%])?", s or "")
    if not m:
        return None
    value = float(m.group(0).rstrip("kKmMbB% ").replace(",", ""))
    return value * {"k": 1e3, "m": 1e6, "b": 1e9}.get((m.group(1) or "").lower(), 1)

def grade(q: dict, answer: str) -> tuple:
    """Return (final answer string, correct?) for an agent's reply."""
    finals = re.findall(r"FINAL:\s*(.+)", answer or "")
    final = finals[-1].strip() if finals else ""
    truth = q["truth_value"]
    if q["kind"] == "name":
        hits = [o for o in q["options"] if re.search(rf"\b{re.escape(o)}\b", final, re.IGNORECASE)]
        return final, hits == [truth]
    v = to_number(final)
    if v is None:
        return final, False
    truth = float(truth)
    if q["kind"] == "money":
        return final, abs(v - truth) <= 0.005 * abs(truth)
    if q["kind"] == "count":
        return final, round(v) == round(truth)
    return final, abs(v - truth) <= 0.1 or abs(v * 100 - truth) <= 0.1   # pct


# --- SQL arm tools -----------------------------------------------------------
SQL_TOOLS = [
    {"type": "function", "function": {"name": "list_tables",
        "description": "List the tables and views in a Unity Catalog schema with their type and comment.",
        "parameters": {"type": "object", "properties": {"schema": {"type": "string", "description": "catalog.schema"}}, "required": ["schema"]}}},
    {"type": "function", "function": {"name": "describe_table",
        "description": "Describe a table or view: columns, types, comments and table properties.",
        "parameters": {"type": "object", "properties": {"table": {"type": "string", "description": "catalog.schema.table"}}, "required": ["table"]}}},
    {"type": "function", "function": {"name": "run_sql",
        "description": "Run a read-only SQL query (SELECT, WITH, SHOW or DESCRIBE) on a Databricks SQL warehouse and return up to 50 rows.",
        "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}},
]

def execute_sql_tool(name: str, args: dict) -> str:
    """Run one SQL-arm tool and return its result as text."""
    if name == "list_tables":
        cat, _, sch = (args.get("schema") or FQ).partition(".")
        res = run_sql(f"SELECT t.table_name, t.table_type, t.comment FROM {cat}.information_schema.tables AS t "
                      f"WHERE t.table_schema = '{sch}' ORDER BY t.table_name")
    elif name == "describe_table":
        res = run_sql(f"DESCRIBE TABLE EXTENDED {args.get('table', '')}", max_rows=200)
    elif name == "run_sql":
        query = (args.get("query") or "").strip()
        if not re.match(r"(?is)^\s*(SELECT|WITH|SHOW|DESCRIBE)\b", query):
            return "ERROR: only read-only statements are allowed"
        res = run_sql(query)
    else:
        return f"ERROR: unknown tool {name}"
    return json.dumps(res, default=str)


# --- Genie arm tools ---------------------------------------------------------
GENIE_TOOL_NAMES = [t["name"] for t in MCP_TOOLS if t["name"] not in ("view_ask", "genie_cancel_response")]
GENIE_TOOLS = [{"type": "function", "function": {
    "name": t["name"], "description": t.get("description", "")[:1000],
    "parameters": {k: v for k, v in (t.get("inputSchema") or {"type": "object", "properties": {}}).items() if k != "$schema"}}}
    for t in MCP_TOOLS if t["name"] in GENIE_TOOL_NAMES]

def execute_genie_tool(name: str, args: dict) -> str:
    """Call one Genie One MCP tool and return its text (plus structured content when present)."""
    if name == "genie_poll_response":
        time.sleep(POLL_SLEEP_S)
    meta = {"warehouse_id": WAREHOUSE_ID} if (PASS_WAREHOUSE_META and name == "genie_ask") else None
    try:
        result = mcp.call_tool(name, args, meta=meta)
    except MCPError as e:
        if "session" not in str(e).lower():
            raise
        mcp.initialize()   # sessions can expire over a long run; reopen once and retry
        result = mcp.call_tool(name, args, meta=meta)
    parts = [c.get("text", "") for c in result.get("content", []) if c.get("type") == "text"]
    if result.get("structuredContent"):
        parts.append(json.dumps(result["structuredContent"], default=str))
    return ("ERROR: " if result.get("isError") else "") + "\n".join(parts)

print(f"SQL arm tools   : {[t['function']['name'] for t in SQL_TOOLS]}")
print(f"Genie arm tools : {GENIE_TOOL_NAMES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Run the SQL arm
# MAGIC
# MAGIC Eight questions, one at a time. Each line prints the agent's `FINAL:` answer next to the truth, with its tool calls, tokens and seconds.

# COMMAND ----------

def run_arm(label: str, tools: list, execute) -> list:
    """Run every question through one arm and print a line per question."""
    rows = []
    for q in QUESTIONS:
        r = run_agent(q, tools, execute)
        final, correct = grade(q, r["answer"])
        r.update({"question": q["id"], "final": final, "correct": correct, "truth": q["truth_value"]})
        rows.append(r)
        print(f"{label} {q['id']}: {'RIGHT' if correct else 'WRONG'}  final={final[:40]!r:<42} truth={q['truth_value']!s:<12} "
              f"calls={r['tool_calls']:<3} tokens={r['tokens']:<6} {r['seconds']}s" + (f"  [{r['error']}]" if r["error"] else ""))
    return rows

sql_rows = run_arm("sql  ", SQL_TOOLS, execute_sql_tool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Run the Genie arm
# MAGIC
# MAGIC Same eight questions, same model, now with the Genie One MCP tools and nothing else. Each question starts a new Genie conversation. The per-question timeout is four minutes, and a question that runs out of time or turns is graded wrong, same as the SQL arm.

# COMMAND ----------

genie_seconds_after_context = round(time.time() - CONTEXT_READY_AT)
genie_rows = run_arm("genie", GENIE_TOOLS, execute_genie_tool)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Side by side
# MAGIC
# MAGIC How to read this: one row per question, the ground truth, each arm's `FINAL:` answer and whether it counted, then the work each arm did to get there. The chart below it puts right answers, average seconds per question and average agent tokens per question next to each other.

# COMMAND ----------

compare = pd.DataFrame([{
    "Question": q["id"], "Truth": str(q["truth_value"]),
    "SQL answer": str(s["final"]), "SQL right": s["correct"], "SQL calls": s["tool_calls"], "SQL tokens": s["tokens"], "SQL seconds": s["seconds"],
    "Genie answer": str(g["final"]), "Genie right": g["correct"], "Genie calls": g["tool_calls"], "Genie polls": g["genie_polls"],
    "Genie tokens": g["tokens"], "Genie seconds": g["seconds"],
} for q, s, g in zip(QUESTIONS, sql_rows, genie_rows)])
display(compare)

def summarize(rows: list) -> dict:
    """Aggregate one arm's per-question rows."""
    n = len(rows)
    return {
        "questions": n,
        "correct": sum(r["correct"] for r in rows),
        "tool_calls": sum(r["tool_calls"] for r in rows),
        "tool_calls_per_question": round(sum(r["tool_calls"] for r in rows) / n, 2),
        "genie_polls": sum(r["genie_polls"] for r in rows),
        "prompt_tokens": sum(r["prompt_tokens"] for r in rows),
        "completion_tokens": sum(r["completion_tokens"] for r in rows),
        "tokens_total": sum(r["tokens"] for r in rows),
        "tokens_per_question": round(sum(r["tokens"] for r in rows) / n),
        "seconds_total": round(sum(r["seconds"] for r in rows), 1),
        "seconds_per_question": round(sum(r["seconds"] for r in rows) / n, 1),
        "errors": sum(1 for r in rows if r["error"]),
    }

arms = {"raw_sql": summarize(sql_rows), "genie_mcp": summarize(genie_rows)}
display(pd.DataFrame(arms).T.astype(str))

# COMMAND ----------

fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))
labels = ["SQL tools", "Genie One MCP"]
colors = ["#6b7280", "#c8102e"]
for ax, key, title in zip(axes, ["correct", "seconds_per_question", "tokens_per_question"],
                          ["Right answers (of 8)", "Seconds per question", "Agent tokens per question"]):
    values = [arms["raw_sql"][key], arms["genie_mcp"][key]]
    bars = ax.bar(labels, values, color=colors)
    ax.bar_label(bars, labels=[f"{v:,}" for v in values])
    ax.set_title(title)
    ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

results = {
    "run_id": RUN_ID,
    "model": MODEL,
    "probe": probe,
    "setup": {
        "metric_view_created": metric_view_created,
        "metric_view_matches_truth": metric_view_matches_truth,
        "instructions_file": instructions_file,
        "rows": {"customers": len(customers_rows), "orders": len(orders_rows), "returns": len(returns_rows)},
        "genie_seconds_after_context": genie_seconds_after_context,
    },
    "dataset": {"seed": SEED, "as_of": str(AS_OF), "traps_that_change_answer": traps_that_change_answer},
    "arms": arms,
    "per_question": [{
        "id": q["id"], "truth": q["truth_value"], "naive": q["naive_value"],
        "raw_sql": {k: s[k] for k in ("final", "correct", "tool_calls", "tokens", "seconds", "error")},
        "genie_mcp": {k: g[k] for k in ("final", "correct", "tool_calls", "genie_polls", "tokens", "seconds", "error")},
    } for q, s, g in zip(QUESTIONS, sql_rows, genie_rows)],
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC On my Free Edition run, the same model got **8 of 8** questions right with the Genie One MCP server as its only tool, and **4 of 8** with plain SQL tools. Neither arm hit an error or ran out of turns.
# MAGIC
# MAGIC The four the SQL arm missed were the four money and percentage questions. It answered `0` for fiscal year 2026 net revenue (the truth was 506,429.54), 338,855.73 for fiscal Q1 2027 (truth 233,228.67), a 56.44% return rate (truth 8.15%) and a 2,256.73 average order value (truth 1,947.81). It got every count and name question right. The table and column comments spelled out every rule it broke, and the metric view sat in the same schema. The rules were there to read. It just didn't apply them.
# MAGIC
# MAGIC A fair note on what each side had: the Genie arm also got the workspace instructions file, which restates the same rules. The metric view matched the ground truth exactly, and all 7 traps changed the answer, so the rules really did decide right from wrong.
# MAGIC
# MAGIC The trade is time. The Genie arm averaged 54.2 seconds per question against 14.2 for the SQL arm, because Genie runs its own agent and the caller polls for the answer (55 polls across the eight questions).
# MAGIC
# MAGIC Read the token numbers with care. The calling agent used 46,874 tokens per question in the Genie arm against 16,038 in the SQL arm, but that's an upper bound set by this notebook's loop: every poll is a full model turn that re-sends the conversation, which is why 369,053 of the Genie arm's 374,990 tokens were prompt tokens. Genie's own tokens aren't counted at all. So it's a measure of this polling loop, not of what Genie uses.
# MAGIC
# MAGIC What that means for a coding agent: put the business rules somewhere the server reads, like a metric view, and you'll get right answers you'd otherwise have to check by hand. Expect each answer to take closer to a minute than a few seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Point your own coding agent at the same service
# MAGIC
# MAGIC The notebook drives the MCP server from Python so both arms can be measured the same way. Day to day, the more useful setup is your coding agent talking to it directly. The [Connect MCPs to AI assistants and coding agents](https://docs.databricks.com/aws/en/agents/mcp-tools/connect-clients) page says "The fastest way to connect Claude Code is with the Unity Gateway CLI." One thing to know first: `ug claude` runs Claude through Databricks, so it needs a workspace with Claude models on it. Free Edition's model roster carries OpenAI's gpt-oss models rather than Anthropic's, which is why this notebook uses an OpenAI-compatible model and its own small MCP client. On a workspace with Claude models:
# MAGIC
# MAGIC ```bash
# MAGIC uv tool install git+https://github.com/databricks/unity-gateway
# MAGIC ug mcp add --agents claude --names system.ai.genie_one_mcp
# MAGIC ug claude
# MAGIC ```
# MAGIC
# MAGIC The same page covers other coding agents, plus a manual route that registers an OAuth application from the account console. Once it's connected, set `RUN_CLEANUP = False` below, run the notebook, and paste the eight questions from Step 4 into Claude Code, each prefixed with the schema name the same way `question_prompt` does. The `Ground truth` column is your answer key.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC `RUN_CLEANUP` is on by default, because the workspace instructions file applies to every Genie One chat in the workspace, not just this notebook. The cell drops the schema (tables and metric view) and deletes the instructions file, but only if this run wrote it. The notebook leaves the Genie conversations the run started alone.

# COMMAND ----------

RUN_CLEANUP = True

if RUN_CLEANUP:
    spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
    print(f"Dropped {FQ} (tables and metric view)")
    if instructions_file == "written":
        code, _ = api("POST", "/api/2.0/workspace/delete", json={"path": INSTRUCTIONS_API_PATH})
        print(f"Deleted /Workspace{INSTRUCTIONS_API_PATH} -> http {code}")
    else:
        print(f"Instructions file was {instructions_file}; nothing to delete.")
else:
    print(f"Skipped. Set RUN_CLEANUP = True to drop {FQ} and remove the instructions file this run wrote.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Where to go next
# MAGIC
# MAGIC - [Genie One MCP server](https://docs.databricks.com/aws/en/agents/mcp-tools/genie-mcp): the service URL, the five tools, the asynchronous ask-and-poll flow and the `warehouse_id` `_meta` parameter.
# MAGIC - [Connect MCPs to AI assistants and coding agents](https://docs.databricks.com/aws/en/agents/mcp-tools/connect-clients): per-client setup for Claude Code, Cursor and others, including the Unity Gateway CLI.
# MAGIC - [Genie Ontology](https://docs.databricks.com/aws/en/genie/genie-ontology): what counts as modeled versus inferred context, and how Genie ranks it.
# MAGIC - [Unity Catalog metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views/): defining a metric once so every tool, agent included, computes it the same way.
# MAGIC - [Chat in Genie One](https://docs.databricks.com/aws/en/genie-one/chat): workspace instructions, memory and the rest of the configuration the MCP server honors.
