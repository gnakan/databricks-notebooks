# Databricks notebook source

# MAGIC %md
# MAGIC # The Agent Bricks CLI on Free Edition: What a Deploy Leaves Behind
# MAGIC
# MAGIC In a recent Databricks release note about the [Agent Bricks CLI](https://docs.databricks.com/aws/en/agents/custom-agents/agent-bricks-cli):
# MAGIC
# MAGIC > "Scaffold a project from a bundled framework template, run it locally, and deploy it to the Databricks agent runtime, with managed memory, sessions, tools, and MLflow tracing wired up from one authenticated command."
# MAGIC
# MAGIC The docs page walks that path from a terminal with an OAuth profile, and promises you can "go from an empty directory to a deployed agent" without wiring things up by hand. Found that line interesting, so I put together an experiment.
# MAGIC
# MAGIC I ran the CLI the way the docs lay it out, from a terminal against a Free Edition workspace, and logged every command. This notebook reads that log, then looks at the workspace from the inside: the app the deploy created, anything it added alongside it, the traces, the rows the agent's model calls left in the gateway's usage table, and what's still there after the CLI's own teardown command.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Two parts: the CLI runs in your terminal, and this notebook runs on Databricks Free Edition afterwards. The notebook takes about 25 minutes, most of it waiting for the agent's calls to reach the usage table.
# MAGIC
# MAGIC **Part 1, your terminal.** The docs list two prerequisites: the [Databricks CLI](https://docs.databricks.com/aws/en/dev-tools/cli/install) on your path, and Python 3.10 or above with pip. Then, in the order the docs give them:
# MAGIC
# MAGIC ```
# MAGIC pip install databricks-agentbricks
# MAGIC databricks auth login --host https://<your-workspace-url> --profile free
# MAGIC agentbricks init --framework langgraph --profile free my-agent
# MAGIC cd my-agent
# MAGIC # docs step 5: set MODEL in agent/agent.py to a model your workspace serves
# MAGIC agentbricks dev
# MAGIC agentbricks deploy my-agent
# MAGIC ```
# MAGIC
# MAGIC If `agentbricks dev` or `agentbricks deploy` stops, the error names what it needs. If the deploy stops on the memory or session store, `agentbricks memory unbind` and `agentbricks sessions unbind` take them out of `agent.toml`, and a second `agentbricks deploy` goes again without them. Talk to the agent at the URL the deploy prints, then run `agentbricks deployments delete agent-bricks-my-agent --yes`.
# MAGIC
# MAGIC Each deploy also gets a [Lakebase](https://docs.databricks.com/aws/en/oltp/) project named `agent-bricks-<name>-runtime-store`, and part 2 checks whether it's still there after `deployments delete`. To find any of those projects whose app is gone, from the same terminal:
# MAGIC
# MAGIC ```
# MAGIC databricks apps list -o json > /tmp/apps.json
# MAGIC databricks postgres list-projects -o json | python3 -c "
# MAGIC import json, re, sys
# MAGIC apps = {a['name'] for a in json.load(open('/tmp/apps.json'))}
# MAGIC for p in json.load(sys.stdin):
# MAGIC     m = re.fullmatch(r'projects/(agent-bricks-.+)-runtime-store', p['name'])
# MAGIC     if m and m.group(1) not in apps:
# MAGIC         print('databricks postgres delete-project', p['name'])"
# MAGIC ```
# MAGIC
# MAGIC It prints one delete command per orphaned project and deletes nothing itself. The notebook's last section does the same sweep, plus the traces experiments and deploy folders.
# MAGIC
# MAGIC **Part 2, this notebook.** Set the `app_name` widget to the app the deploy created (`agent-bricks-<name>`). If you logged your terminal run in the same JSON shape as mine, put its workspace path in `cli_log`; leave it empty and the notebook measures the workspace side only.
# MAGIC
# MAGIC - **Free Edition:** serverless notebook compute, the default. The agent itself runs on [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/).
# MAGIC - **Paid workspace:** the same notebook runs unchanged.
# MAGIC - **Libraries:** no pip installs; the [Databricks SDK for Python](https://docs.databricks.com/aws/en/dev-tools/sdk-python) and Spark are already there.
# MAGIC - **Identity:** your workspace user. Reading `system.ai_gateway.usage` needs admin rights; on Free Edition you're the admin of your own workspace.
# MAGIC - **Cleanup:** the last code cell deletes whatever the deploy left behind that the teardown didn't remove. Set `CLEANUP = False` to keep it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC `USAGE_WAIT_MINUTES` caps how long the notebook waits for the agent's rows in the usage table, and `AGENT_ROWS_WANTED` ends the wait early once that many have landed.

# COMMAND ----------

import json
import time
from datetime import datetime, timedelta, timezone

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("app_name", "")
dbutils.widgets.text("cli_log", "")

USAGE_WAIT_MINUTES = 25              # how long to wait for the agent's rows in system.ai_gateway.usage
AGENT_ROWS_WANTED = 2                # stop waiting once this many agent rows have landed
CLEANUP = True                       # set False to keep what the deploy left behind

w = WorkspaceClient()
USER = w.current_user.me().user_name
CLI_LOG_PATH = dbutils.widgets.get("cli_log").strip()
cli = json.load(open(CLI_LOG_PATH)) if CLI_LOG_PATH else {}
APP = dbutils.widgets.get("app_name").strip() or cli.get("app_name")
assert APP, "Set the app_name widget to the app your deploy created."
NAME = APP.removeprefix("agent-bricks-")

results = {"app_name": APP, "cli_log_loaded": bool(cli)}
print(f"User: {USER}")
print(f"App: {APP}")
print(f"CLI log: {CLI_LOG_PATH or 'none'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before part 1: how much room the workspace has
# MAGIC
# MAGIC A deploy adds one app and one Lakebase project, and a workspace can hold only so many of each; when it's full, the deploy's error says so. Run this cell before part 1 to see what's already there, including Agent Bricks leftovers the sweep at the end can clear. It reads; it changes nothing.

# COMMAND ----------

import re

def lakebase_project_names():
    """Every Lakebase project name in the workspace, following pagination."""
    names, token = [], None
    while True:
        page = w.api_client.do("GET", "/api/2.0/postgres/projects", query={"page_token": token} if token else None)
        names += [p["name"] for p in page.get("projects", [])]
        token = page.get("next_page_token")
        if not token:
            return sorted(names)

room_apps = sorted(a.name for a in w.apps.list())
room_projects = lakebase_project_names()
print(f"Apps: {len(room_apps)}  {room_apps}")
print(f"Lakebase projects: {len(room_projects)}  {room_projects}")
print(f"Agent Bricks runtime stores: {[n for n in room_projects if re.fullmatch(r'projects/agent-bricks-.+-runtime-store', n)]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 as it ran: the CLI log
# MAGIC
# MAGIC One row per CLI command from my terminal run, in order, with its exit code, how long it took and the first error line it printed. The docs say: "The template sets a default model as the MODEL value in agent/agent.py. To use a different model, edit that value." The log records the template's default and whether the [gateway](https://docs.databricks.com/aws/en/ai-gateway/query-model-services) on this workspace serves it.
# MAGIC
# MAGIC On `agentbricks dev`, the docs say it "runs the agent locally against Databricks model serving so you can test it before deploying." On `agentbricks deploy`: "The CLI provisions the bound stores, grants the agent's service principal access to them, and rolls out the deployment." The bound stores are [managed agent memory](https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory) and [managed agent sessions](https://docs.databricks.com/aws/en/agents/agent-memory/managed-sessions), and for both the docs say "Creating a store provisions the backing Lakebase storage automatically."

# COMMAND ----------

if cli:
    steps = cli.get("steps", [])
    print("Every CLI command, in order")
    display(spark.createDataFrame([{k: (str(v) if v is not None else None) for k, v in s.items()} for s in steps]))

    by = {s["step"]: s for s in steps}
    toml = cli.get("init_agent_toml", "")
    scaffolded = by.get("deploy (as scaffolded)", {})
    unbound = by.get("deploy (stores unbound)")
    chat = cli.get("chat") or {}
    deploy_steps = [s for s in steps if s["step"].startswith("deploy (")]
    results["cli"] = {
        "agentbricks_version": cli.get("agentbricks_version"),
        "init_bound_memory": "[memory_store]" in toml,
        "init_bound_sessions": "[session_store]" in toml,
        "init_bound_tracing": "agentbricks_traces" in toml,
        "model": cli.get("model"),
        "dev": cli.get("dev"),
        "deploy_scaffolded": {k: scaffolded.get(k) for k in ("exit_code", "seconds", "first_error")},
        "unbind_exit_codes": [by[k]["exit_code"] for k in ("memory unbind", "sessions unbind") if k in by],
        "deploy_unbound": {k: unbound.get(k) for k in ("exit_code", "seconds", "first_error")} if unbound else None,
        "deployed": cli.get("deployed"),
        "app_url_returned": bool(cli.get("app_url")),
        "commands_to_live_url": sum(1 for s in steps if s["step"] not in ("agentbricks --version", "deployments delete")) if cli.get("deployed") else None,
        "agent_py_edits_to_live_url": int(bool((cli.get("model") or {}).get("edited_agent_py"))),
        "deploy_minutes_total": round(sum(s["seconds"] for s in deploy_steps) / 60, 1),
        "lakebase_projects_added_by_deploy": cli.get("lakebase_projects_added"),
        "chat_tool_called": (chat.get("tool_turn") or {}).get("tool_called"),
        "chat_tool_turn_seconds": (chat.get("tool_turn") or {}).get("seconds"),
        "chat_tool_output": (chat.get("tool_turn") or {}).get("tool_output"),
        "chat_answer": (chat.get("tool_turn") or {}).get("answer"),
        "chat_follow_up_answer": (chat.get("follow_up") or {}).get("answer"),
        "chat_attempts_until_answer": chat.get("attempts_until_answer"),
        "teardown": cli.get("teardown"),
    }
    print(json.dumps({k: results["cli"][k] for k in ("model", "dev", "deploy_scaffolded", "deploy_unbound", "commands_to_live_url")}, indent=1, default=str))
else:
    results["cli"] = None
    print("No CLI log; skipping to the workspace checks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: what's in the workspace now
# MAGIC
# MAGIC Per the docs, "The deployed agent is named agent-bricks-<name>," and `agentbricks init` "binds a default /Shared/agentbricks_traces/<project> MLflow experiment" for [MLflow Tracing](https://docs.databricks.com/aws/en/mlflow/mlflow-tracing). The cell checks four places for anything named after this agent: the apps list, the [Lakebase](https://docs.databricks.com/aws/en/oltp/) projects list, the traces experiments, and the deploy's source folder in your workspace. It also counts the traces in the experiment, since "Tracing is on by default."

# COMMAND ----------

app_listed = any(a.name == APP for a in w.apps.list())

lakebase = lakebase_project_names()
agent_projects = [n for n in lakebase if NAME in n]

experiments = [e for e in w.experiments.list_experiments() if e.name and e.name.startswith(f"/Shared/agentbricks_traces/{NAME}")]
trace_count = None
if experiments:
    try:
        r = w.api_client.do("GET", "/api/2.0/mlflow/traces", query={"experiment_ids": experiments[0].experiment_id, "max_results": 100})
        trace_count = len(r.get("traces", []))
    except Exception as e:
        print(f"Trace count unavailable: {e}")

deploy_folder = f"/Users/{USER}/agentbricks_deployments/{APP}"
try:
    w.workspace.get_status(deploy_folder)
    folder_present = True
except Exception:
    folder_present = False

footprint = [
    {"what": "app", "name": APP, "still_there": app_listed},
    *[{"what": "Lakebase project", "name": n, "still_there": True} for n in agent_projects],
    *[{"what": "traces experiment", "name": e.name, "still_there": True} for e in experiments],
    {"what": "deploy source folder", "name": deploy_folder.replace(f"/Users/{USER}", "~"), "still_there": folder_present},
]
print("What the agent left in the workspace")
display(spark.createDataFrame([{k: str(v) for k, v in f.items()} for f in footprint]))

results["footprint"] = {
    "app_still_listed": app_listed,
    "lakebase_projects": agent_projects,
    "lakebase_projects_in_workspace": len(lakebase),
    "traces_experiments": [e.name for e in experiments],
    "trace_count": trace_count,
    "deploy_folder_present": folder_present,
    "apps_in_workspace": sum(1 for _ in w.apps.list()),
}
print(json.dumps(results["footprint"], indent=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: what the gateway recorded
# MAGIC
# MAGIC [Usage tracking](https://docs.databricks.com/aws/en/ai-gateway/usage-tracking) logs gateway requests to the `system.ai_gateway.usage` [system table](https://docs.databricks.com/aws/en/admin/system-tables/). Two fields in it describe where a call came from. For `invocation_metadata.source`, the docs say "Values include AI_PLAYGROUND, EXTERNAL_CLIENT, AI_QUERY, GUARDRAIL, and MANAGED_AGENT." For `session_metadata.client_session_id`, they say "Use it to group requests made during the same conversation or coding-agent session."
# MAGIC
# MAGIC The agent calls the gateway as the app's service principal, so its rows carry that principal as `requester`. The cell finds the principal (from the log, or from the app if it still exists), then checks the table once a minute for up to `USAGE_WAIT_MINUTES`, stopping early once `AGENT_ROWS_WANTED` rows have landed. It records how long they took and what each field says.

# COMMAND ----------

sp = cli.get("app_service_principal_client_id")
if not sp and app_listed:
    sp = w.apps.get(APP).service_principal_client_id
assert sp, "No service principal: the app is gone and there's no CLI log naming it."
since = cli.get("started_at") or (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
since_ts = datetime.fromisoformat(since).astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

usage_sql = f"""
SELECT
  u.event_time,
  u.requester_type,
  u.destination_model,
  u.invocation_metadata.source AS source,
  u.session_metadata.client_session_id AS client_session_id,
  u.user_agent,
  u.input_tokens,
  u.output_tokens,
  u.status_code
FROM system.ai_gateway.usage AS u
WHERE u.requester = '{sp}'
  AND u.event_time >= TIMESTAMP '{since_ts}'
"""

waited = 0
rows = spark.sql(usage_sql).collect()
while len(rows) < AGENT_ROWS_WANTED and waited < USAGE_WAIT_MINUTES:
    time.sleep(60)
    waited += 1
    rows = spark.sql(usage_sql).collect()

print("What the gateway recorded for the deployed agent")
display(spark.sql(usage_sql + " ORDER BY u.event_time"))

results["usage"] = {
    "minutes_waited": waited,
    "agent_rows": len(rows),
    "agent_sources": sorted({r.source for r in rows if r.source}),
    "agent_requester_types": sorted({r.requester_type for r in rows if r.requester_type}),
    "agent_client_session_id_rows": sum(1 for r in rows if r.client_session_id),
    "agent_user_agents": sorted({(r.user_agent or "")[:60] for r in rows}),
    "agent_models": sorted({r.destination_model for r in rows if r.destination_model}),
    "agent_tokens_total": sum((r.input_tokens or 0) + (r.output_tokens or 0) for r in rows),
}
print(json.dumps(results["usage"], indent=1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC
# MAGIC One line holding every number the findings use.

# COMMAND ----------

print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC The CLI got me from an empty folder to a working agent on Free Edition in five commands and one file edit, with 3.2 minutes spent inside `agentbricks deploy`. The deployed agent called its tool on the first question and, with no session store bound, still remembered that question when I asked about it next.
# MAGIC
# MAGIC The docs' step 5 note on the model mattered here: the template's default, `system.ai.claude-sonnet-4-5`, came back 404 from the gateway, so I set `MODEL` to `system.ai.gpt-oss-120b`, as that step describes. Two things needed a workaround the docs don't mention. The deploy as scaffolded stopped after 3.6 seconds with `error[BAD_REQUEST]: Scale-to-zero must be enabled for this workspace tier.`, which is the memory and session stores; unbinding both let the next deploy go live in 188.2 seconds. And `agentbricks deployments delete` removed the app but left the rest: the Lakebase project `agent-bricks-<name>-runtime-store` that the deploy created, the traces experiment with its 2 traces, and the deploy's source folder. On a workspace that caps Lakebase projects, a leftover store is what stops the next deploy, so the sweep below is worth running after you're done.
# MAGIC
# MAGIC The gateway's usage table logged 3 rows for the agent, all from the app's service principal and all with `invocation_metadata.source` set to `DATABRICKS_APPS`. None carried a `client_session_id`, so the table shows an app calling a model, not one agent conversation. The notebook waited 22 minutes for those rows.
# MAGIC
# MAGIC Where the results point: a deploy error that names the store it couldn't create, and a delete that takes the Runtime Store with it would each take a step out of the path above. The usage table carrying the agent's conversation ID would make it the place to answer what one agent did.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Deletes whatever part 2 found still in the workspace: the app if it's there, the agent's Lakebase projects, its traces experiment and the deploy's source folder.

# COMMAND ----------

if CLEANUP:
    if app_listed:
        w.apps.delete(APP)
        print(f"Deleted app {APP}")
    for n in agent_projects:
        w.api_client.do("DELETE", f"/api/2.0/postgres/{n}")
        print(f"Deleted Lakebase project {n}")
    for e in experiments:
        w.experiments.delete_experiment(e.experiment_id)
        print(f"Deleted experiment {e.name}")
    if folder_present:
        w.workspace.delete(deploy_folder, recursive=True)
        print(f"Deleted {deploy_folder}")
else:
    print("CLEANUP is False; nothing deleted.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sweep: Agent Bricks leftovers from any deploy
# MAGIC
# MAGIC The cleanup above handles one agent. This cell looks for leftovers from every Agent Bricks deploy in the workspace whose app no longer exists: Lakebase projects named `agent-bricks-<name>-runtime-store`, traces experiments under `/Shared/agentbricks_traces/`, and deploy folders under `~/agentbricks_deployments/`. A traces experiment from a project you only ran with `agentbricks dev` shows up too, since it never had an app.
# MAGIC
# MAGIC It lists what it finds and deletes nothing unless you set the `delete_orphans` widget to `yes`. Anything whose app still exists, and any project or experiment that doesn't follow those names, is left alone.

# COMMAND ----------

dbutils.widgets.dropdown("delete_orphans", "no", ["no", "yes"])
live_apps = {a.name for a in w.apps.list()}
orphans = []

for n in lakebase_project_names():
    m = re.fullmatch(r"projects/(agent-bricks-.+)-runtime-store", n)
    if m and m.group(1) not in live_apps:
        orphans.append({"what": "Lakebase project", "name": n})

for e in w.experiments.list_experiments():
    m = re.fullmatch(r"/Shared/agentbricks_traces/(.+)-[a-z0-9]{6}", e.name or "")
    if m and f"agent-bricks-{m.group(1)}" not in live_apps:
        orphans.append({"what": "traces experiment", "name": e.name, "id": e.experiment_id})

deploy_root = f"/Users/{USER}/agentbricks_deployments"
try:
    for o in w.workspace.list(deploy_root):
        if o.path.rsplit("/", 1)[-1] not in live_apps:
            orphans.append({"what": "deploy folder", "name": o.path})
except Exception:
    pass

print(f"Agent Bricks leftovers with no app: {len(orphans)}")
for o in orphans:
    print(f"  {o['what']}: {o['name'].replace(f'/Users/{USER}', '~')}")

if dbutils.widgets.get("delete_orphans") == "yes":
    for o in orphans:
        if o["what"] == "Lakebase project":
            w.api_client.do("DELETE", f"/api/2.0/postgres/{o['name']}")
        elif o["what"] == "traces experiment":
            w.experiments.delete_experiment(o["id"])
        else:
            w.workspace.delete(o["name"], recursive=True)
        print(f"Deleted {o['what']}: {o['name'].replace(f'/Users/{USER}', '~')}")
elif orphans:
    print("Nothing deleted. Set the delete_orphans widget to yes and run this cell again to remove them.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Agent Bricks CLI](https://docs.databricks.com/aws/en/agents/custom-agents/agent-bricks-cli): the install, the six steps and the command reference link.
# MAGIC - [Managed agent memory](https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory): the store `agentbricks memory bind` attaches, and the API behind it.
# MAGIC - [Managed agent sessions](https://docs.databricks.com/aws/en/agents/agent-memory/managed-sessions): durable conversation history, including forking a session.
# MAGIC - [Track model usage](https://docs.databricks.com/aws/en/ai-gateway/usage-tracking): the full `system.ai_gateway.usage` schema and the built-in usage dashboard.
# MAGIC - [Deploy a Databricks app](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/deploy): what `agentbricks deploy` is doing underneath.
