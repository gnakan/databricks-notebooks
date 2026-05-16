# Databricks notebook source
# MAGIC %md
# MAGIC In a recent Databricks blog post about LangGuard's production deployment on [Lakebase](https://docs.databricks.com/aws/en/oltp/projects/):
# MAGIC
# MAGIC > "Lakebase's instant database branching is one of its most operationally valuable capabilities for a governance product. When we create a branch, no data is physically copied."
# MAGIC
# MAGIC Found the "operationally valuable" framing interesting. The article is about agents on Lakebase, so the honest test is to let actual agents drive the branch lifecycle. I put together an experiment where three [Databricks Foundation Models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/) (Llama 4 Maverick, GPT-OSS 120B, and Claude Sonnet 4.6) each call three tool functions (provision, write, teardown) via [function calling](https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling) to exercise the same Lakebase lifecycle end to end.
# MAGIC
# MAGIC The notebook defines three Lakebase tool functions and exposes them as OpenAI-compatible tool schemas. The same user prompt asks each model to provision a branch, write a payload, and tear the branch down; each model gets its own branch id so the runs do not collide. The artifact is the three-model comparison: which tools each model called as structured tool_calls, which it dropped to plain text, per-turn model latency, and the per-model post-teardown confirmation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **A Databricks workspace with Lakebase available.** The notebook was demonstrated on a Premium Azure workspace. Lakebase requires a Premium or Enterprise tier.
# MAGIC - **Foundation Model APIs access on the same workspace.** The pay-per-token endpoints for `databricks-llama-4-maverick`, `databricks-gpt-oss-120b`, and `databricks-claude-sonnet-4-6` are all used. All three support function calling per the Databricks docs.
# MAGIC - **Compute:** the default Serverless runtime. No cluster attach required.
# MAGIC - **Libraries:** `databricks-sdk`, [`psycopg[binary]`](https://www.psycopg.org/psycopg3/docs/), and the [`openai`](https://github.com/openai/openai-python) Python client (used as the OpenAI-compatible client for Foundation Model APIs). The next cell installs all three and restarts the kernel.
# MAGIC - **Identity:** the workspace user runs as their Databricks email; Lakebase maps that to a [Postgres role](https://docs.databricks.com/aws/en/oltp/projects/authentication) with the same identity, and Foundation Model APIs authenticate from the same notebook session. No PAT or external secret to manage.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk "psycopg[binary]>=3.2" "openai>=1.40" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
import uuid

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Branch,
    BranchSpec,
    Duration,
    EndpointStatusState,
    Project,
    ProjectSpec,
)
from openai import OpenAI

w = WorkspaceClient()

# Dynamic naming so reruns do not collide with prior probe resources on the
# same workspace. Lakebase project IDs accept kebab-case; the UUID4 suffix
# keeps the namespace unique across runs and across collaborators.
PROBE_ID = uuid.uuid4().hex[:8]
PROJECT_ID = f"tl-agent-{PROBE_ID}"

# Three models on the Foundation Model APIs surface, each tagged with a short
# id used in branch naming and the comparison table. The branch id distinguishes
# each model's lifecycle on the shared project; the label is the human-readable
# column header.
MODELS = [
    {"id": "llama-4-mvk",  "endpoint": "databricks-llama-4-maverick",   "label": "Llama 4 Maverick"},
    {"id": "gpt-oss-120b", "endpoint": "databricks-gpt-oss-120b",       "label": "GPT-OSS 120B"},
    {"id": "claude-s4-6",  "endpoint": "databricks-claude-sonnet-4-6",  "label": "Claude Sonnet 4.6"},
]

# Workspace user and notebook context. The same context yields the workspace
# URL and the per-session token used to authenticate against Foundation Model
# APIs. No hardcoded secrets.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
current_user = ctx.userName().get()
workspace_url = ctx.apiUrl().get()
workspace_token = ctx.apiToken().get()

# OpenAI-compatible client targeting Databricks serving endpoints. Re-used
# across model calls; only the `model` argument changes per request.
oai = OpenAI(api_key=workspace_token, base_url=f"{workspace_url}/serving-endpoints")

print(f"Workspace user   : {current_user}")
print(f"Probe project id : {PROJECT_ID}")
print("Models under test:")
for m in MODELS:
    print(f"  {m['label']:22s} ({m['endpoint']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stand up the parent project
# MAGIC
# MAGIC Each model operates against the same Lakebase project. Standing one up here keeps the notebook self-contained and gives every model the same starting point. [`w.postgres.create_project`](https://docs.databricks.com/aws/en/oltp/projects/manage-projects) returns a long-running operation; `.wait()` blocks until the project reaches a terminal state. A project ships with a [default branch](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) named `production` and a default read-write compute endpoint attached to it.

# COMMAND ----------

project_spec = ProjectSpec(
    display_name=f"TL Agent Probe {PROBE_ID}",
    pg_version=17,
)
project = Project(spec=project_spec)

t_project_start = time.perf_counter()
created_project = w.postgres.create_project(project=project, project_id=PROJECT_ID).wait()
t_project_elapsed = time.perf_counter() - t_project_start

PROJECT_NAME = created_project.name           # projects/<PROJECT_ID>
DEFAULT_BRANCH_NAME = f"{PROJECT_NAME}/branches/production"

print(f"Project resource name : {PROJECT_NAME}")
print(f"Default branch        : {DEFAULT_BRANCH_NAME}")
print(f"Project create + wait : {t_project_elapsed:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the agent's tool functions
# MAGIC
# MAGIC Three discrete actions: provision a branch, run a write task against it, tear it down. Each is the kind of function an agent framework exposes as a callable tool. Small surface area, one job, idempotent enough to retry.
# MAGIC
# MAGIC The functions wrap the same SDK calls a human notebook author would make. The difference is composition: they accept inputs from a prior step's output (branch name, endpoint name, host), they return values the next step needs, and they handle their own polling. That shape is what makes them callable from a model's tool calls.

# COMMAND ----------

def tool_provision_branch(parent_project: str, branch_id: str, ttl_seconds: int = 3600) -> dict:
    """Provision a Lakebase branch off the project's default branch.

    Returns the resource names and host needed by downstream tools. Polls the
    inherited endpoint until it is reachable so the caller does not race the
    provisioner.
    """
    spec = BranchSpec(
        source_branch=f"{parent_project}/branches/production",
        ttl=Duration(seconds=ttl_seconds),
    )

    t_create_start = time.perf_counter()
    branch = w.postgres.create_branch(
        parent=parent_project,
        branch=Branch(spec=spec),
        branch_id=branch_id,
    ).wait()
    t_create_elapsed = time.perf_counter() - t_create_start

    # A new branch inherits a read-write endpoint from its source on creation.
    # The first run of this experiment's predecessor surfaced this: calling
    # create_endpoint on a fresh branch raises `read_write endpoint already exists`.
    # list_endpoints + poll is the documented pattern.
    t_endpoint_start = time.perf_counter()
    endpoint = list(w.postgres.list_endpoints(parent=branch.name))[0]
    while endpoint.status.current_state not in (
        EndpointStatusState.ACTIVE,
        EndpointStatusState.IDLE,
    ):
        if endpoint.status.current_state == EndpointStatusState.DEGRADED:
            raise RuntimeError(f"Branch endpoint entered DEGRADED state: {endpoint.name}")
        time.sleep(2)
        endpoint = w.postgres.get_endpoint(name=endpoint.name)
    t_endpoint_elapsed = time.perf_counter() - t_endpoint_start

    return {
        "branch_name": branch.name,
        "endpoint_name": endpoint.name,
        "host": endpoint.status.hosts.host,
        "state": endpoint.status.current_state.value,
        "t_create_seconds": t_create_elapsed,
        "t_endpoint_ready_seconds": t_endpoint_elapsed,
    }


def tool_write_task(endpoint_name: str, host: str, actor: str, note: str) -> dict:
    """Run a deterministic write task on the given branch endpoint.

    Creates a small `events` table, inserts the payload, reads it back to
    confirm durability within the branch. Returns the row count and the
    round-trip wall-clock time the agent's caller would measure.
    """
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)

    t_start = time.perf_counter()
    with psycopg.connect(
        host=host,
        port=5432,
        dbname="databricks_postgres",
        user=current_user,
        password=cred.token,
        sslmode="require",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id    SERIAL PRIMARY KEY,
                    actor TEXT NOT NULL,
                    note  TEXT NOT NULL
                )
            """)
            cur.execute(
                "INSERT INTO events (actor, note) VALUES (%s, %s) RETURNING id",
                (actor, note),
            )
            inserted_id = cur.fetchone()[0]
            conn.commit()

            cur.execute("SELECT COUNT(*) FROM events")
            row_count = cur.fetchone()[0]
    t_elapsed = time.perf_counter() - t_start

    return {
        "inserted_id": inserted_id,
        "row_count": row_count,
        "t_round_trip_seconds": t_elapsed,
    }


def tool_teardown_branch(branch_name: str) -> dict:
    """Delete a Lakebase branch.

    The branch's endpoint is owned by the branch and goes with it. Returns the
    teardown wall-clock time.
    """
    t_start = time.perf_counter()
    w.postgres.delete_branch(name=branch_name).wait()
    t_elapsed = time.perf_counter() - t_start

    return {
        "deleted_branch": branch_name,
        "t_teardown_seconds": t_elapsed,
    }


# Dispatch table the agent loop uses to call the function the model picked.
TOOL_REGISTRY = {
    "provision_branch": tool_provision_branch,
    "write_task": tool_write_task,
    "teardown_branch": tool_teardown_branch,
}

print("Tools defined:", ", ".join(TOOL_REGISTRY.keys()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the tool schemas the model sees
# MAGIC
# MAGIC OpenAI-compatible function schemas. Each schema mirrors a tool function's signature: a name, a one-line description so the model knows when to call it, and a JSON schema for the arguments. The model never executes Python; it returns a JSON object naming the tool and the arguments to pass.

# COMMAND ----------

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "provision_branch",
            "description": "Provision a new Lakebase branch off the project's default production branch. Returns the branch resource name, the inherited endpoint name, the host, and the timing breakdown.",
            "parameters": {
                "type": "object",
                "properties": {
                    "parent_project": {
                        "type": "string",
                        "description": "The Lakebase project resource name, in the form 'projects/<project-id>'.",
                    },
                    "branch_id": {
                        "type": "string",
                        "description": "The id to assign to the new branch (kebab-case).",
                    },
                    "ttl_seconds": {
                        "type": "integer",
                        "description": "Time-to-live for the branch in seconds. Default 3600 (one hour) if the user does not specify.",
                    },
                },
                "required": ["parent_project", "branch_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_task",
            "description": "Run a write task against a Lakebase branch endpoint. Creates an `events` table if it does not exist, inserts a row with the given actor and note, and reads back the row count.",
            "parameters": {
                "type": "object",
                "properties": {
                    "endpoint_name": {
                        "type": "string",
                        "description": "The endpoint resource name returned by provision_branch.",
                    },
                    "host": {
                        "type": "string",
                        "description": "The endpoint host returned by provision_branch.",
                    },
                    "actor": {
                        "type": "string",
                        "description": "The actor field for the events row.",
                    },
                    "note": {
                        "type": "string",
                        "description": "The note field for the events row.",
                    },
                },
                "required": ["endpoint_name", "host", "actor", "note"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "teardown_branch",
            "description": "Delete a Lakebase branch. The branch's endpoint is owned by the branch and is deleted with it.",
            "parameters": {
                "type": "object",
                "properties": {
                    "branch_name": {
                        "type": "string",
                        "description": "The branch resource name returned by provision_branch.",
                    },
                },
                "required": ["branch_name"],
            },
        },
    },
]

print(f"Tool schemas registered : {len(TOOL_SCHEMAS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The agent loop
# MAGIC
# MAGIC Standard OpenAI-style tool-use loop. Call the model with the conversation so far and the tool schemas; if the response carries `tool_calls`, run each one against `TOOL_REGISTRY`, append the result as a `tool` role message, and call again. When the model returns a plain assistant message with no tool calls, the loop ends and the assistant text is the final answer.
# MAGIC
# MAGIC The trace list captures each turn's tool name, arguments, result, and wall-clock cost so the post-run summary has the full agent decision history.

# COMMAND ----------

def run_agent_loop(model_endpoint: str, system_prompt: str, user_prompt: str, max_turns: int = 8) -> dict:
    """Run a single-session OpenAI-style tool-use loop against a Databricks model.

    Returns the final assistant message, the per-turn trace, and the total
    wall-clock cost of the agent's session.
    """
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    trace = []
    # Per-loop token totals. Each model forward pass contributes one usage
    # block; we sum once per turn so parallel tool_calls within a turn do
    # not double-count.
    total_prompt_tokens = 0
    total_completion_tokens = 0

    t_loop_start = time.perf_counter()

    for turn in range(max_turns):
        t_turn_start = time.perf_counter()
        completion = oai.chat.completions.create(
            model=model_endpoint,
            messages=messages,
            tools=TOOL_SCHEMAS,
            tool_choice="auto",
        )
        t_turn_elapsed = time.perf_counter() - t_turn_start

        # Token accounting. The OpenAI-compatible response carries a usage
        # block with prompt_tokens (input) and completion_tokens (output).
        # Defensive: a provider that returns None is handled with zeros.
        # Totals accumulate once per turn (one model forward pass) even when
        # the turn produces multiple parallel tool_calls.
        usage = getattr(completion, "usage", None)
        prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
        completion_tokens = getattr(usage, "completion_tokens", 0) or 0
        total_prompt_tokens += prompt_tokens
        total_completion_tokens += completion_tokens

        msg = completion.choices[0].message
        # Append the assistant message to history before tool execution so the
        # next turn has the tool_calls reference it needs.
        messages.append(msg.model_dump(exclude_none=True))

        tool_calls = msg.tool_calls or []
        if not tool_calls:
            trace.append({
                "turn": turn,
                "kind": "assistant_text",
                "t_model_seconds": t_turn_elapsed,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "content": msg.content,
            })
            break

        for call in tool_calls:
            fn_name = call.function.name
            fn_args = json.loads(call.function.arguments)
            fn = TOOL_REGISTRY.get(fn_name)
            if fn is None:
                tool_result = {"error": f"unknown tool: {fn_name}"}
            else:
                try:
                    tool_result = fn(**fn_args)
                except Exception as e:
                    tool_result = {"error": f"{type(e).__name__}: {e}"}

            trace.append({
                "turn": turn,
                "kind": "tool_call",
                "t_model_seconds": t_turn_elapsed,
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "tool": fn_name,
                "arguments": fn_args,
                "result": tool_result,
            })

            messages.append({
                "role": "tool",
                "tool_call_id": call.id,
                "content": json.dumps(tool_result, default=str),
            })
    else:
        trace.append({
            "turn": max_turns,
            "kind": "loop_exhausted",
            "content": f"Agent loop hit the {max_turns}-turn cap before the model returned a plain assistant message.",
        })

    t_loop_elapsed = time.perf_counter() - t_loop_start

    final_assistant = next(
        (entry["content"] for entry in reversed(trace) if entry["kind"] == "assistant_text"),
        None,
    )

    return {
        "model_endpoint": model_endpoint,
        "final_assistant_message": final_assistant,
        "trace": trace,
        "t_loop_seconds": t_loop_elapsed,
        "n_tool_calls": len([t for t in trace if t["kind"] == "tool_call"]),
        "n_assistant_text": len([t for t in trace if t["kind"] == "assistant_text"]),
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
    }


print("Agent loop defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run each model
# MAGIC
# MAGIC The same system prompt, the same user prompt, the same tool schemas. Each model gets its own branch id so the lifecycles do not collide on the shared project. The agent_runs list captures each model's complete result.

# COMMAND ----------

SYSTEM_PROMPT = (
    "You are an agent that operates a Lakebase database via three tool calls: "
    "provision_branch, write_task, teardown_branch. Use the tools in sequence to "
    "fulfill the user's request. Pass values returned by earlier tools into the "
    "arguments of later tools. When the lifecycle is complete, return a short plain-text "
    "summary of what you did. Do not call tools after the teardown step."
)

agent_runs = []

for m in MODELS:
    branch_id = f"branch-{m['id']}"
    user_prompt = (
        f"On the Lakebase project named '{PROJECT_NAME}', provision a new branch with id "
        f"'{branch_id}' (use the default TTL), write one row to the events table on that "
        f"branch with actor='agent-probe' and note='{m['label']} wrote here', and then "
        f"tear the branch down. When you are done, summarize the steps you took and the "
        f"wall-clock cost of each step."
    )

    print(f"\n=== Running {m['label']} ({m['endpoint']}) ===")
    run = run_agent_loop(
        model_endpoint=m['endpoint'],
        system_prompt=SYSTEM_PROMPT,
        user_prompt=user_prompt,
    )
    run["model_label"] = m["label"]
    run["model_id"] = m["id"]
    run["branch_id"] = branch_id
    agent_runs.append(run)

    print(f"  turns (tool_calls + text)  : {run['n_tool_calls']} + {run['n_assistant_text']}")
    print(f"  agent loop wall-clock      : {run['t_loop_seconds']:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect each model's trace
# MAGIC
# MAGIC The trace shows which tool the model picked at each turn, the arguments it generated, and the result the tool returned. The interesting thing across models is which tools each one called as structured tool_calls vs. which it skipped or dropped to plain assistant text.

# COMMAND ----------

EXPECTED_TOOLS = ["provision_branch", "write_task", "teardown_branch"]

def turn_token_label(entry: dict) -> str:
    """Render the per-turn token figures as a compact `in/out` label."""
    pt = entry.get("prompt_tokens")
    ct = entry.get("completion_tokens")
    if pt is None and ct is None:
        return ""
    return f" tok: {pt or 0}/{ct or 0}"


for run in agent_runs:
    print(f"\n--- {run['model_label']} ---")
    seen_turns = set()
    for entry in run["trace"]:
        turn_idx = entry["turn"]
        # A turn with parallel tool_calls produces multiple trace entries with
        # the same turn index and the same usage figures; render tokens only
        # the first time the index appears so the per-turn line is not noisy.
        tok_label = turn_token_label(entry) if turn_idx not in seen_turns else ""
        seen_turns.add(turn_idx)

        if entry["kind"] == "tool_call":
            result_preview = {k: v for k, v in entry['result'].items() if not isinstance(v, (dict, list))}
            print(f"  Turn {turn_idx} [{entry['t_model_seconds']:.2f}s{tok_label}] -> {entry['tool']}")
            print(f"    args  : {entry['arguments']}")
            print(f"    result: {result_preview}")
        elif entry["kind"] == "assistant_text":
            text = entry["content"] or "(empty)"
            text_preview = text if len(text) <= 220 else text[:220] + " ..."
            print(f"  Turn {turn_idx} [{entry['t_model_seconds']:.2f}s{tok_label}] -> assistant_text")
            print(f"    content: {text_preview}")
        else:
            print(f"  Turn {turn_idx} -> {entry['kind']}: {entry.get('content','')}")

    # Per-run structural verdict and total token accounting
    called_tools = [e["tool"] for e in run["trace"] if e["kind"] == "tool_call"]
    missed = [t for t in EXPECTED_TOOLS if t not in called_tools]
    if missed:
        print(f"  ! missed structured tool calls: {missed}")
    else:
        print(f"  all three tools called as structured tool_calls")
    print(f"  total tokens (prompt / completion / total): "
          f"{run['total_prompt_tokens']} / {run['total_completion_tokens']} / "
          f"{run['total_prompt_tokens'] + run['total_completion_tokens']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pull lifecycle timings out of each trace
# MAGIC
# MAGIC Each tool function returns its own wall-clock timing. Aggregating across each model's trace gives the tool-time vs. model-time split per model.

# COMMAND ----------

def summarize_run(run: dict) -> dict:
    """Compute per-model totals from the trace."""
    tool_entries = [t for t in run["trace"] if t["kind"] == "tool_call"]

    provision = next((e for e in tool_entries if e["tool"] == "provision_branch"), None)
    write = next((e for e in tool_entries if e["tool"] == "write_task"), None)
    teardown = next((e for e in tool_entries if e["tool"] == "teardown_branch"), None)

    # Defensive .get(): a tool that the model called but that raised an
    # exception inside the function body returns {"error": "..."} rather than
    # the expected timing dict. Keep the "called_X" booleans truthful (the
    # model did emit the structured tool_call) while showing missing timing
    # values as None in the table.
    t_create = provision["result"].get("t_create_seconds") if provision else None
    t_endpoint = provision["result"].get("t_endpoint_ready_seconds") if provision else None
    t_write = write["result"].get("t_round_trip_seconds") if write else None
    t_teardown = teardown["result"].get("t_teardown_seconds") if teardown else None

    # Surface any tool-call error so the writeup can name it.
    tool_errors = {
        entry["tool"]: entry["result"]["error"]
        for entry in tool_entries
        if isinstance(entry.get("result"), dict) and "error" in entry["result"]
    }

    provision_total = (
        (t_create + t_endpoint)
        if (t_create is not None and t_endpoint is not None)
        else None
    )
    tool_time = sum(
        v for v in [provision_total, t_write, t_teardown] if v is not None
    )
    model_time = sum(t["t_model_seconds"] for t in run["trace"])
    branch_name = provision["result"].get("branch_name") if provision else None

    return {
        "branch_name": branch_name,
        "n_tool_calls": run["n_tool_calls"],
        "called_provision": provision is not None,
        "called_write": write is not None,
        "called_teardown": teardown is not None,
        "tool_errors": tool_errors,
        "t_provision_total": provision_total,
        "t_write": t_write,
        "t_teardown": t_teardown,
        "tool_time_seconds": tool_time,
        "model_time_seconds": model_time,
        "loop_wall_clock_seconds": run["t_loop_seconds"],
        "total_prompt_tokens": run["total_prompt_tokens"],
        "total_completion_tokens": run["total_completion_tokens"],
    }


for run in agent_runs:
    run["summary"] = summarize_run(run)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm per-model teardown
# MAGIC
# MAGIC For each model's branch, check whether it is gone after the agent's loop completed. Two checks: filtered list_branches (should return zero matches), and get_branch (should raise). A False here means the agent claimed to tear down but the branch is still present.

# COMMAND ----------

# Cache the post-loop branch list once so per-model checks are O(1).
remaining_branches = list(w.postgres.list_branches(parent=PROJECT_NAME))
remaining_branch_names = {b.name for b in remaining_branches}

for run in agent_runs:
    branch_name = run["summary"]["branch_name"]
    if not branch_name:
        run["summary"]["teardown_confirmed"] = None
        run["summary"]["teardown_note"] = "provision never returned a branch_name"
        continue

    list_match = branch_name in remaining_branch_names

    get_raised = False
    get_error = None
    try:
        w.postgres.get_branch(name=branch_name)
    except Exception as e:
        get_raised = True
        get_error = f"{type(e).__name__}: {e}"

    run["summary"]["teardown_confirmed"] = (not list_match) and get_raised
    run["summary"]["teardown_note"] = (
        f"list match: {list_match}; get_branch raised: {get_error or 'no'}"
    )

# Show a quick per-model check before the table cell.
for run in agent_runs:
    print(f"{run['model_label']:22s} teardown_confirmed = {run['summary']['teardown_confirmed']}")
    print(f"  {run['summary']['teardown_note']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison table
# MAGIC
# MAGIC One row per model. Columns: did the model call each tool as a structured tool_call, the per-step wall-clock cost, total tool time, total model time, total agent loop wall-clock, and whether teardown was confirmed.

# COMMAND ----------

def yn(value):
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "-"


def fmt(value):
    return f"{value:.2f}s" if isinstance(value, (int, float)) else "-"


headers = [
    "Model", "Calls", "prov", "write", "tear", "prov_t", "write_t", "tear_t",
    "tool_total", "model_total", "loop_wc", "tok_in", "tok_out", "td_ok",
]
widths = [22, 6, 5, 6, 5, 8, 8, 8, 10, 11, 8, 8, 8, 6]

def row_str(values):
    return "  ".join(str(v).ljust(w) for v, w in zip(values, widths))

print(row_str(headers))
print(row_str(["-" * (w - 1) for w in widths]))

for run in agent_runs:
    s = run["summary"]
    print(row_str([
        run["model_label"],
        s["n_tool_calls"],
        yn(s["called_provision"]),
        yn(s["called_write"]),
        yn(s["called_teardown"]),
        fmt(s["t_provision_total"]),
        fmt(s["t_write"]),
        fmt(s["t_teardown"]),
        fmt(s["tool_time_seconds"]),
        fmt(s["model_time_seconds"]),
        fmt(s["loop_wall_clock_seconds"]),
        s["total_prompt_tokens"],
        s["total_completion_tokens"],
        yn(s["teardown_confirmed"]),
    ]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Three models, three lifecycles, one comparison table. The same OpenAI-compatible client points at three different Foundation Model API endpoints; the only variable changed across runs is the model name. Whatever divergence the table shows is the model surface, not the integration surface.
# MAGIC
# MAGIC The two things worth reading off the table: which models orchestrated the full three-tool sequence as structured tool_calls, and how the model-time vs. tool-time split landed. The Lakebase tool time is what a hand-coded notebook would also pay; the model time is the price of letting the model decide the sequence. When teardown_confirmed is False for a model, the agent dropped the teardown call to plain text instead of emitting it as a structured tool_call, and the project-level cleanup cell below is what actually keeps the workspace from accumulating stranded branches.
# MAGIC
# MAGIC The article framed Lakebase's instant branching as "operationally valuable." What that looks like with multiple models in the loop is a single prompt, three tool schemas, and three different orchestration outcomes. The 10-[unarchived-branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) cap and 20-[concurrent-active-computes](https://docs.databricks.com/aws/en/oltp/projects/manage-projects) cap per project are the real concurrency limits an agentic workflow has to design around, which is the practical reason the project-level cleanup is non-optional regardless of which model the agent is wrapping.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC The project-level delete cascades to any remaining branches and endpoints. Every model's branch was either torn down by the agent or is removed here, so no probe resources remain after this cell.

# COMMAND ----------

try:
    w.postgres.delete_project(name=PROJECT_NAME).wait()
    print(f"Deleted project  : {PROJECT_NAME}")
except Exception as e:
    print(f"Project cleanup error (continuing): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Lakebase Autoscaling overview](https://docs.databricks.com/aws/en/oltp/projects/): the parent docs page for the Lakebase feature surface this notebook uses.
# MAGIC - [Manage Lakebase branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches): canonical reference for `create_branch`, `delete_branch`, TTL semantics, the 10-unarchived-branch cap.
# MAGIC - [Function calling on Databricks](https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling): the OpenAI-compatible tool-calling pattern this notebook uses against Foundation Model APIs.
# MAGIC - [Foundation Model APIs overview](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/): the pay-per-token serving surface that hosts `databricks-llama-4-maverick`, `databricks-gpt-oss-120b`, and `databricks-claude-sonnet-4-6`.
# MAGIC - [Inside one of the first production deployments of Lakebase](https://www.databricks.com/blog/inside-one-first-production-deployments-lakebase-langguards-agentic-workflow-governance-engine): the source article that triggered this experiment.

# COMMAND ----------

# Final summary. Emits structured results for automated runs via dbutils.notebook.exit.
# Interactive runs see this as a JSON payload printed by the workspace UI; automated
# runs see it in jobs/runs/get-output.result. Defensive against missing variables so
# a partial run still returns whatever did succeed.
_summary = {
    "PROJECT_NAME": globals().get("PROJECT_NAME"),
    "DEFAULT_BRANCH_NAME": globals().get("DEFAULT_BRANCH_NAME"),
    "t_project_elapsed": globals().get("t_project_elapsed"),
    "models": [m["endpoint"] for m in MODELS] if "MODELS" in globals() else None,
}

if "agent_runs" in globals():
    _summary["runs"] = [
        {
            "model_label": run["model_label"],
            "model_endpoint": run["model_endpoint"],
            "branch_id": run["branch_id"],
            "summary": run["summary"],
            "final_assistant_message": run["final_assistant_message"],
            "trace": run["trace"],
        }
        for run in agent_runs
    ]

dbutils.notebook.exit(json.dumps(_summary, default=str))
