# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC In a recent [Latent Space episode](https://latent.space/p/opus55/) about how Anthropic ships Claude:
# MAGIC
# MAGIC > "Anthropic separates the session log and harness from the sandbox that executes code."
# MAGIC
# MAGIC The design principle is straightforward: the harness is over here, the hands are over there. The brain decides which tool to call and what arguments to pass. Python executes the call and hands the result back. The brain never reaches into the execution layer directly.
# MAGIC
# MAGIC I put the same separation to work on Databricks Foundation Model APIs. This notebook wired `databricks-llama-4-maverick` as the brain, dispatched three Python tool functions as hands, and checked four validation conditions I set before the first run.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hypothesis and validation conditions, set before the first run
# MAGIC
# MAGIC If the brain/hands separation Anthropic describes for shipping Claude is reproduced on Databricks Foundation Model APIs (FMAPI), where an FMAPI model is the orchestrating brain and Python tool functions are the typed hands, then:
# MAGIC
# MAGIC 1. **Brain isolation** -- the brain never executes a tool call directly; it only returns tool_call specifications
# MAGIC 2. **Hand typing** -- each hand receives only the typed arguments the brain specifies; no extra arguments are injected
# MAGIC 3. **Synthesis without re-routing** -- the brain collects and synthesizes tool outputs without re-routing them through the tool layer
# MAGIC 4. **Trace completeness** -- the complete orchestration round-trip for a 3-step task is logged as a single MLflow trace with per-step spans
# MAGIC
# MAGIC **Reject if any condition fails on two consecutive runs.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- serverless compute, no cluster needed
# MAGIC
# MAGIC
# MAGIC **Libraries:** Foundation Model APIs use the OpenAI-compatible SDK. MLflow manual tracing requires `mlflow[databricks]>=3.1.0`; Free Edition ships 2.11.4, so the next cell upgrades it. `dbutils.library.restartPython()` restarts the Python environment so the upgrade takes effect before imports run.
# MAGIC
# MAGIC **Identity and auth:** The notebook resolves the current user and workspace URL from the notebook context and reads the notebook token from `dbutils.secrets` for the FMAPI call. No credentials to paste in.
# MAGIC
# MAGIC **What you need:** A Databricks workspace on [Free Edition](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/) with Foundation Model APIs enabled. The `databricks-llama-4-maverick` endpoint should be available at `{workspace_url}/serving-endpoints`.

# COMMAND ----------

# DBTITLE 1,Install MLflow 3.11+ and OpenAI SDK
%pip install "mlflow[databricks]>=3.11.0" openai --quiet
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import json
import time
import uuid
from typing import Any

import mlflow
from openai import OpenAI

# COMMAND ----------

# DBTITLE 1,Configuration
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = ctx.apiUrl().get().rstrip("/")
USER = ctx.userName().get()
TOKEN = ctx.apiToken().get()

MODEL = "databricks-llama-4-maverick"
RUN_ID = uuid.uuid4().hex[:8]

mlflow.set_tracking_uri("databricks")
EXPERIMENT_NAME = f"/Users/{USER}/fmapi-brain-hands-orchestration-traces"  # not the notebook's own path
mlflow.set_experiment(EXPERIMENT_NAME)

print(f"Workspace: {WORKSPACE_URL}")
print(f"User:      {USER}")
print(f"Model:     {MODEL}")
print(f"Run ID:    {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate: Foundation Model APIs on Free Edition
# MAGIC
# MAGIC Foundation Model APIs are listed as available on Databricks Free Edition. This cell confirms the endpoint responds before the orchestration loop runs. If the call fails, the notebook records GATE_FAIL and stops.

# COMMAND ----------

# DBTITLE 1,Gate -- FMAPI endpoint responds
gate_result = {"status": None, "detail": None}

try:
    client = OpenAI(
        api_key=TOKEN,
        base_url=f"{WORKSPACE_URL}/serving-endpoints",
    )
    probe = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": "Reply with the single word: READY"}],
        max_tokens=5,
    )
    reply = probe.choices[0].message.content.strip()
    gate_result["status"] = "PASS"
    gate_result["detail"] = f"Model replied: {reply}"
    print(f"Gate: PASS -- {reply}")
except Exception as exc:
    gate_result["status"] = "GATE_FAIL"
    gate_result["detail"] = str(exc)
    print(f"Gate: FAIL -- {exc}")
    dbutils.notebook.exit(json.dumps({"gate": gate_result}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Brain definition
# MAGIC
# MAGIC The brain is `databricks-llama-4-maverick` called through the [Foundation Model REST API](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) via the OpenAI-compatible SDK. It receives the task and the tool schemas, returns `tool_calls` for each step, and produces the final synthesis once all tools have reported back.
# MAGIC
# MAGIC The brain never calls a Python function. It returns structured JSON describing which function to call and with which arguments. The orchestration loop (below) reads that JSON and dispatches to the correct hand.

# COMMAND ----------

# DBTITLE 1,Brain -- FMAPI tool-calling wrapper
def call_brain(messages: list[dict], tools: list[dict]) -> dict:
    """
    Send messages + tool schemas to the FMAPI brain.
    Returns the raw response dict for downstream inspection.
    """
    response = client.chat.completions.create(
        model=MODEL,
        messages=messages,
        tools=tools,
        tool_choice="auto",
    )
    choice = response.choices[0]
    return {
        "role": choice.message.role,
        "content": choice.message.content,
        "tool_calls": [
            {
                "id": tc.id,
                "type": tc.type,
                "function": {
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                },
            }
            for tc in (choice.message.tool_calls or [])
        ],
        "finish_reason": choice.finish_reason,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hand definitions
# MAGIC
# MAGIC Three Python functions act as typed hands. Each receives exactly the argument set the brain specified. The function signature is the contract; the brain's JSON must satisfy it.

# COMMAND ----------

# DBTITLE 1,Hands -- three typed Python tool functions
def search_docs(query: str, top_k: int = 3) -> dict:
    """
    Simulates a document search over a knowledge base.
    Returns the top_k most relevant document titles and snippets.
    """
    # Simulated corpus -- real implementation would call a vector index
    corpus = [
        {"title": "Databricks Agent Framework Overview", "snippet": "Foundation Model APIs expose curated open models via a REST endpoint compatible with the OpenAI SDK."},
        {"title": "MLflow Tracing Manual", "snippet": "mlflow.start_span creates a named span. Nested calls produce child spans."},
        {"title": "Brain-Hands Separation", "snippet": "The harness decides what to call. Python executes the call. Results flow back to the harness."},
        {"title": "LLaMA 4 Maverick Capabilities", "snippet": "databricks-llama-4-maverick supports tool_calls via the Chat Completions API."},
        {"title": "Serverless Compute Overview", "snippet": "serverless notebooks start in under 10 seconds and scale to zero when idle."},
    ]
    hits = [doc for doc in corpus if any(w.lower() in doc["title"].lower() or w.lower() in doc["snippet"].lower() for w in query.split())]
    return {"query": query, "results": hits[:top_k], "count": min(len(hits), top_k)}


def get_weather(location: str, units: str = "celsius") -> dict:
    """
    Simulates a weather lookup for a named location.
    Returns current temperature and conditions.
    """
    # Simulated weather data
    weather_data = {
        "san francisco": {"temp": 18, "conditions": "partly cloudy", "humidity": 72},
        "new york": {"temp": 12, "conditions": "overcast", "humidity": 65},
        "london": {"temp": 9, "conditions": "rainy", "humidity": 88},
        "tokyo": {"temp": 20, "conditions": "clear", "humidity": 55},
    }
    loc_key = location.lower().strip()
    data = weather_data.get(loc_key, {"temp": 15, "conditions": "unknown", "humidity": 60})
    if units == "fahrenheit":
        data["temp"] = round(data["temp"] * 9 / 5 + 32, 1)
    return {"location": location, "units": units, **data}


def calculate(expression: str) -> dict:
    """
    Evaluates a safe arithmetic expression.
    Returns the numeric result and the original expression.
    """
    # Safe eval: only allow digits, operators, parens, spaces, and decimal points
    allowed = set("0123456789+-*/()., ")
    if not all(c in allowed for c in expression):
        return {"expression": expression, "result": None, "error": "Unsafe characters in expression"}
    try:
        result = eval(expression, {"__builtins__": {}})  # noqa: S307
        return {"expression": expression, "result": result, "error": None}
    except Exception as exc:
        return {"expression": expression, "result": None, "error": str(exc)}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool registry
# MAGIC
# MAGIC The registry maps function names (as the brain sees them) to Python callables and to their JSON Schema descriptions. The orchestration loop uses the callable map to dispatch; the schema list goes to the brain so it knows what tools exist.

# COMMAND ----------

# DBTITLE 1,Tool registry -- schemas and callable map
TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "search_docs",
            "description": "Search a knowledge base of technical documentation and return the top matching documents.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The search query string"},
                    "top_k": {"type": "integer", "description": "Number of results to return (default 3)"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_weather",
            "description": "Get current weather conditions for a named city.",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {"type": "string", "description": "City name, e.g. San Francisco"},
                    "units": {"type": "string", "enum": ["celsius", "fahrenheit"], "description": "Temperature unit"},
                },
                "required": ["location"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "calculate",
            "description": "Evaluate a safe arithmetic expression and return the numeric result.",
            "parameters": {
                "type": "object",
                "properties": {
                    "expression": {"type": "string", "description": "Arithmetic expression, e.g. '(18 * 1.8) + 32'"},
                },
                "required": ["expression"],
            },
        },
    },
]

TOOL_CALLABLES: dict[str, Any] = {
    "search_docs": search_docs,
    "get_weather": get_weather,
    "calculate": calculate,
}

print(f"Registered {len(TOOL_CALLABLES)} tools: {list(TOOL_CALLABLES.keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestration loop with MLflow tracing
# MAGIC
# MAGIC The loop runs the 3-step task end-to-end inside a single MLflow trace. The brain span wraps the full round-trip. Each hand call gets its own child span. The loop records which conditions were met after every tool dispatch.
# MAGIC
# MAGIC **Task:** "Search for how brain-hands separation works in agent frameworks, check the weather in San Francisco, then convert the temperature to Fahrenheit and calculate 10% above that value."
# MAGIC
# MAGIC This task forces three tool calls in a natural sequence, giving the loop a concrete way to demonstrate all four validation conditions.

# COMMAND ----------

# DBTITLE 1,Orchestration loop -- brain drives, Python dispatches
TASK = (
    "Search for how brain-hands separation works in agent frameworks. "
    "Then get the current weather in San Francisco in celsius. "
    "Finally, convert that temperature to Fahrenheit and calculate 10 percent above that value."
)

# Validation tracking
conditions = {
    "COND_1_BRAIN_ISOLATION": True,  # Flipped to False if brain executes directly
    "COND_2_HAND_TYPING": True,       # Flipped to False if args mismatch schema
    "COND_3_SYNTHESIS_NOT_REROUTED": True,  # Flipped to False if synthesis goes through a tool
    "COND_4_TRACE_COMPLETENESS": False,     # Set True after trace spans are verified
}

tool_calls_made = []      # List of {name, args, result} for each dispatch
intermediate_steps = []   # List of {finish_reason} for each brain turn
step_timings = []         # List of {step, kind, seconds} for each brain turn and hand call
synthesis_text = None
trace_id = None

messages = [{"role": "user", "content": TASK}]

start_time = time.time()
child_span_names = []

with mlflow.start_span(name="brain_orchestration") as brain_span:
    brain_span.set_inputs({"task": TASK, "model": MODEL})
    trace_id = brain_span.trace_id

    # Agentic loop: continue until the brain signals stop
    max_turns = 10
    for turn in range(max_turns):
        brain_started = time.time()
        brain_response = call_brain(messages, TOOL_SCHEMAS)
        step_timings.append({"step": f"Brain, turn {turn + 1}", "kind": "brain", "seconds": time.time() - brain_started})
        intermediate_steps.append({"turn": turn, "finish_reason": brain_response["finish_reason"]})

        if brain_response["finish_reason"] == "tool_calls":
            # Brain returned tool_call specs -- dispatch each to the correct hand
            for tc in brain_response["tool_calls"]:
                fn_name = tc["function"]["name"]
                fn_args_raw = tc["function"]["arguments"]
                fn_args = json.loads(fn_args_raw) if isinstance(fn_args_raw, str) else fn_args_raw

                # Condition 2 check: args keys must be a subset of the schema's known properties
                schema_props = {
                    frozenset(s["function"]["parameters"]["properties"].keys())
                    for s in TOOL_SCHEMAS if s["function"]["name"] == fn_name
                }
                known_keys = schema_props.pop() if schema_props else frozenset()
                if not set(fn_args.keys()).issubset(known_keys):
                    conditions["COND_2_HAND_TYPING"] = False

                # Execute the hand inside its own MLflow child span
                callable_fn = TOOL_CALLABLES.get(fn_name)
                if callable_fn is None:
                    conditions["COND_1_BRAIN_ISOLATION"] = False  # Unknown function -- isolation breach
                    tool_result = {"error": f"Unknown tool: {fn_name}"}
                else:
                    with mlflow.start_span(name=f"hand_{fn_name}") as hand_span:
                        hand_span.set_inputs(fn_args)
                        hand_started = time.time()
                        tool_result = callable_fn(**fn_args)
                        step_timings.append({"step": f"Hand, {fn_name}", "kind": "hand", "seconds": time.time() - hand_started})
                        hand_span.set_outputs(tool_result)
                        hand_span.set_attributes({"tool_call_id": tc["id"]})
                        child_span_names.append(f"hand_{fn_name}")

                tool_calls_made.append({
                    "id": tc["id"],
                    "name": fn_name,
                    "args": fn_args,
                    "result": tool_result,
                })

                # Return result to the brain as a tool message (not routed back through the tool)
                messages.append({
                    "role": "assistant",
                    "content": brain_response["content"],
                    "tool_calls": [
                        {"id": tc["id"], "type": "function", "function": {"name": fn_name, "arguments": fn_args_raw}}
                    ],
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": json.dumps(tool_result),
                })

        elif brain_response["finish_reason"] == "stop":
            # Brain produced the final synthesis
            synthesis_text = brain_response["content"]

            # Condition 3 check: synthesis must not be an empty string and must not contain tool call specs
            if synthesis_text and "tool_calls" in synthesis_text.lower() and "\"type\": \"function\"" in synthesis_text:
                conditions["COND_3_SYNTHESIS_NOT_REROUTED"] = False

            brain_span.set_outputs({"synthesis": synthesis_text, "tool_calls_made": len(tool_calls_made)})
            break
        else:
            # Unexpected finish reason
            brain_span.set_attributes({"unexpected_finish_reason": brain_response["finish_reason"]})
            break

elapsed = time.time() - start_time
# trace_id was captured from brain_span.trace_id inside the with block

print(f"Orchestration complete in {elapsed:.1f}s")
print(f"Tool calls made: {len(tool_calls_made)}")
print(f"Synthesis: {synthesis_text[:200] if synthesis_text else 'None'}...")
print(f"MLflow trace ID: {trace_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Condition 4: MLflow trace completeness
# MAGIC
# MAGIC The trace should have one brain span at the root and one child span per tool call dispatched. The check reads the completed trace and counts the spans.

# COMMAND ----------

# DBTITLE 1,Trace completeness check
# Trace completeness: verified inline during orchestration
# brain_span is root (1 span), child_span_names has one entry per tool call dispatched
expected_children = len(tool_calls_made)
actual_children = len(child_span_names)

conditions["COND_4_TRACE_COMPLETENESS"] = (
    trace_id is not None
    and actual_children == expected_children
    and actual_children > 0
)

print(f"Trace ID:       {trace_id}")
print(f"Root span:      brain_orchestration (1 expected)")
print(f"Child spans:    {actual_children} (expected {expected_children})")
print(f"Span names:     {['brain_orchestration'] + child_span_names}")
print(f"Trace complete: {conditions['COND_4_TRACE_COMPLETENESS']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where the time went
# MAGIC
# MAGIC Each bar is one step of the run, in order: a brain turn is one round trip to the Foundation Model API, a hand call is one Python function the loop dispatched. The chart reads the timings the loop recorded.

# COMMAND ----------

# DBTITLE 1,Where the time went, step by step
# hide-code
import matplotlib.pyplot as plt

NAVY_CHART, NAVY_600_CHART, GRAY_TEXT_CHART, OAT_LIGHT_CHART = "#1B3139", "#1B5162", "#5A6F77", "#F9F7F4"
LAVA_CHART = "#FF3621"

labels = [t["step"] for t in step_timings]
seconds = [t["seconds"] for t in step_timings]
colours = [NAVY_CHART if t["kind"] == "brain" else LAVA_CHART for t in step_timings]

fig, ax = plt.subplots(figsize=(9, 0.45 * len(labels) + 1.4))
fig.patch.set_facecolor(OAT_LIGHT_CHART)
ax.set_facecolor(OAT_LIGHT_CHART)
ax.barh(range(len(labels)), seconds, color=colours)
ax.set_yticks(range(len(labels)))
ax.set_yticklabels(labels, color=NAVY_CHART)
ax.invert_yaxis()
ax.set_xlabel("Seconds", color=GRAY_TEXT_CHART)
ax.tick_params(axis="x", colors=GRAY_TEXT_CHART)
for side in ("top", "right"):
    ax.spines[side].set_visible(False)
for side in ("left", "bottom"):
    ax.spines[side].set_color(GRAY_TEXT_CHART)
brain_total = sum(t["seconds"] for t in step_timings if t["kind"] == "brain")
hand_total = sum(t["seconds"] for t in step_timings if t["kind"] == "hand")
ax.set_title(f"Brain turns: {brain_total:.1f}s in navy. Hand calls: {hand_total:.2f}s in lava.",
             loc="left", color=NAVY_CHART, fontsize=12)
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verdict
# MAGIC
# MAGIC Four conditions, set before the first run. The run passes only when all four hold; one condition failing means the hypothesis is rejected for this run.
# MAGIC
# MAGIC One thing to know before you read the printout: the coded checks for conditions 3 and 4 are looser than the conditions as written. They confirm the last reply isn't a tool spec dressed up as JSON and that every hand call got its span. They don't confirm that every step ran or that the reply is an answer. The next section adds that check, and the findings at the end read the conditions as written.

# COMMAND ----------

# DBTITLE 1,Verdict -- four conditions
CONDITION_LABELS = {
    "COND_1_BRAIN_ISOLATION": "Brain isolation: brain never executed a tool call directly",
    "COND_2_HAND_TYPING": "Hand typing: each hand received only the typed args the brain specified",
    "COND_3_SYNTHESIS_NOT_REROUTED": "Synthesis not re-routed: brain synthesized outputs directly",
    "COND_4_TRACE_COMPLETENESS": "Trace completeness: single trace with per-step spans",
}

passed = sum(1 for v in conditions.values() if v)
total = len(conditions)
verdict = "PASS" if passed == total else "FAIL"

print(f"\n{'='*60}")
print(f"VERDICT: {verdict}  ({passed}/{total} conditions met)")
print(f"{'='*60}")
for key, label in CONDITION_LABELS.items():
    status = "PASS" if conditions[key] else "FAIL"
    print(f"  [{status}]  {label}")
print(f"{'='*60}\n")
print(f"Tool calls dispatched: {len(tool_calls_made)}")
print(f"Elapsed:               {elapsed:.1f}s")
print(f"MLflow trace ID:       {trace_id}")
print(f"\nSynthesis preview:\n{synthesis_text[:400] if synthesis_text else 'None'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 2: did the task actually finish?
# MAGIC
# MAGIC The four conditions above check how the brain and the hands talk to each other. None of them checks whether the task got done. On my first run all four held, and the job still wasn't finished: on its third turn the model wrote the `calculate` call out as plain text, `calculate(expression=...)`, and stopped. Python never ran it, so there was no answer to synthesize.
# MAGIC
# MAGIC So this round adds one check I didn't set before the first run, and labels it that way: **the task finished** when every step (`search_docs`, `get_weather`, `calculate`) ran as a real tool call and the brain's last reply is an answer, not a tool call written as text. The four original conditions stay exactly as they were. The hypothesis says to reject on a condition failing two runs in a row, so the loop runs three more times, each in its own MLflow trace.

# COMMAND ----------

# DBTITLE 1,Round 2 -- three more runs, with a completion check
import re
import statistics

REQUIRED_TOOLS = {"search_docs", "get_weather", "calculate"}
TEXT_CALL = re.compile(r"\b(" + "|".join(sorted(REQUIRED_TOOLS)) + r")\s*\(")
ROUND_2_RUNS = 3


def task_finished(calls: list[dict], final_text) -> dict:
    """Did every required step run as a real tool call, and is the last reply an answer rather than a call written as text?"""
    called = {c["name"] for c in calls}
    written_as_text = bool(final_text) and bool(TEXT_CALL.search(final_text))
    return {
        "steps_called": sorted(called),
        "steps_missing": sorted(REQUIRED_TOOLS - called),
        "call_written_as_text": written_as_text,
        "finished": REQUIRED_TOOLS <= called and bool(final_text) and not written_as_text,
    }


def run_once(run_number: int) -> dict:
    """One full brain/hands round trip in its own MLflow trace; returns the four conditions, the completion check and timing."""
    conds = {"COND_1_BRAIN_ISOLATION": True, "COND_2_HAND_TYPING": True,
             "COND_3_SYNTHESIS_NOT_REROUTED": True, "COND_4_TRACE_COMPLETENESS": False}
    calls, spans, final_text = [], [], None
    msgs = [{"role": "user", "content": TASK}]
    started = time.time()
    with mlflow.start_span(name=f"brain_orchestration_round2_run{run_number}") as root:
        root.set_inputs({"task": TASK, "model": MODEL})
        run_trace_id = root.trace_id
        for _ in range(10):
            resp = call_brain(msgs, TOOL_SCHEMAS)
            if resp["finish_reason"] == "tool_calls":
                for tc in resp["tool_calls"]:
                    name, raw = tc["function"]["name"], tc["function"]["arguments"]
                    args = json.loads(raw) if isinstance(raw, str) else raw
                    known = next((set(t["function"]["parameters"]["properties"]) for t in TOOL_SCHEMAS
                                  if t["function"]["name"] == name), set())
                    if not set(args) <= known:
                        conds["COND_2_HAND_TYPING"] = False
                    fn = TOOL_CALLABLES.get(name)
                    if fn is None:
                        conds["COND_1_BRAIN_ISOLATION"] = False
                        result = {"error": f"Unknown tool: {name}"}
                    else:
                        with mlflow.start_span(name=f"hand_{name}") as hand:
                            hand.set_inputs(args)
                            result = fn(**args)
                            hand.set_outputs(result)
                            spans.append(name)
                    calls.append({"name": name, "args": args})
                    msgs.append({"role": "assistant", "content": resp["content"],
                                 "tool_calls": [{"id": tc["id"], "type": "function",
                                                 "function": {"name": name, "arguments": raw}}]})
                    msgs.append({"role": "tool", "tool_call_id": tc["id"], "content": json.dumps(result)})
            else:
                final_text = resp["content"]
                if final_text and "tool_calls" in final_text.lower() and "\"type\": \"function\"" in final_text:
                    conds["COND_3_SYNTHESIS_NOT_REROUTED"] = False
                root.set_outputs({"synthesis": final_text, "tool_calls_made": len(calls)})
                break
    conds["COND_4_TRACE_COMPLETENESS"] = run_trace_id is not None and len(spans) == len(calls) > 0
    return {"run": run_number, "seconds": round(time.time() - started, 1), "conditions": conds,
            "conditions_held": sum(conds.values()), "tool_calls": [c["name"] for c in calls],
            "final_text": (final_text or "")[:300], "trace_id": run_trace_id,
            **task_finished(calls, final_text)}


round_1_check = task_finished(tool_calls_made, synthesis_text)
round_2 = [run_once(i) for i in range(1, ROUND_2_RUNS + 1)]
round_2_finished = sum(r["finished"] for r in round_2)
typical_seconds = statistics.median(r["seconds"] for r in round_2)

print(f"First run: {passed}/{total} conditions held; task finished: {round_1_check['finished']}"
      f" (missing: {round_1_check['steps_missing'] or 'none'}; call written as text: {round_1_check['call_written_as_text']})")
for r in round_2:
    print(f"Run {r['run']}: {r['conditions_held']}/4 conditions held; calls {r['tool_calls']}; "
          f"finished: {r['finished']}; written as text: {r['call_written_as_text']}; {r['seconds']}s")
    print(f"   last reply: {r['final_text'][:160]!r}")
print(f"\nTask finished on {round_2_finished} of {ROUND_2_RUNS} round 2 runs; typical run {typical_seconds:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The run at a glance

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
import html

# Databricks brand palette: navy and oat carry the page, lava marks a condition that did not hold.
OAT_LIGHT, OAT, WHITE = "#F9F7F4", "#EEEDE9", "#FFFFFF"
NAVY, NAVY_600, GRAY_TEXT, GRAY_LINES = "#1B3139", "#1B5162", "#5A6F77", "#DCE0E2"
LAVA = "#FF3621"
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"  # unquoted: it sits inside quoted style attributes
FONTS = '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'


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
    """A rule whose length carries a value, so a row's size reads without a chart. pop=True draws it in lava."""
    width = max(4, 100 * value / largest) if largest else 4
    return f"<div style='height:3px;background:{LAVA if pop else NAVY};width:{width:.0f}%;margin:10px 0 12px'></div>"


def item(text: str, number=None, under=(), muted: bool = False, note: str = "") -> str:
    """One entry in a list: an optional number column, the text, a muted note, and entries hanging under it."""
    num = (f"<span style='font-size:11px;font-weight:700;color:{NAVY_600};font-variant-numeric:tabular-nums;"
           f"flex:0 0 34px;padding-top:2px'>{esc(number)}</span>") if number is not None else ""
    style = f"color:{GRAY_TEXT};font-style:italic" if muted else f"color:{NAVY}"
    extra = f" <span style='color:{GRAY_TEXT};font-size:11px'>{esc(note)}</span>" if note else ""
    sub = (f"<div style='font-size:12px;color:{GRAY_TEXT};margin-top:2px;line-height:1.5'>"
           + "<br>".join(f"<span style='color:{GRAY_LINES}'>&#9492;</span> {esc(u)}" for u in under) + "</div>") if under else ""
    return (f"<div style='break-inside:avoid;padding:3px 0;display:flex;gap:8px'>{num}"
            f"<div><span style='{style}'>{esc(text)}</span>{extra}{sub}</div></div>")


def columns(entries: list[str], width: int = 220) -> str:
    """Flow item() entries into as many columns as fit."""
    return f"<div style='column-width:{width}px;column-gap:28px;font-size:14px'>{''.join(entries)}</div>"


def row(name: str, count=None, number=None, tag: str = "", tag_pop: bool = False, note: str = "",
        rule: str = "", body: str = "") -> str:
    """One group in the structure: a hairline, its number and name, a tag, a note, a count, a measured rule, then its body."""
    num = f"<span style='font-size:12px;font-weight:700;color:{NAVY_600}'>No. {esc(number)}</span>" if number is not None else ""
    tag_html = f"<span style='font-size:12px;font-weight:700;color:{LAVA if tag_pop else NAVY_600}'>{esc(tag)}</span>" if tag else ""
    note_html = f"<span style='font-size:12px;color:{GRAY_TEXT}'>{esc(note)}</span>" if note else ""
    count_html = f"<span style='margin-left:auto;font-size:30px;font-weight:700;color:{NAVY}'>{esc(count)}</span>" if count is not None else ""
    return (f"<div style='padding:22px 0 18px;border-top:1px solid {GRAY_LINES}'>"
            f"<div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap'>{num}"
            f"<span style='font-size:26px;font-weight:700;color:{NAVY}'>{esc(name)}</span>{tag_html}{note_html}{count_html}</div>"
            f"{rule}{body}</div>")


def callout(text: str) -> str:
    """A single lava-edged line for the one thing the reader should not miss, such as a call that moved between runs."""
    return (f"<div style='padding:14px 18px;margin-bottom:22px;background:{OAT};border-left:4px solid {LAVA};"
            f"font-size:15px;color:{NAVY}'>{text}</div>")


def panel(headline: str, dek: str, body: str) -> str:
    """The whole display: oat ground, navy top edge, DM Sans, a headline, a dek, then the body."""
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 30px;font-family:{SANS};"
            f"border-top:6px solid {NAVY}'>"
            f"<div style='font-size:40px;font-weight:700;line-height:1.1;max-width:760px'>{esc(headline)}</div>"
            f"<div style='font-size:17px;color:{GRAY_TEXT};line-height:1.45;max-width:680px;margin:12px 0 26px'>{dek}</div>"
            f"{body}</div>")


brain_turns = sum(1 for t in step_timings if t["kind"] == "brain")
failed = [CONDITION_LABELS[k] for k, v in conditions.items() if not v]

runs_finished = round_2_finished + int(round_1_check["finished"])
runs_total = ROUND_2_RUNS + 1
body = stats(
    stat(passed, "validation conditions held on the first run", out_of=total),
    stat(runs_finished, "runs where the task actually finished", out_of=runs_total),
    stat(len(tool_calls_made), "hand calls the brain asked for on the first run", out_of=len(REQUIRED_TOOLS)),
    stat(f"{typical_seconds:.1f}s", f"typical run, task to last reply ({ROUND_2_RUNS} runs)"),
)
if failed:
    body += callout("Did not hold: " + esc("; ".join(failed)))
if runs_finished < runs_total:
    body += callout(esc(f"The conditions can all hold while the job goes unfinished: in {runs_total - runs_finished} of {runs_total} runs "
                        "a step never ran as a real tool call."))

dispatches = [
    item(tc["name"], number=i, under=[f"asked with {json.dumps(tc['args'])[:90]}",
                                      f"returned {str(tc['result'])[:90]}"])
    for i, tc in enumerate(tool_calls_made, 1)
]
body += row("What the brain asked the hands to do", count=len(tool_calls_made), body=columns(dispatches, width=380))
body += "".join(
    row(CONDITION_LABELS[k].split(":")[0], tag="held" if v else "did not hold", tag_pop=not v,
        note=CONDITION_LABELS[k].split(":", 1)[1].strip())
    for k, v in conditions.items()
)
runs = [("First run", round_1_check, f"{passed}/{total} conditions")] + [
    (f"Round 2, run {r['run']}", r, f"{r['conditions_held']}/4 conditions") for r in round_2]
body += row("Did the task finish?", count=f"{runs_finished}/{runs_total}", body=columns([
    item(name, under=[cond, "missing: " + (", ".join(chk["steps_missing"]) or "nothing"),
                      "a call came back as text" if chk["call_written_as_text"] else "no call came back as text"],
         note="finished" if chk["finished"] else "not finished", muted=not chk["finished"])
    for name, chk, cond in runs
], width=220))

displayHTML(panel(
    f"{passed} of {total} conditions held; the task finished in {runs_finished} of {runs_total} runs",
    esc(f"{MODEL} planned the task and Python ran each step. The list shows every call on the first run; the four rows are the validation conditions set before it; the last row is the completion check added after it."),
    body,
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# DBTITLE 1,RESULTS_JSON
RESULTS_JSON = {
    "run_id": RUN_ID,
    "model": MODEL,
    "elapsed_seconds": round(elapsed, 1),
    "tool_calls_made": len(tool_calls_made),
    "brain_turns": sum(1 for t in step_timings if t["kind"] == "brain"),
    "step_timings": [{"step": t["step"], "kind": t["kind"], "seconds": round(t["seconds"], 3)} for t in step_timings],
    "conditions": conditions,
    "verdict": verdict,
    "passed": passed,
    "total_conditions": total,
    "mlflow_trace_id": trace_id,
    "round_1_task_check": round_1_check,
    "round_2": round_2,
    "round_2_finished": round_2_finished,
    "orchestration_repeats": ROUND_2_RUNS,
    "typical_run_seconds": round(typical_seconds, 1),
    "gate": gate_result,
    "tool_dispatch_log": [
        {"name": tc["name"], "args": tc["args"], "result_keys": list(tc["result"].keys()) if isinstance(tc["result"], dict) else str(type(tc["result"]))}
        for tc in tool_calls_made
    ],
}

print("RESULTS_JSON", json.dumps(RESULTS_JSON))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC **The plumbing held every time.** On the first pass the brain, `databricks-llama-4-maverick`, asked for 3 tool calls over 4 round trips. Python dispatched each one to its hand with only the arguments the schema names (the `search_docs` call came in as a query plus `top_k` 3), and MLflow logged the whole run as one trace with a child span per hand call. The coded checks called it a pass, 4 of 4. Round 2 ran the loop 3 more times and the coded checks held 4 of 4 on every one.
# MAGIC
# MAGIC **The task never finished.** The completion check I added after the first run found the same thing on all four runs in this export: the brain's last reply was a `calculate` call written out as text, with no answer after it. On round 2 run 2 the whole reply was `calculate(expression=(64.4 * 0.1) + 64.4)`. On run 3 the brain called `search_docs` and `get_weather`, then wrote `calculate(expression=((18 * 1.8) + 32) * 1.1)` as text, so the conversion never ran. Finished: 0 of 4. My first run, before this check existed, went the same way after 2 real calls.
# MAGIC
# MAGIC **The verdict.** Condition 3 says the brain collects and synthesizes tool outputs, and condition 4 says the complete round trip is logged. Read as written, neither happened on four runs in a row, and the hypothesis says to reject on two. So the hypothesis isn't supported for this model, even though the coded checks passed. The tool-calling interface and MLflow tracing did what the docs say; the gap is in the model's last turn, where the call came back in the message content instead of in `tool_calls`. It looks like Llama's own tool-call format leaking into the text, but that's an inference: the run only shows the text. Databricks' [function calling docs](https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling) flag this one up front: "If using Llama 4 Maverick, the current function calling solution is optimized for single turn function calls." This task needs three turns, so the result fits the docs.
# MAGIC
# MAGIC **In plain words:** check that the job got done, not just that the hand-offs were clean. A typical round 2 run took 7.8 seconds, and only one model was tested, so this says nothing about how other Foundation Model API models behave.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC This notebook tested the brain/hands separation on a simulated corpus with three toy functions. The interesting version wires in real Unity Catalog tools: `list_tables`, `run_query`, and `describe_table`. The brain orchestrates a multi-step data question without ever writing SQL itself. That is the architecture the article described.
# MAGIC
# MAGIC The same docs page says "For multi-turn function calling Databricks recommends the supported Claude models." Running these four conditions and the completion check against one of them is the obvious next run; keep the completion check, because the coded conditions alone passed a run that never finished.
# MAGIC
# MAGIC The current loop is sequential. Databricks supports async calls; running independent tool calls in parallel while the brain waits changes the trace shape and the token economics. The trace completeness condition would need updating to handle concurrent child spans.
# MAGIC
# MAGIC The article also notes that Anthropic stores credentials separately from the execution sandbox. On Databricks, Databricks Secrets fills that role. A follow-on piece could verify that secrets never appear in the MLflow trace -- the credential isolation condition Anthropic described is testable here too.
