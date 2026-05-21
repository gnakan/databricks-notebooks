# Databricks notebook source
# MAGIC %md
# MAGIC In a recent Towards Data Science post about agentic LLM cost:
# MAGIC
# MAGIC > "LLM cost doesn't just come from calling the model too often. It also comes from repeatedly paying to process the same tokens again and again."
# MAGIC
# MAGIC Found "repeatedly paying to process the same tokens" interesting, so I put together an experiment. The article surveys six general patterns that keep agent token spend down (prompt caching, semantic caching, lazy tool loading, context compaction, model routing, subagent delegation) and frames the bill as accumulated tokens-to-completion rather than tokens-per-call. The Databricks angle is that three of those patterns map cleanly to capabilities already on Databricks: [prompt caching](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) on Databricks-hosted Claude, lazy tool loading as an agent-side code pattern, and context compaction as an agent-side code pattern. This notebook runs the same three-step research-and-summarize agent task twice on `databricks-claude-sonnet-4-5` via [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis), once with default behavior and once with all three optimizations applied, and reports total tokens consumed end-to-end for both runs.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **Databricks workspace.** The notebook was demonstrated on a Premium Azure workspace where `databricks-claude-sonnet-4-5` is available via Foundation Model APIs. Other tiers should work as long as the same endpoint resolves on your workspace; check the [supported models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) reference to confirm.
# MAGIC - **Foundation Model APIs access.** The pay-per-token endpoint `databricks-claude-sonnet-4-5` is used throughout. Claude is the relevant model here because the documented [`cache_control` parameter](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) on Databricks Foundation Model APIs is accepted only by Databricks-hosted Claude endpoints at the time of this run.
# MAGIC - **Compute:** the default serverless runtime. No cluster attach required.
# MAGIC - **Libraries:** [`openai`](https://github.com/openai/openai-python) Python client (pointed at Databricks serving endpoints) and `databricks-sdk` for identity resolution. The next cell installs both and restarts the kernel. The notebook also uses `requests` and `json` from the standard library; nothing else.
# MAGIC - **Identity:** the workspace user runs as their Databricks email; the notebook context supplies the workspace URL and a per-session token. No PAT or external secret to manage.

# COMMAND ----------

# MAGIC %pip install --upgrade "openai>=1.40" databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
import uuid
from typing import Any

from openai import OpenAI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Dynamic resolution for the workspace user, the workspace URL, and the per-session token. Nothing is hardcoded except the model endpoint name, and that lives as a top-level variable so a reader can swap it for any other Databricks-hosted Claude endpoint listed in the [supported models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) reference. `PROBE_ID` is a UUID4 prefix used to keep run logs distinguishable across reruns and across collaborators sharing the same workspace.

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
current_user = ctx.userName().get()
workspace_url = ctx.apiUrl().get()
workspace_token = ctx.apiToken().get()

PROBE_ID = uuid.uuid4().hex[:8]

# Databricks-hosted Claude. The prompt-caching parameter on Foundation Model
# APIs is accepted on this family of endpoints per the API reference. Swap for
# any other databricks-claude-* endpoint to repeat the comparison on another
# variant.
MODEL = "databricks-claude-sonnet-4-5"

# OpenAI client targeting Databricks serving endpoints. The OpenAI-compatible
# surface returns the standard `usage` object (prompt_tokens, completion_tokens,
# total_tokens) on every chat completion, which is what the run accounting
# below leans on.
oai = OpenAI(api_key=workspace_token, base_url=f"{workspace_url}/serving-endpoints")

print(f"Workspace user   : {current_user}")
print(f"Probe run id     : {PROBE_ID}")
print(f"Model endpoint   : {MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the agent's tools
# MAGIC
# MAGIC Three tool functions that together do a small research-and-summarize task. Each one is stubbed against a fixed local data source so the run is fully deterministic and reproducible across workspaces; the point of the experiment is how the agent loop consumes tokens, not the tool implementations. A real version would back these by Unity Catalog tables, a vector search index, or external HTTP calls.
# MAGIC
# MAGIC - `lookup_company` returns a small structured profile for a named company.
# MAGIC - `fetch_recent_filings` returns three recent quarterly highlights for that company.
# MAGIC - `score_sentiment` returns a numeric sentiment score and a one-line rationale on a passed-in text blob.
# MAGIC
# MAGIC The deterministic stub keeps the comparison honest. Real-world tools add their own latency and token cost on top, but the same workflow conclusions apply.

# COMMAND ----------

# Tiny in-memory store. The company is fictional. The ticker, financials, and
# filing highlights are invented to give the agent loop something realistic-
# shaped to summarize, but no reader should treat the numbers as anything other
# than placeholder data for the loop comparison.
COMPANY_PROFILES = {
    "example-utilities-inc": {
        "ticker": "EXMPL",
        "sector": "Regulated Utilities (synthetic example)",
        "hq": "Fictional, USA",
        "employees": 4200,
        "summary": (
            "Synthetic mid-cap regulated utility used as a stub for the agent loop. "
            "Generation mix, rate-base growth, and reporting cadence are written to "
            "look like real utility filings so the model has something representative "
            "to summarize, but the entity does not exist."
        ),
    }
}

COMPANY_FILINGS = {
    "example-utilities-inc": [
        {
            "quarter": "Q1 2026",
            "revenue_usd_m": 1240,
            "highlight": "First full quarter under the new transmission-cost recovery rider; rate-base growth came in at 6.4 percent year over year.",
        },
        {
            "quarter": "Q4 2025",
            "revenue_usd_m": 1490,
            "highlight": "Winter peak load 4 percent above forecast on a colder-than-normal December; called for moderate storm-cost deferral.",
        },
        {
            "quarter": "Q3 2025",
            "revenue_usd_m": 1180,
            "highlight": "Closed a 220 MW solar PPA with a five-year escalator; expected to displace roughly 8 percent of remaining coal generation.",
        },
    ]
}


def lookup_company(company_slug: str) -> dict[str, Any]:
    """Return a small structured profile for a company by kebab-case slug."""
    profile = COMPANY_PROFILES.get(company_slug)
    if profile is None:
        return {"error": f"unknown company: {company_slug}"}
    return {"company_slug": company_slug, **profile}


def fetch_recent_filings(company_slug: str, limit: int = 3) -> dict[str, Any]:
    """Return the most recent `limit` quarterly filings for the named company."""
    rows = COMPANY_FILINGS.get(company_slug, [])
    return {"company_slug": company_slug, "filings": rows[:limit]}


def score_sentiment(text: str) -> dict[str, Any]:
    """Return a deterministic sentiment score for a text blob.

    Toy scoring: count a small bag of polarity words. The intent is a stable,
    reproducible numeric output so the agent's downstream reasoning is not the
    confounder we're measuring.
    """
    positive = {"growth", "above", "displace", "closed", "recovery"}
    negative = {"deferral", "storm", "above-forecast", "cost"}
    text_lower = text.lower()
    pos_hits = sum(1 for w in positive if w in text_lower)
    neg_hits = sum(1 for w in negative if w in text_lower)
    raw = pos_hits - neg_hits
    score = max(-1.0, min(1.0, raw / 4.0))
    return {
        "score": round(score, 3),
        "positive_hits": pos_hits,
        "negative_hits": neg_hits,
        "rationale": f"{pos_hits} positive markers, {neg_hits} negative markers, normalized to [-1, 1].",
    }


TOOL_REGISTRY = {
    "lookup_company": lookup_company,
    "fetch_recent_filings": fetch_recent_filings,
    "score_sentiment": score_sentiment,
}

# OpenAI-compatible tool schemas. The default run gets all three in the system
# prompt from turn 1; the optimized run uses lazy loading (see further down).
TOOL_SCHEMAS_FULL = [
    {
        "type": "function",
        "function": {
            "name": "lookup_company",
            "description": "Return a structured profile for a company by kebab-case slug.",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_slug": {
                        "type": "string",
                        "description": "Kebab-case company identifier, e.g. 'example-utilities-inc'.",
                    }
                },
                "required": ["company_slug"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "fetch_recent_filings",
            "description": "Return the most recent quarterly filings for a company.",
            "parameters": {
                "type": "object",
                "properties": {
                    "company_slug": {
                        "type": "string",
                        "description": "Kebab-case company identifier.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of recent quarters to return. Defaults to 3.",
                    },
                },
                "required": ["company_slug"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "score_sentiment",
            "description": "Return a numeric sentiment score in [-1, 1] for a text blob and a one-line rationale.",
            "parameters": {
                "type": "object",
                "properties": {
                    "text": {
                        "type": "string",
                        "description": "The text to score.",
                    }
                },
                "required": ["text"],
            },
        },
    },
]

print(f"Tool registry      : {list(TOOL_REGISTRY.keys())}")
print(f"Tool schemas (full): {len(TOOL_SCHEMAS_FULL)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Probe: what does the `usage` object look like for a cached request on this workspace?
# MAGIC
# MAGIC The [Foundation Model APIs reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) documents a `cache_control` field on text, image, reasoning, and tool-call content blocks, accepted only by Databricks-hosted Claude models. The control sits on individual content blocks (Anthropic-native pattern), not as a top-level request parameter. The [proprietary Foundation Model Serving pricing page](https://www.databricks.com/product/pricing/proprietary-foundation-model-serving) prices `cache_writes` and `cache_reads` as separate DBU rates, which is the strong signal that cached-token counts surface in the response (the billing has to attribute them somewhere).
# MAGIC
# MAGIC The cell sends one minimal completion with a long, marked-as-ephemeral system block (Claude's caching threshold is typically around 1024 tokens, so the block has to be large enough to be cacheable) and prints the raw `usage` object. The cost-conversion cell further down keys off whichever cache fields show up here.

# COMMAND ----------

# Cacheable padding for the probe. Claude's prompt caching has a minimum token
# threshold (around 1024 tokens for Sonnet) before the cache control takes effect;
# this padding pushes the block above that threshold so the probe can observe
# the cache fields in the response.
_PROBE_PADDING = (
    "Background context for the assistant. " * 200
)

probe_result = {"ok": False, "usage_keys": [], "raw_usage": None, "error": None}
try:
    probe = oai.chat.completions.create(
        model=MODEL,
        messages=[
            {
                "role": "system",
                "content": [
                    {
                        "type": "text",
                        "text": _PROBE_PADDING,
                        "cache_control": {"type": "ephemeral"},
                    },
                    {
                        "type": "text",
                        "text": "You answer in one short sentence.",
                    },
                ],
            },
            {"role": "user", "content": "Say hello."},
        ],
        max_tokens=32,
    )
    raw_usage = probe.usage.model_dump() if probe.usage is not None else None
    probe_result["ok"] = True
    probe_result["raw_usage"] = raw_usage
    probe_result["usage_keys"] = sorted(raw_usage.keys()) if raw_usage else []
    print(f"[gate 1] completion returned         -> OK")
    print(f"[gate 2] usage object present        -> {'OK' if raw_usage else 'FAIL'}")
    cached_field_present = bool(
        raw_usage and (
            "cached_tokens" in raw_usage
            or "prompt_tokens_details" in raw_usage
            or "cache_read_input_tokens" in raw_usage
            or "cache_creation_input_tokens" in raw_usage
        )
    )
    print(f"[gate 3] cache token field present   -> {'OK' if cached_field_present else 'FAIL (treat as billing-side observation)'}")
    print(f"Usage keys returned: {probe_result['usage_keys']}")
    print(f"Raw usage: {json.dumps(raw_usage, default=str)}")
    probe_result["cached_field_present"] = cached_field_present
except Exception as exc:  # noqa: BLE001
    probe_result["error"] = repr(exc)
    print(f"[gate 1] completion returned         -> FAIL: {exc!r}")
    print("Falling back to total-tokens accounting only. The comparison below still runs.")
    probe_result["cached_field_present"] = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## The agent loop
# MAGIC
# MAGIC Standard OpenAI-style tool-use loop. Call the model with the conversation so far and a tool list, run any returned `tool_calls`, append the tool results, and call again. The loop ends when the assistant returns a plain text response with no tool calls. The function accepts an optional `optimizations` dict that controls which of the three patterns to apply on this run: prompt caching, lazy tool loading, and context compaction. With all three off, this is the default behavior the article uses as the baseline. With all three on, it is the optimized run the article frames as the savings.
# MAGIC
# MAGIC One detail worth calling out: per-turn token totals come from the `usage` block on each model forward pass. Parallel tool calls within a single turn share one usage block, so summing once per turn avoids double counting.

# COMMAND ----------

SYSTEM_PROMPT_BASE = (
    "You are a research assistant. When a user asks you to summarize a company's "
    "recent financial sentiment, your job is to:\n"
    "  1. Look up the company's profile.\n"
    "  2. Fetch its three most recent quarterly filings.\n"
    "  3. Score the sentiment of the combined filings text.\n"
    "  4. Return a short summary that references the profile, the filings, and the sentiment score.\n"
    "Use the tools provided. Do not invent data."
)

# A longer system prompt used to make the prompt-caching savings observable on
# a small loop. In production an agent's system prompt commonly runs in the
# thousands of tokens; this padding stands in for that without obscuring the
# instructions.
SYSTEM_PROMPT_PADDING = (
    "\n\nDomain context (cacheable):\n"
    + ("- Companies in this evaluation are mid-cap regulated utilities or comparable energy-sector entities. "
       "Their reporting cadence is quarterly. Filings reference revenue in millions of USD, generation mix, "
       "rate-base growth percentages, and policy events such as transmission-cost recovery riders or storm-cost "
       "deferrals. Sentiment scoring should weight forward-looking growth language positively and deferral or "
       "cost-overrun language negatively. Always tie the final summary to a numeric sentiment score in [-1, 1].\n"
       ) * 6
)


def _compact_messages(messages: list[dict[str, Any]], keep_tail: int = 2) -> list[dict[str, Any]]:
    """Context compaction: collapse older tool-result rounds into a short summary.

    Keep the system + user message at the front, keep the most recent
    `keep_tail` turns verbatim, and replace anything between with a single
    'assistant' note that lists the prior tool calls. Cheap and deterministic;
    the goal is to drop redundant tool-result payload from the prompt, not to
    summarize the answer.
    """
    if len(messages) <= 2 + keep_tail:
        return messages
    head = messages[:2]
    tail = messages[-keep_tail:]
    middle = messages[2:-keep_tail]
    prior_tool_names = []
    for m in middle:
        if m.get("role") == "assistant" and m.get("tool_calls"):
            for tc in m["tool_calls"]:
                fn_name = tc.get("function", {}).get("name") if isinstance(tc, dict) else getattr(getattr(tc, "function", None), "name", None)
                if fn_name:
                    prior_tool_names.append(fn_name)
    note = {
        "role": "assistant",
        "content": (
            "Prior tool calls in this session, results already incorporated: "
            + (", ".join(prior_tool_names) if prior_tool_names else "none")
            + "."
        ),
    }
    return head + [note] + tail


def _tools_for_turn(turn: int, lazy_tools: bool) -> list[dict[str, Any]]:
    """Lazy tool loading: only advertise the tool the agent needs at this step.

    Default ordering follows the system prompt's step plan. The lazy schedule
    keeps the tool list short on each turn, which keeps prompt tokens lower
    on every model forward pass.
    """
    if not lazy_tools:
        return TOOL_SCHEMAS_FULL
    if turn == 0:
        return [TOOL_SCHEMAS_FULL[0]]  # lookup_company
    if turn == 1:
        return [TOOL_SCHEMAS_FULL[1]]  # fetch_recent_filings
    if turn == 2:
        return [TOOL_SCHEMAS_FULL[2]]  # score_sentiment
    return TOOL_SCHEMAS_FULL


def run_agent_loop(
    user_prompt: str,
    optimizations: dict[str, bool],
    max_turns: int = 8,
) -> dict[str, Any]:
    """Run a single research-and-summarize agent session and return the trace.

    `optimizations` may carry the keys `prompt_caching`, `lazy_tools`, and
    `compact_context`. Any unset key defaults to False, which is the default
    (non-optimized) behavior the article uses as the baseline.
    """
    prompt_caching = bool(optimizations.get("prompt_caching", False))
    lazy_tools = bool(optimizations.get("lazy_tools", False))
    compact_context = bool(optimizations.get("compact_context", False))

    # Prompt caching on Databricks-hosted Claude is requested via a `cache_control`
    # field on individual content blocks, not a top-level parameter. The padding is
    # what's cacheable; the base instructions stay uncached so they can change run
    # to run without invalidating the cache. The Anthropic threshold for caching
    # is around 1024 tokens, so the padding has to be substantial to take effect.
    if prompt_caching:
        system_content: Any = [
            {
                "type": "text",
                "text": SYSTEM_PROMPT_PADDING,
                "cache_control": {"type": "ephemeral"},
            },
            {
                "type": "text",
                "text": SYSTEM_PROMPT_BASE,
            },
        ]
    else:
        system_content = SYSTEM_PROMPT_BASE + SYSTEM_PROMPT_PADDING

    messages: list[dict[str, Any]] = [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_prompt},
    ]

    trace: list[dict[str, Any]] = []
    total_prompt_tokens = 0
    total_completion_tokens = 0

    t_loop_start = time.perf_counter()

    for turn in range(max_turns):
        if compact_context:
            messages = _compact_messages(messages)

        tools_for_this_turn = _tools_for_turn(turn, lazy_tools)

        # Prompt caching, when enabled, rides on the system message's content
        # blocks (assembled above), not on the request kwargs.
        request_kwargs: dict[str, Any] = {
            "model": MODEL,
            "messages": messages,
            "tools": tools_for_this_turn,
            "tool_choice": "auto",
        }

        t_turn_start = time.perf_counter()
        completion = oai.chat.completions.create(**request_kwargs)
        t_turn_elapsed = time.perf_counter() - t_turn_start

        usage = getattr(completion, "usage", None)
        prompt_tokens = getattr(usage, "prompt_tokens", 0) or 0
        completion_tokens = getattr(usage, "completion_tokens", 0) or 0
        raw_usage = usage.model_dump() if usage is not None else None
        total_prompt_tokens += prompt_tokens
        total_completion_tokens += completion_tokens

        msg = completion.choices[0].message
        messages.append(msg.model_dump(exclude_none=True))

        tool_calls = msg.tool_calls or []
        if not tool_calls:
            trace.append({
                "turn": turn,
                "kind": "assistant_text",
                "t_model_seconds": round(t_turn_elapsed, 3),
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "raw_usage": raw_usage,
                "tools_offered": [t["function"]["name"] for t in tools_for_this_turn],
                "content": msg.content,
            })
            break

        # Execute each tool call against the local registry.
        for call in tool_calls:
            fn_name = call.function.name
            try:
                fn_args = json.loads(call.function.arguments or "{}")
            except json.JSONDecodeError:
                fn_args = {}
            fn = TOOL_REGISTRY.get(fn_name)
            if fn is None:
                tool_result: Any = {"error": f"unknown tool: {fn_name}"}
            else:
                try:
                    tool_result = fn(**fn_args)
                except Exception as exc:  # noqa: BLE001
                    tool_result = {"error": repr(exc)}
            messages.append({
                "role": "tool",
                "tool_call_id": call.id,
                "content": json.dumps(tool_result, default=str),
            })
            trace.append({
                "turn": turn,
                "kind": "tool_call",
                "tool_name": fn_name,
                "tool_args": fn_args,
                "tool_result_keys": list(tool_result.keys()) if isinstance(tool_result, dict) else None,
                "t_model_seconds": round(t_turn_elapsed, 3),
                "prompt_tokens": prompt_tokens,
                "completion_tokens": completion_tokens,
                "raw_usage": raw_usage,
                "tools_offered": [t["function"]["name"] for t in tools_for_this_turn],
            })
    else:
        # max_turns exhausted without a plain assistant message.
        trace.append({
            "turn": max_turns,
            "kind": "loop_exhausted",
            "t_model_seconds": 0.0,
            "prompt_tokens": 0,
            "completion_tokens": 0,
        })

    t_loop_elapsed = time.perf_counter() - t_loop_start

    return {
        "trace": trace,
        "total_prompt_tokens": total_prompt_tokens,
        "total_completion_tokens": total_completion_tokens,
        "total_tokens": total_prompt_tokens + total_completion_tokens,
        "loop_seconds": round(t_loop_elapsed, 3),
        "optimizations": optimizations,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 1: default behavior (baseline)
# MAGIC
# MAGIC Same task, default tool loading (all three tools advertised on every turn), no prompt caching parameter, no context compaction. This is what the article calls out as the unoptimized baseline: each turn re-sends the full system prompt and the full tool list, and tool results from earlier turns ride along in the conversation history on every subsequent forward pass.

# COMMAND ----------

USER_PROMPT = (
    "Please summarize the recent financial sentiment for example-utilities-inc. "
    "Use the tools to pull the profile, the most recent filings, and a sentiment score, "
    "then write a three-sentence summary that references all three."
)

default_run = run_agent_loop(
    user_prompt=USER_PROMPT,
    optimizations={
        "prompt_caching": False,
        "lazy_tools": False,
        "compact_context": False,
    },
)

print("DEFAULT RUN")
print("-----------")
print(f"Loop wall-clock      : {default_run['loop_seconds']} seconds")
print(f"Total prompt tokens  : {default_run['total_prompt_tokens']}")
print(f"Total completion tk  : {default_run['total_completion_tokens']}")
print(f"Total tokens         : {default_run['total_tokens']}")
print(f"Turns recorded       : {len(default_run['trace'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 2: optimized (prompt caching, lazy tools, context compaction)
# MAGIC
# MAGIC Same task, same model, same user prompt. Three changes:
# MAGIC
# MAGIC 1. **Prompt caching** marks the padded portion of the system message as `cache_control: {"type": "ephemeral"}`, the form Databricks-hosted Claude expects. The first turn pays cache-write rates on that block; every subsequent turn in the loop reads the same block at cache-read rates, which the pricing page lists at roughly one-tenth of the input rate.
# MAGIC 2. **Lazy tool loading** advertises only the tool the agent needs on the current step. The full schema list is reserved for turns beyond the planned step sequence.
# MAGIC 3. **Context compaction** collapses earlier tool-result rounds into a short note, keeping the most recent two turns verbatim. The intent is to drop redundant tool-result payload from the prompt on the later forward passes.

# COMMAND ----------

optimized_run = run_agent_loop(
    user_prompt=USER_PROMPT,
    optimizations={
        "prompt_caching": True,
        "lazy_tools": True,
        "compact_context": True,
    },
)

print("OPTIMIZED RUN")
print("-------------")
print(f"Loop wall-clock      : {optimized_run['loop_seconds']} seconds")
print(f"Total prompt tokens  : {optimized_run['total_prompt_tokens']}")
print(f"Total completion tk  : {optimized_run['total_completion_tokens']}")
print(f"Total tokens         : {optimized_run['total_tokens']}")
print(f"Turns recorded       : {len(optimized_run['trace'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run 3: caching alone (the isolation run)
# MAGIC
# MAGIC One change vs the default: `prompt_caching: True`. Lazy tools and context compaction are left off. The point of this run is to read the caching lever cleanly, without the interaction effect that made the stacked optimized run net-negative. If caching is a real lever, this run should produce the same answer in the same turn count as the default, but with the bulk of the system-message prompt tokens billing at the cache-read rate (roughly one-tenth of the input rate) instead of the input rate.

# COMMAND ----------

cache_only_run = run_agent_loop(
    user_prompt=USER_PROMPT,
    optimizations={
        "prompt_caching": True,
        "lazy_tools": False,
        "compact_context": False,
    },
)

print("CACHE-ONLY RUN")
print("--------------")
print(f"Loop wall-clock      : {cache_only_run['loop_seconds']} seconds")
print(f"Total prompt tokens  : {cache_only_run['total_prompt_tokens']}")
print(f"Total completion tk  : {cache_only_run['total_completion_tokens']}")
print(f"Total tokens         : {cache_only_run['total_tokens']}")
print(f"Turns recorded       : {len(cache_only_run['trace'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Side-by-side: total tokens to completion
# MAGIC
# MAGIC The headline number is total tokens consumed end-to-end. Token-level cost on Foundation Model APIs is published per million tokens of input and output, so a tokens-to-completion delta translates directly to a cost-per-completed-task delta. The per-turn breakdown is the supporting evidence for where the delta lives: if it concentrates in `prompt_tokens` across early turns, lazy tools and compaction did the work; if it concentrates in the cached portion of `prompt_tokens` on later turns, prompt caching did the work; if it concentrates in `completion_tokens`, the optimizations changed how the model decomposed the task and the comparison needs a closer read.

# COMMAND ----------

def _render_run(label: str, run: dict[str, Any]) -> None:
    print(label)
    print("-" * len(label))
    print(f"  Loop wall-clock      : {run['loop_seconds']} seconds")
    print(f"  Total prompt tokens  : {run['total_prompt_tokens']}")
    print(f"  Total completion tk  : {run['total_completion_tokens']}")
    print(f"  Total tokens         : {run['total_tokens']}")
    print(f"  Turns recorded       : {len(run['trace'])}")
    print(f"  Per-turn breakdown:")
    for row in run["trace"]:
        tag = row.get("tool_name") or row.get("kind")
        print(
            f"    turn {row['turn']:>2} | {str(tag):24s} | "
            f"p={row.get('prompt_tokens', 0):>5} c={row.get('completion_tokens', 0):>4} "
            f"| tools_offered={row.get('tools_offered')}"
        )


_render_run("DEFAULT", default_run)
print()
_render_run("OPTIMIZED", optimized_run)
print()
_render_run("CACHE-ONLY", cache_only_run)


def _token_delta(label: str, baseline: dict[str, Any], comparison: dict[str, Any]) -> None:
    delta_tokens = baseline["total_tokens"] - comparison["total_tokens"]
    if baseline["total_tokens"] > 0:
        delta_pct = round(100.0 * delta_tokens / baseline["total_tokens"], 2)
    else:
        delta_pct = 0.0
    print(label)
    print("-" * len(label))
    print(f"  Baseline total tokens    : {baseline['total_tokens']}")
    print(f"  Comparison total tokens  : {comparison['total_tokens']}")
    print(f"  Absolute token delta     : {delta_tokens}")
    print(f"  Percent saved            : {delta_pct} percent")
    print()


print()
_token_delta("DELTA: default → optimized (stacked)", default_run, optimized_run)
_token_delta("DELTA: default → cache-only", default_run, cache_only_run)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost per completed task in DBU
# MAGIC
# MAGIC Tokens become budget when multiplied by the published DBU rates on the [proprietary Foundation Model Serving pricing page](https://www.databricks.com/product/pricing/proprietary-foundation-model-serving). The Claude Sonnet 4.5 Global rates are the constants in the cell below; the In-geo column is included as a comment so a reader running on geo-pinned endpoints can swap. Cache writes and cache reads price separately from the input rate, which is why the probe cell's `usage` object matters here: if `cached_tokens` (or the equivalent field) is exposed, the cost calculation splits cached and uncached input tokens; if not, the cell falls back to the input rate on all prompt tokens and reports the bound as a slight overestimate of the optimized cost.
# MAGIC
# MAGIC The cost cell below pairs the token totals with their DBU equivalents. Token count is the primary measurement upstream; DBU is what the token mix actually costs once you split the prompt-token block between cached and uncached at their separate rates. Read the token-count delta first, then check whether the cost-side picture follows the same direction. On the caching arm in particular, the token count can stay essentially flat while the DBU number drops sharply.

# COMMAND ----------

# Claude Sonnet 4.5, Global endpoint, all context lengths.
# Source: databricks.com/product/pricing/proprietary-foundation-model-serving
# In-geo rates (commented) apply when calling a geo-pinned endpoint.
DBU_PER_M_INPUT = 42.857          # In-geo: 47.143
DBU_PER_M_OUTPUT = 214.286        # In-geo: 235.715
DBU_PER_M_CACHE_WRITE = 53.571    # In-geo: 58.928
DBU_PER_M_CACHE_READ = 4.286      # In-geo: 4.715


def _split_prompt_tokens(run: dict[str, Any]) -> tuple[int, int, int]:
    """Return (uncached_prompt, cache_read, cache_write) totals for a run.

    Reads whichever cache-token field the probe cell observed on this workspace.
    Falls back to (total_prompt_tokens, 0, 0) when no cache field is exposed; the
    resulting input cost is a slight overestimate of the optimized run.

    Parallel tool calls share one usage block per turn, so this helper dedupes
    by `turn` and reads usage once per forward pass.
    """
    total_prompt = run["total_prompt_tokens"]
    cache_read = 0
    cache_write = 0
    seen_turns: set[int] = set()
    for row in run["trace"]:
        turn_id = row.get("turn")
        if turn_id in seen_turns:
            continue
        seen_turns.add(turn_id)
        usage = row.get("raw_usage") or {}
        details = usage.get("prompt_tokens_details") or {}
        # Databricks-hosted Claude returns cache_read_input_tokens and
        # cache_creation_input_tokens as top-level fields on the usage object.
        # The `prompt_tokens_details.cached_tokens` and top-level `cached_tokens`
        # paths are the OpenAI-side names; both fall-throughs stay as defensive
        # reads for any other endpoint that returns the alternate shape.
        cache_read += int(
            usage.get("cache_read_input_tokens", 0)
            or details.get("cached_tokens", 0)
            or usage.get("cached_tokens", 0)
            or 0
        )
        cache_write += int(
            usage.get("cache_creation_input_tokens", 0)
            or details.get("cache_creation_input_tokens", 0)
            or 0
        )
    uncached = max(total_prompt - cache_read - cache_write, 0)
    return uncached, cache_read, cache_write


def _cost_dbu(run: dict[str, Any]) -> dict[str, float]:
    uncached, cache_read, cache_write = _split_prompt_tokens(run)
    input_dbu = uncached / 1_000_000 * DBU_PER_M_INPUT
    cache_read_dbu = cache_read / 1_000_000 * DBU_PER_M_CACHE_READ
    cache_write_dbu = cache_write / 1_000_000 * DBU_PER_M_CACHE_WRITE
    output_dbu = run["total_completion_tokens"] / 1_000_000 * DBU_PER_M_OUTPUT
    total = input_dbu + cache_read_dbu + cache_write_dbu + output_dbu
    return {
        "uncached_input_tokens": uncached,
        "cache_read_tokens": cache_read,
        "cache_write_tokens": cache_write,
        "completion_tokens": run["total_completion_tokens"],
        "input_dbu": round(input_dbu, 6),
        "cache_read_dbu": round(cache_read_dbu, 6),
        "cache_write_dbu": round(cache_write_dbu, 6),
        "output_dbu": round(output_dbu, 6),
        "total_dbu": round(total, 6),
    }


default_cost = _cost_dbu(default_run)
optimized_cost = _cost_dbu(optimized_run)
cache_only_cost = _cost_dbu(cache_only_run)

for label, cost in (
    ("DEFAULT", default_cost),
    ("OPTIMIZED (stacked)", optimized_cost),
    ("CACHE-ONLY", cache_only_cost),
):
    print(label)
    print("-" * len(label))
    print(f"  Uncached input tokens : {cost['uncached_input_tokens']}")
    print(f"  Cache-read tokens     : {cost['cache_read_tokens']}")
    print(f"  Cache-write tokens    : {cost['cache_write_tokens']}")
    print(f"  Completion tokens     : {cost['completion_tokens']}")
    print(f"  Input DBU             : {cost['input_dbu']}")
    print(f"  Cache-read DBU        : {cost['cache_read_dbu']}")
    print(f"  Cache-write DBU       : {cost['cache_write_dbu']}")
    print(f"  Output DBU            : {cost['output_dbu']}")
    print(f"  TOTAL DBU per task    : {cost['total_dbu']}")
    print()


def _dbu_delta(label: str, baseline_cost: dict[str, float], comparison_cost: dict[str, float]) -> None:
    delta = baseline_cost["total_dbu"] - comparison_cost["total_dbu"]
    if baseline_cost["total_dbu"] > 0:
        pct = round(100.0 * delta / baseline_cost["total_dbu"], 2)
    else:
        pct = 0.0
    print(label)
    print("-" * len(label))
    print(f"  Baseline cost (DBU)   : {baseline_cost['total_dbu']}")
    print(f"  Comparison cost (DBU) : {comparison_cost['total_dbu']}")
    print(f"  Absolute DBU saved    : {round(delta, 6)}")
    print(f"  Percent saved (DBU)   : {pct} percent")
    print()


_dbu_delta("DBU DELTA: default → optimized (stacked)", default_cost, optimized_cost)
_dbu_delta("DBU DELTA: default → cache-only", default_cost, cache_only_cost)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Emit a structured result for headless reruns
# MAGIC
# MAGIC `dbutils.notebook.exit` serializes the three run summaries plus their DBU costs into a single JSON string so a headless `databricks jobs submit` run returns the comparison via `notebook_output.result`. When the notebook is run interactively this still prints fine; the exit just adds a machine-readable artifact for the headless path.

# COMMAND ----------

def _run_summary(run: dict[str, Any], cost: dict[str, float]) -> dict[str, Any]:
    return {
        "loop_seconds": run["loop_seconds"],
        "turns": len(run["trace"]),
        "total_prompt_tokens": run["total_prompt_tokens"],
        "total_completion_tokens": run["total_completion_tokens"],
        "total_tokens": run["total_tokens"],
        "cost_dbu": cost,
        "trace": run["trace"],
    }


_result_payload = {
    "model": MODEL,
    "probe": {
        "usage_keys": probe_result.get("usage_keys"),
        "raw_usage": probe_result.get("raw_usage"),
        "cached_field_present": probe_result.get("cached_field_present"),
        "error": probe_result.get("error"),
    },
    "runs": {
        "default": _run_summary(default_run, default_cost),
        "optimized_stacked": _run_summary(optimized_run, optimized_cost),
        "cache_only": _run_summary(cache_only_run, cache_only_cost),
    },
}

dbutils.notebook.exit(json.dumps(_result_payload, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I learned
# MAGIC
# MAGIC Three notes for the reader. First, the headline is total tokens consumed end-to-end. Token count is the primary measurement because it captures both loop completion (a 2x token explosion that never finishes is the most important fact on the stacked run) and the downstream cost. The DBU delta is a derived number that follows from tokens once the rates are applied; read it after the token-count delta, not before. Second, the per-turn breakdown is the part to look at when interpreting either number. Three optimizations applied together can hide which one is doing the work. Disabling them one at a time in the `optimizations` dict and rerunning isolates the contribution of each. Third, the cost cell's input rate is the Claude Sonnet 4.5 Global pay-per-token rate; teams running geo-pinned endpoints or other Claude variants should swap the constants for the right row of the pricing page.
# MAGIC
# MAGIC The usage-probe cell at the top recorded the exact `usage` object this workspace returned. If a `cached_tokens` or `prompt_tokens_details` block landed in the response, the cost cell read the cache-read and cache-write counts and priced them at the documented `cache reads` and `cache writes` DBU rates. If the response did not carry those fields on this workspace, the cost cell counted all prompt tokens at the uncached input rate, which slightly overestimates the optimized cost; the true savings show up on the billing-side line item for cached tokens.
# MAGIC
# MAGIC No cleanup is needed; the notebook holds no workspace-side resources beyond the chat completion calls themselves.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis). Landing page for the pay-per-token and provisioned-throughput modes used to serve Databricks-hosted Claude and the other models referenced here.
# MAGIC - [Foundation Model APIs reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference). Request and response format, including the prompt-caching parameter and the `usage` object fields the run accounting reads.
# MAGIC - [Supported models on Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models). Current list of Databricks-hosted models, including the Claude variants where prompt caching is documented as accepted.
# MAGIC - [Build generative AI apps on Databricks](https://docs.databricks.com/aws/en/generative-ai/agent-framework/build-genai-apps). Overview of the agent surface, including the `ResponsesAgent` interface a production version of this loop would wrap.
# MAGIC - [Author an agent](https://docs.databricks.com/aws/en/generative-ai/agent-framework/author-agent). Canonical tool-calling agent patterns on Databricks, the version of this loop a team would ship rather than the bare client used here for the comparison.
# MAGIC - [Proprietary Foundation Model Serving pricing](https://www.databricks.com/product/pricing/proprietary-foundation-model-serving). DBU rates per million input, output, cache-write, and cache-read tokens for each Anthropic, OpenAI, and Meta model on the service. The cost cell's constants come from the Claude Sonnet 4.5 Global row here.
