# Databricks notebook source
# MAGIC %md
# MAGIC # Should I Wait for the Tram? Rules vs Models in the Agent That Decides
# MAGIC
# MAGIC In a recent [Databricks Community article](https://community.databricks.com/t5/community-articles/should-i-wait-for-the-tram-adding-agents-to-a-live-helsinki/m-p/173060#M1824) about adding agents to a live Helsinki transit app:
# MAGIC
# MAGIC > "I'd also like to hear how others split decisions between rules and models in their agents."
# MAGIC
# MAGIC Found that question interesting, so I put together an experiment. In the article, the bike, walk or wait agent keeps the call in a small Python rule with unit tests, and the model only picks the tools and explains the result. This notebook answers the question with a run: the same tool results go through the article's split (the rule decides, a model explains) and the opposite one (a model decides from the same policy, written out in words). Three [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis) models take turns, and the hard cases are close calls and a live tram feed that's gone quiet, which is where the article's own agent tripped up.
# MAGIC
# MAGIC **Platform:** Databricks Free Edition, serverless notebook compute. About 10 minutes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before you run it
# MAGIC
# MAGIC The notebook runs on Databricks Free Edition on serverless notebook compute, with nothing to attach. A paid workspace runs it unchanged, as long as the three [model endpoints](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) below are served there. The next cell installs the [`openai`](https://github.com/openai/openai-python) Python client (with a newer `httpcore`, so it matches the `h11` the client pulls in) and restarts Python; everything else ships with the runtime. You don't need a key: the notebook reads the workspace URL and a session token from the notebook context.
# MAGIC
# MAGIC Every call lands as a row in a [Delta table](https://docs.databricks.com/aws/en/delta/) under `workspace.tram_rules_vs_models`, so you can query the run afterwards, and the cleanup cell at the end drops the schema.
# MAGIC
# MAGIC The live feed, the weather API and the route planner from the article aren't wired in here. I built the answers those calls return as plain dictionaries instead, so every model sees exactly the same inputs and you can repeat the run.

# COMMAND ----------

# MAGIC %pip install --quiet "openai>=1.40" "httpcore>=1.0.9"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import json
import re
import statistics
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from openai import OpenAI, RateLimitError, APIStatusError

CATALOG = "workspace"
SCHEMA = "tram_rules_vs_models"

# The article's agent runs on databricks-gpt-oss-120b. The other two are a large and a
# small instruction model from the same Free Edition roster, so a model swap is a real swap.
MODELS = {
    "gpt-oss-120b": "databricks-gpt-oss-120b",
    "llama-4-maverick": "databricks-llama-4-maverick",
    "llama-3-1-8b": "databricks-meta-llama-3-1-8b-instruct",
}

# A decision should come out the same every time, so every call runs at temperature 0.
# REPEATS asks each model the same question three times to check that it does.
TEMPERATURE = 0
REPEATS = 3
WORKERS = 4

# Set keep_tables to true to leave the calls table in place after the run.
dbutils.widgets.dropdown("keep_tables", "false", ["false", "true"])
KEEP_TABLES = dbutils.widgets.get("keep_tables") == "true"

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = ctx.apiUrl().getOrElse(None).rstrip("/")
TOKEN = ctx.apiToken().getOrElse(None)

llm = OpenAI(api_key=TOKEN, base_url=f"{WORKSPACE_URL}/serving-endpoints")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Workspace: {WORKSPACE_URL}")
print(f"Schema:    {CATALOG}.{SCHEMA}")
print(f"Models:    {', '.join(MODELS.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## One function for every model call
# MAGIC
# MAGIC Databricks [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/model-serving/score-foundation-models) take the OpenAI client as is. The `gpt-oss` models return their answer as a list of typed parts (a reasoning part, then a text part), so the function keeps only the text. It retries on a rate limit, and it records tokens and time taken for every call, whichever split asked for it.

# COMMAND ----------

# DBTITLE 1,call_model
def call_model(endpoint: str, system: str, user: str, max_tokens: int) -> dict:
    """Run one chat completion and return the visible text, token counts and latency."""
    for attempt in range(6):
        try:
            started = time.perf_counter()
            resp = llm.chat.completions.create(
                model=endpoint,
                messages=[{"role": "system", "content": system}, {"role": "user", "content": user}],
                temperature=TEMPERATURE,
                max_tokens=max_tokens,
            )
            latency_ms = int((time.perf_counter() - started) * 1000)
            break
        except RateLimitError:
            time.sleep(2 ** attempt)
        except APIStatusError as err:
            if err.status_code >= 500 and attempt < 5:
                time.sleep(2 ** attempt)
                continue
            raise
    else:
        raise RuntimeError(f"{endpoint}: still rate limited after six attempts")

    content = resp.choices[0].message.content
    if isinstance(content, list):
        text = "".join(p.get("text", "") for p in content if isinstance(p, dict) and p.get("type") == "text")
    else:
        text = content or ""
    usage = resp.usage
    return {
        "text": text.strip(),
        "prompt_tokens": usage.prompt_tokens if usage else None,
        "completion_tokens": usage.completion_tokens if usage else None,
        "latency_ms": latency_ms,
    }

# COMMAND ----------

# DBTITLE 1,Gate: every model answers
# Each gate call is also the warm-up, so no timed call below pays a cold start.
gates_ok = True
for i, (label, endpoint) in enumerate(MODELS.items(), start=1):
    try:
        out = call_model(endpoint, "Reply with one word.", "Say ready.", max_tokens=400)
        print(f"[gate {i}] {endpoint}: {out['text'][:20]!r} -> OK")
    except Exception as err:
        gates_ok = False
        print(f"[gate {i}] {endpoint}: {type(err).__name__}: {str(err)[:160]} -> FAIL")

assert gates_ok, "A model endpoint didn't answer. Check the names in MODELS against Serving in the sidebar."

# COMMAND ----------

# MAGIC %md
# MAGIC ## The tool results
# MAGIC
# MAGIC The article's agent asks its tools before it answers: the route planner for the next tram, its ride time and the walk, the live position feed for how late that tram is running, the city bike dock, and the weather forecast. In the article the tools are [Unity Catalog functions](https://docs.databricks.com/aws/en/udf/unity-catalog), and the agent calls them through [function calling](https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling). Here each scenario is the set of answers those tools would hand back, so the only thing that changes between the two splits is who makes the call.
# MAGIC
# MAGIC There are sixteen scenarios over six Helsinki tram trips. In twelve of them the feed is live and everything is present. Some are clear wins for one option; the rest are within a minute, or turn on a detail like a tram running late enough to lose, an empty bike dock, or rain just over or just under the line.
# MAGIC
# MAGIC The other four have the feed down. In two, the position feed says so, which is the article's own fix: "The fix went into the tool, not the prompt: it now always says whether the feed is live." In the other two it just comes back empty, which is the article's original failure: "With the feed switched off, an empty tool result made the model say confidently that there was no bunching." In all four the scheduled tram looks fastest on paper.

# COMMAND ----------

# DBTITLE 1,Scenarios
def trip(sid, origin, dest, due, ride, late, walk, bike, bikes, rain, feed="live"):
    """Build the tool results one trip returns. feed is live, down (flagged) or empty (no flag)."""
    if feed == "live":
        positions = {"feed_live": True, "tram_late_min": late}
    elif feed == "down":
        positions = {"feed_live": False}
    else:
        positions = {}
    return {
        "id": sid,
        "group": {"live": "feed live", "down": "feed down, flagged", "empty": "feed down, silent"}[feed],
        "tools": {
            "route_planner": {"from": origin, "to": dest, "next_tram_due_min": due, "tram_ride_min": ride, "walk_min": walk},
            "positions": positions,
            "city_bikes": {"bikes_at_dock": bikes, "bike_min": bike},
            "weather": {"rain_mm_next_hour": rain},
        },
    }

SCENARIOS = [
    trip("s01", "Kamppi", "Kauppatori", due=2, ride=7, late=0, walk=20, bike=11, bikes=4, rain=0.0),
    trip("s02", "Rautatientori", "Hakaniemi", due=9, ride=5, late=3, walk=14, bike=7, bikes=0, rain=0.0),
    trip("s03", "Senaatintori", "Kaivopuisto", due=6, ride=8, late=1, walk=24, bike=9, bikes=7, rain=0.0),
    trip("s04", "Töölön tulli", "Ooppera", due=1, ride=4, late=0, walk=13, bike=6, bikes=3, rain=2.5),
    trip("s05", "Hietalahdentori", "Kauppatori", due=12, ride=6, late=4, walk=16, bike=8, bikes=5, rain=3.0),
    trip("s06", "Lasipalatsi", "Kallion virastotalo", due=4, ride=9, late=0, walk=27, bike=10, bikes=2, rain=0.0),
    trip("s07", "Kamppi", "Kauppatori", due=5, ride=7, late=2, walk=15, bike=16, bikes=3, rain=0.0),
    trip("s08", "Rautatientori", "Hakaniemi", due=4, ride=5, late=3, walk=11, bike=9, bikes=0, rain=0.0),
    trip("s09", "Senaatintori", "Kaivopuisto", due=3, ride=8, late=1, walk=24, bike=11, bikes=1, rain=0.0),
    trip("s10", "Töölön tulli", "Ooppera", due=6, ride=4, late=0, walk=10, bike=6, bikes=4, rain=1.2),
    trip("s11", "Hietalahdentori", "Kauppatori", due=7, ride=6, late=2, walk=16, bike=14, bikes=2, rain=0.4),
    trip("s12", "Lasipalatsi", "Kallion virastotalo", due=2, ride=9, late=5, walk=27, bike=15, bikes=3, rain=0.0),
    trip("s13", "Kamppi", "Kauppatori", due=1, ride=7, late=None, walk=20, bike=11, bikes=4, rain=0.0, feed="down"),
    trip("s14", "Rautatientori", "Hakaniemi", due=2, ride=5, late=None, walk=14, bike=7, bikes=0, rain=0.0, feed="down"),
    trip("s15", "Senaatintori", "Kaivopuisto", due=0, ride=8, late=None, walk=24, bike=9, bikes=5, rain=0.0, feed="empty"),
    trip("s16", "Töölön tulli", "Ooppera", due=1, ride=4, late=None, walk=13, bike=6, bikes=3, rain=0.0, feed="empty"),
]
print(f"{len(SCENARIOS)} scenarios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split 1: the rule decides
# MAGIC
# MAGIC The policy is short. Take whichever option gets you there first. Count the tram only when the position feed is live and gives a lateness reading, and add that lateness to the wait. Count the bike only when the dock has a bike and the forecast is under 1 mm of rain in the next hour. On a tie, walk, because walking doesn't depend on anything else showing up.
# MAGIC
# MAGIC That's the whole rule, in Python, with the article's habit of unit tests next to it. It also hands back the margin to the runner-up, which is how the notebook sorts the feed-live trips into clear calls and close calls (one minute or less).

# COMMAND ----------

# DBTITLE 1,The rule
RAIN_LIMIT_MM = 1.0
TIE_ORDER = ["WALK", "BIKE", "WAIT"]  # on a tie, the earlier option wins

def rule_decide(tools: dict) -> dict:
    """Pick WAIT, WALK or BIKE from the tool results, and say which options counted."""
    jp, pos, bikes, weather = tools["route_planner"], tools["positions"], tools["city_bikes"], tools["weather"]
    options = {"WALK": jp["walk_min"]}
    tram_live = pos.get("feed_live") is True and pos.get("tram_late_min") is not None
    if tram_live:
        options["WAIT"] = jp["next_tram_due_min"] + pos["tram_late_min"] + jp["tram_ride_min"]
    if bikes["bikes_at_dock"] > 0 and weather["rain_mm_next_hour"] < RAIN_LIMIT_MM:
        options["BIKE"] = bikes["bike_min"]
    ranked = sorted(options.items(), key=lambda kv: (kv[1], TIE_ORDER.index(kv[0])))
    margin = ranked[1][1] - ranked[0][1] if len(ranked) > 1 else None
    return {"advice": ranked[0][0], "minutes": options, "margin_min": margin, "tram_counted": tram_live}

# Unit tests: one per clause of the policy.
assert rule_decide(SCENARIOS[0]["tools"])["advice"] == "WAIT"    # tram fastest
assert rule_decide(SCENARIOS[1]["tools"])["advice"] == "WALK"    # no bikes at the dock
assert rule_decide(SCENARIOS[4]["tools"])["advice"] == "WALK"    # bike fastest, but raining
assert rule_decide(SCENARIOS[7]["tools"])["advice"] == "WALK"    # the lateness flips it
assert rule_decide(SCENARIOS[9]["tools"])["advice"] == "WALK"    # a tie goes to walking
assert rule_decide(SCENARIOS[12]["tools"])["tram_counted"] is False  # feed flagged down
assert rule_decide(SCENARIOS[14]["tools"])["tram_counted"] is False  # feed empty, no flag
RULE_TESTS = 7
print(f"{RULE_TESTS} rule tests passed")

for s in SCENARIOS:
    r = rule_decide(s["tools"])
    s["rule"] = r
    if s["group"] == "feed live":
        s["call"] = "close" if r["margin_min"] <= 1 else "clear"
    else:
        s["call"] = "feed down"

display(pd.DataFrame([
    {"scenario": s["id"], "trip": f"{s['tools']['route_planner']['from']} to {s['tools']['route_planner']['to']}",
     "group": s["group"], "call": s["call"], "rule says": s["rule"]["advice"],
     "minutes by option": json.dumps(s["rule"]["minutes"]), "margin (min)": s["rule"]["margin_min"]}
    for s in SCENARIOS
]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The two prompts
# MAGIC
# MAGIC Both splits send the same tool results as JSON. Split 1 is the article's design: the model is handed the rule's advice and writes a one-line explanation. In split 2 it reads the policy from the cell above, word for word, and makes the call itself. Neither prompt changes between models.

# COMMAND ----------

# DBTITLE 1,Prompts
POLICY = (
    "Take whichever option gets the rider there first. "
    "Count the tram only when the position feed is live and gives a lateness reading, and add that lateness to the wait. "
    f"Count the bike only when the dock has a bike and the forecast is under {RAIN_LIMIT_MM} mm of rain in the next hour. "
    "On a tie, walk."
)

EXPLAIN_SYSTEM = (
    "You explain a transit recommendation to a rider in Helsinki. The advice has already been decided; "
    "don't change it. Write one sentence that explains it from the tool results."
)

DECIDE_SYSTEM = (
    "You advise a rider in Helsinki whether to WAIT for the tram, WALK, or take a city BIKE. "
    f"Policy: {POLICY}\n"
    "Answer in exactly two lines:\nADVICE: WAIT or WALK or BIKE\nWHY: one sentence."
)

def tool_block(s: dict) -> str:
    """The tool results as the model sees them."""
    return "Tool results:\n" + json.dumps(s["tools"], ensure_ascii=False, indent=2)

ADVICE_RE = re.compile(r"ADVICE:\W*(WAIT|WALK|BIKE)", re.IGNORECASE)

def parse_advice(text: str) -> list:
    """Every ADVICE line in the answer, in order. The first is what an app reading the format acts on."""
    return [a.upper() for a in ADVICE_RE.findall(text or "")] or ["UNPARSED"]

print("Split 2 system prompt:\n" + DECIDE_SYSTEM)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run both splits
# MAGIC
# MAGIC Split 1 asks each model once per scenario for an explanation; the advice is the rule's, so it can't move. Split 2 asks each model three times per scenario, at temperature 0, for the advice itself. That's 48 calls for split 1 and 144 for split 2, four at a time. Every call becomes a row in `calls`.

# COMMAND ----------

# DBTITLE 1,Run
def run_one(job: tuple) -> dict:
    """One call: (split, model label, scenario, repeat)."""
    split, label, s, rep = job
    if split == "rule decides":
        user = tool_block(s) + f"\n\nAdvice: {s['rule']['advice']}"
        out = call_model(MODELS[label], EXPLAIN_SYSTEM, user, max_tokens=600)
        lines = [s["rule"]["advice"]]
    else:
        out = call_model(MODELS[label], DECIDE_SYSTEM, tool_block(s), max_tokens=600)
        lines = parse_advice(out["text"])
    advice, last_advice = lines[0], lines[-1]
    return {
        "split": split, "model": label, "scenario": s["id"], "group": s["group"], "call": s["call"],
        "repeat": rep, "rule_advice": s["rule"]["advice"], "advice": advice,
        "matches_rule": advice == s["rule"]["advice"], "last_advice": last_advice,
        "last_matches_rule": last_advice == s["rule"]["advice"], "changed_its_mind": len(set(lines)) > 1,
        "text": out["text"],
        "prompt_tokens": out["prompt_tokens"], "completion_tokens": out["completion_tokens"],
        "latency_ms": out["latency_ms"],
    }

jobs = [("rule decides", m, s, 1) for m in MODELS for s in SCENARIOS]
jobs += [("model decides", m, s, r) for m in MODELS for s in SCENARIOS for r in range(1, REPEATS + 1)]

started = time.perf_counter()
with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    rows = list(pool.map(run_one, jobs))
print(f"{len(rows)} calls in {time.perf_counter() - started:.0f}s")

calls = pd.DataFrame(rows)
calls["total_tokens"] = calls["prompt_tokens"] + calls["completion_tokens"]
spark.createDataFrame(calls).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.calls")
print(f"Saved to {CATALOG}.{SCHEMA}.calls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Every call, scenario by scenario
# MAGIC
# MAGIC One row per scenario, one column per decider. The first column is the rule; the other three are each model making the call itself. A cell is the advice the model gave on most of its three tries, and it turns red when that advice isn't the rule's. A dot in the corner means the three tries didn't all agree.

# COMMAND ----------

# DBTITLE 1,Every call, scenario by scenario
# hide-code
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

OAT_LIGHT, OAT, NAVY, NAVY_500, GRAY_TEXT, GRAY_LINES, LAVA, WHITE = (
    "#F9F7F4", "#EEEDE9", "#1B3139", "#618794", "#5A6F77", "#DCE0E2", "#FF3621", "#FFFFFF")
plt.rcParams["font.family"] = "DejaVu Sans"  # ships with matplotlib, so serverless has it

decided = calls[calls["split"] == "model decides"]
majority = decided.groupby(["scenario", "model"])["advice"].agg(lambda a: a.value_counts().index[0]).unstack()
wobbly = decided.groupby(["scenario", "model"])["advice"].nunique().unstack() > 1
columns = ["rule"] + list(MODELS)

fig, ax = plt.subplots(figsize=(10, 9.2), facecolor=OAT_LIGHT)
ax.set_facecolor(OAT_LIGHT)
for r, s in enumerate(SCENARIOS):
    y = len(SCENARIOS) - 1 - r
    for c, col in enumerate(columns):
        advice = s["rule"]["advice"] if col == "rule" else majority.loc[s["id"], col]
        miss = col != "rule" and advice != s["rule"]["advice"]
        face = LAVA if miss else (NAVY if col == "rule" else OAT)
        ink = WHITE if (miss or col == "rule") else NAVY
        ax.add_patch(Rectangle((c + 0.04, y + 0.06), 0.92, 0.88, facecolor=face, edgecolor=OAT_LIGHT))
        ax.text(c + 0.5, y + 0.5, advice, ha="center", va="center", fontsize=10, fontweight="bold", color=ink)
        if col != "rule" and wobbly.loc[s["id"], col]:
            ax.plot(c + 0.86, y + 0.8, "o", markersize=4, color=ink)
    trip = s["tools"]["route_planner"]
    ax.text(-0.1, y + 0.5, f"{s['id']}  {trip['from']} to {trip['to']}", ha="right", va="center", fontsize=9, color=NAVY)
    ax.text(len(columns) + 0.1, y + 0.5, s["call"], ha="left", va="center", fontsize=9, color=GRAY_TEXT)
for c, col in enumerate(columns):
    ax.text(c + 0.5, len(SCENARIOS) + 0.25, "Rule" if col == "rule" else col, ha="center", va="bottom",
            fontsize=10, fontweight="bold", color=NAVY)
for boundary in (4, 10):  # between the call groups, counted from the bottom row
    ax.plot([-3.2, len(columns) + 1.2], [boundary, boundary], color=GRAY_LINES, linewidth=1, clip_on=False)
ax.set_xlim(0, len(columns))
ax.set_ylim(0, len(SCENARIOS) + 1)
ax.axis("off")
fig.suptitle("Who made which call", x=0.02, ha="left", fontsize=15, fontweight="bold", color=NAVY)
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Did the model's call match the rule?
# MAGIC
# MAGIC The table below counts, per model, how many split 2 answers matched the rule, by kind of call. A model that follows the written policy perfectly matches every time. `UNPARSED` means the answer had no readable `ADVICE:` line, and it counts as a miss.
# MAGIC
# MAGIC The advice that counts is the first `ADVICE:` line, because that's what an app reading the two-line format would act on. Some answers write a second `ADVICE:` line after reworking the numbers, so the notebook also counts those and checks how many matched the rule on their last line.

# COMMAND ----------

# DBTITLE 1,Agreement with the rule
decides = calls[calls["split"] == "model decides"]
print("How often each model's own call matched the rule, by kind of call")
agreement = (
    decides.groupby(["model", "call"])["matches_rule"].agg(["sum", "count"])
    .rename(columns={"sum": "matched the rule", "count": "answers"}).reset_index()
)
display(agreement)

print("Answers that changed their advice partway through, and whether the last line matched the rule")
display(
    decides.groupby("model")
    .agg(changed_its_mind=("changed_its_mind", "sum"), first_line_matched=("matches_rule", "sum"),
         last_line_matched=("last_matches_rule", "sum"), answers=("matches_rule", "size"))
    .reset_index()
)

print("Answers that went against the rule (first 140 characters of each)")
misses = decides[~decides["matches_rule"]].assign(text=lambda f: f["text"].str.replace("\n", " ").str.slice(0, 140))
display(misses[["model", "scenario", "call", "repeat", "rule_advice", "advice", "last_advice", "text"]])

# COMMAND ----------

# DBTITLE 1,Matched the rule, by kind of call
# hide-code
kinds = ["clear", "close", "feed down"]
shades = {"gpt-oss-120b": NAVY, "llama-4-maverick": NAVY_500, "llama-3-1-8b": GRAY_LINES}
fig, ax = plt.subplots(figsize=(10, 4.6), facecolor=OAT_LIGHT)
ax.set_facecolor(OAT_LIGHT)
width = 0.26
for i, label in enumerate(MODELS):
    for k, kind in enumerate(kinds):
        part = decides[(decides["model"] == label) & (decides["call"] == kind)]
        share = part["matches_rule"].mean() * 100
        x = k + (i - 1) * width
        ax.bar(x, share, width * 0.92, color=shades[label], label=label if k == 0 else None)
        ax.text(x, share + 2, f"{int(part['matches_rule'].sum())}/{len(part)}", ha="center", va="bottom", fontsize=9, color=NAVY)
ax.set_xticks(range(len(kinds)))
ax.set_xticklabels(["Clear calls", "Close calls (1 minute or less)", "Tram feed down"], fontsize=10, color=NAVY)
ax.set_ylim(0, 112)
ax.set_ylabel("Answers that matched the rule (%)", fontsize=10, color=GRAY_TEXT)
ax.tick_params(colors=GRAY_TEXT, length=0)
for side in ("top", "right", "left"):
    ax.spines[side].set_visible(False)
ax.spines["bottom"].set_color(GRAY_LINES)
ax.yaxis.grid(True, color=GRAY_LINES, linewidth=0.8)
ax.set_axisbelow(True)
ax.set_title("When the model makes the call, how often it matches the rule", loc="left", fontsize=14, fontweight="bold", color=NAVY)
ax.legend(frameon=False, ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.12), fontsize=9)
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Swap the model: does the advice move?
# MAGIC
# MAGIC The article says: "The model picks the tools and explains the result, so a different model can change the wording but not the advice." Here's that measured both ways. A scenario counts as "same advice from all three models" when every answer from every model for it is the same option, and "steady" for one model when its three repeats agree.

# COMMAND ----------

# DBTITLE 1,Same advice across models
def all_same(frame: pd.DataFrame) -> int:
    """Count the scenarios where every answer in frame is the same option."""
    return int((frame.groupby("scenario")["advice"].nunique() == 1).sum())

rule_split = calls[calls["split"] == "rule decides"]
same_rule = all_same(rule_split)
same_model = all_same(decides)
distinct_wording = int(rule_split.groupby("scenario")["text"].nunique().mean().round())

steady = {m: int((g.groupby("scenario")["advice"].nunique() == 1).sum()) for m, g in decides.groupby("model")}

print("Scenarios where all three models gave the same advice")
display(pd.DataFrame([
    {"split": "rule decides", "same advice from all three models": same_rule, "scenarios": len(SCENARIOS)},
    {"split": "model decides", "same advice from all three models": same_model, "scenarios": len(SCENARIOS)},
]))
print(f"Rule decides: typical number of different explanations per scenario across the three models: {distinct_wording}")
print("Model decides: scenarios where a model's three repeats agreed:", steady)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The feed-down trips
# MAGIC
# MAGIC In all four, the scheduled tram looks fastest, and the policy says not to count it. The table shows how many split 2 answers still said to wait, with the feed flagged as down and with the feed silently empty.

# COMMAND ----------

# DBTITLE 1,Tram advice with no live feed
feed_down = decides[decides["call"] == "feed down"]
print("Answers that said WAIT with no live tram feed")
tram_anyway = (
    feed_down.assign(said_wait=feed_down["advice"] == "WAIT")
    .groupby(["model", "group"])["said_wait"].agg(["sum", "count"])
    .rename(columns={"sum": "said WAIT", "count": "answers"}).reset_index()
)
display(tram_anyway)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What each split costs
# MAGIC
# MAGIC Tokens and time per call, typical (the median) across every call in the split. In split 1 the decision itself costs nothing: the rule runs in microseconds and the model only writes the sentence.

# COMMAND ----------

# DBTITLE 1,Tokens and latency
cost = (
    calls.groupby(["split", "model"])
    .agg(typical_tokens=("total_tokens", "median"), typical_latency_ms=("latency_ms", "median"), calls=("latency_ms", "size"))
    .reset_index()
)
display(cost)

# COMMAND ----------

# DBTITLE 1,Tokens and time per call
# hide-code
fig, axes = plt.subplots(1, 2, figsize=(10, 4.2), facecolor=OAT_LIGHT)
for ax, (column, title) in zip(axes, [("typical_tokens", "Tokens per call (typical)"), ("typical_latency_ms", "Time per call, ms (typical)")]):
    ax.set_facecolor(OAT_LIGHT)
    for i, label in enumerate(MODELS):
        for j, (split, shade) in enumerate([("rule decides", NAVY), ("model decides", NAVY_500)]):
            value = float(cost[(cost["split"] == split) & (cost["model"] == label)][column].iloc[0])
            y = i + (j - 0.5) * 0.36
            ax.barh(y, value, 0.33, color=shade, label=split if i == 0 else None)
            ax.text(value, y, f" {value:,.0f}", va="center", fontsize=9, color=NAVY)
    ax.set_yticks(range(len(MODELS)))
    ax.set_yticklabels(list(MODELS), fontsize=10, color=NAVY)
    ax.invert_yaxis()
    ax.set_title(title, loc="left", fontsize=12, fontweight="bold", color=NAVY)
    ax.tick_params(colors=GRAY_TEXT, length=0)
    ax.set_xticks([])
    for side in ("top", "right", "bottom", "left"):
        ax.spines[side].set_visible(False)
axes[0].legend(frameon=False, ncol=2, loc="upper center", bbox_to_anchor=(1.1, -0.06), fontsize=9)
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
import html

OAT_LIGHT, OAT, WHITE = "#F9F7F4", "#EEEDE9", "#FFFFFF"
NAVY, NAVY_600, GRAY_TEXT, GRAY_LINES = "#1B3139", "#1B5162", "#5A6F77", "#DCE0E2"
LAVA = "#FF3621"
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"
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
    """A rule whose length carries a value. pop=True draws it in lava."""
    width = max(4, 100 * value / largest) if largest else 4
    return f"<div style='height:3px;background:{LAVA if pop else NAVY};width:{width:.0f}%;margin:10px 0 12px'></div>"


def row(name: str, count=None, tag: str = "", tag_pop: bool = False, note: str = "", rule: str = "", body: str = "") -> str:
    """One group: a hairline, its name, a tag, a note, a count, a measured rule, then its body."""
    tag_html = f"<span style='font-size:12px;font-weight:700;color:{LAVA if tag_pop else NAVY_600}'>{esc(tag)}</span>" if tag else ""
    note_html = f"<span style='font-size:12px;color:{GRAY_TEXT}'>{esc(note)}</span>" if note else ""
    count_html = f"<span style='margin-left:auto;font-size:30px;font-weight:700;color:{NAVY}'>{esc(count)}</span>" if count is not None else ""
    return (f"<div style='padding:22px 0 18px;border-top:1px solid {GRAY_LINES}'>"
            f"<div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap'>"
            f"<span style='font-size:22px;font-weight:700;color:{NAVY}'>{esc(name)}</span>{tag_html}{note_html}{count_html}</div>"
            f"{rule}{body}</div>")


def panel(headline: str, dek: str, body: str) -> str:
    """The whole display: oat ground, navy top edge, DM Sans, a headline, a dek, then the body."""
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 30px;font-family:{SANS};"
            f"border-top:6px solid {NAVY}'>"
            f"<div style='font-size:40px;font-weight:700;line-height:1.1;max-width:760px'>{esc(headline)}</div>"
            f"<div style='font-size:17px;color:{GRAY_TEXT};line-height:1.45;max-width:680px;margin:12px 0 26px'>{dek}</div>"
            f"{body}</div>")


per_model = decides.groupby("model")["matches_rule"].agg(["sum", "count"])
close = decides[decides["call"] == "close"].groupby("model")["matches_rule"].agg(["sum", "count"])
wait_no_feed = feed_down.groupby("model")["advice"].apply(lambda a: int((a == "WAIT").sum()))

body = stats(
    stat(same_rule, "scenarios with the same advice from all three models, rule deciding", out_of=len(SCENARIOS)),
    stat(same_model, "scenarios with the same advice from all three models, model deciding", out_of=len(SCENARIOS)),
    stat(int(wait_no_feed.sum()), "model answers that said wait for a tram with no live feed", out_of=len(feed_down)),
)
for label in MODELS:
    matched, total = int(per_model.loc[label, "sum"]), int(per_model.loc[label, "count"])
    c_matched, c_total = int(close.loc[label, "sum"]), int(close.loc[label, "count"])
    misses = total - matched
    body += row(
        label, count=f"{matched}/{total}", tag=f"{misses} against the rule" if misses else "matched every time",
        tag_pop=misses > 0, note=f"close calls {c_matched}/{c_total}, waited with no feed {int(wait_no_feed.get(label, 0))}",
        rule=measured_rule(matched, total, pop=misses > 0),
    )

displayHTML(panel(
    "Who makes the call",
    "The same sixteen sets of tool results, decided by a seven-test Python rule and by each model reading the same policy. "
    "Each row is one model deciding for itself: how many of its answers matched the rule.",
    body,
))

# COMMAND ----------

# DBTITLE 1,Results
def by_model(frame: pd.DataFrame) -> dict:
    """matched/total per model label for one slice of split 2."""
    g = frame.groupby("model")["matches_rule"].agg(["sum", "count"])
    return {m: {"matched": int(g.loc[m, "sum"]), "answers": int(g.loc[m, "count"])} for m in g.index}

results = {
    "models": MODELS,
    "temperature": TEMPERATURE,
    "scenarios": len(SCENARIOS),
    "scenario_calls": {k: int(v) for k, v in pd.Series([s["call"] for s in SCENARIOS]).value_counts().items()},
    "rule": {"unit_tests_passed": RULE_TESTS},
    "model_decides": {
        "repeats": REPEATS,
        "agreement": by_model(decides),
        "agreement_clear": by_model(decides[decides["call"] == "clear"]),
        "agreement_close": by_model(decides[decides["call"] == "close"]),
        "unparsed": int((decides["advice"] == "UNPARSED").sum()),
        "changed_its_mind": {m: int(v) for m, v in decides.groupby("model")["changed_its_mind"].sum().items()},
        "agreement_last_line": {m: {"matched": int(g["last_matches_rule"].sum()), "answers": int(len(g))} for m, g in decides.groupby("model")},
        "steady_scenarios": steady,
    },
    "same_advice_all_models": {"rule_decides": same_rule, "model_decides": same_model},
    "rule_decides_distinct_wording": distinct_wording,
    "feed_down": {
        "answers": int(len(feed_down)),
        "said_wait_total": int(wait_no_feed.sum()),
        "said_wait_flagged": int(((feed_down["group"] == "feed down, flagged") & (feed_down["advice"] == "WAIT")).sum()),
        "said_wait_silent": int(((feed_down["group"] == "feed down, silent") & (feed_down["advice"] == "WAIT")).sum()),
        "said_wait_by_model": {m: int(v) for m, v in wait_no_feed.items()},
    },
    "typical_tokens": {f"{r.split}|{r.model}": int(r.typical_tokens) for r in cost.itertuples()},
    "typical_latency_ms": {f"{r.split}|{r.model}": int(r.typical_latency_ms) for r in cost.itertuples()},
    "latency_calls_per_cell": {f"{r.split}|{r.model}": int(r.calls) for r in cost.itertuples()},
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC The results suggest keeping the call in the rule and letting the model explain it, which is how the article's agent already works.
# MAGIC
# MAGIC With the rule deciding, all three models gave the same advice on all 16 scenarios. A typical scenario got 3 different explanations from the three models, so the wording moved and the advice didn't. With each model making the call from the same policy in words, the three agreed on 3 of 16.
# MAGIC
# MAGIC Deciding for itself, gpt-oss-120b matched the rule on 47 of 48 answers, and on 18 of 18 close calls. Llama 4 Maverick matched 24 of 48, and Llama 3.1 8B matched 9. Every answer had a readable `ADVICE:` line, so each miss was a different call rather than a format problem. Maverick's misses were all wait where the rule said bike: on one trip it added the tram up to 15 minutes, against a 14-minute bike ride, and still said wait. The 8B said wait every time. Maverick also reworked its numbers and wrote a second `ADVICE:` line in 3 answers; counted by that last line, it matched 27.
# MAGIC
# MAGIC The feed-down trips are where the article's own agent tripped, and they're where the models did worst. With the tram feed down, 22 of 36 answers still told the rider to wait for the tram: 13 of 18 when the feed came back silently empty, and 9 of 18 even when the tool said the feed was down. gpt-oss-120b did it once, on a silent feed. The rule never counts that tram, flag or no flag.
# MAGIC
# MAGIC Temperature 0 didn't make every answer repeat, either. gpt-oss-120b gave the same advice on all three tries in 15 of 16 scenarios, so if you let a model decide, test it more than once.
# MAGIC
# MAGIC Explaining the rule's call also took fewer tokens than making it, for every model: gpt-oss-120b used 469 tokens per call, typical, to explain and 591 to decide.
# MAGIC
# MAGIC One caveat: the tool results for six Helsinki trips are set up in the notebook, not read from a live feed, and the rule and policy are mine, modelled on the article's description rather than its code.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC The only thing the notebook created is the `workspace.tram_rules_vs_models` schema and its `calls` table. To keep querying the run, set the `keep_tables` widget at the top to `true` before you run it, and this cell leaves them alone.

# COMMAND ----------

# DBTITLE 1,Drop the schema
if KEEP_TABLES:
    print(f"keep_tables is true: left {CATALOG}.{SCHEMA}.calls in place")
else:
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    print(f"Dropped {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Function calling on Databricks](https://docs.databricks.com/aws/en/machine-learning/model-serving/function-calling): how a model picks tools on Foundation Model APIs, the part the article keeps with the model.
# MAGIC - [SQL and Python UDFs in Unity Catalog](https://docs.databricks.com/aws/en/udf/unity-catalog): where a rule like this one can live as a governed function, callable from SQL, notebooks and agents.
# MAGIC - [Structured outputs on Databricks](https://docs.databricks.com/aws/en/machine-learning/model-serving/structured-outputs): constrain a model's answer to a schema when it does make the call.
# MAGIC - [Supported Foundation Model APIs models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models): the endpoints to swap into `MODELS`.
# MAGIC - [Databricks Apps](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/): where the article's agent loop runs.
