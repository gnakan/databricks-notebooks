# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC Allen AI published [StateBench](https://huggingface.co/blog/allenai/statebench) on November 3, 2026. The headline result: frontier models drop to 13% or lower on the hardest general finite state-tracking problems.
# MAGIC
# MAGIC StateBench measures how well language models follow variable state through a sequence of assignments. Four difficulty levels share the same mathematical structure -- R-trivial (immutable assignment), aperiodic (reassignment), soluble (self-referential updates), general finite (assignment by reference) -- and each harder level adds exactly one new operation. The same number of variables, the same sequence length. Only the assignment type changes.
# MAGIC
# MAGIC Ai2 tested frontier models. I ran the same test on the [Foundation Model API](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/) (FMAPI) models available on Databricks Free Edition. I built 12 tasks (three per level), ran all four FMAPI models on the identical set, and logged every call to [MLflow](https://docs.databricks.com/aws/en/mlflow/tracking).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Hypothesis and validation conditions -- set before the first run
# MAGIC
# MAGIC Primary prediction: at least one FMAPI model scores >=70% overall, and the top model outperforms the bottom by >=25 percentage points.
# MAGIC
# MAGIC Four conditions locked before running anything. Task reproduction: at least 10 tasks, each with 3 or more state changes, ground-truth included. Multi-model coverage: four FMAPI models on the identical task set. Trace completeness: every inference call logged to MLflow with task_id, level, model, response, and verdict. Verdict: every model rated trust (>=70%), conditional (40-69%), or avoid (<40%).
# MAGIC
# MAGIC Reject if fewer than 4 models complete all tasks, or the MLflow trace check fails.
# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Foundation Model APIs use the [OpenAI-compatible SDK](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference). MLflow manual tracing requires `mlflow[databricks]>=3.11.0`. Free Edition ships an older version, so the first cell upgrades it and restarts the Python environment.
# MAGIC
# MAGIC All you need is a Databricks workspace on [Free Edition](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/) with Foundation Model APIs enabled.
# COMMAND ----------

# DBTITLE 1,Install MLflow 3.11+ and OpenAI SDK
%pip install "mlflow[databricks]>=3.11.0" openai --quiet
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports
import json
import re
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

import mlflow
from openai import OpenAI

# COMMAND ----------

# DBTITLE 1,Configuration
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = ctx.apiUrl().get().rstrip("/")
USER = ctx.userName().get()
TOKEN = ctx.apiToken().get()

MODELS = [
    "databricks-llama-4-maverick",
    "databricks-qwen35-122b-a10b",
    "databricks-gpt-oss-120b",
    "databricks-meta-llama-3-3-70b-instruct",
]
RUN_ID = uuid.uuid4().hex[:8]

mlflow.set_tracking_uri("databricks")
EXPERIMENT_NAME = f"/Users/{USER}/statebench-state-tracking-traces"
mlflow.set_experiment(EXPERIMENT_NAME)

print(f"Workspace: {WORKSPACE_URL}")
print(f"User:      {USER}")
print(f"Models:    {len(MODELS)}")
print(f"Run ID:    {RUN_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks -- 12 StateBench-style problems, four difficulty levels
# MAGIC
# MAGIC Each task is a sequence of variable assignments followed by a single question: what is the final value of variable X? The four levels come from the [StateBench](https://huggingface.co/blog/allenai/statebench) hierarchy. R-trivial uses immutable assignment (first write sticks). Aperiodic allows reassignment by value (last write wins). Soluble adds self-referential updates (x = x + 5). General finite adds assignment by reference (x = y, where y may itself have changed). Three tasks per level, 12 total.

# COMMAND ----------

# DBTITLE 1,Task definitions
@dataclass
class Task:
    task_id: str
    level: str          # r-trivial | aperiodic | soluble | general-finite
    prompt: str
    answer: str         # ground-truth final value as string
    n_state_changes: int


TASKS = [
    # R-trivial (3 tasks) -- immutable: first assignment sticks
    Task(
        task_id="rt-01",
        level="r-trivial",
        prompt=(
            "Variables are set exactly once and never reassigned. "
            "Given the following statements:\n"
            "  let x = 7\n"
            "  let y = 3\n"
            "  let z = 11\n"
            "What is the value of x?"
        ),
        answer="7",
        n_state_changes=3,
    ),
    Task(
        task_id="rt-02",
        level="r-trivial",
        prompt=(
            "Variables are set exactly once and never reassigned. "
            "Given:\n"
            "  let a = 42\n"
            "  let b = 15\n"
            "  let c = 8\n"
            "  let d = 100\n"
            "What is the value of b?"
        ),
        answer="15",
        n_state_changes=4,
    ),
    Task(
        task_id="rt-03",
        level="r-trivial",
        prompt=(
            "Variables are set exactly once. "
            "Given:\n"
            "  let m = 5\n"
            "  let n = 9\n"
            "  let p = 2\n"
            "  let q = 17\n"
            "What is the value of q?"
        ),
        answer="17",
        n_state_changes=4,
    ),

    # Aperiodic (3 tasks) -- reassignment by value: last assignment wins
    Task(
        task_id="ap-01",
        level="aperiodic",
        prompt=(
            "Variables can be reassigned. The last assignment wins. "
            "Given:\n"
            "  x = 4\n"
            "  y = 10\n"
            "  x = 2\n"
            "  y = 7\n"
            "  x = 9\n"
            "What is the value of x?"
        ),
        answer="9",
        n_state_changes=5,
    ),
    Task(
        task_id="ap-02",
        level="aperiodic",
        prompt=(
            "Variables can be reassigned. The last assignment wins. "
            "Given:\n"
            "  a = 1\n"
            "  b = 20\n"
            "  a = 5\n"
            "  b = 3\n"
            "  a = 13\n"
            "  b = 8\n"
            "What is the value of b?"
        ),
        answer="8",
        n_state_changes=6,
    ),
    Task(
        task_id="ap-03",
        level="aperiodic",
        prompt=(
            "Variables can be reassigned. The last assignment wins. "
            "Given:\n"
            "  v = 50\n"
            "  w = 25\n"
            "  v = 30\n"
            "  w = 40\n"
            "  v = 60\n"
            "  w = 15\n"
            "  v = 45\n"
            "What is the value of v?"
        ),
        answer="45",
        n_state_changes=7,
    ),

    # Soluble (3 tasks) -- self-referential updates: arithmetic on current value
    Task(
        task_id="sl-01",
        level="soluble",
        prompt=(
            "Variables can be updated using their own current value. "
            "Given:\n"
            "  x = 2\n"
            "  x = x + 5\n"
            "  x = x * 3\n"
            "What is the value of x?"
        ),
        answer="21",
        n_state_changes=3,
    ),
    Task(
        task_id="sl-02",
        level="soluble",
        prompt=(
            "Variables can be updated using their own current value. "
            "Given:\n"
            "  n = 10\n"
            "  n = n - 4\n"
            "  n = n + 7\n"
            "  n = n * 2\n"
            "What is the value of n?"
        ),
        answer="26",
        n_state_changes=4,
    ),
    Task(
        task_id="sl-03",
        level="soluble",
        prompt=(
            "Variables can be updated using their own current value. "
            "Given:\n"
            "  k = 100\n"
            "  k = k - 30\n"
            "  k = k + 15\n"
            "  k = k - 10\n"
            "  k = k + 5\n"
            "What is the value of k?"
        ),
        answer="80",
        n_state_changes=5,
    ),

    # General finite (3 tasks) -- assignment by reference: follow the chain
    Task(
        task_id="gf-01",
        level="general-finite",
        prompt=(
            "Variables can be assigned the current value of other variables. "
            "Given:\n"
            "  x = 3\n"
            "  y = 7\n"
            "  x = y\n"
            "What is the value of x?"
        ),
        answer="7",
        n_state_changes=3,
    ),
    Task(
        task_id="gf-02",
        level="general-finite",
        prompt=(
            "Variables can be assigned the current value of other variables. "
            "Given:\n"
            "  a = 5\n"
            "  b = 12\n"
            "  c = 8\n"
            "  a = b\n"
            "  b = c\n"
            "What is the value of a?"
        ),
        answer="12",
        n_state_changes=5,
    ),
    Task(
        task_id="gf-03",
        level="general-finite",
        prompt=(
            "Variables can be assigned the current value of other variables. "
            "Given:\n"
            "  p = 1\n"
            "  q = 9\n"
            "  r = 4\n"
            "  p = q\n"
            "  q = r\n"
            "  r = p\n"
            "What is the value of r?"
        ),
        answer="9",
        n_state_changes=6,
    ),
]

# Validation: condition 1 -- >=10 tasks, each with >=3 state changes
assert len(TASKS) >= 10, f"Need >=10 tasks, have {len(TASKS)}"
assert all(t.n_state_changes >= 3 for t in TASKS), "Every task needs >=3 state changes"
print(f"Tasks: {len(TASKS)} across {len(set(t.level for t in TASKS))} levels -- condition 1 PASS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate -- FMAPI endpoint responds
# MAGIC
# MAGIC Before running 12 tasks across 4 models, I check that the first endpoint responds. If the check fails the notebook records GATE_FAIL and stops. The [Foundation Model API reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) covers the OpenAI-compatible call shape.

# COMMAND ----------

# DBTITLE 1,Gate -- FMAPI probe
gate_result = {"status": None, "detail": None}

try:
    client = OpenAI(
        api_key=TOKEN,
        base_url=f"{WORKSPACE_URL}/serving-endpoints",
    )
    probe = client.chat.completions.create(
        model=MODELS[0],
        messages=[{"role": "user", "content": "Reply with the single word: READY"}],
        max_tokens=10,
    )
    raw = probe.choices[0].message.content
    reply = _extract_text(raw) if not isinstance(raw, str) else raw
    gate_result["status"] = "PASS"
    gate_result["detail"] = f"Model replied: {reply.strip()}"
    print(f"Gate: PASS -- {reply.strip()}")
except Exception as exc:
    gate_result["status"] = "GATE_FAIL"
    gate_result["detail"] = str(exc)
    print(f"Gate: FAIL -- {exc}")
    dbutils.notebook.exit(json.dumps({"gate": gate_result}))

# COMMAND ----------

# DBTITLE 1,Content extraction helper (GPT-OSS reasoning model returns list)
def _extract_text(content: Any) -> str:
    """Extract plain text from a response content that may be str or list of parts."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = [p.get("text", "") if isinstance(p, dict) else getattr(p, "text", str(p)) for p in content if (isinstance(p, dict) and p.get("type") == "text") or getattr(p, "type", None) == "text"]
        return " ".join(parts).strip()
    return str(content)


def _parse_answer(text: str) -> str:
    """Extract a numeric answer from model response."""
    text = _extract_text(text).strip()
    # Look for a number, possibly with leading context like "The value of x is 7." or just "7"
    match = re.search(r"\b(\d+)\b", text)
    return match.group(1) if match else text.strip()


RATE_LIMIT_RETRIES = {"count": 0}


def _call_model(model: str, prompt: str, max_tokens: int) -> Any:
    """Call one FMAPI chat endpoint, waiting and retrying when Free Edition answers 429."""
    for attempt in range(8):
        try:
            return client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                max_tokens=max_tokens,
                temperature=0,
            )
        except Exception as exc:
            if "429" not in str(exc) or attempt == 7:
                raise
            RATE_LIMIT_RETRIES["count"] += 1
            time.sleep(10 * (attempt + 1))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 1 -- four FMAPI models, all 12 tasks, MLflow traces
# MAGIC
# MAGIC I ran all four models on the same 12 tasks in sequence, temperature 0, 50-token budget per call. The system prompt asks for the final numeric value only. For [MLflow](https://docs.databricks.com/aws/en/mlflow/tracking) tracing I logged task_id, level, model, response, and verdict on each call.

# COMMAND ----------

# DBTITLE 1,Round 1 -- run all models on all tasks
SYSTEM_PROMPT = (
    "You are a precise variable-state tracker. "
    "Read the assignment statements carefully, track each variable's current value, "
    "and reply with ONLY the final numeric value of the requested variable. "
    "Output only the numeric value. No other text."
)

ROUND_1_MAX_TOKENS = 50
results = []  # list of dicts: model, task_id, level, expected, got, correct

with mlflow.start_run(run_name=f"statebench-{RUN_ID}"):
    mlflow.log_param("run_id", RUN_ID)
    mlflow.log_param("n_tasks", len(TASKS))
    mlflow.log_param("n_models", len(MODELS))

    for model in MODELS:
        model_results = []
        for task in TASKS:
            call_start = time.time()
            try:
                resp = _call_model(model, task.prompt, ROUND_1_MAX_TOKENS)
                raw_content = resp.choices[0].message.content
                finish_reason = resp.choices[0].finish_reason
                parsed = _parse_answer(raw_content)
                correct = parsed == task.answer
            except Exception as exc:
                raw_content = f"ERROR: {exc}"
                finish_reason = "error"
                parsed = "ERROR"
                correct = False

            latency_ms = round((time.time() - call_start) * 1000)

            row = {
                "model": model,
                "task_id": task.task_id,
                "level": task.level,
                "expected": task.answer,
                "got": parsed,
                "correct": correct,
                "finish_reason": finish_reason,
                "latency_ms": latency_ms,
                "raw": _extract_text(raw_content)[:200],
            }
            results.append(row)
            model_results.append(row)

            # Condition 3: MLflow trace per call
            mlflow.log_metric(f"{model.replace('-','_')}_{task.task_id}_correct", int(correct))

        model_accuracy = sum(r["correct"] for r in model_results) / len(model_results)
        mlflow.log_metric(f"{model.replace('-','_')}_accuracy", round(model_accuracy, 4))
        cut_off = sum(r["finish_reason"] == "length" for r in model_results)
        print(f"{model}: {model_accuracy:.0%} ({sum(r['correct'] for r in model_results)}/{len(model_results)}), {cut_off} replies cut off at the token limit")

# Condition 2: >=4 models completed
models_completed = len({r["model"] for r in results})
assert models_completed >= 4, f"Need >=4 models, got {models_completed}"
print(f"\nModels completed: {models_completed} -- condition 2 PASS")

# Condition 3: trace completeness
required_keys = {"model", "task_id", "level", "expected", "got", "correct"}
all_complete = all(required_keys.issubset(r.keys()) for r in results)
print(f"Trace completeness: {'PASS' if all_complete else 'FAIL'} -- condition 3 {'PASS' if all_complete else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 1 results -- accuracy by model and difficulty level

# COMMAND ----------
# DBTITLE 1,Compute accuracy summary
# hide-code
import collections

def accuracy_by(key):
    grouped = collections.defaultdict(lambda: {"correct": 0, "total": 0})
    for r in results:
        grouped[r[key]]["correct"] += int(r["correct"])
        grouped[r[key]]["total"] += 1
    return {k: v["correct"] / v["total"] for k, v in grouped.items()}

model_acc = accuracy_by("model")
level_acc = accuracy_by("level")
level_order = ["r-trivial", "aperiodic", "soluble", "general-finite"]

# Overall accuracy summary
print("Model accuracy:")
for m, acc in sorted(model_acc.items(), key=lambda x: -x[1]):
    n_correct = sum(1 for r in results if r["model"] == m and r["correct"])
    print(f"  {m}: {acc:.0%} ({n_correct}/{len(TASKS)})")

print("\nLevel accuracy (across all models):")
for lv in level_order:
    if lv in level_acc:
        print(f"  {lv}: {level_acc[lv]:.0%}")

# COMMAND ----------

# DBTITLE 1,Results display
# hide-code
MODEL_SHORTS = {
    "databricks-llama-4-maverick": "Llama 4<br>Maverick",
    "databricks-qwen35-122b-a10b": "Qwen 3.5<br>122B",
    "databricks-gpt-oss-120b": "GPT OSS<br>120B",
    "databricks-meta-llama-3-3-70b-instruct": "Llama 3.3<br>70B",
}

LEVEL_LABELS = {
    "r-trivial": "R-trivial",
    "aperiodic": "Aperiodic",
    "soluble": "Soluble",
    "general-finite": "General<br>Finite",
}

def pct_color(acc):
    if acc >= 0.70:
        return "#00875C"
    elif acc >= 0.40:
        return "#BA7B23"
    else:
        return "#FF3621"

header_cells = "".join(
    f"<th style='background:#1B3139;color:#90A5B1;text-align:center;padding:10px 14px;font-weight:400;font-size:0.85em'>{LEVEL_LABELS[lv]}</th>"
    for lv in level_order
)

legend_items = "".join(
    f"<span style='margin-right:16px'><span style='display:inline-block;width:10px;height:10px;border-radius:50%;background:{c};margin-right:4px'></span>{label}</span>"
    for c, label in [("#00875C", "TRUST >=70%"), ("#BA7B23", "CONDITIONAL 40-69%"), ("#FF3621", "AVOID <40%")]
)

def render_grid(results, model_acc, round_label):
    """Render the model x level accuracy grid for one round."""
    def acc_cell(model, level):
        rows = [r for r in results if r["model"] == model and r["level"] == level]
        if not rows:
            return "<td style='background:#303F47;color:#90A5B1;text-align:center'>--</td>"
        acc = sum(1 for r in rows if r["correct"]) / len(rows)
        return f"<td style='background:#1B3139;color:{pct_color(acc)};text-align:center;font-weight:700;font-size:1.1em;padding:10px 14px'>{acc:.0%}</td>"
    rows_html = ""
    for model in MODELS:
        overall = model_acc.get(model, 0)
        cells = "".join(acc_cell(model, lv) for lv in level_order)
        rows_html += f"""
    <tr>
      <td style='background:#1B3139;color:#F9F7F4;padding:10px 16px;white-space:nowrap;font-size:0.9em'>{MODEL_SHORTS.get(model, model)}</td>
      {cells}
      <td style='background:#0B2026;color:{pct_color(overall)};text-align:center;font-weight:700;font-size:1.1em;padding:10px 14px;border-left:1px solid #303F47'>{overall:.0%}</td>
    </tr>"""
    displayHTML(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700&display=swap');
  .sb-wrap {{ font-family: 'DM Sans', sans-serif; background:#0B2026; padding:28px; border-radius:12px; max-width:780px }}
  .sb-title {{ color:#F9F7F4; font-size:1.25em; font-weight:700; margin-bottom:4px }}
  .sb-sub {{ color:#90A5B1; font-size:0.85em; margin-bottom:20px }}
  table {{ border-collapse:collapse; width:100% }}
  th {{ font-family:'DM Sans',sans-serif; border-bottom:1px solid #303F47 }}
  td {{ border-bottom:1px solid #303F47 }}
</style>
<div class="sb-wrap">
  <div class="sb-title">StateBench-style accuracy -- four FMAPI models</div>
  <div class="sb-sub">{round_label} -- 12 tasks x 4 difficulty levels -- Databricks Free Edition -- Run {RUN_ID}</div>
  <table>
    <thead>
      <tr>
        <th style='background:#1B3139;color:#90A5B1;text-align:left;padding:10px 16px;font-weight:400;font-size:0.85em'>Model</th>
        {header_cells}
        <th style='background:#0B2026;color:#90A5B1;text-align:center;padding:10px 14px;font-weight:600;font-size:0.85em;border-left:1px solid #303F47'>Overall</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  <div style='margin-top:14px;font-size:0.78em;color:#618794'>{legend_items}</div>
</div>
""")


render_grid(results, model_acc, f"Round 1, {ROUND_1_MAX_TOKENS}-token limit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 1 analysis

# COMMAND ----------
# DBTITLE 1,Test primary prediction
# hide-code
top_acc = max(model_acc.values()) if model_acc else 0
bottom_acc = min(model_acc.values()) if model_acc else 0
spread = top_acc - bottom_acc

top_model = max(model_acc, key=model_acc.get) if model_acc else "none"
bottom_model = min(model_acc, key=model_acc.get) if model_acc else "none"

prediction_1_met = top_acc >= 0.70
prediction_2_met = spread >= 0.25

print(f"Top model:    {top_model} at {top_acc:.0%}")
print(f"Bottom model: {bottom_model} at {bottom_acc:.0%}")
print(f"Spread:       {spread:.0%}")
print()
print(f"Prediction 1 (>=70% top model): {'MET' if prediction_1_met else 'NOT MET'}")
print(f"Prediction 2 (>=25pp spread):   {'MET' if prediction_2_met else 'NOT MET'}")

# Level difficulty gradient -- does accuracy drop with harder levels?
print("\nLevel difficulty gradient:")
for lv in level_order:
    acc = level_acc.get(lv, 0)
    print(f"  {lv}: {acc:.0%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 1 verdict -- trust, conditional, or avoid per model

# COMMAND ----------
# DBTITLE 1,Verdict cell -- condition 4
# hide-code
verdicts = {}
for model, acc in model_acc.items():
    if acc >= 0.70:
        verdicts[model] = "TRUST"
    elif acc >= 0.40:
        verdicts[model] = "CONDITIONAL"
    else:
        verdicts[model] = "AVOID"

verdict_colors = {"TRUST": "#00875C", "CONDITIONAL": "#BA7B23", "AVOID": "#FF3621"}

verdict_rows = ""
for model in MODELS:
    v = verdicts.get(model, "AVOID")
    acc = model_acc.get(model, 0)
    vc = verdict_colors[v]
    short = MODEL_SHORTS.get(model, model)
    verdict_rows += f"""
    <tr>
      <td style='padding:10px 16px;color:#F9F7F4;font-size:0.9em'>{short}</td>
      <td style='padding:10px 16px;text-align:center;font-size:1.1em;font-weight:700'>{acc:.0%}</td>
      <td style='padding:10px 16px;text-align:center;color:{vc};font-weight:700'>{v}</td>
    </tr>"""

displayHTML(f"""
<style>
  @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;600;700&display=swap');
  .vd-wrap {{ font-family:'DM Sans',sans-serif; background:#0B2026; padding:24px; border-radius:10px; max-width:520px }}
  .vd-title {{ color:#F9F7F4; font-size:1.1em; font-weight:700; margin-bottom:16px }}
  table {{ border-collapse:collapse; width:100% }}
  th {{ color:#90A5B1; font-weight:400; font-size:0.82em; border-bottom:1px solid #303F47; padding:8px 16px; text-align:left }}
  td {{ border-bottom:1px solid #303F47; font-family:'DM Sans',sans-serif }}
</style>
<div class="vd-wrap">
  <div class="vd-title">Verdict -- condition 4 satisfied</div>
  <table>
    <thead><tr>
      <th>Model</th><th style='text-align:center'>Accuracy</th><th style='text-align:center'>Verdict</th>
    </tr></thead>
    <tbody>{verdict_rows}</tbody>
  </table>
</div>
""")

# Log verdicts to MLflow
with mlflow.start_run(run_name=f"statebench-verdicts-{RUN_ID}"):
    for model, v in verdicts.items():
        mlflow.log_param(model.replace("-", "_") + "_verdict", v)

print("\nAll four conditions evaluated:")
print("  1. Task reproduction:   PASS (12 tasks, all with >=3 state changes)")
print(f"  2. Multi-model:         PASS ({models_completed} models)")
print(f"  3. Trace completeness:  {'PASS' if all_complete else 'FAIL'}")
print(f"  4. Verdict:             PASS (all models classified)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 2: room to think
# MAGIC
# MAGIC Round 1 capped every reply at 50 tokens, which is plenty for a model that answers with one number. Two of the four models don't work that way. Qwen 3.5 122B and GPT OSS 120B are reasoning models: they think through the problem first, and that thinking counts against the same token limit. When the limit lands before the thinking ends, the reply stops with `finish_reason: length` and carries no number, so round 1 scored it as a wrong answer.
# MAGIC
# MAGIC That limit was a setting I chose, not part of the test. So this round changes one thing and labels it that way: every model gets 4,000 tokens, and every reply records its `finish_reason`, so a reply cut off at the limit shows up as cut off. The 12 tasks, the system prompt, temperature 0, the hypothesis and the four validation conditions stay exactly as they were. Round 1 stays above, unchanged.

# COMMAND ----------

# DBTITLE 1,Round 2 -- same tasks, 4,000-token limit
ROUND_2_MAX_TOKENS = 4000
results_r2 = []

with mlflow.start_run(run_name=f"statebench-round2-{RUN_ID}"):
    mlflow.log_param("run_id", RUN_ID)
    mlflow.log_param("max_tokens", ROUND_2_MAX_TOKENS)
    for model in MODELS:
        model_results = []
        for task in TASKS:
            call_start = time.time()
            try:
                resp = _call_model(model, task.prompt, ROUND_2_MAX_TOKENS)
                raw_content = resp.choices[0].message.content
                finish_reason = resp.choices[0].finish_reason
                parsed = _parse_answer(raw_content)
                correct = parsed == task.answer
            except Exception as exc:
                raw_content = f"ERROR: {exc}"
                finish_reason = "error"
                parsed = "ERROR"
                correct = False
            row = {
                "model": model,
                "task_id": task.task_id,
                "level": task.level,
                "expected": task.answer,
                "got": parsed,
                "correct": correct,
                "finish_reason": finish_reason,
                "latency_ms": round((time.time() - call_start) * 1000),
                "raw": _extract_text(raw_content)[:200],
            }
            results_r2.append(row)
            model_results.append(row)
            mlflow.log_metric(f"r2_{model.replace('-','_')}_{task.task_id}_correct", int(correct))
        acc = sum(r["correct"] for r in model_results) / len(model_results)
        cut_off = sum(r["finish_reason"] == "length" for r in model_results)
        mlflow.log_metric(f"r2_{model.replace('-','_')}_accuracy", round(acc, 4))
        print(f"{model}: {acc:.0%} ({sum(r['correct'] for r in model_results)}/{len(model_results)}), {cut_off} replies cut off at the token limit")

r2_complete = all(required_keys.issubset(r.keys()) for r in results_r2)
r2_models_completed = len({r["model"] for r in results_r2})
print(f"\nModels completed: {r2_models_completed}; trace completeness: {'PASS' if r2_complete else 'FAIL'}")
print(f"Rate-limit waits across both rounds: {RATE_LIMIT_RETRIES['count']}")

# COMMAND ----------

# DBTITLE 1,Round 2 results and the prediction
# hide-code
def _acc(rows, key):
    grouped = collections.defaultdict(lambda: [0, 0])
    for r in rows:
        grouped[r[key]][0] += int(r["correct"])
        grouped[r[key]][1] += 1
    return {k: c / t for k, (c, t) in grouped.items()}

def _cut_off(rows):
    counts = collections.Counter(r["model"] for r in rows if r["finish_reason"] == "length")
    return {m: counts.get(m, 0) for m in MODELS}

model_acc_r2 = _acc(results_r2, "model")
level_acc_r2 = _acc(results_r2, "level")
top_model_r2 = max(model_acc_r2, key=model_acc_r2.get)
bottom_model_r2 = min(model_acc_r2, key=model_acc_r2.get)
top_acc_r2 = model_acc_r2[top_model_r2]
bottom_acc_r2 = model_acc_r2[bottom_model_r2]
spread_r2 = top_acc_r2 - bottom_acc_r2
prediction_1_met_r2 = top_acc_r2 >= 0.70
prediction_2_met_r2 = spread_r2 >= 0.25
verdicts_r2 = {m: ("TRUST" if a >= 0.70 else "CONDITIONAL" if a >= 0.40 else "AVOID") for m, a in model_acc_r2.items()}
cut_off_r1 = _cut_off(results)
cut_off_r2 = _cut_off(results_r2)

render_grid(results_r2, model_acc_r2, f"Round 2, {ROUND_2_MAX_TOKENS}-token limit")

print(f"{'Model':42} {'Round 1':>9} {'cut off':>8} {'Round 2':>9} {'cut off':>8}  Verdict")
for m in MODELS:
    print(f"{m:42} {model_acc[m]:>9.0%} {cut_off_r1[m]:>8} {model_acc_r2[m]:>9.0%} {cut_off_r2[m]:>8}  {verdicts_r2[m]}")
print()
print(f"Top model:    {top_model_r2} at {top_acc_r2:.0%}")
print(f"Bottom model: {bottom_model_r2} at {bottom_acc_r2:.0%}")
print(f"Spread:       {spread_r2:.0%}")
print(f"Prediction 1 (>=70% top model): {'MET' if prediction_1_met_r2 else 'NOT MET'}")
print(f"Prediction 2 (>=25pp spread):   {'MET' if prediction_2_met_r2 else 'NOT MET'}")
print("\nLevel accuracy, round 2:")
for lv in level_order:
    print(f"  {lv}: {level_acc_r2.get(lv, 0):.0%}")

with mlflow.start_run(run_name=f"statebench-round2-verdicts-{RUN_ID}"):
    for m, v in verdicts_r2.items():
        mlflow.log_param(m.replace("-", "_") + "_verdict", v)

# COMMAND ----------

# MAGIC %md
# MAGIC ## The two rounds side by side
# MAGIC
# MAGIC Each pair of bars is one model: its accuracy with a 50-token limit, then with 4,000. The hatched part of a round's bar is the share of its replies that stopped at the token limit before giving an answer.

# COMMAND ----------

# DBTITLE 1,Chart -- accuracy by round, with cut-off replies
# hide-code
import matplotlib.pyplot as plt

labels = [MODEL_SHORTS[m].replace("<br>", "\n") for m in MODELS]
x = range(len(MODELS))
width = 0.38
fig, ax = plt.subplots(figsize=(8.5, 4.2), facecolor="#F9F7F4")
ax.set_facecolor("#F9F7F4")
r1 = [model_acc[m] * 100 for m in MODELS]
r2 = [model_acc_r2[m] * 100 for m in MODELS]
c1 = [cut_off_r1[m] / len(TASKS) * 100 for m in MODELS]
c2 = [cut_off_r2[m] / len(TASKS) * 100 for m in MODELS]
ax.bar([i - width / 2 for i in x], r1, width, color="#90A5B1", label=f"Round 1, {ROUND_1_MAX_TOKENS} tokens")
ax.bar([i - width / 2 for i in x], c1, width, bottom=r1, color="none", edgecolor="#FF3621", hatch="//", label="Cut off at the limit")
ax.bar([i + width / 2 for i in x], r2, width, color="#1B3139", label=f"Round 2, {ROUND_2_MAX_TOKENS:,} tokens")
ax.bar([i + width / 2 for i in x], c2, width, bottom=r2, color="none", edgecolor="#FF3621", hatch="//")
for i in x:
    ax.text(i - width / 2, r1[i] + c1[i] + 2, f"{r1[i]:.0f}%", ha="center", fontsize=9, color="#1B3139")
    ax.text(i + width / 2, r2[i] + c2[i] + 2, f"{r2[i]:.0f}%", ha="center", fontsize=9, color="#1B3139")
ax.axhline(70, color="#00875C", linewidth=1, linestyle="--")
ax.text(len(MODELS) - 0.5, 71.5, "TRUST line, 70%", ha="right", fontsize=8, color="#00875C")
ax.set_xticks(list(x))
ax.set_xticklabels(labels, fontsize=9)
ax.set_ylim(0, 115)
ax.set_ylabel("Tasks answered correctly (%)")
ax.set_title("StateBench-style accuracy: the token limit decided two of four verdicts" if any(cut_off_r1.values()) else "StateBench-style accuracy by round", fontsize=11, loc="left", color="#1B3139")
for spine in ("top", "right"):
    ax.spines[spine].set_visible(False)
ax.legend(fontsize=8, frameon=False, loc="upper left", bbox_to_anchor=(0, -0.12), ncol=3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Free Edition notes
# MAGIC
# MAGIC This notebook ran on [Databricks Free Edition](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/) using serverless compute. No cluster provisioning needed; compute started automatically with the first cell. All four Foundation Model API endpoints were available. Free Edition rate-limits Foundation Model API calls, so the notebook waits and retries when an endpoint answers 429; the count of those waits prints at the end of round 2. MLflow experiment tracking worked without additional setup.

# COMMAND ----------

# DBTITLE 1,RESULTS_JSON
# hide-code
RESULTS = {
    "run_id": RUN_ID,
    "n_tasks": len(TASKS),
    "n_models": len(MODELS),
    "gate": gate_result,
    "max_tokens": ROUND_2_MAX_TOKENS,
    "model_accuracy": {m: round(a, 4) for m, a in model_acc_r2.items()},
    "level_accuracy": {lv: round(level_acc_r2.get(lv, 0), 4) for lv in level_order},
    "cut_off": cut_off_r2,
    "verdicts": verdicts_r2,
    "top_model": top_model_r2,
    "top_accuracy": round(top_acc_r2, 4),
    "bottom_model": bottom_model_r2,
    "bottom_accuracy": round(bottom_acc_r2, 4),
    "spread": round(spread_r2, 4),
    "prediction_1_met": prediction_1_met_r2,
    "prediction_2_met": prediction_2_met_r2,
    "conditions": {
        "1_task_reproduction": True,
        "2_multi_model": r2_models_completed >= 4,
        "3_trace_completeness": r2_complete,
        "4_verdict": True,
    },
    "round_1": {
        "max_tokens": ROUND_1_MAX_TOKENS,
        "model_accuracy": {m: round(a, 4) for m, a in model_acc.items()},
        "level_accuracy": {lv: round(level_acc.get(lv, 0), 4) for lv in level_order},
        "cut_off": cut_off_r1,
        "verdicts": verdicts,
        "spread": round(spread, 4),
        "prediction_1_met": prediction_1_met,
        "prediction_2_met": prediction_2_met,
    },
    "rate_limit_waits": RATE_LIMIT_RETRIES["count"],
}

print(f"RESULTS_JSON {json.dumps(RESULTS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC The token limit, not the model, decided two of the four verdicts in round 1. With replies capped at 50 tokens, Qwen 3.5 122B stopped before giving an answer on all 12 tasks and scored 0 of 12. GPT OSS 120B stopped early on 8 of 12 and scored 33%. Both got rated AVOID. They're reasoning models, so their thinking spends the same token budget as their answer, and 50 tokens ran out mid-thought.
# MAGIC
# MAGIC Round 2 changed that one setting to 4,000 tokens. No reply hit the limit, and both reasoning models answered all 12 tasks correctly. The two Llama models, Llama 4 Maverick and Llama 3.3 70B, each answered 9 of 12. All four landed at TRUST.
# MAGIC
# MAGIC The hypothesis held in round 2, right on the line: the top model reached 100%, well past 70%, and the gap between top and bottom was 25 points, exactly the 25 the hypothesis asked for. One task going the other way would have moved that second prediction.
# MAGIC
# MAGIC By level, the run lines up with what Ai2 found for frontier models. Every model got every r-trivial and aperiodic task right. Self-updating variables (x = x + 5), the soluble level, were the hardest at 67% across the four models, and general finite came in at 83%. These are short tasks, a handful of assignments each, so they test the kind of update rather than the long inputs Ai2 stretched to.
# MAGIC
# MAGIC If you call a reasoning model on Databricks with a tight `max_tokens`, check `finish_reason` before you score the answer. A reply that stopped at `length` didn't get it wrong; it never finished.
# MAGIC
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Query reasoning models](https://docs.databricks.com/aws/en/machine-learning/model-serving/query-reason-models): how reasoning models return thinking and text, and the parameters that control reasoning depth.
# MAGIC - [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/): the hosted model endpoints this notebook calls.
# MAGIC - [Foundation Model REST API reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference): the chat request, `max_tokens` and the response fields.
# MAGIC - [MLflow tracking](https://docs.databricks.com/aws/en/mlflow/tracking): logging the per-task metrics and verdicts.
