# Databricks notebook source
# MAGIC %md
# MAGIC In a recent Databricks blog post about evaluation-first AI agents:
# MAGIC
# MAGIC > "not by shipping more agents, but by making evaluation the primary way agents get built, tested, and operated"
# MAGIC
# MAGIC Found "making evaluation the primary way agents get built" interesting, so I put together an experiment. The [Zepto case study](https://www.databricks.com/blog/evaluation-first-ai-agents-how-zepto-scales-customer-support-databricks-and-mlflow) describes a golden dataset and large language model (LLM) judge scoring as the gate between development and production. This notebook builds that gate, runs two [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis) endpoints through it, and then takes the gate apart when the first number comes back wrong.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on Databricks Free Edition. End to end it takes about 90 seconds of compute, so budget your time for reading rather than waiting.
# MAGIC
# MAGIC - **Compute:** serverless (default). No cluster attach needed.
# MAGIC - **Libraries:** the next cell pins `mlflow` and installs `databricks-agents`, then restarts Python. The serverless default is a newer `mlflow`, so this is a real downgrade and not a no-op.
# MAGIC - **Auth:** workspace session credentials. No API key needed.
# MAGIC - **Quota note:** this runs 20 agent calls plus the judge calls behind them. That counts against the Free Edition daily Foundation Model APIs quota, and it sits well inside the daily limit.

# COMMAND ----------

# MAGIC %pip install "mlflow==2.21.0" "databricks-agents==0.16.0" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import pandas as pd
import matplotlib.pyplot as plt
from mlflow.deployments import get_deploy_client
from mlflow.tracking import MlflowClient

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Workspace URL and user come from the notebook context. The [MLflow experiment](https://docs.databricks.com/aws/en/mlflow/tracking) lands under your home directory so it shows up in the Experiments tab with no admin setup. Both model variants are declared here: swap either for anything on the [supported models list](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models).

# COMMAND ----------

ctx    = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
USER   = ctx.userName().get()

EXPT_NAME = f"/Users/{USER}/agent-eval-gate-mlflow"

MODEL_A = "databricks-llama-4-maverick"
MODEL_B = "databricks-meta-llama-3-3-70b-instruct"

PASS_THRESHOLD = 0.80  # the gate: 80% correctness required to ship

mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
experiment = mlflow.set_experiment(EXPT_NAME)

print(f"User:       {USER}")
print(f"Experiment: {EXPT_NAME}")
print(f"Models:     {MODEL_A}, {MODEL_B}")
print(f"Gate:       {PASS_THRESHOLD:.0%} correctness")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The golden dataset
# MAGIC
# MAGIC The Zepto article calls the golden dataset "the single source of truth for evaluating agent behavior." So I wrote one: 10 customer support scenarios for Stark Industries. Half of it is routine order and refund traffic. The rest is the messier stuff, like a duplicate charge or a discount code that died on the way to checkout.
# MAGIC
# MAGIC Each row follows the [evaluation set schema](https://docs.databricks.com/aws/en/generative-ai/agent-evaluation/evaluation-set): a `request` and a list of `expected_facts` the answer has to cover. Four rows carry two expected facts, six carry three. I wrote them in a generic style similar to acceptance criteria: quickly and without thinking hard about the wording.
# MAGIC
# MAGIC Remember that last sentence. It turns out to be the whole experiment.

# COMMAND ----------

eval_set = [
    {
        "request": {"messages": [{"role": "user", "content": "Where is my Stark Industries order #SI-44721? It has been 3 days."}]},
        "expected_facts": [
            "acknowledge the 3-day wait",
            "provide tracking information or the next step to find it",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "I received the wrong item in my Stark Industries order. What do I do?"}]},
        "expected_facts": [
            "initiate a return or exchange process",
            "acknowledge the error",
            "provide a timeline for resolution",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "I want to return an item I bought 45 days ago. Is that possible?"}]},
        "expected_facts": [
            "state the return policy window",
            "explain what options are available after the return window",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "My refund has not shown up after 2 weeks. Who do I contact?"}]},
        "expected_facts": [
            "acknowledge the refund delay",
            "provide an escalation path",
            "mention the expected refund timeline",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "Can I change my delivery address after I placed the order?"}]},
        "expected_facts": [
            "explain whether address change is possible",
            "provide steps or alternative options",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "The Stark Industries arc reactor kit shows out of stock. Will it be restocked?"}]},
        "expected_facts": [
            "acknowledge the current stock status",
            "offer to notify when restocked or suggest an alternative",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "I placed an order but have not received a confirmation email."}]},
        "expected_facts": [
            "acknowledge the missing confirmation",
            "suggest checking spam or verifying the email address",
            "offer to resend or confirm the order exists",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "How do I cancel an order that has not shipped yet?"}]},
        "expected_facts": [
            "explain the cancellation process",
            "confirm whether the order is still cancellable",
            "describe the refund timeline on cancellation",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "I was charged twice for the same order. Please fix this."}]},
        "expected_facts": [
            "acknowledge the duplicate charge",
            "initiate investigation or escalation",
            "provide a resolution timeline",
        ],
    },
    {
        "request": {"messages": [{"role": "user", "content": "I got a discount code but the site says it is invalid."}]},
        "expected_facts": [
            "acknowledge the issue",
            "verify code validity or offer an alternative path",
            "offer to apply the discount manually if the code is valid",
        ],
    },
]

fact_counts = pd.Series([len(r["expected_facts"]) for r in eval_set]).value_counts().sort_index()
print(f"Golden dataset: {len(eval_set)} scenarios")
for n, c in fact_counts.items():
    print(f"  {c} rows with {n} expected facts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The two agent variants
# MAGIC
# MAGIC Same system prompt, different endpoint. The `@mlflow.trace` decorator captures each call as a span so the [judges](https://docs.databricks.com/aws/en/generative-ai/agent-evaluation/llm-judge-metrics) can read the execution alongside the final answer.
# MAGIC
# MAGIC The prompt is deliberately thin. This experiment is about the gate, not about prompt tuning.

# COMMAND ----------

SYSTEM_PROMPT = (
    "You are a customer support agent for Stark Industries. "
    "You help customers with: order status, returns, refunds, delivery address changes, and product availability. "
    "Keep responses concise and actionable. "
    "If you cannot resolve something directly, explain the escalation path clearly."
)

deploy_client = get_deploy_client("databricks")


def make_agent_fn(model_name: str):
    """Build the callable that evaluation hands each request to.

    Args:
        model_name: Foundation Model APIs endpoint name.

    Returns:
        Callable taking a request dict and returning the chat completion.
    """
    @mlflow.trace(span_type="AGENT")
    def agent(messages: list) -> dict:
        return deploy_client.predict(
            endpoint=model_name,
            inputs={"messages": [{"role": "system", "content": SYSTEM_PROMPT}] + messages},
        )
    return lambda request: agent(**request)


agent_a = make_agent_fn(MODEL_A)
agent_b = make_agent_fn(MODEL_B)

print("Agent functions ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running the gate
# MAGIC
# MAGIC `mlflow.evaluate()` with `model_type="databricks-agent"` turns on the built-in Databricks judges. Because the rows carry `expected_facts`, the correctness judge runs alongside safety, and the two `global_guidelines` below add a pass or fail check on tone and next steps for every response.
# MAGIC
# MAGIC Each variant gets its own run under the same experiment, so you can line them up in the Experiments UI afterward. Each run also gets a second copy of its scores logged as `Correctness %`, `Safety %` and so on, because the harness's own metric names are long enough that the UI truncates them to `response/llm_judg...` and you cannot tell two judges apart.

# COMMAND ----------

global_guidelines = {
    "actionable": ["The response must give the customer a clear next step or resolution path."],
    "professional": ["The response must be professional and empathetic in tone."],
}

print(f"Evaluating: {MODEL_A}")
with mlflow.start_run(run_name="eval-maverick") as run_a:
    result_a = mlflow.evaluate(
        data=eval_set,
        model=agent_a,
        model_type="databricks-agent",
        evaluator_config={"databricks-agent": {"global_guidelines": global_guidelines}},
    )
    run_id_a = run_a.info.run_id
print(f"  Run ID: {run_id_a}")

# COMMAND ----------

print(f"Evaluating: {MODEL_B}")
with mlflow.start_run(run_name="eval-llama-3-3-70b") as run_b:
    result_b = mlflow.evaluate(
        data=eval_set,
        model=agent_b,
        model_type="databricks-agent",
        evaluator_config={"databricks-agent": {"global_guidelines": global_guidelines}},
    )
    run_id_b = run_b.info.run_id
print(f"  Run ID: {run_id_b}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The gate decision
# MAGIC
# MAGIC Pass rates per judge, then the correctness number against the threshold. A rating that comes back as neither yes nor no gets reported rather than quietly counted as a failure, because a judge that didn't run and a judge that returned `no` are different problems.

# COMMAND ----------

def aggregate_pass_rates(result) -> dict:
    """Compute per-judge pass rates from an evaluation result.

    Judge rating columns end with '/rating' and hold 'yes' or 'no'. Anything
    that is neither is counted and reported rather than silently failed.

    Args:
        result: The object returned by mlflow.evaluate().

    Returns:
        Dict mapping judge name to pass rate rounded to two decimals.
    """
    df = result.tables["eval_results"]
    rates = {}
    for col in [c for c in df.columns if c.endswith("/rating")]:
        judge = col.split("/")[-2]
        vals = df[col].astype("string").str.lower()
        unscored = int(vals.isna().sum())
        if unscored:
            print(f"  warning: {judge} returned no rating on {unscored} row(s)")
        rates[judge] = round(int((vals == "yes").sum()) / len(df), 2)
    return rates


def get_correctness(rates: dict) -> float:
    """Pull the correctness pass rate, which is the gate metric.

    Args:
        rates: Output of aggregate_pass_rates().

    Returns:
        Correctness pass rate, or 0.0 when the judge did not run.
    """
    for key in ("correctness", "agent_correctness"):
        if key in rates:
            return rates[key]
    print("  warning: no correctness judge in results")
    return 0.0


def log_readable_metrics(run_id: str, rates: dict) -> None:
    """Log the same pass rates under names that read at a glance.

    The harness logs its own aggregates under keys like
    `response/llm_judged/correctness/rating/percentage`. The Experiments UI
    prints the key verbatim as the column header, where it truncates to
    `response/llm_judg...` and two different judges become indistinguishable.
    These duplicate the same numbers as `Correctness %`, `Safety %` and so on,
    so the runs table is readable without widening every column.

    Args:
        run_id: The finished MLflow run to annotate.
        rates: Output of aggregate_pass_rates(), values between 0 and 1.
    """
    client = MlflowClient()
    for judge, rate in rates.items():
        label = judge.replace("_", " ").title()
        client.log_metric(run_id, f"{label} %", round(rate * 100, 1))


rates_a, rates_b = aggregate_pass_rates(result_a), aggregate_pass_rates(result_b)
log_readable_metrics(run_id_a, rates_a)
log_readable_metrics(run_id_b, rates_b)
score_a, score_b = get_correctness(rates_a), get_correctness(rates_b)
gate_a = "PASS" if score_a >= PASS_THRESHOLD else "FAIL"
gate_b = "PASS" if score_b >= PASS_THRESHOLD else "FAIL"

summary = pd.DataFrame({"Maverick": rates_a, "Llama 3.3 70B": rates_b}).fillna(0.0)
print(summary.to_string(float_format=lambda v: f"{v:.0%}"))
print()
print(f"Gate at {PASS_THRESHOLD:.0%} correctness")
print(f"  {MODEL_A:<45}: {score_a:.0%}  {gate_a}")
print(f"  {MODEL_B:<45}: {score_b:.0%}  {gate_b}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Something is wrong with this result
# MAGIC
# MAGIC Both variants fail, and they fail badly. But look at the other judges before you go blame the models. Safety and the actionable guideline pass on every row, and professional stays high. Only correctness collapses.
# MAGIC
# MAGIC That shape doesn't say "the agent gave bad answers." A model writing genuinely bad customer support answers would drag the guideline judges down with it. Something about correctness specifically is failing, and correctness is the one judge that reads my hand-written `expected_facts`.
# MAGIC
# MAGIC So the suspect isn't the model. It's my dataset.

# COMMAND ----------

judges = sorted(set(rates_a) | set(rates_b))
x, w = range(len(judges)), 0.38
fig, ax = plt.subplots(figsize=(10, 5))
ax.bar([i - w / 2 for i in x], [rates_a.get(j, 0.0) for j in judges], w, label="Maverick", color="#1F77B4")
ax.bar([i + w / 2 for i in x], [rates_b.get(j, 0.0) for j in judges], w, label="Llama 3.3 70B", color="#FF7F0E")
ax.axhline(PASS_THRESHOLD, color="red", linestyle="--", linewidth=1.2, label=f"{PASS_THRESHOLD:.0%} gate")
ax.set_xticks(list(x))
ax.set_xticklabels([j.replace("_", " ").title() for j in judges], rotation=20, ha="right")
ax.set_ylabel("Pass rate")
ax.set_ylim(0, 1.08)
ax.set_title("Every judge passes except the one reading my expected facts")
ax.legend(loc="lower right")
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading the rationales
# MAGIC
# MAGIC The correctness judge writes a rationale for every row, and that's where the answer is. Pull the failures and read what it actually objected to.

# COMMAND ----------

def failure_report(result, label: str) -> pd.DataFrame:
    """Build a per-row view of correctness outcomes and their rationales.

    Args:
        result: The object returned by mlflow.evaluate().
        label: Display name for the model variant.

    Returns:
        DataFrame with one row per scenario.
    """
    df = result.tables["eval_results"]
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "model": label,
            "question": r["request"]["messages"][-1]["content"][:60],
            "n_facts": len(r["expected_facts"]),
            "correct": str(r["response/llm_judged/correctness/rating"]),
            "rationale": str(r["response/llm_judged/correctness/rationale"]),
        })
    return pd.DataFrame(rows)


report = pd.concat([failure_report(result_a, "Maverick"), failure_report(result_b, "Llama 3.3 70B")])
display(report)

# COMMAND ----------

failures = report[report.correct.str.lower() != "yes"]
for _, r in failures.head(3).iterrows():
    print(f"[{r['model']}] {r['question']}  ({r['n_facts']} expected facts)")
    print(f"  {r['rationale']}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC They all read the same way. The judge walks the list, confirms the facts the answer did cover, names the one it didn't, and returns `no`. There's no partial credit: a single unmet fact fails the whole scenario.
# MAGIC
# MAGIC That alone explains a lot of the collapse, because most of my rows carry three facts. But there's a second thing hiding in the wording, and it's the one I didn't see coming. Several of my facts contain the word `or`:

# COMMAND ----------

or_facts = [(i, f) for i, row in enumerate(eval_set) for f in row["expected_facts"] if " or " in f]
print(f"{len(or_facts)} of {sum(len(r['expected_facts']) for r in eval_set)} expected facts contain an 'or':\n")
for i, f in or_facts:
    print(f"  row {i}: {f}")

# COMMAND ----------

# MAGIC %md
# MAGIC I wrote those as choices. Either branch should satisfy the fact. The question is whether the judge reads them that way, and the live run can't answer it: the model writes a different answer every time, so I can't separate a judge effect from model noise.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Isolating the judge
# MAGIC
# MAGIC So I froze the response. Below is the exact Maverick answer from the run above, hardcoded. `mlflow.evaluate()` takes a precomputed `response` and skips calling the model entirely, which means the answer text is now a constant and the only thing changing between runs is how I worded the expected fact.
# MAGIC
# MAGIC These are the validation conditions. If the wording is what's driving the gate, removing the `or` should flip the row to pass with the answer untouched. If the answer was genuinely deficient, every wording should keep failing.

# COMMAND ----------

FROZEN_QUESTION = "The Stark Industries arc reactor kit shows out of stock. Will it be restocked?"
FROZEN_ANSWER = (
    "The Stark Industries arc reactor kit is currently out of stock. I've checked our inventory, "
    "and we're expecting a new shipment in 2-3 weeks. I'll go ahead and put you on the backorder "
    "list, and you'll receive an email notification the moment it's available for purchase."
)

wordings = {
    "as I wrote it (contains 'or')": [
        "acknowledge the current stock status",
        "offer to notify when restocked or suggest an alternative",
    ],
    "same fact, 'or' clause removed": [
        "acknowledge the current stock status",
        "offer to notify when restocked",
    ],
    "just the notify half": ["offer to notify when restocked"],
    "just the alternative half": ["suggest an alternative product"],
}

judged = []
for name, facts in wordings.items():
    row = {
        "request": {"messages": [{"role": "user", "content": FROZEN_QUESTION}]},
        "response": {"choices": [{"message": {"role": "assistant", "content": FROZEN_ANSWER}}]},
        "expected_facts": facts,
    }
    with mlflow.start_run(run_name=f"wording::{name}") as run:
        res = mlflow.evaluate(data=[row], model_type="databricks-agent")
    log_readable_metrics(run.info.run_id, aggregate_pass_rates(res))
    r = res.tables["eval_results"].iloc[0]
    judged.append({
        "wording": name,
        "n_facts": len(facts),
        "correct": str(r["response/llm_judged/correctness/rating"]),
        "rationale": str(r["response/llm_judged/correctness/rationale"]),
    })

wording_df = pd.DataFrame(judged)
print(wording_df[["wording", "n_facts", "correct"]].to_string(index=False))

# COMMAND ----------

display(wording_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Same answer, every time. Delete two words from the expected fact and the row flips from fail to pass.
# MAGIC
# MAGIC The last two conditions show why. Split into single facts, "notify when restocked" passes on its own and "suggest an alternative" fails on its own. The judge scored both halves of that `or` independently and then demanded both, which is what you'd see if it read the fact as a list rather than a choice.

# COMMAND ----------

# MAGIC %md
# MAGIC ## How much does fact count cost you?
# MAGIC
# MAGIC If the judge needs every fact in the list, then each fact you add is another chance to fail the row. That predicts something specific and checkable: rows with three facts should pass less often than rows with two, for both models, independent of the question.

# COMMAND ----------

by_count = (
    report.assign(passed=report.correct.str.lower().eq("yes"))
          .groupby(["model", "n_facts"])
          .agg(rows=("passed", "size"), passed=("passed", "sum"))
)
by_count["pass_rate"] = (by_count.passed / by_count.rows).round(2)
print(by_count.to_string())

pooled = (
    report.assign(passed=report.correct.str.lower().eq("yes"))
          .groupby("n_facts")
          .agg(rows=("passed", "size"), passed=("passed", "sum"))
)
pooled["pass_rate"] = (pooled.passed / pooled.rows).round(2)
print()
print("Both models pooled:")
print(pooled.to_string())

# COMMAND ----------

fig, ax = plt.subplots(figsize=(7, 4.5))
labels = [f"{n} expected facts" for n in pooled.index]
ax.bar(labels, pooled.pass_rate, 0.5, color=["#1F77B4", "#FF7F0E"])
ax.axhline(PASS_THRESHOLD, color="red", linestyle="--", linewidth=1.2, label=f"{PASS_THRESHOLD:.0%} gate")
for i, (n, r) in enumerate(zip(pooled.rows, pooled.pass_rate)):
    ax.text(i, r + 0.02, f"{r:.0%}  (n={n})", ha="center")
ax.set_ylabel("Correctness pass rate")
ax.set_ylim(0, 1.08)
ax.set_title("Pass rate falls with every fact you add to the row")
ax.legend(loc="upper right")
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What I learned
# MAGIC
# MAGIC The gate number I got out of this notebook was mostly a measurement of my own writing.
# MAGIC
# MAGIC The correctness judge is strict and literal. It needs every entry in `expected_facts` supported by the answer, there's no partial credit, and it doesn't read an `or` inside a fact as a choice. That's defensible behavior for a deployment gate. You want a gate that's hard to argue with. But it means the list you hand it is doing more work than the threshold you picked, and I didn't know that when I typed 80% into a variable.
# MAGIC
# MAGIC Two things I'd do differently, and both are about the dataset, not the model:
# MAGIC
# MAGIC **Write atomic facts.** One fact, one claim, no conjunctions. An `or` isn't reliably read as a choice: some of mine were, the arc reactor one wasn't, and nothing in the score tells you which way it went. If you catch yourself writing `or`, split it into two facts or narrow it to one.
# MAGIC
# MAGIC **Hold fact count steady across rows.** My three-fact rows and my two-fact rows weren't measuring the same thing at the same difficulty, so pooling them into one percentage produced a number that doesn't mean much. If some scenarios genuinely need more facts, score them as their own group.
# MAGIC
# MAGIC Which brings me back to the Zepto piece. They call the golden dataset the source of truth, and I read that as a statement about coverage: get enough scenarios and you're covered. After this run I read it as a statement about precision. The dataset isn't just what you test, it's the measuring instrument, and an instrument you wrote in ten minutes will tell you ten-minute things.
# MAGIC
# MAGIC The eval harness took about 25 lines. The dataset is where the work is. That part of the article was right, just not for the reason I assumed.
# MAGIC
# MAGIC A caveat, and it's the reason this stopped being a model comparison. I ran the whole notebook four times. Maverick came out ahead twice and Llama 3.3 came out ahead twice, so the gap between them at 10 scenarios is noise, not a ranking. Don't read those two bars against each other.
# MAGIC
# MAGIC What held every time: the three-fact rows produced one pass out of 48, and the wording test returned the same four answers on all four runs. Trust those. The frozen response is what makes the second one worth anything, because the wording was the only thing that moved.
# MAGIC
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Evaluation sets](https://docs.databricks.com/aws/en/generative-ai/agent-evaluation/evaluation-set): the input schema for `expected_facts` and which judges each field turns on
# MAGIC - [LLM judge metrics](https://docs.databricks.com/aws/en/generative-ai/agent-evaluation/llm-judge-metrics): what each built-in judge checks and how the yes or no rating is produced
# MAGIC - [Agent evaluation](https://docs.databricks.com/aws/en/mlflow/llm-evaluate): running `mlflow.evaluate()` with `model_type="databricks-agent"`
# MAGIC - [Custom metrics](https://docs.databricks.com/aws/en/generative-ai/agent-evaluation/custom-metrics): where to go if you want partial credit across a fact list instead of all or nothing
# MAGIC - [MLflow tracking](https://docs.databricks.com/aws/en/mlflow/tracking): how runs and evaluation artifacts are organized when you compare versions
