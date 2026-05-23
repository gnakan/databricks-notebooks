# Databricks notebook source
# MAGIC %md
# MAGIC In a recent Towards Data Science post about the compute bill that reasoning models drive:
# MAGIC
# MAGIC > "While a model pauses to think, it generates hidden reasoning tokens. These tokens never appear in the final chat bubble, but they represent a massive surge in billable compute on your monthly invoice."
# MAGIC
# MAGIC Found "hidden reasoning tokens" interesting, so I put together a scaffolded experiment. The article argues that reasoning models earn their premium on tasks that need multi-step logic, and lose it on extraction or classification, and recommends task-complexity routing as the governance answer. This notebook is a test of that routing claim on Databricks. Three task complexities (simple extraction, multi-step reasoning, mixed-signal classification) run against two pay-per-token endpoints on [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis): [`databricks-gpt-oss-120b`](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) at `reasoning_effort: "high"` (the reasoning arm) and [`databricks-meta-llama-3-3-70b-instruct`](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) at default settings (the non-reasoning base arm). The artifact is a table showing how many tokens each model spends per right answer, plus a verdict against a hypothesis whose thresholds are locked below before the run.
# MAGIC
# MAGIC **Method note on metric choice.** Tokens, not dollars. The article's vocabulary is "hidden reasoning tokens": the unobserved completion tokens a reasoning model spends on its chain-of-thought before producing the visible answer. This notebook tests the article's claim in its native unit. `completion_tokens_per_success` is the metric the verdict function applies the 3x and 2x thresholds against. Databricks's [`usage` object](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) rolls reasoning tokens into the same `completion_tokens` integer that billing keys off, so this metric simultaneously honors the article's framing and reflects the actual completion-side token load a team will see on either model.
# MAGIC
# MAGIC **Method note on the base arm choice.** I originally chose `databricks-gpt-oss-20b` as the base arm, on the assumption that "same-family at a smaller size" gives a clean control. A probe before the run surfaced that the 20B variant on Databricks also returns a reasoning content block in `message.content`, so it is itself a reasoning model on this platform and is not a valid non-reasoning baseline. I swapped the base arm to `databricks-meta-llama-3-3-70b-instruct` (a strong instruction-tuned non-reasoning model) before the data-generating run. The hypothesis thresholds (3x and 2x) were not changed; the verdict function below is unchanged. Only the metric switched, from a commercial unit that varies by tier and region to the universal one the article actually argues about.
# MAGIC
# MAGIC ## Hypothesis and validation conditions
# MAGIC
# MAGIC This section sets the experiment up before the data exists. Nothing in here gets edited after the run.
# MAGIC
# MAGIC **Claim under test.** A reasoning-tier endpoint on [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis) spends meaningfully more completion tokens per call than a comparable non-reasoning endpoint, but the token premium only earns out on tasks above a complexity threshold. Below the threshold, the non-reasoning endpoint finishes the same task using a fraction of the tokens. Above the threshold, the reasoning endpoint spends fewer tokens per success because the non-reasoning endpoint needs retries or returns wrong answers.
# MAGIC
# MAGIC **Hypothesis.** If task-complexity routing earns out, then both of these must hold on this run:
# MAGIC
# MAGIC 1. On the simple extraction task, `databricks-gpt-oss-120b` completion tokens per right answer is at least three times `databricks-meta-llama-3-3-70b-instruct` completion tokens per right answer. (Reasoning is over-spending here.)
# MAGIC 2. On the mixed-signal classification task, `databricks-meta-llama-3-3-70b-instruct` completion tokens per right answer is at least two times `databricks-gpt-oss-120b` completion tokens per right answer. (Base is under-performing here.)
# MAGIC
# MAGIC If both conditions hold, the hypothesis is `supported`: routing by task complexity is justified. If either condition fails, the hypothesis is `not_supported`: one model wins across the complexity range and the routing argument does not hold for this task suite. If both runs complete but the metrics are inside a defensible measurement-noise band (defined below), the verdict is `inconclusive`.
# MAGIC
# MAGIC **Validation conditions.**
# MAGIC
# MAGIC - Both endpoints run all three tasks with identical user prompts, identical `temperature=0`, and identical `max_tokens=2048`. The only difference between arms is the endpoint name and, on the reasoning arm, `reasoning_effort="high"`.
# MAGIC - A ground-truth answer set for every task is defined in code below before the model calls run. The graders are deterministic Python functions, also defined below; they do not call a model.
# MAGIC - Token capture is itemized per call from the `usage` object on each response. `completion_tokens` is the verdict-input metric; on the reasoning arm it includes both the chain-of-thought and the visible answer per the [API reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference). `prompt_tokens` is reported for transparency but is not in the verdict. Prompts are identical across arms so any prompt-token differential is tokenizer-driven noise.
# MAGIC - Success is exact-match against ground truth for the extraction task, normalized-answer match for the multi-step reasoning task, and label match against the rubric for the mixed-signal classification task. The rubrics live in code below; nothing is graded after the run.
# MAGIC - Completion-tokens-per-success is reported as `(total completion tokens across all items in the cell) / (count of successful items)`. A cell with zero successes is reported as infinite tokens-per-success and is treated as a failure on that condition.
# MAGIC - Measurement-noise band: if completion tokens per right answer on the reasoning arm and the base arm are within 1.5x of each other on a task, that task is recorded as a tie and does not satisfy either hypothesis condition.
# MAGIC - The verdict at the bottom of the notebook is computed by a deterministic function against these criteria. No post-hoc adjustment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **Databricks workspace.** The notebook was demonstrated on a Premium Azure workspace where both `databricks-gpt-oss-120b` and `databricks-meta-llama-3-3-70b-instruct` are available via [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis). The published [Foundation Model APIs limits](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/limits) table is documented for Enterprise tier workspaces; Premium workspaces with both endpoints active are sufficient for this run. Check [supported models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models) to confirm both endpoints resolve on yours.
# MAGIC - **Foundation Model APIs access.** The pay-per-token endpoints `databricks-gpt-oss-120b` (reasoning arm) and `databricks-meta-llama-3-3-70b-instruct` (non-reasoning base arm) are used throughout. The reasoning arm sets `reasoning_effort="high"`, which the [API reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) documents as accepted by the GPT-OSS family.
# MAGIC - **Compute:** Databricks Serverless
# MAGIC - **Libraries:** the [`openai`](https://github.com/openai/openai-python) Python client pointed at Databricks serving endpoints, plus `databricks-sdk` for identity resolution. The next cell installs both and restarts the kernel. The notebook also uses `json` and `time` from the standard library and `pandas`, `matplotlib`, and `numpy` from the Databricks Serverless runtime for the summary visuals.
# MAGIC - **Identity:** the workspace user runs as their Databricks email; the notebook context supplies the workspace URL and a per-session token. No PAT or external secret to manage.

# COMMAND ----------

# MAGIC %pip install --upgrade "openai>=1.40" databricks-sdk --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import time
from typing import Any, Callable

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from openai import OpenAI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Dynamic resolution for the workspace user, the workspace URL, and the per-session token. The two endpoint names live as top-level variables so a reader can swap them for a different reasoning-base pair (for example a Claude reasoning tier against a Claude base tier) and re-run the comparison.

# COMMAND ----------

ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
current_user = ctx.userName().get()
workspace_url = ctx.apiUrl().get()
workspace_token = ctx.apiToken().get()

# The reasoning arm. `reasoning_effort` is accepted by the GPT-OSS family per
# the Foundation Model APIs reference; the parameter is sent on every call on
# this arm.
REASONING_MODEL = "databricks-gpt-oss-120b"

# The non-reasoning base arm. Strong instruction-tuned non-reasoning model;
# returns a plain-string `message.content` and does not consume reasoning
# tokens. See the Method note in the opening cell for why the original
# same-family base arm was swapped before the data-generating run.
BASE_MODEL = "databricks-meta-llama-3-3-70b-instruct"

# OpenAI client targeting Databricks serving endpoints. Returns a standard
# usage object on every chat completion; both arms surface `prompt_tokens`
# and `completion_tokens` integers that this notebook uses end-to-end.
oai = OpenAI(api_key=workspace_token, base_url=f"{workspace_url}/serving-endpoints")

print(f"Workspace user      : {current_user}")
print(f"Reasoning endpoint  : {REASONING_MODEL}")
print(f"Base endpoint       : {BASE_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm both endpoints are reachable
# MAGIC
# MAGIC One trivial call to each endpoint before the task suite runs. Fails fast if either endpoint is unreachable, billing isn't enabled, or the region doesn't have the model. The call also confirms how Databricks structures the `usage` object. The [API reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference) mentions a `reasoning_tokens` field "only applicable to reasoning models," but on Databricks the chain-of-thought tokens roll up into `completion_tokens` rather than being broken out separately. The experiment captures `completion_tokens` for that reason.

# COMMAND ----------

try:
    oai.chat.completions.create(
        model=REASONING_MODEL,
        messages=[{"role": "user", "content": "Reply with the single word: ok."}],
        max_tokens=32,
        temperature=0,
        extra_body={"reasoning_effort": "high"},
    )
    print("Reasoning endpoint reachable.")
except Exception as exc:  # noqa: BLE001
    print(f"Reasoning endpoint NOT reachable: {exc!r}")

try:
    oai.chat.completions.create(
        model=BASE_MODEL,
        messages=[{"role": "user", "content": "Reply with the single word: ok."}],
        max_tokens=32,
        temperature=0,
    )
    print("Base endpoint reachable.")
except Exception as exc:  # noqa: BLE001
    print(f"Base endpoint NOT reachable: {exc!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define the three tasks and their ground-truth answer sets
# MAGIC
# MAGIC The three complexity levels are defined below as a list of items per task, with the ground-truth answer attached to each item. Defining these in code, before the model calls, is the load-bearing piece of the scaffold: a reader can argue any one threshold or rubric, but the threshold and rubric were locked before the data existed.
# MAGIC
# MAGIC **Task 1: Simple structured extraction.** A short sentence about a Stark Industries division's quarterly revenue. Pull the company slug (the division name in kebab-case) and the revenue number into a JSON object. Success is exact-match on both fields.
# MAGIC
# MAGIC **Task 2: Multi-step reasoning.** A small puzzle that requires the model to combine three facts to answer one question. Success is exact-match on the final numeric or short-string answer after normalization.
# MAGIC
# MAGIC **Task 3: Mixed-signal classification.** A short snippet of customer feedback that mixes positive and negative signals. The label set is `{positive, negative, mixed}`. Success is label match against the rubric, which encodes the "the right label is `mixed` when both polarities are present" rule explicitly.

# COMMAND ----------

# Task 1: simple structured extraction. Five items.
TASK1_ITEMS = [
    {
        "id": "t1-01",
        "input": "Stark-Studios reported quarterly revenue of 412 million USD in Q1 2026.",
        "ground_truth": {"company_slug": "stark-studios", "revenue_usd_m": 412},
    },
    {
        "id": "t1-02",
        "input": "Stark-Games posted Q1 2026 revenue of 87 million USD across console and mobile.",
        "ground_truth": {"company_slug": "stark-games", "revenue_usd_m": 87},
    },
    {
        "id": "t1-03",
        "input": "Stark-Sports closed the quarter at 1,240 million USD in revenue.",
        "ground_truth": {"company_slug": "stark-sports", "revenue_usd_m": 1240},
    },
    {
        "id": "t1-04",
        "input": "Stark-Media disclosed quarterly revenue of 56 million USD.",
        "ground_truth": {"company_slug": "stark-media", "revenue_usd_m": 56},
    },
    {
        "id": "t1-05",
        "input": "Stark-Streaming reported 909 million USD in revenue for the most recent quarter.",
        "ground_truth": {"company_slug": "stark-streaming", "revenue_usd_m": 909},
    },
]

TASK1_INSTRUCTION = (
    "Extract the company slug and the revenue figure from the sentence into a JSON object "
    "with exactly two keys: `company_slug` (the kebab-case company name in lowercase) and "
    "`revenue_usd_m` (the revenue figure as an integer number of millions of USD). Respond "
    "with the JSON object only, no surrounding prose."
)


def grade_task1(model_output: str, ground_truth: dict[str, Any]) -> bool:
    """Exact-match grader for Task 1. Parses JSON out of the model output."""
    try:
        text = model_output.strip()
        # Tolerate a wrapped code fence; strip it before parsing.
        if text.startswith("```"):
            text = text.strip("`")
            if text.startswith("json"):
                text = text[4:]
            text = text.strip()
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return False
    return (
        isinstance(parsed, dict)
        and parsed.get("company_slug") == ground_truth["company_slug"]
        and parsed.get("revenue_usd_m") == ground_truth["revenue_usd_m"]
    )


# COMMAND ----------

# Task 2: multi-step reasoning. Five items. Each item carries three facts and a question
# that requires combining all three.
TASK2_ITEMS = [
    {
        "id": "t2-01",
        "input": (
            "Three facts about Stark-Streaming:\n"
            "  1. The service has 12 million subscribers.\n"
            "  2. Average revenue per subscriber per month is 14 USD.\n"
            "  3. Operating costs are 60 percent of monthly revenue.\n"
            "Question: what is the monthly operating profit in millions of USD? "
            "Respond with only the integer number of millions."
        ),
        "ground_truth": "67",  # 12 * 14 * 0.4 = 67.2 -> 67 after truncation/rounding
        "accepted_alternatives": {"67", "67.2", "67.20", "$67", "67 million"},
    },
    {
        "id": "t2-02",
        "input": (
            "Three facts about a Stark-Games mobile title:\n"
            "  1. Daily active users: 4 million.\n"
            "  2. Average sessions per user per day: 3.\n"
            "  3. Average ad impressions per session: 5.\n"
            "Question: how many ad impressions per day does the game generate? "
            "Respond with only the integer answer."
        ),
        "ground_truth": "60000000",
        "accepted_alternatives": {"60000000", "60,000,000", "60 million", "60M"},
    },
    {
        "id": "t2-03",
        "input": (
            "Three facts about the Stark-Sports league:\n"
            "  1. Regular-season games per team: 82.\n"
            "  2. Number of teams: 30.\n"
            "  3. Each game involves exactly two teams.\n"
            "Question: how many regular-season games are played in total across the league? "
            "Respond with only the integer answer."
        ),
        "ground_truth": "1230",
        "accepted_alternatives": {"1230", "1,230"},
    },
    {
        "id": "t2-04",
        "input": (
            "Three facts about the Stark-Studios film slate:\n"
            "  1. The studio releases 8 films per year.\n"
            "  2. The average budget per film is 75 million USD.\n"
            "  3. Marketing costs are an additional 40 percent of the production budget.\n"
            "Question: what is the total annual spend across production and marketing in millions of USD? "
            "Respond with only the integer answer."
        ),
        "ground_truth": "840",
        "accepted_alternatives": {"840", "$840", "840 million"},
    },
    {
        "id": "t2-05",
        "input": (
            "Three facts about a Stark-Live concert tour:\n"
            "  1. The tour has 50 dates.\n"
            "  2. Average attendance per date is 15,000.\n"
            "  3. Average ticket price is 80 USD.\n"
            "Question: what is the gross ticket revenue for the tour in millions of USD? "
            "Respond with only the integer answer."
        ),
        "ground_truth": "60",
        "accepted_alternatives": {"60", "$60", "60 million"},
    },
]

TASK2_INSTRUCTION = (
    "Solve the multi-step problem stated in the prompt. Respond with only the final "
    "answer, formatted as the prompt requests. Do not show your reasoning."
)


def grade_task2(model_output: str, ground_truth_item: dict[str, Any]) -> bool:
    """Normalized-match grader for Task 2."""
    text = model_output.strip().rstrip(".")
    # Accept any of the predefined alternative spellings.
    if text in ground_truth_item["accepted_alternatives"]:
        return True
    # Also accept the canonical answer with surrounding whitespace removed.
    return text == ground_truth_item["ground_truth"]


# COMMAND ----------

# Task 3: mixed-signal classification. Five items. Each item is a short snippet of
# customer feedback that mixes signals; the label set is {positive, negative, mixed}.
TASK3_ITEMS = [
    {
        "id": "t3-01",
        "input": (
            "Customer feedback: \"The new dashboard layout is clearly faster and I appreciate the dark mode, but the search "
            "results are completely broken since the update and I've had to go back to the old workflow three times this week.\""
        ),
        "ground_truth": "mixed",
    },
    {
        "id": "t3-02",
        "input": (
            "Customer feedback: \"Honestly the rollout has been painless from day one. Onboarding was the cleanest I've "
            "had with any product in the category and my team is already running our weekly planning meetings out of it.\""
        ),
        "ground_truth": "positive",
    },
    {
        "id": "t3-03",
        "input": (
            "Customer feedback: \"Three crashes in two days, the export feature swallowed our quarterly numbers, and support "
            "took 36 hours to respond. We are evaluating alternatives.\""
        ),
        "ground_truth": "negative",
    },
    {
        "id": "t3-04",
        "input": (
            "Customer feedback: \"Love what they're trying to do with the new collaboration features and the UI direction is "
            "promising, although the latency on shared documents is unacceptable and pricing went up 30 percent at renewal.\""
        ),
        "ground_truth": "mixed",
    },
    {
        "id": "t3-05",
        "input": (
            "Customer feedback: \"Cannot recommend. The product crashes on Windows 11, the mobile app is missing core features "
            "that the web version had two years ago, and the roadmap has been stagnant for the entire renewal cycle.\""
        ),
        "ground_truth": "negative",
    },
]

TASK3_INSTRUCTION = (
    "Classify the customer feedback into exactly one of these labels: `positive`, `negative`, "
    "or `mixed`. The label `mixed` is the correct choice when the feedback contains "
    "non-trivial positive AND non-trivial negative content; pick a single-polarity label "
    "only when the other polarity is genuinely absent. Respond with only the single label "
    "word, lowercase, no surrounding punctuation."
)


def grade_task3(model_output: str, ground_truth: str) -> bool:
    """Label-match grader for Task 3 with the rubric encoded above."""
    text = model_output.strip().lower().rstrip(".").strip("`'\" ")
    return text == ground_truth


print(f"Task 1 items: {len(TASK1_ITEMS)}")
print(f"Task 2 items: {len(TASK2_ITEMS)}")
print(f"Task 3 items: {len(TASK3_ITEMS)}")
print(f"Grading functions: {[fn.__name__ for fn in (grade_task1, grade_task2, grade_task3)]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The call helper
# MAGIC
# MAGIC One function that takes an endpoint name, a task instruction, and an input string, and returns the response text plus the usage object. The reasoning arm sends `reasoning_effort="high"` via `extra_body` (the OpenAI client passes it through to the Databricks endpoint); the base arm sends nothing extra. The function is the only place the two arms differ; everything else (temperature, max_tokens, message structure) is identical across arms.

# COMMAND ----------

def call_model(model: str, instruction: str, user_input: str) -> dict[str, Any]:
    """Run one chat completion and return text + usage + wall clock."""
    extra_body: dict[str, Any] | None = None
    if model == REASONING_MODEL:
        extra_body = {"reasoning_effort": "high"}

    request_kwargs: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": instruction},
            {"role": "user", "content": user_input},
        ],
        "temperature": 0,
        "max_tokens": 2048,
    }
    if extra_body:
        request_kwargs["extra_body"] = extra_body

    t_start = time.perf_counter()
    completion = oai.chat.completions.create(**request_kwargs)
    t_elapsed = time.perf_counter() - t_start

    usage = completion.usage.model_dump() if completion.usage is not None else {}
    raw_content = completion.choices[0].message.content
    # Databricks GPT-OSS endpoints return `content` as a list of typed parts
    # (`{"type": "reasoning", ...}` and `{"type": "text", "text": "..."}`).
    # Non-reasoning endpoints return a plain string. Normalize both shapes to
    # the visible-response text the grader expects.
    if isinstance(raw_content, list):
        text = "".join(
            part.get("text", "")
            for part in raw_content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    else:
        text = raw_content or ""
    return {
        "text": text,
        "usage": usage,
        "wall_clock_seconds": round(t_elapsed, 3),
    }


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1: simple structured extraction, both arms
# MAGIC
# MAGIC Runs `TASK1_ITEMS` against the reasoning arm and the base arm in sequence. Records the raw response, the success flag, the per-call token counts, and the wall clock. The aggregation cell at the bottom computes completion tokens per right answer for every cell of the 2x3 grid.

# COMMAND ----------

def run_task(
    task_name: str,
    items: list[dict[str, Any]],
    instruction: str,
    grader: Callable[..., bool],
    grader_key: str,
) -> dict[str, Any]:
    """Run one task across both arms and return the result block."""
    cells: dict[str, dict[str, Any]] = {}
    for arm_name, model in (("reasoning", REASONING_MODEL), ("base", BASE_MODEL)):
        per_item: list[dict[str, Any]] = []
        for item in items:
            try:
                call = call_model(model, instruction, item["input"])
                graded = grader(call["text"], item[grader_key])
                per_item.append({
                    "id": item["id"],
                    "success": bool(graded),
                    "wall_clock_seconds": call["wall_clock_seconds"],
                    "prompt_tokens": int(call["usage"].get("prompt_tokens") or 0),
                    "completion_tokens": int(call["usage"].get("completion_tokens") or 0),
                    "response": call["text"],
                })
            except Exception as exc:  # noqa: BLE001
                per_item.append({
                    "id": item["id"],
                    "success": False,
                    "wall_clock_seconds": 0.0,
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "response": f"ERROR: {exc!r}",
                })

        successes = sum(1 for r in per_item if r["success"])
        total_completion_tokens = sum(r["completion_tokens"] for r in per_item)
        total_prompt_tokens = sum(r["prompt_tokens"] for r in per_item)
        total_tokens = total_prompt_tokens + total_completion_tokens
        tokens_per_success = (
            round(total_completion_tokens / successes, 2) if successes > 0 else float("inf")
        )
        cells[arm_name] = {
            "model": model,
            "items": per_item,
            "successes": successes,
            "total_items": len(per_item),
            "total_prompt_tokens": total_prompt_tokens,
            "total_completion_tokens": total_completion_tokens,
            "total_tokens": total_tokens,
            "completion_tokens_per_success": tokens_per_success,
        }

    return {"task": task_name, "cells": cells}


task1_result = run_task(
    "task1_extraction",
    TASK1_ITEMS,
    TASK1_INSTRUCTION,
    grade_task1,
    grader_key="ground_truth",
)

for arm in ("reasoning", "base"):
    cell = task1_result["cells"][arm]
    print(f"Task 1 / {arm:9s} ({cell['model']}): {cell['successes']}/{cell['total_items']} pass, "
          f"prompt {cell['total_prompt_tokens']}, completion {cell['total_completion_tokens']}, "
          f"total {cell['total_tokens']}, tokens/right-answer {cell['completion_tokens_per_success']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 2: multi-step reasoning, both arms
# MAGIC
# MAGIC Same shape as Task 1, different items and grader. This is the middle complexity rung; the article's framing is that this is where reasoning starts to earn out, but Task 2 is not part of the verdict. The two validation conditions apply to Task 1 (simple) and Task 3 (mixed-signal); Task 2 is reported for context.

# COMMAND ----------

# Grader needs the whole item to look up alternatives; thin adapter to fit the
# run_task signature.
def _grade_task2_adapter(model_output: str, ground_truth_item: dict[str, Any]) -> bool:
    return grade_task2(model_output, ground_truth_item)


# Make the whole item available to the adapter by passing it through the
# `grader_key` channel.
TASK2_FOR_GRADING = [{**item, "_full_item": item} for item in TASK2_ITEMS]
task2_result = run_task(
    "task2_multi_step",
    TASK2_FOR_GRADING,
    TASK2_INSTRUCTION,
    _grade_task2_adapter,
    grader_key="_full_item",
)

for arm in ("reasoning", "base"):
    cell = task2_result["cells"][arm]
    print(f"Task 2 / {arm:9s} ({cell['model']}): {cell['successes']}/{cell['total_items']} pass, "
          f"prompt {cell['total_prompt_tokens']}, completion {cell['total_completion_tokens']}, "
          f"total {cell['total_tokens']}, tokens/right-answer {cell['completion_tokens_per_success']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 3: mixed-signal classification, both arms
# MAGIC
# MAGIC The validation condition for this task is that the base arm's completion tokens per right answer is at least 2x the reasoning arm's completion tokens per right answer. The rubric for `mixed` is encoded in the instruction and in the ground truth, so it cannot be re-stated after the run. A miss on a `mixed` item that the model returned `positive` for is a real signal; the rubric was locked.

# COMMAND ----------

task3_result = run_task(
    "task3_mixed_signal_classification",
    TASK3_ITEMS,
    TASK3_INSTRUCTION,
    grade_task3,
    grader_key="ground_truth",
)

for arm in ("reasoning", "base"):
    cell = task3_result["cells"][arm]
    print(f"Task 3 / {arm:9s} ({cell['model']}): {cell['successes']}/{cell['total_items']} pass, "
          f"prompt {cell['total_prompt_tokens']}, completion {cell['total_completion_tokens']}, "
          f"total {cell['total_tokens']}, tokens/right-answer {cell['completion_tokens_per_success']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary table and verdict
# MAGIC
# MAGIC Three views of the same data, then the verdict.
# MAGIC
# MAGIC **How to read the table:**
# MAGIC
# MAGIC - **Right** is how many of the 5 items the arm got correct.
# MAGIC - **Prompt + Completion = Total** tokens consumed by that arm on that task. Total is the headline number for "how much did this burn."
# MAGIC - **Tokens per right answer** counts completion tokens only (the chain-of-thought lives there on the reasoning arm) divided by the count of right answers. This is the metric the validation conditions key off. It captures the article's "hidden reasoning tokens" claim directly. Lower is better.
# MAGIC - The validation conditions below compare arms on that last column for Task 1 (simple) and Task 3 (mixed-signal).

# COMMAND ----------

# Build a pandas DataFrame from the three task results so the rest of this
# section can render the same data three ways (table, scoreboard, bar chart).
TASK_DISPLAY_NAMES = {
    "task1_extraction": "Simple extraction",
    "task2_multi_step": "Multi-step arithmetic",
    "task3_mixed_signal_classification": "Mixed-signal classification",
}

summary_rows = []
for result in (task1_result, task2_result, task3_result):
    for arm in ("reasoning", "base"):
        cell = result["cells"][arm]
        cps = cell["completion_tokens_per_success"]
        summary_rows.append({
            "Task": TASK_DISPLAY_NAMES[result["task"]],
            "Arm": arm,
            "Right": f"{cell['successes']}/{cell['total_items']}",
            "Prompt": cell["total_prompt_tokens"],
            "Completion": cell["total_completion_tokens"],
            "Total": cell["total_tokens"],
            "Tokens per right answer": cps if cps != float("inf") else None,
        })

summary_df = pd.DataFrame(summary_rows)
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right-answer scoreboard
# MAGIC
# MAGIC Six cells, one per arm-task combination. Cell color tracks the right-answer rate (green when the arm got all 5; red when most were wrong). Catches at a glance where each arm holds and where it falls down.

# COMMAND ----------

task_keys = ["task1_extraction", "task2_multi_step", "task3_mixed_signal_classification"]
task_results = {task1_result["task"]: task1_result, task2_result["task"]: task2_result, task3_result["task"]: task3_result}
arms = ["reasoning", "base"]

rates = np.zeros((len(arms), len(task_keys)))
cell_labels = [["" for _ in task_keys] for _ in arms]
for i, arm in enumerate(arms):
    for j, t in enumerate(task_keys):
        cell = task_results[t]["cells"][arm]
        rates[i, j] = cell["successes"] / cell["total_items"]
        cell_labels[i][j] = f"{cell['successes']}/{cell['total_items']}"

fig, ax = plt.subplots(figsize=(9, 2.8))
ax.imshow(rates, cmap="RdYlGn", vmin=0, vmax=1, aspect="auto")
ax.set_xticks(range(len(task_keys)))
ax.set_xticklabels([TASK_DISPLAY_NAMES[t] for t in task_keys], fontsize=11)
ax.set_yticks(range(len(arms)))
ax.set_yticklabels([a.capitalize() for a in arms], fontsize=11)
for i in range(len(arms)):
    for j in range(len(task_keys)):
        ax.text(j, i, cell_labels[i][j], ha="center", va="center",
                fontsize=22, fontweight="bold", color="white")
ax.set_title("Right answers per task and arm (of 5)", pad=14, fontsize=12)
ax.tick_params(axis="both", length=0)
plt.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tokens per right answer, with validation thresholds
# MAGIC
# MAGIC One grouped bar per task: reasoning vs base. Log-scale y-axis because the values span an order of magnitude. The two dashed lines mark the validation thresholds the hypothesis would need to clear:
# MAGIC
# MAGIC - Condition 1 line sits at 3x the base bar on the simple-extraction task. Reasoning has to cross this line for Condition 1 to be met.
# MAGIC - Condition 2 line sits at 2x the reasoning bar on the mixed-signal task. Base has to cross this line for Condition 2 to be met.

# COMMAND ----------

reasoning_cps = []
base_cps = []
for t in task_keys:
    cells = task_results[t]["cells"]
    r = cells["reasoning"]["completion_tokens_per_success"]
    b = cells["base"]["completion_tokens_per_success"]
    reasoning_cps.append(r if r != float("inf") else 0.0)
    base_cps.append(b if b != float("inf") else 0.0)

x = np.arange(len(task_keys))
bar_w = 0.36

fig, ax = plt.subplots(figsize=(11, 5.5))
ax.bar(x - bar_w / 2, reasoning_cps, bar_w,
       label="Reasoning (databricks-gpt-oss-120b)", color="#C62828")
ax.bar(x + bar_w / 2, base_cps, bar_w,
       label="Base (databricks-meta-llama-3-3-70b-instruct)", color="#1565C0")

# Annotate each bar with its numeric value.
for xi, val in zip(x - bar_w / 2, reasoning_cps):
    if val > 0:
        ax.text(xi, val * 1.06, f"{val:.1f}", ha="center", va="bottom", fontsize=10, color="#C62828", fontweight="bold")
for xi, val in zip(x + bar_w / 2, base_cps):
    if val > 0:
        ax.text(xi, val * 1.06, f"{val:.1f}", ha="center", va="bottom", fontsize=10, color="#1565C0", fontweight="bold")

# Validation threshold lines.
t1_base = base_cps[0]
if t1_base > 0:
    ax.hlines(t1_base * 3, x[0] - bar_w, x[0] + bar_w,
              colors="#C62828", linestyles="dashed", linewidth=2,
              label="Condition 1 threshold: 3x base on simple extraction")

t3_reasoning = reasoning_cps[2]
if t3_reasoning > 0:
    ax.hlines(t3_reasoning * 2, x[2] - bar_w, x[2] + bar_w,
              colors="#1565C0", linestyles="dashed", linewidth=2,
              label="Condition 2 threshold: 2x reasoning on mixed-signal")

ax.set_yscale("log")
ax.set_ylabel("Tokens per right answer (log scale)")
ax.set_title("Tokens per right answer, by task and arm", pad=14)
ax.set_xticks(x)
ax.set_xticklabels([TASK_DISPLAY_NAMES[t] for t in task_keys])
ax.legend(loc="upper right", fontsize=9, framealpha=0.95)
ax.grid(axis="y", which="both", alpha=0.25)
ax.set_axisbelow(True)
plt.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

def compute_verdict(
    task1: dict[str, Any],
    task3: dict[str, Any],
    noise_band_multiple: float = 1.5,
    extraction_threshold: float = 3.0,
    mixed_signal_threshold: float = 2.0,
) -> dict[str, Any]:
    """Apply the two validation conditions and return the verdict."""
    t1_reasoning_cps = task1["cells"]["reasoning"]["completion_tokens_per_success"]
    t1_base_cps = task1["cells"]["base"]["completion_tokens_per_success"]
    t3_reasoning_cps = task3["cells"]["reasoning"]["completion_tokens_per_success"]
    t3_base_cps = task3["cells"]["base"]["completion_tokens_per_success"]

    def ratio(numer: float, denom: float) -> float:
        if denom == 0:
            return float("inf")
        if denom == float("inf") or numer == float("inf"):
            return float("inf")
        return numer / denom

    def in_noise_band(a: float, b: float) -> bool:
        if a == float("inf") or b == float("inf"):
            return False
        if min(a, b) == 0:
            return False
        return max(a, b) / min(a, b) < noise_band_multiple

    cond1_ratio = ratio(t1_reasoning_cps, t1_base_cps)
    cond1_met = cond1_ratio >= extraction_threshold
    cond1_tie = in_noise_band(t1_reasoning_cps, t1_base_cps)

    cond2_ratio = ratio(t3_base_cps, t3_reasoning_cps)
    cond2_met = cond2_ratio >= mixed_signal_threshold
    cond2_tie = in_noise_band(t3_reasoning_cps, t3_base_cps)

    if cond1_met and cond2_met:
        verdict = "supported"
    elif cond1_tie or cond2_tie:
        verdict = "inconclusive"
    else:
        verdict = "not_supported"

    return {
        "verdict": verdict,
        "condition_1_extraction": {
            "ratio_reasoning_over_base": cond1_ratio,
            "threshold": extraction_threshold,
            "met": cond1_met,
            "tie_in_noise_band": cond1_tie,
        },
        "condition_2_mixed_signal": {
            "ratio_base_over_reasoning": cond2_ratio,
            "threshold": mixed_signal_threshold,
            "met": cond2_met,
            "tie_in_noise_band": cond2_tie,
        },
    }


verdict = compute_verdict(task1_result, task3_result)
c1 = verdict["condition_1_extraction"]
c2 = verdict["condition_2_mixed_signal"]
c1_status = "met" if c1["met"] else ("tie" if c1["tie_in_noise_band"] else "not met")
c2_status = "met" if c2["met"] else ("tie" if c2["tie_in_noise_band"] else "not met")

print("\nVERDICT")
print("-------")
print(f"Verdict: {verdict['verdict']}")
print()
print(f"Condition 1 (simple extraction):")
print(f"  Reasoning used {c1['ratio_reasoning_over_base']:.1f}x more completion tokens per right answer than base.")
print(f"  Threshold: {c1['threshold']:.0f}x. Result: {c1_status}.")
print()
print(f"Condition 2 (mixed-signal classification):")
if c2["ratio_base_over_reasoning"] >= 1.0:
    print(f"  Base used {c2['ratio_base_over_reasoning']:.1f}x more completion tokens per right answer than reasoning.")
else:
    inverted = 1.0 / c2["ratio_base_over_reasoning"] if c2["ratio_base_over_reasoning"] > 0 else float("inf")
    print(f"  Base used {c2['ratio_base_over_reasoning']:.3f}x what reasoning used per right answer. Base is {inverted:.0f}x cheaper, the opposite direction the article predicted.")
print(f"  Threshold: {c2['threshold']:.0f}x. Result: {c2_status}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the numbers say
# MAGIC
# MAGIC The verdict above is the deterministic answer to the hypothesis from section 1. Both validation conditions had to hold for `supported`; anything else is `not_supported` or `inconclusive`. What the verdict means for a routing decision in your own workload depends on which tasks in this suite most closely resemble what you actually run in production. Task 1 (structured extraction) and Task 3 (mixed-signal classification) are the verdict inputs. Task 2 (multi-step arithmetic) is reported for context and tends to be where the reasoning advantage is largest.
# MAGIC
# MAGIC ### Where to go next
# MAGIC
# MAGIC - [Foundation Model APIs](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis): landing page for pay-per-token and provisioned-throughput modes on Databricks-hosted models.
# MAGIC - [Foundation Model APIs reference](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/api-reference): request and response shapes, including the `reasoning_effort` parameter and the `usage.completion_tokens` integer that this notebook keys off.
# MAGIC - [Foundation Model APIs supported models](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models): current list of Databricks-hosted models, including the reasoning and non-reasoning endpoints used in this run.
# MAGIC - [Foundation Model APIs limits](https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/limits): documented input/output tokens-per-minute limits per endpoint.
