# Databricks notebook source
# MAGIC %md
# MAGIC # Where Is Delivery Slowing Down? A DevOps Genie Agent on the Godot Repo
# MAGIC
# MAGIC I built a [Genie Agent](https://docs.databricks.com/aws/en/genie-agents/) for a DevOps job: point it at a year of history from the
# MAGIC open-source [Godot game engine](https://github.com/godotengine/godot) and ask where changes slow down on their way to a release.
# MAGIC Godot's public repo shows when each pull request (PR) merged and which release first shipped it, and how long continuous
# MAGIC integration (CI) took to go green again whenever a merge broke the build. Those are the numbers the DevOps Research and
# MAGIC Assessment ([DORA](https://dora.dev/guides/dora-metrics/)) delivery metrics are made of.
# MAGIC
# MAGIC The first decision was what to point the agent at, and a recent
# MAGIC [Databricks Community thread](https://community.databricks.com/t5/generative-ai/genie-space-delta-tables-or-metric-view/td-p/160285)
# MAGIC asked exactly that (Genie Agents were still called Genie spaces then):
# MAGIC
# MAGIC > "My question is, is it better to point the genie space to the raw table or create a metric view with all the possible measures someone might ask? Considering this will also be enriched in the future with more data."
# MAGIC > Source: Databricks Community, Generative AI board
# MAGIC
# MAGIC The first reply says raw tables are fine for a proof of concept and a
# MAGIC [metric view](https://docs.databricks.com/aws/en/uc-semantics/metric-views/) is the way to go for production. When the poster asked
# MAGIC whether giving one agent both would confuse it, the next reply said to hide the raw tables and expose only the metric view. Neither
# MAGIC reply was marked as the answer.
# MAGIC
# MAGIC So I built the agent three ways and tested all three before trusting any of them. One sees only raw tables, one sees only metric
# MAGIC views, and one sees both. Each answers the same twelve [benchmark questions](https://docs.databricks.com/aws/en/genie/benchmarks),
# MAGIC three rounds over, and Databricks grades every answer. Then the agent that scored best goes to work on four questions about where
# MAGIC Godot's delivery slows down.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC **What you need:**
# MAGIC - A Databricks Free Edition workspace. The whole notebook runs on [Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).
# MAGIC - Serverless notebook compute, plus a [SQL warehouse](https://docs.databricks.com/aws/en/compute/sql-warehouse/) for Genie to run its queries on.
# MAGIC   The Serverless Starter Warehouse that comes with Free Edition works, and the configuration cell finds it.
# MAGIC - Permission to create a schema, a [volume](https://docs.databricks.com/aws/en/volumes/), tables and views in the `workspace` catalog.
# MAGIC
# MAGIC **No pip installs.** The notebook uses `requests` and the standard library, both already on serverless.
# MAGIC
# MAGIC **Runtime:** about 18 minutes. The benchmark runs take most of it, since each agent answers the twelve questions three times over.
# MAGIC
# MAGIC **What it creates:** a schema `godot_delivery` in `workspace` holding one volume, six tables and four metric views, plus three
# MAGIC Genie Agents in your home folder. The cleanup cell at the end removes all of it.
# MAGIC
# MAGIC **The data:** a pinned snapshot of `godotengine/godot`, pulled from the GitHub REST API on Oct 5, 2026 and covering
# MAGIC Oct 5, 2025 to Oct 5, 2026. It records code areas, release lines and author types, never a contributor's name: this is about the agent,
# MAGIC not a grade for the people who maintain Godot. The notebook downloads the snapshot from the repo it ships in. An optional cell near
# MAGIC the end pulls a fresh copy with your own GitHub token.

# COMMAND ----------

# DBTITLE 1,Imports
import hashlib
import json
import os
import re
import statistics
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import pandas as pd
import requests

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration
# MAGIC
# MAGIC `EVAL_ROUNDS` is how many times each agent answers the full benchmark. Genie writes new SQL every time it's asked, so one
# MAGIC round can't tell a lucky answer from a dependable one.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "workspace"          # Free Edition's writable catalog
SCHEMA = "godot_delivery"
VOLUME = "snapshot"
WAREHOUSE_ID = None            # None picks the workspace's serverless warehouse; paste an ID to choose one
EVAL_ROUNDS = 3                # full benchmark runs per agent
EVAL_TIMEOUT_MIN = 30          # give up on a round after this long

SNAPSHOT_URL = "https://raw.githubusercontent.com/gnakan/databricks-notebooks/main/experiments/godot-genie-metric-view/snapshot"
SNAPSHOT_FILES = ["pull_requests.csv", "pr_code_areas.csv", "pr_change_kinds.csv", "releases.csv",
                  "release_commits.csv", "ci_runs.csv", "snapshot.json", "fetch_snapshot.py"]

# Workspace identity and API access come from the notebook session, never hardcoded.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = ctx.apiUrl().get().rstrip("/")
TOKEN = ctx.apiToken().get()
USER = ctx.userName().get()

FQ = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"


def api(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    """Call a workspace REST endpoint as the notebook user and return (status, parsed body)."""
    resp = requests.request(method, f"{HOST}{path}", headers={"Authorization": f"Bearer {TOKEN}"},
                            json=body, timeout=120)
    try:
        return resp.status_code, resp.json() if resp.content else {}
    except ValueError:
        return resp.status_code, {"raw": resp.text[:500]}


if WAREHOUSE_ID is None:
    code, body = api("GET", "/api/2.0/sql/warehouses")
    warehouses = body.get("warehouses", [])
    serverless = [w for w in warehouses if w.get("enable_serverless_compute")]
    WAREHOUSE_ID = (serverless or warehouses or [{}])[0].get("id")
assert WAREHOUSE_ID, "No SQL warehouse found. Create one, or set WAREHOUSE_ID above."

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.{VOLUME}")
print(f"Schema {FQ}, volume {VOLUME_PATH}, warehouse {WAREHOUSE_ID}, user {USER}")

# COMMAND ----------

# DBTITLE 1,Display helpers
# hide-code
import html

# Databricks brand palette (every hex is in databricks-palette.yaml): navy, oat and white carry the
# page, and lava is the one pop, used only where something needs a second look.
OAT_LIGHT, OAT, WHITE = "#F9F7F4", "#EEEDE9", "#FFFFFF"
NAVY, NAVY_600, NAVY_400, NAVY_300 = "#1B3139", "#1B5162", "#90A5B1", "#C4CCD6"
GRAY_TEXT, GRAY_LINES = "#5A6F77", "#DCE0E2"
LAVA = "#FF3621"
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"  # unquoted: it sits inside quoted style attributes
MONO = "Menlo, Consolas, monospace"
FONTS = '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'


def esc(value) -> str:
    """Escape any run value before it goes into the HTML."""
    return html.escape(str(value))


def frame(headline: str, dek: str, body: str) -> str:
    """One display: oat ground, navy top edge, DM Sans, a headline, a dek (already HTML), then the body."""
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:30px 34px 26px;font-family:{SANS};"
            f"border-top:6px solid {NAVY}'>"
            f"<div style='font-size:28px;font-weight:700;line-height:1.15;max-width:780px'>{esc(headline)}</div>"
            f"<div style='font-size:15px;color:{GRAY_TEXT};line-height:1.45;max-width:720px;margin:10px 0 22px'>{dek}</div>"
            f"{body}</div>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Which Genie this is
# MAGIC
# MAGIC "Genie for DevOps" can mean three different products, and it's easy to read about one and picture another. This notebook
# MAGIC builds [Genie Agents](https://docs.databricks.com/aws/en/genie-agents/): they answer questions from the data you point them at.
# MAGIC [Genie Code](https://docs.databricks.com/aws/en/genie-code/) writes and fixes code, and
# MAGIC [Genie ZeroOps](https://www.databricks.com/blog/introducing-genie-zeroops) is the one that acts on jobs and pipelines.

# COMMAND ----------

# DBTITLE 1,Three products called Genie
# hide-code
GENIES = [
    ("Genie Agents", "Built here, three times",
     "A Genie Agent is a domain-specific natural-language chat interface in Databricks where users ask questions of their data and get back SQL queries, results tables, and visualizations.",
     "https://docs.databricks.com/aws/en/genie-agents/", "Genie Agents docs", True),
    ("Genie Code", "Not used here",
     "Genie Code is the AI coding and data assistant for developers and technical practitioners in the Databricks workspace.",
     "https://docs.databricks.com/aws/en/genie-code/", "Genie Code docs", False),
    ("Genie ZeroOps", "Not used here; entering private preview",
     "monitors your data and AI assets (such as pipelines, jobs, tables and ML models) and takes action before or when things go wrong",
     "https://www.databricks.com/blog/introducing-genie-zeroops", "Introducing Genie ZeroOps", False),
]
cols = "".join(
    f"<div style='flex:1;min-width:220px;border-top:{4 if here else 1}px solid {NAVY if here else GRAY_LINES};padding-top:12px'>"
    f"<div style='font-size:18px;font-weight:700;color:{NAVY}'>{esc(name)}</div>"
    f"<div style='font-size:12px;font-weight:700;color:{NAVY_600 if here else GRAY_TEXT};margin:2px 0 10px'>{esc(role)}</div>"
    f"<div style='font-size:14px;color:{NAVY if here else GRAY_TEXT};line-height:1.45;font-style:italic'>&ldquo;{esc(quote)}&rdquo;</div>"
    f"<div style='font-size:12px;margin-top:8px'><a href='{url}' style='color:{NAVY_600}'>{esc(src)}</a></div></div>"
    for name, role, quote, url, src, here in GENIES)
displayHTML(frame("Three products called Genie, and the one this notebook builds",
                  "Each in Databricks' own words. A Genie Agent answers questions from data; it doesn't change anything.",
                  f"<div style='display:flex;gap:26px;flex-wrap:wrap'>{cols}</div>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## The data: six raw tables
# MAGIC
# MAGIC Godot ships from one `master` branch and keeps several stable release lines alive at once, so its delivery history has the same
# MAGIC joins a real DevOps dashboard has. A change lands on `master` as a merged PR, ships in the next feature release, and if it fixes
# MAGIC something that matters it gets backported to the release lines already out. A backport is a commit on a release branch whose message
# MAGIC ends with `(cherry picked from commit <sha>)`. When I pulled the snapshot I resolved each of those shas to the `master` PR it came
# MAGIC from, through GitHub's `commits/{sha}/pulls` endpoint, so `release_commits.pr_number` points at the original PR whether the
# MAGIC commit arrived in a merge or a backport. Godot removes the `cherrypick:4.x` labels once a pick is done, so the labels can't carry that.
# MAGIC
# MAGIC | Table | One row per |
# MAGIC |---|---|
# MAGIC | `pull_requests` | merged PR, any base branch |
# MAGIC | `pr_code_areas` | PR and code area label (`topic:` or `platform:`); a PR can carry several |
# MAGIC | `pr_change_kinds` | PR and change kind label (bug, regression, enhancement and so on) |
# MAGIC | `releases` | stable release published in the window |
# MAGIC | `release_commits` | commit included in a 4.6, 4.7 or 3.6 release, typed `merge`, `cherry_pick` or `direct` |
# MAGIC | `ci_runs` | run of Godot's top-level CI workflow, with its attempt count |
# MAGIC
# MAGIC The cell downloads the CSVs into the volume (skipping any already there), loads them as Delta tables, and gives every table and
# MAGIC column a comment with [`COMMENT ON`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-comment). The
# MAGIC [Genie best practices page](https://docs.databricks.com/aws/en/genie-agents/best-practices) says "Genie uses Unity Catalog column names
# MAGIC and descriptions to generate responses," and the poster would document their sales table too. A raw-tables agent with no comments
# MAGIC would be a straw man.

# COMMAND ----------

# DBTITLE 1,Download the snapshot and load the raw tables
for name in SNAPSHOT_FILES:
    target = f"{VOLUME_PATH}/{name}"
    if os.path.exists(target):
        continue
    resp = requests.get(f"{SNAPSHOT_URL}/{name}", timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Could not download {name} ({resp.status_code}) from {SNAPSHOT_URL}. "
                           f"Copy the snapshot folder into {VOLUME_PATH} by hand and rerun this cell.")
    with open(target, "wb") as fh:
        fh.write(resp.content)

SNAPSHOT_META = json.loads(open(f"{VOLUME_PATH}/snapshot.json").read())

# Every table and column gets a comment, because Genie reads them.
TABLES = {
    "pull_requests": ("Merged pull requests in godotengine/godot, one row per PR, merged between the snapshot's window start and end.", [
        ("pr_number", "INT", "GitHub pull request number"),
        ("created_at", "TIMESTAMP", "When the PR was opened (UTC)"),
        ("merged_at", "TIMESTAMP", "When the PR was merged (UTC)"),
        ("base_branch", "STRING", "Branch the PR merged into: master, or a release branch such as 4.6 or 3.x"),
        ("author_type", "STRING", "human or bot"),
        ("author_association", "STRING", "GitHub author association: MEMBER of the godotengine organization, COLLABORATOR, CONTRIBUTOR (merged before) or NONE"),
        ("milestone", "STRING", "Milestone the PR was assigned to, usually a version such as 4.6"),
        ("merge_commit_sha", "STRING", "Commit the merge produced on the base branch"),
    ]),
    "pr_code_areas": ("Code area labels on merged PRs, one row per PR and label. A PR can carry several areas.", [
        ("pr_number", "INT", "GitHub pull request number, joins to pull_requests.pr_number"),
        ("label_kind", "STRING", "topic (an engine subsystem) or platform (a target platform)"),
        ("code_area", "STRING", "The area, for example editor, rendering, gdscript, android"),
    ]),
    "pr_change_kinds": ("Change kind labels on merged PRs, one row per PR and label.", [
        ("pr_number", "INT", "GitHub pull request number, joins to pull_requests.pr_number"),
        ("change_kind", "STRING", "bug, regression, enhancement, crash, performance, usability, documentation, discussion or feature proposal"),
    ]),
    "releases": ("Stable releases of Godot published in the snapshot window, one row per release.", [
        ("release_tag", "STRING", "Git tag, for example 4.6.2-stable"),
        ("release_line", "STRING", "Major.minor line the release belongs to, for example 4.6"),
        ("version", "STRING", "Version number without the -stable suffix"),
        ("release_type", "STRING", "feature (x.y) or patch (x.y.z)"),
        ("published_at", "TIMESTAMP", "When the release was published on GitHub (UTC)"),
    ]),
    "release_commits": ("Commits that shipped in each 4.6, 4.7 and 3.6 release, from a tag-to-tag compare, one row per release and commit.", [
        ("release_tag", "STRING", "Release the commit shipped in, joins to releases.release_tag"),
        ("commit_sha", "STRING", "Commit SHA"),
        ("committed_at", "TIMESTAMP", "Commit time (UTC)"),
        ("is_merge_commit", "BOOLEAN", "True when the commit has more than one parent"),
        ("commit_kind", "STRING", "merge (a PR merge commit), cherry_pick (a backport carrying a cherry-picked-from trailer) or direct"),
        ("pr_number", "INT", "The master PR this commit delivers: from the merge message, or resolved from the cherry-pick trailer. Null for direct commits"),
        ("cherry_picked_from_sha", "STRING", "For a backport, the master commit it was cherry-picked from"),
        ("author_type", "STRING", "human or bot"),
    ]),
    "ci_runs": ("Runs of Godot's top-level GitHub Actions CI workflow (runner.yml), one row per run.", [
        ("run_id", "BIGINT", "GitHub Actions run ID"),
        ("event", "STRING", "What triggered the run: pull_request or push"),
        ("branch", "STRING", "For push runs, the branch pushed to (master, a release branch, or other). Null for pull_request runs"),
        ("pr_number", "INT", "For pull_request runs, the PR number when GitHub reports it"),
        ("head_sha", "STRING", "Commit the run tested"),
        ("status", "STRING", "Run status, completed for finished runs"),
        ("conclusion", "STRING", "success, failure, cancelled or startup_failure"),
        ("run_attempt", "INT", "How many times the run was attempted; above 1 means it was re-run"),
        ("created_at", "TIMESTAMP", "When the run was created (UTC)"),
        ("run_started_at", "TIMESTAMP", "When the latest attempt started (UTC)"),
        ("updated_at", "TIMESTAMP", "When the run last changed, its finish time for completed runs (UTC)"),
    ]),
}

def sql_text(text: str) -> str:
    """Escape a comment for a single-quoted SQL string."""
    return text.replace("\\", "\\\\").replace("'", "\\'")


row_counts = {}
for table, (table_comment, columns) in TABLES.items():
    ddl = ", ".join(f"{c} {t}" for c, t, _ in columns)
    df = (spark.read.option("header", True).option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ssX")
          .schema(ddl).csv(f"{VOLUME_PATH}/{table}.csv"))
    df.write.mode("overwrite").option("overwriteSchema", True).saveAsTable(f"{FQ}.{table}")
    spark.sql(f"COMMENT ON TABLE {FQ}.{table} IS '{sql_text(table_comment)}'")
    for c, _, col_comment in columns:
        spark.sql(f"ALTER TABLE {FQ}.{table} ALTER COLUMN {c} COMMENT '{sql_text(col_comment)}'")
    row_counts[table] = spark.table(f"{FQ}.{table}").count()

print("Raw tables loaded, every table and column commented:")
display(pd.DataFrame([{"table": t, "rows": n} for t, n in row_counts.items()]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Godot's year, drawn
# MAGIC
# MAGIC The chart lays Godot's four release lines side by side across the year, with each stable release on the day it shipped. Under each
# MAGIC patch release on the 3.6, 4.6 and 4.7 lines is the number of PRs it carried back from `master`; those are the lines the snapshot traced.
# MAGIC The bars at the bottom count the PRs merged into `master` each month. That's the pattern the benchmark questions ask about: changes
# MAGIC pile up on `master`, ship in a feature release, and the ones that matter get carried back to the lines already out.

# COMMAND ----------

# DBTITLE 1,Godot's year of releases
# hide-code
rel = spark.sql(f"""
    SELECT r.release_tag, r.release_line, r.version, r.release_type, r.published_at,
           COUNT(DISTINCT CASE WHEN c.commit_kind = 'cherry_pick' THEN c.pr_number END) AS backported_prs,
           COUNT(c.commit_sha) AS traced_commits
    FROM {FQ}.releases r LEFT JOIN {FQ}.release_commits c ON r.release_tag = c.release_tag
    GROUP BY ALL ORDER BY r.published_at""").toPandas()
merges = spark.sql(f"""SELECT DATE_TRUNC('MONTH', merged_at) AS month, COUNT(*) AS prs FROM {FQ}.pull_requests
                       WHERE base_branch = 'master' GROUP BY ALL ORDER BY month""").toPandas()
rel["t"] = pd.to_datetime(rel.published_at, utc=True)
merges["t"] = pd.to_datetime(merges.month, utc=True)
w_start = pd.Timestamp(SNAPSHOT_META["window_start"], tz="UTC")
w_end = pd.Timestamp(SNAPSHOT_META["window_end"], tz="UTC")

W, LEFT, RIGHT, LANE, TOP = 940, 70, 24, 66, 16
span = (w_end - w_start).total_seconds()
x = lambda t: LEFT + (t - w_start).total_seconds() / span * (W - LEFT - RIGHT)
lines = sorted(rel.release_line.unique(), key=lambda v: tuple(int(n) for n in v.split(".")), reverse=True)
svg = []
for i, line in enumerate(lines):
    y = TOP + i * LANE + 34
    svg.append(f"<text x='0' y='{y + 5}' font-size='15' font-weight='700' fill='{NAVY}'>{esc(line)}</text>"
               f"<line x1='{LEFT}' y1='{y}' x2='{W - RIGHT}' y2='{y}' stroke='{GRAY_LINES}' stroke-width='2'/>")
    for _, r in rel[rel.release_line == line].iterrows():
        cx = x(r.t)
        feature = r.release_type == "feature"
        svg.append(f"<circle cx='{cx:.1f}' cy='{y}' r='{9 if feature else 6}' fill='{NAVY if feature else WHITE}' "
                   f"stroke='{NAVY}' stroke-width='2'/>"
                   f"<text x='{cx:.1f}' y='{y - 15}' font-size='12' font-weight='700' fill='{NAVY}' text-anchor='middle'>{esc(r.version)}</text>")
        if not feature and r.traced_commits:
            svg.append(f"<text x='{cx:.1f}' y='{y + 22}' font-size='11' fill='{GRAY_TEXT}' text-anchor='middle'>"
                       f"{r.backported_prs} backported</text>")
axis_y = TOP + len(lines) * LANE + 18
svg.append(f"<line x1='{LEFT}' y1='{axis_y}' x2='{W - RIGHT}' y2='{axis_y}' stroke='{NAVY_300}'/>")
month = pd.Timestamp(year=w_start.year, month=w_start.month, day=1, tz="UTC") + pd.offsets.MonthBegin(1)
while month < w_end:
    label = month.strftime("%b %Y") if month.month == 1 else month.strftime("%b")
    svg.append(f"<line x1='{x(month):.1f}' y1='{axis_y}' x2='{x(month):.1f}' y2='{axis_y + 5}' stroke='{NAVY_300}'/>"
               f"<text x='{x(month):.1f}' y='{axis_y + 18}' font-size='11' fill='{GRAY_TEXT}' text-anchor='middle'>{label}</text>")
    month += pd.offsets.MonthBegin(1)
bar_base, bar_h = axis_y + 100, 64
peak = merges.prs.max()
svg.append(f"<text x='0' y='{bar_base - bar_h + 10}' font-size='11' fill='{GRAY_TEXT}'>PRs merged</text>"
           f"<text x='0' y='{bar_base - bar_h + 24}' font-size='11' fill='{GRAY_TEXT}'>into master</text>")
for _, m in merges.iterrows():
    in_window = (min(m.t + pd.offsets.MonthBegin(1), w_end) - max(m.t, w_start)).days
    if in_window < 15:
        continue  # a few days of a month would read as a slow month
    x0 = max(x(m.t), LEFT)
    x1 = min(x(m.t + pd.offsets.MonthBegin(1)), W - RIGHT)
    h = bar_h * m.prs / peak
    svg.append(f"<rect x='{x0 + 3:.1f}' y='{bar_base - h:.1f}' width='{max(x1 - x0 - 6, 1):.1f}' height='{h:.1f}' fill='{NAVY_400}'/>"
               f"<text x='{(x0 + x1) / 2:.1f}' y='{bar_base + 13}' font-size='10' fill='{GRAY_TEXT}' text-anchor='middle'>{m.prs}</text>")
height = bar_base + 22
traced = ", ".join(sorted({l for l in rel[rel.traced_commits > 0].release_line}, key=lambda v: tuple(int(n) for n in v.split("."))))
print("Godot's year of releases, by line")
displayHTML(frame(f"{len(rel)} stable releases on {len(lines)} lines, {int(merges.prs.sum()):,} PRs into master",
                  f"Filled circles are feature releases, open circles patch releases. Backports are traced on the {esc(traced)} lines.",
                  f"<svg width='{W}' height='{height}' viewBox='0 0 {W} {height}' font-family='{SANS}'>{''.join(svg)}</svg>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### One PR's trip to release
# MAGIC
# MAGIC Lead time counts the days between a merge and the *first* release that carried the change. Godot makes that harder than it sounds: a fix
# MAGIC merged into `master` can reach a patch release on an older line as a backport before the next feature release ships it. The cell
# MAGIC picks the `master` PR that shipped in the most releases and draws its trip.

# COMMAND ----------

# DBTITLE 1,One PR's trip to release
# hide-code
# Prefer a PR that reached a patch release as a backport before a feature release shipped it,
# then the one that shipped in the most releases, then the earliest merged.
trip = spark.sql(f"""
    WITH stops AS (
      SELECT c.pr_number, r.release_tag, r.release_type, r.published_at, c.commit_kind
      FROM {FQ}.release_commits c JOIN {FQ}.releases r ON c.release_tag = r.release_tag
      WHERE c.pr_number IS NOT NULL),
    per_pr AS (
      SELECT pr_number, COUNT(DISTINCT release_tag) AS releases,
             MIN_BY(commit_kind, published_at) AS first_kind,
             MAX(CASE WHEN release_type = 'feature' THEN 1 ELSE 0 END) AS has_feature
      FROM stops GROUP BY pr_number)
    SELECT p.pr_number, p.merged_at, s.releases
    FROM {FQ}.pull_requests p JOIN per_pr s ON p.pr_number = s.pr_number
    WHERE p.base_branch = 'master'
    ORDER BY CASE WHEN s.first_kind = 'cherry_pick' AND s.has_feature = 1 THEN 0 ELSE 1 END,
             s.releases DESC, p.merged_at
    LIMIT 1""").first()
stops = spark.sql(f"""
    SELECT r.release_tag, r.published_at, MIN(c.commit_kind) AS commit_kind
    FROM {FQ}.release_commits c JOIN {FQ}.releases r ON c.release_tag = r.release_tag
    WHERE c.pr_number = {trip.pr_number} GROUP BY ALL ORDER BY r.published_at""").toPandas()
areas = sorted(r.code_area for r in spark.sql(f"SELECT code_area FROM {FQ}.pr_code_areas WHERE pr_number = {trip.pr_number}").collect())
merged = pd.Timestamp(trip.merged_at)
merged = merged.tz_localize("UTC") if merged.tzinfo is None else merged
stops["t"] = pd.to_datetime(stops.published_at, utc=True)
stops["days"] = (stops.t - merged).dt.total_seconds() / 86400


def days_text(d: float) -> str:
    """Whole days, singular when it is one."""
    n = round(d)
    return f"{n} day" if n == 1 else f"{n} days"


# Stops are spaced evenly, not by date: the days are in the labels, and a stop a day after the merge
# would otherwise sit on top of it.
W, PAD, y = 940, 80, 64
step = (W - 2 * PAD) / max(len(stops), 1)
svg = [f"<line x1='{PAD}' y1='{y}' x2='{PAD + step * len(stops):.1f}' y2='{y}' stroke='{GRAY_LINES}' stroke-width='2'/>",
       f"<circle cx='{PAD}' cy='{y}' r='7' fill='{WHITE}' stroke='{NAVY}' stroke-width='2'/>",
       f"<text x='{PAD}' y='{y - 18}' font-size='12' font-weight='700' fill='{NAVY}' text-anchor='middle'>merged into master</text>",
       f"<text x='{PAD}' y='{y + 26}' font-size='11' fill='{GRAY_TEXT}' text-anchor='middle'>{merged:%b %d, %Y}</text>"]
for i, r in stops.reset_index(drop=True).iterrows():
    cx = PAD + step * (i + 1)
    first = i == 0
    how = "as a backport" if r.commit_kind == "cherry_pick" else "merged in"
    svg.append(f"<circle cx='{cx:.1f}' cy='{y}' r='{9 if first else 6}' fill='{NAVY}'/>"
               f"<text x='{cx:.1f}' y='{y - 18}' font-size='12' font-weight='{700 if first else 400}' fill='{NAVY}' "
               f"text-anchor='middle'>{esc(r.release_tag)}</text>"
               f"<text x='{cx:.1f}' y='{y + 26}' font-size='11' fill='{GRAY_TEXT}' text-anchor='middle'>"
               f"{days_text(r.days)} after merge, {how}</text>")
print(f"One PR's trip: #{trip.pr_number}")
displayHTML(frame(f"PR #{trip.pr_number} shipped in {len(stops)} releases",
                  f"Code area: {esc(', '.join(areas) or 'unlabeled')}. Its lead time counts only the first stop: "
                  f"<b style='color:{NAVY}'>{days_text(stops.days.iloc[0])}</b>, to {esc(stops.release_tag.iloc[0])}.",
                  f"<svg width='{W}' height='110' viewBox='0 0 {W} 110' font-family='{SANS}'>{''.join(svg)}</svg>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Four metric views
# MAGIC
# MAGIC You write a metric view in YAML. It names a source, the dimensions you group and filter by, and the measures, which you query with
# MAGIC [`MEASURE()`](https://docs.databricks.com/aws/en/sql/language-manual/functions/measure). I wrote one per fact grain, following
# MAGIC [Create a metric view](https://docs.databricks.com/aws/en/uc-semantics/metric-views/create) and the
# MAGIC [YAML reference](https://docs.databricks.com/aws/en/uc-semantics/metric-views/yaml-reference):
# MAGIC
# MAGIC - `godot_change_metrics`, one row per merged PR. Its source query finds the first stable release that carried each PR, through
# MAGIC   a merge or a backport, so lead time is a measure rather than a three-table join.
# MAGIC - `godot_code_area_metrics`, one row per PR and code area, counting distinct PRs so a PR with two areas counts once in each, with
# MAGIC   time to merge and lead time per area.
# MAGIC - `godot_release_metrics`, one row per release and commit, for release counts and backported PRs per release.
# MAGIC - `godot_ci_metrics`, one row per CI run. Its source query works out, for each failed run on `master`, how long until the next
# MAGIC   successful one: the time it took to get the build back to green.
# MAGIC
# MAGIC Every dimension and measure has a comment, and the rates are percentages.

# COMMAND ----------

# DBTITLE 1,Create the metric views
METRIC_VIEWS = {
    "godot_change_metrics": f"""
version: 1.1
comment: "Merged pull requests in godotengine/godot and how long each took to merge and to reach a stable release."
source: |
  WITH first_release AS (
    SELECT c.pr_number,
           MIN(r.published_at) AS first_release_at,
           MIN_BY(r.release_tag, r.published_at) AS first_release_tag,
           MIN_BY(c.commit_kind, r.published_at) AS first_release_commit_kind
    FROM {FQ}.release_commits c
    JOIN {FQ}.releases r ON c.release_tag = r.release_tag
    WHERE c.pr_number IS NOT NULL
    GROUP BY c.pr_number
  )
  SELECT p.pr_number, p.base_branch, p.author_type, p.author_association, p.merged_at,
         (unix_timestamp(p.merged_at) - unix_timestamp(p.created_at)) / 3600.0 AS hours_to_merge,
         f.first_release_tag,
         CASE WHEN f.first_release_commit_kind = 'cherry_pick' THEN 'backport'
              WHEN f.first_release_tag IS NOT NULL THEN 'release' END AS shipped_via,
         (unix_timestamp(f.first_release_at) - unix_timestamp(p.merged_at)) / 86400.0 AS days_to_first_release
  FROM {FQ}.pull_requests p
  LEFT JOIN first_release f ON p.pr_number = f.pr_number
dimensions:
  - name: Base Branch
    expr: base_branch
    comment: "Branch the PR merged into: master, or a release branch such as 4.6 or 3.x"
  - name: Author Type
    expr: author_type
    comment: "human or bot"
  - name: Author Association
    expr: author_association
    comment: "MEMBER of the godotengine organization, COLLABORATOR, CONTRIBUTOR or NONE"
  - name: Merged Month
    expr: "DATE_TRUNC('MONTH', merged_at)"
    comment: "Month the PR was merged"
  - name: First Release
    expr: first_release_tag
    comment: "First stable release that included the PR, null if it has not shipped"
  - name: Shipped Via
    expr: shipped_via
    comment: "release when the PR first shipped in a release cut from its branch, backport when it first shipped as a cherry-pick"
measures:
  - name: Merged PRs
    expr: COUNT(1)
    comment: "Number of merged pull requests"
  - name: Member PR Percentage
    expr: 100.0 * COUNT(1) FILTER (WHERE author_association = 'MEMBER') / COUNT(1)
    comment: "Percentage of merged PRs opened by members of the godotengine organization, 0 to 100"
  - name: Median Hours To Merge
    expr: MEDIAN(hours_to_merge)
    comment: "Median hours from opening a PR to merging it"
  - name: Shipped PRs
    expr: COUNT(first_release_tag)
    comment: "Merged PRs that have shipped in a stable release"
  - name: Median Days To First Release
    expr: MEDIAN(days_to_first_release)
    comment: "Lead time: median days from merge to the first stable release that included the PR, over PRs that shipped"
""",
    "godot_code_area_metrics": f"""
version: 1.1
comment: "Merged pull requests by code area label, with how long they took to merge and to reach a stable release. A PR with several areas counts once in each area."
source: |
  WITH first_release AS (
    SELECT c.pr_number, MIN(r.published_at) AS first_release_at
    FROM {FQ}.release_commits c
    JOIN {FQ}.releases r ON c.release_tag = r.release_tag
    WHERE c.pr_number IS NOT NULL
    GROUP BY c.pr_number
  )
  SELECT a.pr_number, a.label_kind, a.code_area, p.base_branch, p.merged_at,
         (unix_timestamp(p.merged_at) - unix_timestamp(p.created_at)) / 3600.0 AS hours_to_merge,
         (unix_timestamp(f.first_release_at) - unix_timestamp(p.merged_at)) / 86400.0 AS days_to_first_release
  FROM {FQ}.pr_code_areas a
  JOIN {FQ}.pull_requests p ON a.pr_number = p.pr_number
  LEFT JOIN first_release f ON a.pr_number = f.pr_number
dimensions:
  - name: Code Area
    expr: code_area
    comment: "Code area label, for example editor, rendering, gdscript, android"
  - name: Label Kind
    expr: label_kind
    comment: "topic (an engine subsystem) or platform (a target platform)"
  - name: Base Branch
    expr: base_branch
    comment: "Branch the PR merged into"
  - name: Merged Month
    expr: "DATE_TRUNC('MONTH', merged_at)"
    comment: "Month the PR was merged"
measures:
  - name: Merged PRs
    expr: COUNT(DISTINCT pr_number)
    comment: "Number of distinct merged pull requests carrying the label"
  - name: Median Hours To Merge
    expr: MEDIAN(hours_to_merge)
    comment: "Median hours from opening a PR to merging it, for PRs carrying the label"
  - name: Median Days To First Release
    expr: MEDIAN(days_to_first_release)
    comment: "Lead time: median days from merge to the first stable release that included the PR, over PRs carrying the label that shipped"
""",
    "godot_release_metrics": f"""
version: 1.1
comment: "Stable Godot releases in the window and what shipped in them, including backported pull requests."
source: |
  SELECT r.release_tag, r.release_line, r.release_type, r.published_at, c.commit_sha, c.commit_kind, c.pr_number
  FROM {FQ}.releases r
  LEFT JOIN {FQ}.release_commits c ON r.release_tag = c.release_tag
dimensions:
  - name: Release
    expr: release_tag
    comment: "Release tag, for example 4.6.2-stable"
  - name: Release Line
    expr: release_line
    comment: "Major.minor line, for example 4.6"
  - name: Release Type
    expr: release_type
    comment: "feature (x.y) or patch (x.y.z)"
  - name: Published Month
    expr: "DATE_TRUNC('MONTH', published_at)"
    comment: "Month the release was published"
measures:
  - name: Releases
    expr: COUNT(DISTINCT release_tag)
    comment: "Number of stable releases"
  - name: Patch Releases
    expr: COUNT(DISTINCT release_tag) FILTER (WHERE release_type = 'patch')
    comment: "Number of patch releases"
  - name: Shipped PRs
    expr: COUNT(DISTINCT pr_number)
    comment: "Distinct master PRs that shipped in the release, by merge or backport"
  - name: Backported PRs
    expr: COUNT(DISTINCT pr_number) FILTER (WHERE commit_kind = 'cherry_pick')
    comment: "Distinct master PRs that reached the release as a cherry-picked backport"
""",
    "godot_ci_metrics": f"""
version: 1.1
comment: "Runs of Godot's top-level CI workflow, and how long master took to get back to a successful run after a failure."
source: |
  SELECT run_id, event, branch, conclusion, run_attempt, created_at,
         CASE WHEN event = 'push' AND branch = 'master' AND conclusion = 'failure'
              THEN (unix_timestamp(next_success_at) - unix_timestamp(created_at)) / 3600.0 END AS hours_to_next_success
  FROM (
    SELECT *, MIN(CASE WHEN conclusion = 'success' THEN created_at END)
                OVER (PARTITION BY event, branch ORDER BY created_at
                      ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS next_success_at
    FROM {FQ}.ci_runs
  )
dimensions:
  - name: Event
    expr: event
    comment: "What triggered the run: pull_request or push"
  - name: Branch
    expr: branch
    comment: "For push runs, the branch pushed to; null for pull_request runs"
  - name: Conclusion
    expr: conclusion
    comment: "success, failure, cancelled or startup_failure"
  - name: Created Month
    expr: "DATE_TRUNC('MONTH', created_at)"
    comment: "Month the run was created"
measures:
  - name: Runs
    expr: COUNT(1)
    comment: "Number of CI runs"
  - name: Failure Percentage
    expr: 100.0 * COUNT(1) FILTER (WHERE conclusion = 'failure') / COUNT(1) FILTER (WHERE conclusion IN ('success', 'failure'))
    comment: "Failed runs as a percentage of runs that finished as success or failure, 0 to 100"
  - name: Rerun Percentage
    expr: 100.0 * COUNT(1) FILTER (WHERE run_attempt > 1) / COUNT(1)
    comment: "Percentage of runs attempted more than once, 0 to 100"
  - name: Median Hours To Next Success
    expr: MEDIAN(hours_to_next_success)
    comment: "For failed runs on master pushes, median hours from the failed run to the next successful master run, by creation time"
""",
}

for name, yaml_body in METRIC_VIEWS.items():
    spark.sql(f"CREATE OR REPLACE VIEW {FQ}.{name} WITH METRICS LANGUAGE YAML AS $${yaml_body}$$")
print("Metric views created:", ", ".join(METRIC_VIEWS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the docs say
# MAGIC
# MAGIC The [Genie best practices page](https://docs.databricks.com/aws/en/genie-agents/best-practices) leans toward the metric view:
# MAGIC "Metric views are particularly effective for Genie Agents because they pre-define metrics, dimensions, and aggregations."
# MAGIC The [metric views overview](https://docs.databricks.com/aws/en/uc-semantics/metric-views/) puts the point as defining a metric
# MAGIC once, "so you can define metrics once and query them at runtime." Neither page says what happens when an agent has both.
# MAGIC
# MAGIC Size isn't the constraint here either: "Genie Agents support up to 50 tables, views, or metric views." The agent with both
# MAGIC carries ten.
# MAGIC
# MAGIC The [benchmarks page](https://docs.databricks.com/aws/en/genie/benchmarks) says how answers get graded: "Benchmark questions
# MAGIC run as new conversations," and "The generated SQL and results are then compared against the" benchmark's SQL answer. A result
# MAGIC is Good when Genie "generates a result set with numeric values that round to the same 4 significant digits," and Bad when it
# MAGIC "generates a result set that includes extra columns compared to the result set produced by the" SQL answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Twelve benchmark questions
# MAGIC
# MAGIC Each question has an answer SQL written against the raw tables: the ground truth, run once here so you can see every expected
# MAGIC answer before any agent is asked. Each also has the same question asked of the metric views, and the cell checks the two agree,
# MAGIC so a mistake in a view's definition can't pass as a Genie mistake.
# MAGIC
# MAGIC The questions cover the four DORA metrics as they map onto an open-source engine: a stable release is the deployment, a failed CI run
# MAGIC on `master` is the change failure, and the time until the next successful run is the time to restore. Questions 5, 6, 7 and 12 are the
# MAGIC ones that need the joins and window logic the metric views carry.

# COMMAND ----------

# DBTITLE 1,Benchmark questions and expected answers
T = {t: f"{FQ}.{t}" for t in TABLES}
MV = {m: f"{FQ}.{m}" for m in METRIC_VIEWS}

BENCHMARKS = [
    {"id": "q01", "short": "Merged PRs into master",
     "question": "How many pull requests were merged into the master branch?",
     "sql": f"SELECT COUNT(*) AS merged_prs FROM {T['pull_requests']} WHERE base_branch = 'master'",
     "mv_sql": f"SELECT MEASURE(`Merged PRs`) FROM {MV['godot_change_metrics']} WHERE `Base Branch` = 'master'"},
    {"id": "q02", "short": "Patch releases",
     "question": "How many patch releases were published?",
     "sql": f"SELECT COUNT(*) AS patch_releases FROM {T['releases']} WHERE release_type = 'patch'",
     "mv_sql": f"SELECT MEASURE(`Patch Releases`) FROM {MV['godot_release_metrics']}"},
    {"id": "q03", "kind": "label", "short": "Busiest release line",
     "question": "Which release line had the most stable releases published?",
     "sql": f"SELECT release_line, COUNT(*) AS releases FROM {T['releases']} GROUP BY release_line ORDER BY releases DESC LIMIT 1",
     "mv_sql": f"SELECT `Release Line`, MEASURE(`Releases`) AS releases FROM {MV['godot_release_metrics']} GROUP BY ALL ORDER BY releases DESC LIMIT 1"},
    {"id": "q04", "short": "Hours from open to merge",
     "question": "What is the median time from opening to merging, in hours, for pull requests merged into master?",
     "sql": f"SELECT MEDIAN((unix_timestamp(merged_at) - unix_timestamp(created_at)) / 3600.0) AS median_hours_to_merge FROM {T['pull_requests']} WHERE base_branch = 'master'",
     "mv_sql": f"SELECT MEASURE(`Median Hours To Merge`) FROM {MV['godot_change_metrics']} WHERE `Base Branch` = 'master'"},
    {"id": "q05", "short": "Days from merge to release",
     "question": "For pull requests merged into master that shipped in a stable release, what is the median number of days from merge to the first stable release that included them?",
     "sql": f"""WITH first_release AS (
                  SELECT c.pr_number, MIN(r.published_at) AS first_release_at
                  FROM {T['release_commits']} c JOIN {T['releases']} r ON c.release_tag = r.release_tag
                  WHERE c.pr_number IS NOT NULL GROUP BY c.pr_number)
                SELECT MEDIAN((unix_timestamp(f.first_release_at) - unix_timestamp(p.merged_at)) / 86400.0) AS median_days_to_first_release
                FROM {T['pull_requests']} p JOIN first_release f ON p.pr_number = f.pr_number
                WHERE p.base_branch = 'master'""",
     "mv_sql": f"SELECT MEASURE(`Median Days To First Release`) FROM {MV['godot_change_metrics']} WHERE `Base Branch` = 'master'"},
    {"id": "q06", "short": "PRs backported to 4.6",
     "question": "How many distinct master pull requests were backported into 4.6 patch releases?",
     "sql": f"""SELECT COUNT(DISTINCT c.pr_number) AS backported_prs
                FROM {T['release_commits']} c JOIN {T['releases']} r ON c.release_tag = r.release_tag
                WHERE c.commit_kind = 'cherry_pick' AND r.release_line = '4.6' AND r.release_type = 'patch'""",
     "mv_sql": f"SELECT MEASURE(`Backported PRs`) FROM {MV['godot_release_metrics']} WHERE `Release Line` = '4.6' AND `Release Type` = 'patch'"},
    {"id": "q07", "kind": "label", "short": "Biggest 4.6 backport release",
     "question": "Which 4.6 patch release included the most backported pull requests?",
     "sql": f"""SELECT c.release_tag, COUNT(DISTINCT c.pr_number) AS backported_prs
                FROM {T['release_commits']} c JOIN {T['releases']} r ON c.release_tag = r.release_tag
                WHERE c.commit_kind = 'cherry_pick' AND r.release_line = '4.6' AND r.release_type = 'patch'
                GROUP BY c.release_tag ORDER BY backported_prs DESC LIMIT 1""",
     "mv_sql": f"SELECT `Release`, MEASURE(`Backported PRs`) AS b FROM {MV['godot_release_metrics']} WHERE `Release Line` = '4.6' AND `Release Type` = 'patch' GROUP BY ALL ORDER BY b DESC LIMIT 1"},
    {"id": "q08", "short": "Master PRs from members",
     "question": "What percentage of pull requests merged into master were opened by members of the Godot organization?",
     "sql": f"SELECT 100.0 * AVG(CASE WHEN author_association = 'MEMBER' THEN 1 ELSE 0 END) AS member_pr_pct FROM {T['pull_requests']} WHERE base_branch = 'master'",
     "mv_sql": f"SELECT MEASURE(`Member PR Percentage`) FROM {MV['godot_change_metrics']} WHERE `Base Branch` = 'master'"},
    {"id": "q09", "kind": "label", "short": "Busiest code area",
     "question": "Which code area label appeared on the most merged pull requests?",
     "sql": f"SELECT code_area, COUNT(DISTINCT pr_number) AS merged_prs FROM {T['pr_code_areas']} GROUP BY code_area ORDER BY merged_prs DESC LIMIT 1",
     "mv_sql": f"SELECT `Code Area`, MEASURE(`Merged PRs`) AS n FROM {MV['godot_code_area_metrics']} GROUP BY ALL ORDER BY n DESC LIMIT 1"},
    {"id": "q10", "short": "Master CI failure %",
     "question": "Of the CI runs triggered by pushes to master that finished as success or failure, what percentage failed?",
     "sql": f"""SELECT 100.0 * COUNT_IF(conclusion = 'failure') / COUNT(*) AS failure_pct
                FROM {T['ci_runs']} WHERE event = 'push' AND branch = 'master' AND conclusion IN ('success', 'failure')""",
     "mv_sql": f"SELECT MEASURE(`Failure Percentage`) FROM {MV['godot_ci_metrics']} WHERE `Event` = 'push' AND `Branch` = 'master'"},
    {"id": "q11", "short": "CI runs re-run %",
     "question": "What percentage of all CI runs needed more than one attempt?",
     "sql": f"SELECT 100.0 * COUNT_IF(run_attempt > 1) / COUNT(*) AS rerun_pct FROM {T['ci_runs']}",
     "mv_sql": f"SELECT MEASURE(`Rerun Percentage`) FROM {MV['godot_ci_metrics']}"},
    {"id": "q12", "short": "Hours from red to green",
     "question": "For each failed CI run on pushes to master, how many hours passed until the next successful run on master, measured between the runs' creation times? Give the median.",
     "sql": f"""WITH master_runs AS (
                  SELECT created_at, conclusion FROM {T['ci_runs']} WHERE event = 'push' AND branch = 'master'),
                failures AS (
                  SELECT f.created_at, MIN(s.created_at) AS next_success_at
                  FROM master_runs f JOIN master_runs s ON s.conclusion = 'success' AND s.created_at > f.created_at
                  WHERE f.conclusion = 'failure' GROUP BY f.created_at)
                SELECT MEDIAN((unix_timestamp(next_success_at) - unix_timestamp(created_at)) / 3600.0) AS median_hours_to_next_success
                FROM failures""",
     "mv_sql": f"SELECT MEASURE(`Median Hours To Next Success`) FROM {MV['godot_ci_metrics']}"},
]
for b in BENCHMARKS:
    b["sql"] = re.sub(r"\s+", " ", b["sql"]).strip()  # one line each, the way the benchmark stores it


def cells_of(rows: list) -> list[list]:
    """Rows from spark or the API as plain lists of cell values."""
    return [list(r) for r in rows]


def as_number(value):
    """A cell as a float, or None when it isn't numeric."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def norm_label(value) -> str:
    """Lowercase a label and drop the prefixes and suffixes Godot's names carry (topic:, -stable)."""
    s = str(value).strip().lower()
    s = re.sub(r"^(topic|platform):", "", s)
    return re.sub(r"-stable$", "", s)


def expected_from(rows: list, kind: str) -> dict:
    """The expected answer: the first row's first cell, as a label for a which-question, else as a number."""
    first = rows[0][0]
    return {"kind": "label", "value": str(first)} if kind == "label" else {"kind": "number", "value": as_number(first)}


def close(a: float, b: float) -> bool:
    """Within 1 percent, or 0.05 for small values."""
    return abs(a - b) <= max(0.01 * abs(b), 0.05)


def value_match(expected: dict, rows: list) -> bool:
    """Does a result carry the expected answer? Lenient on shape, strict on the value.

    A number matches when a cell is within 1 percent of it, read as is, as a fraction of 100, or times 100,
    in a result of at most three rows. A label matches when it is in the only row, or in the row with the
    largest last number when the result ranks several.
    """
    if not rows:
        return False
    if expected["kind"] == "number":
        if len(rows) > 3:
            return False
        e = expected["value"]
        nums = [n for r in rows for n in map(as_number, r) if n is not None]
        return any(close(n, e) or close(n * 100, e) or close(n / 100, e) for n in nums)
    row = rows[0]
    if len(rows) > 1:
        scored = [(next((as_number(c) for c in reversed(r) if as_number(c) is not None), None), r) for r in rows]
        scored = [s for s in scored if s[0] is not None]
        if scored:
            row = max(scored, key=lambda s: s[0])[1]
    return any(norm_label(c) == norm_label(expected["value"]) for c in row if c is not None)


for b in BENCHMARKS:
    b["expected"] = expected_from(cells_of(spark.sql(b["sql"]).collect()), b.get("kind", "number"))
    b["mv_agrees"] = value_match(b["expected"], cells_of(spark.sql(b["mv_sql"]).collect()))

metric_view_checks_passed = sum(b["mv_agrees"] for b in BENCHMARKS)
print(f"Expected answers from the raw tables; the metric views agree on {metric_view_checks_passed} of {len(BENCHMARKS)}.")
display(pd.DataFrame([{"#": b["id"], "question": b["question"],
                       "expected answer": f"{b['expected']['value']:,.2f}" if b["expected"]["kind"] == "number" else b["expected"]["value"],
                       "metric views agree": b["mv_agrees"]} for b in BENCHMARKS]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Three Genie Agents
# MAGIC
# MAGIC The three agents are identical apart from their data sources: same warehouse, same one-paragraph text instruction, same twelve
# MAGIC benchmarks with the same SQL answers. The instruction only says what the data is and which window it covers. The measures are
# MAGIC written into the metric views for two of the agents. The raw-tables agent gets none, so it has to work each one out from the columns,
# MAGIC which is what pointing Genie at a raw table means.
# MAGIC
# MAGIC The agents are created through the [Genie Agents API](https://docs.databricks.com/aws/en/genie-agents/conversation-api), with the
# MAGIC tables, metric views and benchmarks in the `serialized_space` payload.

# COMMAND ----------

# DBTITLE 1,Create the three agents
INSTRUCTION = ("This agent answers questions about how the open-source Godot game engine repository (github.com/godotengine/godot) "
               "delivers changes: merged pull requests, stable releases, backports and CI runs, from Oct 5, 2025 to Oct 5, 2026. "
               "Every question is about that window.")

ARMS = {
    "raw_tables": {"label": "Raw tables", "tables": sorted(T.values()), "metric_views": []},
    "metric_views": {"label": "Metric views", "tables": [], "metric_views": sorted(MV.values())},
    "both": {"label": "Both", "tables": sorted(T.values()), "metric_views": sorted(MV.values())},
}


def hexid(*parts: str) -> str:
    """A stable 32-hex id, so the benchmark ids are the same in every agent."""
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def serialized_space(arm: dict) -> str:
    """The agent definition: its own data sources plus the instruction and benchmarks every agent shares."""
    data_sources = {}
    if arm["tables"]:
        data_sources["tables"] = [{"identifier": t} for t in arm["tables"]]
    if arm["metric_views"]:
        data_sources["metric_views"] = [{"identifier": m} for m in arm["metric_views"]]
    questions = sorted(({"id": hexid("benchmark", b["id"]), "question": [b["question"]],
                         "answer": [{"format": "SQL", "content": [b["sql"]]}]} for b in BENCHMARKS),
                       key=lambda q: q["id"])
    return json.dumps({
        "version": 2,
        "data_sources": data_sources,
        "instructions": {"text_instructions": [{"id": hexid("instruction"), "content": [INSTRUCTION]}]},
        "benchmarks": {"questions": questions},
    })


QID_TO_BENCHMARK = {hexid("benchmark", b["id"]): b for b in BENCHMARKS}
for key, arm in ARMS.items():
    code, body = api("POST", "/api/2.0/genie/spaces", {
        "title": f"Godot delivery: {arm['label'].lower()}",
        "description": "Genie Agent over the Godot engine's delivery history, built by the godot-genie-metric-view notebook.",
        "warehouse_id": WAREHOUSE_ID,
        "parent_path": f"/Workspace/Users/{USER}",
        "serialized_space": serialized_space(arm),
    })
    if code != 200 or "space_id" not in body:
        raise RuntimeError(f"Creating the {arm['label']} agent failed ({code}): {body}")
    arm["space_id"] = body["space_id"]
    arm["objects"] = len(arm["tables"]) + len(arm["metric_views"])
    print(f"{arm['label']:<13} {arm['objects']:>2} objects  {HOST}/genie/rooms/{arm['space_id']}")

# COMMAND ----------

# DBTITLE 1,What each agent can see
# hide-code
def measures_of(yaml_body: str) -> list[str]:
    """Measure names from a metric view's YAML."""
    return re.findall(r"^  - name: (.+)$", yaml_body.split("measures:", 1)[1], re.M)


def table_items() -> str:
    """The raw tables with their row counts."""
    return "".join(f"<div style='padding:3px 0;font-size:14px'>{esc(t)} <span style='color:{GRAY_TEXT};font-size:12px'>"
                   f"{row_counts[t]:,} rows</span></div>" for t in TABLES)


def view_items(with_measures: bool) -> str:
    """The metric views, optionally with the measures each one defines."""
    out = ""
    for name, body in METRIC_VIEWS.items():
        sub = (f"<div style='font-size:12px;color:{GRAY_TEXT};line-height:1.5;margin-left:12px'>"
               + "<br>".join(esc(m) for m in measures_of(body)) + "</div>") if with_measures else ""
        out += f"<div style='padding:4px 0;font-size:14px'>{esc(name)}{sub}</div>"
    return out


def column(arm: dict, body: str) -> str:
    """One agent's column."""
    return (f"<div style='flex:1;min-width:230px;border-top:3px solid {NAVY};padding-top:10px'>"
            f"<div style='font-size:16px;font-weight:700'>{esc(arm['label'])}</div>"
            f"<div style='font-size:12px;color:{GRAY_TEXT};margin-bottom:8px'>{arm['objects']} objects</div>{body}</div>")


body = (column(ARMS["raw_tables"], table_items())
        + column(ARMS["metric_views"], view_items(True))
        + column(ARMS["both"], table_items() + f"<div style='border-top:1px solid {GRAY_LINES};margin:6px 0'></div>" + view_items(False)))
displayHTML(frame("What each agent can see",
                  "Same instruction, same benchmarks, same warehouse. The measures listed under each metric view are defined once in its YAML; "
                  "the raw-tables agent has to work each one out from the columns.",
                  f"<div style='display:flex;gap:26px;flex-wrap:wrap'>{body}</div>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run the benchmarks
# MAGIC
# MAGIC Each round starts one benchmark evaluation per agent, all three at once, and waits for them to finish. Then it pulls every
# MAGIC question's result: Databricks' assessment and the SQL Genie wrote. Expect about three minutes a round.

# COMMAND ----------

# DBTITLE 1,Run the benchmark rounds
def run_eval(space_id: str) -> dict:
    """Run one benchmark evaluation of every question and return the per-question details once it finishes."""
    t0 = time.monotonic()
    code, run = api("POST", f"/api/2.0/genie/spaces/{space_id}/eval-runs", {})
    if code != 200 or "eval_run_id" not in run:
        return {"error": f"start failed ({code}): {run}", "seconds": 0, "results": []}
    run_id = run["eval_run_id"]
    while run.get("eval_run_status") not in ("DONE", "FAILED", "CANCELLED", "ERROR") and time.monotonic() - t0 < EVAL_TIMEOUT_MIN * 60:
        time.sleep(15)
        _, run = api("GET", f"/api/2.0/genie/spaces/{space_id}/eval-runs/{run_id}")
    seconds = round(time.monotonic() - t0, 1)
    _, listing = api("GET", f"/api/2.0/genie/spaces/{space_id}/eval-runs/{run_id}/results")
    details = []
    for res in listing.get("eval_results", []):
        _, d = api("GET", f"/api/2.0/genie/spaces/{space_id}/eval-runs/{run_id}/results/{res['result_id']}")
        actual = (d.get("actual_response") or [{}])[0]
        details.append({"question_id": res["benchmark_question_id"], "assessment": d.get("assessment") or "NONE",
                        "sql": actual.get("response") if actual.get("response_type") == "SQL" else None})
    return {"status": run.get("eval_run_status"), "seconds": seconds, "results": details}


attempts = []
for rnd in range(1, EVAL_ROUNDS + 1):
    with ThreadPoolExecutor(max_workers=len(ARMS)) as pool:
        runs = dict(zip(ARMS, pool.map(lambda k: run_eval(ARMS[k]["space_id"]), ARMS)))
    for key, run in runs.items():
        ARMS[key].setdefault("round_seconds", []).append(run["seconds"])
        if run.get("error"):
            print(f"round {rnd} {ARMS[key]['label']}: {run['error']}")
        for res in run["results"]:
            b = QID_TO_BENCHMARK.get(res["question_id"])
            if b:
                attempts.append({"arm": key, "round": rnd, "q": b["id"], **res})
    print(f"round {rnd}: " + ", ".join(
        f"{ARMS[k]['label']} {sum(a['assessment'] == 'GOOD' for a in attempts if a['arm'] == k and a['round'] == rnd)}"
        f"/{len(BENCHMARKS)} Good in {runs[k]['seconds']:.0f}s" for k in ARMS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A second check on every answer
# MAGIC
# MAGIC Databricks' grade checks the shape of an answer as well as its value, so a percentage returned as a fraction, or a correct number
# MAGIC next to a column the SQL answer doesn't have, can read as Bad. So the cell below runs every query Genie wrote again and checks whether the expected number or label
# MAGIC is in what comes back: within 1 percent for a number, the top row for a which-question. It also notes which objects each query
# MAGIC read, which is how the agent with both shows whether it reached for the metric views or the raw tables. The grade and the value check
# MAGIC are both recorded; neither replaces the other.

# COMMAND ----------

# DBTITLE 1,Re-run every generated query and check its value
if not attempts:
    raise RuntimeError("No benchmark results came back from any round; check the messages printed above.")
by_id = {b["id"]: b for b in BENCHMARKS}
for a in attempts:
    sql = a.get("sql") or ""
    low = sql.lower().replace("`", "")  # Genie backticks each part of a name: `workspace`.`godot_delivery`.`releases`
    # Match schema-qualified names only, so a column alias such as "AS releases" isn't read as the releases table.
    a["uses_metric_views"] = any(re.search(rf"\b{SCHEMA}\.{m}\b", low) for m in METRIC_VIEWS)
    a["uses_raw_tables"] = any(re.search(rf"\b{SCHEMA}\.{t}\b", low) for t in TABLES)
    a["value_match"] = False
    a["error"] = None
    if sql:
        try:
            rows = spark.sql(sql.strip().rstrip(";")).limit(50).collect()
            a["value_match"] = value_match(by_id[a["q"]]["expected"], cells_of(rows))
        except Exception as exc:  # a query that fails here is recorded, not raised
            a["error"] = str(exc).splitlines()[0][:200]

attempts_df = pd.DataFrame(attempts)
grid = []
for b in BENCHMARKS:
    row = {"#": b["id"], "question": b["short"]}
    for key, arm in ARMS.items():
        sub = attempts_df[(attempts_df.arm == key) & (attempts_df.q == b["id"])]
        row[f"{arm['label']}: Good"] = f"{(sub.assessment == 'GOOD').sum()}/{len(sub)}"
        row[f"{arm['label']}: right value"] = f"{sub.value_match.sum()}/{len(sub)}"
    grid.append(row)
print("Benchmark results by question, across all rounds")
display(pd.DataFrame(grid))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Databricks' grade against the value check
# MAGIC
# MAGIC Every answer lands in one of four boxes: graded Good or not, and carrying the right value or not. An answer graded Bad that still
# MAGIC carries the right value is a shape problem (a fraction for a percentage, an extra column), not a wrong answer.

# COMMAND ----------

# DBTITLE 1,Grade against value
# hide-code
def box(n: int, pop: bool = False) -> str:
    """One count in the two-by-two."""
    return (f"<td style='padding:10px 14px;font-size:24px;font-weight:700;text-align:center;border:1px solid {GRAY_LINES};"
            f"background:{WHITE};color:{LAVA if pop and n else NAVY}'>{n}</td>")


grids = ""
for key, arm in ARMS.items():
    sub = attempts_df[attempts_df.arm == key]
    good = sub.assessment == "GOOD"
    gg, gw = int((good & sub.value_match).sum()), int((good & ~sub.value_match).sum())
    bg, bw = int((~good & sub.value_match).sum()), int((~good & ~sub.value_match).sum())
    head = f"<td></td><td style='font-size:12px;color:{GRAY_TEXT};padding:4px 8px'>right value</td><td style='font-size:12px;color:{GRAY_TEXT};padding:4px 8px'>wrong value</td>"
    grids += (f"<div style='flex:1;min-width:230px'><div style='font-size:16px;font-weight:700;margin-bottom:6px'>{esc(arm['label'])}</div>"
              f"<table style='border-collapse:collapse'><tr>{head}</tr>"
              f"<tr><td style='font-size:12px;color:{GRAY_TEXT};padding-right:8px'>graded Good</td>{box(gg)}{box(gw, True)}</tr>"
              f"<tr><td style='font-size:12px;color:{GRAY_TEXT};padding-right:8px'>graded Bad or for review</td>{box(bg, True)}{box(bw)}</tr>"
              f"</table></div>")
print("Databricks' grade against the value check")
displayHTML(frame("Databricks' grade against the value check",
                  f"Every answer from every round, {len(BENCHMARKS) * EVAL_ROUNDS} per agent. The grade and the value check disagree "
                  f"in the lava-numbered boxes.",
                  f"<div style='display:flex;gap:30px;flex-wrap:wrap'>{grids}</div>"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Same question, three agents' SQL
# MAGIC
# MAGIC The two questions that need the most work on raw tables: lead time to the first release, and the time `master` took to
# MAGIC get back to a successful CI run. Each column is round 1's answer from one agent, exactly as Genie wrote it.

# COMMAND ----------

# DBTITLE 1,Same question, three agents' SQL
# hide-code
SHOWCASE = ["q05", "q12"]


def objects_read(sql: str) -> list[str]:
    """The tables and metric views a query reads, by schema-qualified name."""
    low = (sql or "").lower().replace("`", "")
    return [n for n in list(TABLES) + list(METRIC_VIEWS) if re.search(rf"\b{SCHEMA}\.{n}\b", low)]


def sql_column(key: str, qid: str) -> str:
    """One agent's round-1 answer to one question."""
    sub = attempts_df[(attempts_df.arm == key) & (attempts_df.q == qid)].sort_values("round")
    if sub.empty:
        return ""
    a = sub.iloc[0]
    sql = a.sql or "(no SQL returned)"
    good = a.assessment == "GOOD"
    reads = objects_read(a.sql)
    meta = (f"<span style='font-weight:700;color:{NAVY if good else LAVA}'>{esc(a.assessment)}</span>"
            f" &middot; {'right value' if a.value_match else 'wrong value'}"
            f" &middot; {len(sql.splitlines())} line{'s' if len(sql.splitlines()) != 1 else ''} &middot; reads {len(reads)}")
    return (f"<div style='flex:1;min-width:260px'>"
            f"<div style='font-size:15px;font-weight:700'>{esc(ARMS[key]['label'])}</div>"
            f"<div style='font-size:12px;color:{GRAY_TEXT};margin:2px 0 6px'>{meta}</div>"
            f"<div style='font-size:11px;color:{GRAY_TEXT};margin-bottom:6px'>{esc(', '.join(reads))}</div>"
            f"<pre style='font-family:{MONO};font-size:11px;line-height:1.45;white-space:pre-wrap;word-break:break-word;margin:0;"
            f"background:{WHITE};border:1px solid {GRAY_LINES};padding:10px;color:{NAVY}'>{esc(sql)}</pre></div>")


sections = ""
for qid in SHOWCASE:
    b = by_id[qid]
    sections += (f"<div style='border-top:1px solid {GRAY_LINES};padding:18px 0 8px'>"
                 f"<div style='font-size:16px;font-weight:700;max-width:820px'>{esc(b['question'])}</div>"
                 f"<div style='font-size:13px;color:{GRAY_TEXT};margin:4px 0 12px'>Expected answer: "
                 f"{b['expected']['value']:,.2f}</div>"
                 f"<div style='display:flex;gap:16px;flex-wrap:wrap'>{''.join(sql_column(k, qid) for k in ARMS)}</div></div>")
print("Same question, three agents' SQL")
displayHTML(frame("Same question, three agents' SQL",
                  "Round 1, unedited. Lava marks an answer Databricks didn't grade Good.", sections))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What the agent with both reached for
# MAGIC
# MAGIC This is the follow-up the poster asked in the thread. For each answer from the agent that had both, the cell counts whether its
# MAGIC SQL read the metric views or the raw tables, and lists every query it wrote so you can read them yourself.

# COMMAND ----------

# DBTITLE 1,Objects the agent with both queried
# hide-code
both = attempts_df[attempts_df.arm == "both"].copy()
both["read"] = both.apply(lambda a: "both kinds" if a.uses_metric_views and a.uses_raw_tables
                          else "metric views" if a.uses_metric_views
                          else "raw tables" if a.uses_raw_tables else "no SQL", axis=1)
SWATCH = {"metric views": NAVY, "both kinds": NAVY_400, "raw tables": LAVA, "no SQL": WHITE}


def square(kind: str) -> str:
    """One round's answer, coloured by what its SQL read."""
    return (f"<span title='{esc(kind)}' style='display:inline-block;width:22px;height:22px;margin-right:5px;"
            f"background:{SWATCH[kind]};border:1px solid {NAVY_300}'></span>")


key_row = "".join(f"<span style='margin-right:18px;font-size:12px;color:{GRAY_TEXT}'>{square(k)}{esc(k)}</span>" for k in SWATCH)
strip = ""
for b in BENCHMARKS:
    reads = both[both.q == b["id"]].sort_values("round")["read"].tolist()
    strip += (f"<tr style='border-top:1px solid {GRAY_LINES}'><td style='padding:6px 10px 6px 0;font-size:12px;color:{GRAY_TEXT}'>"
              f"{esc(b['id'][1:])}</td><td style='padding:6px 16px 6px 0;font-size:14px'>{esc(b['short'])}</td>"
              f"<td style='padding:6px 0'>{''.join(square(r) for r in reads)}</td></tr>")
print("What the agent with both queried, by question")
displayHTML(frame("Where the agent with both went",
                  f"One square per round. The thread's worry was that an agent given both would mix them up; "
                  f"this is what its SQL actually read.",
                  f"<div style='margin-bottom:12px'>{key_row}</div><table style='border-collapse:collapse;width:100%'>{strip}</table>"))
display(both[["q", "round", "assessment", "value_match", "read", "sql"]].fillna({"sql": ""}).sort_values(["q", "round"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Put the agent to work
# MAGIC
# MAGIC Benchmarks say whether an agent gets known answers right. What a DevOps team needs from it is answers to the questions nobody has
# MAGIC worked out yet. The cell picks the agent with the most answers graded Good (a tie goes to the agent with fewer objects), asks it four questions
# MAGIC about where Godot's delivery slows down through the [Genie Agents API](https://docs.databricks.com/aws/en/genie-agents/conversation-api),
# MAGIC and shows each answer with the SQL Genie wrote. These questions have no answer key, so read the SQL before you trust a number.
# MAGIC The agent finds where the time goes; changing the process is still a person's call.

# COMMAND ----------

# DBTITLE 1,Ask the best agent where delivery slows down
DEVOPS_QUESTIONS = [
    ("lead-by-area", "Which code areas have the longest median days from merge to the first stable release? Show the ten slowest."),
    ("backport-vs-release", "Do pull requests that first ship as a backport reach a stable release faster than ones that wait for a feature release? Compare the median days from merge to first release."),
    ("rerun-trend", "What percentage of CI runs needed more than one attempt in each month?"),
    ("restore-trend", "For failed CI runs on pushes to master, what was the median number of hours until the next successful master run, by month?"),
]
best_key = max(ARMS, key=lambda k: (int((attempts_df[attempts_df.arm == k].assessment == "GOOD").sum()), -ARMS[k]["objects"]))
best_space = ARMS[best_key]["space_id"]


def ask(space_id: str, question: str) -> dict:
    """Ask one question in a new conversation and return Genie's text, SQL and result rows."""
    code, msg = api("POST", f"/api/2.0/genie/spaces/{space_id}/start-conversation", {"content": question})
    if code != 200:
        return {"status": f"start failed ({code})", "text": str(msg)[:300], "sql": None, "columns": [], "rows": []}
    conv, mid = msg["conversation_id"], msg.get("message_id") or msg.get("id")
    t0 = time.monotonic()
    while msg.get("status") not in ("COMPLETED", "FAILED", "CANCELLED") and time.monotonic() - t0 < 300:
        time.sleep(5)
        _, msg = api("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conv}/messages/{mid}")
    out = {"status": msg.get("status"), "text": "", "sql": None, "columns": [], "rows": []}
    for att in msg.get("attachments") or []:
        if att.get("text") and att["text"].get("content"):
            out["text"] += ("\n" if out["text"] else "") + att["text"]["content"]
        if att.get("query"):
            out["sql"] = att["query"].get("query")
            _, res = api("GET", f"/api/2.0/genie/spaces/{space_id}/conversations/{conv}/messages/{mid}"
                                f"/attachments/{att['attachment_id']}/query-result")
            sr = res.get("statement_response", {})
            out["columns"] = [c["name"] for c in sr.get("manifest", {}).get("schema", {}).get("columns", [])]
            out["rows"] = sr.get("result", {}).get("data_array") or []
    return out


devops_answers = {qid: {"question": q, **ask(best_space, q)} for qid, q in DEVOPS_QUESTIONS}
print(f"Asked the {ARMS[best_key]['label'].lower()} agent ({best_key}); "
      + ", ".join(f"{qid}: {a['status']} ({len(a['rows'])} rows)" for qid, a in devops_answers.items()))

# COMMAND ----------

# DBTITLE 1,Where the agent says delivery slows down
# hide-code
def genie_text(text: str) -> str:
    """Genie's answer as HTML: escaped, with its **bold** and - bullets kept."""
    out = []
    for line in esc(text).splitlines():
        line = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", line)
        out.append(f"<li>{line[2:]}</li>" if line.startswith("- ") else f"<p style='margin:4px 0'>{line}</p>")
    return re.sub(r"((?:<li>.*?</li>)+)", r"<ul style='margin:4px 0 4px 18px;padding:0'>\1</ul>", "".join(out))


def result_bars(columns: list, rows: list) -> str:
    """The result as labelled bars when it is one label column and a number; otherwise a small table."""
    if not rows:
        return f"<div style='font-size:13px;color:{GRAY_TEXT}'>No rows came back.</div>"
    numeric = [i for i in range(len(columns)) if all(as_number(r[i]) is not None for r in rows if r[i] is not None)]
    labels = [i for i in range(len(columns)) if i not in numeric]
    if len(rows) > 1 and numeric:
        v = numeric[-1]
        lab = labels[0] if labels else 0
        top = rows[:15]
        peak = max((as_number(r[v]) or 0) for r in top) or 1
        bars = "".join(
            f"<div style='display:flex;align-items:center;gap:10px;padding:3px 0'>"
            f"<div style='flex:0 0 190px;font-size:13px;color:{NAVY};text-align:right'>{esc(str(r[lab])[:10] if 'month' in columns[lab].lower() else r[lab])}</div>"
            f"<div style='flex:1;background:{GRAY_LINES};height:12px'><div style='height:12px;width:{100 * (as_number(r[v]) or 0) / peak:.0f}%;"
            f"background:{NAVY}'></div></div>"
            f"<div style='flex:0 0 70px;font-size:13px;color:{NAVY}'>{(as_number(r[v]) or 0):,.1f}</div></div>" for r in top)
        return f"<div style='font-size:12px;color:{GRAY_TEXT};margin-bottom:4px'>{esc(columns[v])}</div>{bars}"
    head = "".join(f"<th style='text-align:left;padding:4px 10px;font-size:12px;color:{GRAY_TEXT}'>{esc(c)}</th>" for c in columns)
    body = "".join("<tr>" + "".join(f"<td style='padding:4px 10px;font-size:13px;border-top:1px solid {GRAY_LINES}'>{esc(c)}</td>"
                                    for c in r) + "</tr>" for r in rows[:10])
    return f"<table style='border-collapse:collapse'><tr>{head}</tr>{body}</table>"


blocks = ""
for qid, a in devops_answers.items():
    sql_html = (f"<details style='margin-top:10px'><summary style='font-size:12px;color:{NAVY_600};cursor:pointer'>"
                f"The SQL Genie wrote</summary><pre style='font-family:{MONO};font-size:11px;white-space:pre-wrap;"
                f"background:{WHITE};border:1px solid {GRAY_LINES};padding:10px;margin:6px 0 0;color:{NAVY}'>{esc(a['sql'])}</pre></details>"
                if a["sql"] else f"<div style='font-size:12px;color:{LAVA}'>No SQL came back ({esc(a['status'])}).</div>")
    blocks += (f"<div style='border-top:1px solid {GRAY_LINES};padding:18px 0 10px'>"
               f"<div style='font-size:17px;font-weight:700;max-width:820px'>{esc(a['question'])}</div>"
               f"<div style='display:flex;gap:28px;flex-wrap:wrap;margin-top:10px'>"
               f"<div style='flex:1;min-width:280px;font-size:14px;line-height:1.5;color:{NAVY}'>{genie_text(a['text']) or '&nbsp;'}</div>"
               f"<div style='flex:1;min-width:320px'>{result_bars(a['columns'], a['rows'])}</div></div>{sql_html}</div>")
print("Asking the agent where delivery slows down")
AGENT_NAME = {"raw_tables": "the raw-tables agent", "metric_views": "the metric-views agent", "both": "the agent with both"}
displayHTML(frame(f"Asking {AGENT_NAME[best_key]} where delivery slows down",
                  "Four questions with no answer key, each in a new conversation. On the left, Genie's own words; on the right, "
                  "the rows its query returned.", blocks))

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
def arm_summary(key: str) -> dict:
    """Totals for one agent across every round."""
    sub = attempts_df[attempts_df.arm == key]
    return {"n": len(sub), "good": int((sub.assessment == "GOOD").sum()), "value": int(sub.value_match.sum())}


summaries = {k: arm_summary(k) for k in ARMS}
per_q = {(k, b["id"]): attempts_df[(attempts_df.arm == k) & (attempts_df.q == b["id"])] for k in ARMS for b in BENCHMARKS}

head = "".join(
    f"<div style='flex:1;min-width:170px;border-top:3px solid {NAVY};padding-top:10px'>"
    f"<div style='font-size:15px;font-weight:700;color:{NAVY}'>{esc(ARMS[k]['label'])}</div>"
    f"<div style='font-size:12px;color:{GRAY_TEXT}'>{ARMS[k]['objects']} objects</div>"
    f"<div style='font-size:34px;font-weight:700;color:{NAVY};margin-top:8px'>{s['good']}"
    f"<span style='font-size:16px;color:{GRAY_TEXT}'> / {s['n']} Good</span></div>"
    f"<div style='font-size:13px;color:{GRAY_TEXT}'>{s['value']} / {s['n']} with the right value</div></div>"
    for k, s in summaries.items())

rows_html, any_split = "", False
for b in BENCHMARKS:
    goods = {k: int((per_q[(k, b["id"])].assessment == "GOOD").sum()) for k in ARMS}
    split = max(goods.values()) == EVAL_ROUNDS and min(goods.values()) == 0
    any_split = any_split or split
    cells = ""
    for k in ARMS:
        g = goods[k]
        width = 100 * g / EVAL_ROUNDS if EVAL_ROUNDS else 0
        missed = split and g == 0  # drawn in lava: another agent got this question every round
        weight = "700" if missed else "400"
        cells += (f"<td style='padding:8px 10px;width:22%'><div style='font-size:13px;font-weight:{weight};"
                  f"color:{LAVA if missed else NAVY}'>{g} / {EVAL_ROUNDS}</div>"
                  f"<div style='height:3px;background:{LAVA if missed else GRAY_LINES};margin-top:5px'>"
                  f"<div style='height:3px;width:{width:.0f}%;background:{NAVY}'></div></div></td>")
    rows_html += (f"<tr style='border-top:1px solid {GRAY_LINES}'>"
                  f"<td style='padding:8px 10px;font-size:13px;color:{GRAY_TEXT};width:5%'>{esc(b['id'][1:])}</td>"
                  f"<td style='padding:8px 10px;font-size:14px;color:{NAVY}'>{esc(b['short'])}</td>{cells}</tr>")

both_raw = int(both.uses_raw_tables.sum())
both_mv = int(both.uses_metric_views.sum())
callout = (f"<div style='padding:14px 18px;margin:22px 0 0;background:{OAT};border-left:4px solid {LAVA if both_raw else NAVY};"
           f"font-size:15px;color:{NAVY}'>The agent that had both wrote {both_mv} of its {len(both)} queries against the metric views "
           f"and {both_raw} against the raw tables.</div>")

table = (f"<table style='border-collapse:collapse;width:100%;margin-top:26px'>"
         f"<tr><td></td><td style='font-size:12px;color:{GRAY_TEXT};padding:0 10px 6px'>Question</td>"
         + "".join(f"<td style='font-size:12px;color:{GRAY_TEXT};padding:0 10px 6px'>{esc(ARMS[k]['label'])}: Good</td>" for k in ARMS)
         + f"</tr>{rows_html}</table>")

displayHTML(
    f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 30px;font-family:{SANS};border-top:6px solid {NAVY}'>"
    f"<div style='font-size:36px;font-weight:700;line-height:1.1;max-width:760px'>Three Genie Agents, one Godot repo, {len(BENCHMARKS)} questions</div>"
    f"<div style='font-size:16px;color:{GRAY_TEXT};line-height:1.45;max-width:700px;margin:12px 0 26px'>"
    f"Each agent answered every benchmark question {EVAL_ROUNDS} times. The bars count the rounds Databricks graded Good"
    f"{'; lava marks a question one agent got every round and another never did' if any_split else ''}.</div>"
    f"<div style='display:flex;gap:28px;flex-wrap:wrap'>{head}</div>{table}{callout}</div>"
)

# COMMAND ----------

# DBTITLE 1,RESULTS_JSON
def arm_results(key: str) -> dict:
    """Every number a finding can cite for one agent."""
    sub = attempts_df[attempts_df.arm == key]
    n = len(sub)
    return {
        "attempts": n,
        "good": int((sub.assessment == "GOOD").sum()),
        "bad": int((sub.assessment == "BAD").sum()),
        "needs_review": int((sub.assessment == "NEEDS_REVIEW").sum()),
        "other_assessment": int((~sub.assessment.isin(["GOOD", "BAD", "NEEDS_REVIEW"])).sum()),
        "good_rate": round(float((sub.assessment == "GOOD").mean()), 4) if n else None,
        "value_matches": int(sub.value_match.sum()),
        "value_match_rate": round(float(sub.value_match.mean()), 4) if n else None,
        "bad_but_value_matched": int(((sub.assessment != "GOOD") & sub.value_match).sum()),
        "good_but_value_missed": int(((sub.assessment == "GOOD") & ~sub.value_match).sum()),
        "no_sql": int(sub.sql.isna().sum()),
        "rerun_errors": int(sub.error.notna().sum()),
        "answers_using_metric_views": int(sub.uses_metric_views.sum()),
        "answers_using_raw_tables": int(sub.uses_raw_tables.sum()),
        "objects": ARMS[key]["objects"],
        "median_eval_round_seconds": float(statistics.median(ARMS[key]["round_seconds"])),
        "per_question": {b["id"]: {"good": int((per_q[(key, b["id"])].assessment == "GOOD").sum()),
                                   "value": int(per_q[(key, b["id"])].value_match.sum())} for b in BENCHMARKS},
    }


results = {
    "snapshot_as_of": SNAPSHOT_META["as_of"],
    "snapshot_counts": SNAPSHOT_META["counts"],
    "table_rows": row_counts,
    "benchmark_questions": len(BENCHMARKS),
    "eval_rounds": EVAL_ROUNDS,
    "metric_view_checks_passed": metric_view_checks_passed,
    "expected": {b["id"]: b["expected"]["value"] for b in BENCHMARKS},
    "arms": {k: arm_results(k) for k in ARMS},
    "devops_questions": {
        "agent": best_key,
        "asked": len(devops_answers),
        "answered_with_sql": sum(1 for a in devops_answers.values() if a["sql"]),
        "per_question": {qid: {"status": a["status"], "rows": len(a["rows"]), "columns": a["columns"],
                               "reads_metric_views": bool(a["sql"]) and any(m in a["sql"].lower().replace("`", "") for m in METRIC_VIEWS),
                               "first_rows": a["rows"][:10]}
                         for qid, a in devops_answers.items()},
    },
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC Give it both. Over three rounds of the same twelve questions, the agent that could see the raw tables and the metric views
# MAGIC was graded Good on 36 of 36 answers. The metric-views agent got 34, and the raw-tables agent got 29.
# MAGIC
# MAGIC Lead time is where the raw tables let the agent down. Asked for the days from merge to the first stable release, the raw-tables
# MAGIC agent missed in all three rounds: its SQL counted only feature releases and left out backports, so it decided for itself what "first
# MAGIC release" means. The metric view has that definition written down, and the agent with both used it and got lead time right every round.
# MAGIC
# MAGIC The metric-views agent's two misses both came in round 1, and they trace back to my instruction rather than the views. The instruction
# MAGIC says the data runs from Oct 5, 2025, and the agent turned that into a filter on the monthly dimension, which drops October 2025. If you
# MAGIC put dates in a Genie Agent's instructions, check how it uses them.
# MAGIC
# MAGIC The thread's worry was that an agent given both would be confused. This one wrote 29 of its 36 queries against the metric views and
# MAGIC 7 against the raw tables, and none of its answers were wrong.
# MAGIC
# MAGIC Put to work on Godot's delivery, it found that a PR that first ships as a backport reaches a stable release in a median of 30 days,
# MAGIC against 70 for one that waits for a feature release. About one CI run in six needed a second attempt, and May 2026 was the worst
# MAGIC month at 23%. The question about which code areas are slowest failed, because the SQL Genie wrote broke a rule of the code-area
# MAGIC metric view, so read the SQL before you act on an answer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: refresh the snapshot from GitHub
# MAGIC
# MAGIC The snapshot is pinned so every reader benchmarks the same data. To pull a fresh one, store a GitHub token in a
# MAGIC [secret scope](https://docs.databricks.com/aws/en/security/secrets/) (a token with no scopes is enough for a public repo; GitHub
# MAGIC allows 60 unauthenticated requests an hour and the pull takes thousands), set `REFRESH = True`, and run this cell, then rerun from
# MAGIC "Download the snapshot" onward. It runs the same `fetch_snapshot.py` that built the pinned copy. The compares are pinned to the
# MAGIC release tags that existed on Oct 5, 2026, so add new tags to its `COMPARES` list to cover releases after that.

# COMMAND ----------

# DBTITLE 1,Refresh the snapshot (off by default)
REFRESH = False
SECRET_SCOPE, SECRET_KEY = "github", "token"

if REFRESH:
    env = dict(os.environ, GITHUB_TOKEN=dbutils.secrets.get(SECRET_SCOPE, SECRET_KEY))
    script = f"{VOLUME_PATH}/fetch_snapshot.py"
    out = subprocess.run([sys.executable, script, "--as-of", date.today().isoformat(), "--out", VOLUME_PATH],
                         env=env, capture_output=True, text=True)
    print(out.stderr[-2000:])
    out.check_returncode()
else:
    print("Refresh is off; the pinned snapshot stays in place.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Set `CLEAN_UP = True` to move the three agents to the trash and drop the schema with its tables, metric views and volume. It's off
# MAGIC by default so you can open the agents and ask them your own questions first.

# COMMAND ----------

# DBTITLE 1,Cleanup
CLEAN_UP = False

if CLEAN_UP:
    for arm in ARMS.values():
        if arm.get("space_id"):
            api("DELETE", f"/api/2.0/genie/spaces/{arm['space_id']}")
    spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
    print(f"Trashed {len(ARMS)} agents and dropped {FQ}.")
else:
    print("Cleanup is off. The agents are in your home folder; set CLEAN_UP = True to remove everything.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Curate an effective Genie Agent](https://docs.databricks.com/aws/en/genie-agents/best-practices): Databricks' guidance on data
# MAGIC   sources, SQL expressions, example SQL and text instructions.
# MAGIC - [Test and monitor a Genie Agent](https://docs.databricks.com/aws/en/genie/benchmarks): how benchmarks are written, run and graded.
# MAGIC - [Unity Catalog metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views/): defining dimensions and measures once
# MAGIC   for SQL, dashboards and Genie.
# MAGIC - [Query metric views](https://docs.databricks.com/aws/en/uc-semantics/metric-views/query): how `MEASURE()` and grouping work.
# MAGIC - [Use the Genie Agents API](https://docs.databricks.com/aws/en/genie-agents/conversation-api): creating agents and asking them
# MAGIC   questions from code.
