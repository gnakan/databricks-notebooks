# Databricks notebook source

# MAGIC %md
# MAGIC In a recent Databricks [blog post](https://www.databricks.com/blog/introducing-aidecide-make-fast-decisions-your-governed-data) introducing Databricks' [ai_decide function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_decide):
# MAGIC
# MAGIC > "Because ai_decide is optimized for fast decisions, it yields lower latency and cost than an LLM on similar tasks."
# MAGIC
# MAGIC "Optimized for fast decisions" caught my eye. Routing a support ticket is that kind of decision, and plenty of teams already make it with Databricks' [ai_classify function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify). So, I put together an experiment. The notebook builds 200 labelled Stark Industries support tickets, runs both functions over the same rows, and scores each one against the labels. It does that twice: once with bare team names, which is how I first ran it, and once with a one-line description of each team. The validation conditions below are checked on the second round.
# MAGIC
# MAGIC **Hypothesis:** If the ai_decide function is a drop-in for routing, it should pick the right team as often as the ai_classify function does and finish the batch faster. Its escalation probability should also catch the tickets that need to jump the queue.
# MAGIC
# MAGIC **Validation conditions** (set before the run). The ai_decide function's team accuracy lands no more than 2 percentage points below the ai_classify function's on the same rows. Its batch time for the same single question, the middle of three runs after a warm-up, comes in under the ai_classify function's. And with the escalation question at a 0.5 threshold, it flags at least 90% of the tickets labelled for escalation, with at least 80% precision.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC This runs on Databricks Free Edition in a serverless notebook, with nothing to pip install. The tickets go into a schema the notebook creates, and the last cell drops it unless you set the `keep_tables` widget to true.

# COMMAND ----------

import json
import time
import statistics
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, IntegerType
)

USER = spark.sql("SELECT current_user()").first()[0]
CATALOG = "workspace"
SCHEMA = "stark_tl_ai_decide_exp"
TABLE = f"{CATALOG}.{SCHEMA}.support_tickets"

# Set keep_tables to true to leave the answer tables in place after the run, to read ticket by ticket
dbutils.widgets.text("keep_tables", "false")
KEEP_TABLES = dbutils.widgets.get("keep_tables").strip().lower() == "true"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Catalog: {CATALOG}, Schema: {SCHEMA}, User: {USER}, keep tables: {KEEP_TABLES}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate
# MAGIC The ai_classify and ai_decide functions have to resolve before anything else runs. If either one fails here, the error printed above the assert says why.

# COMMAND ----------

gate_results = {"ai_decide_resolves": False, "ai_classify_resolves": False}

GATE_QUESTION = json.dumps({
    "team": {"type": "choice", "instructions": "Which team?", "criteria": {"Eng": None, "Ops": None}}
})

try:
    r = spark.sql(
        f"SELECT ai_decide('test ticket', '{GATE_QUESTION}'):response.answers.team.choice::string AS r"
    ).first()[0]
    gate_results["ai_decide_resolves"] = r is not None
    print(f"ai_decide gate: PASS (choice: {r})")
except Exception as e:
    print(f"ai_decide gate: FAIL: {e}")

try:
    r = spark.sql(
        "SELECT ai_classify('test ticket', '[\"Eng\",\"Ops\"]', map('version', '2.1'))"
        ":response[0].value::string AS r"
    ).first()[0]
    gate_results["ai_classify_resolves"] = r is not None
    print(f"ai_classify gate: PASS (label: {r})")
except Exception as e:
    print(f"ai_classify gate: FAIL: {e}")

assert gate_results["ai_decide_resolves"], "ai_decide did not resolve; cannot continue."
assert gate_results["ai_classify_resolves"], "ai_classify did not resolve; cannot continue."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the ticket set
# MAGIC I wrote the labels first and the ticket text to fit them, so the routing answer sits in the words and nowhere else. There are 200 distinct tickets, 50 per team, and 29 of them need escalating. The last eight in each team are borderline on purpose: they mention another team's territory, and you have to read the whole ticket to route it.

# COMMAND ----------

TEAMS = ["Engineering", "Billing", "Security", "Operations"]
LABELS_JSON = json.dumps(TEAMS)

F, T = False, True  # escalate
# (description, escalate, urgency 1-5). Last 8 of each team are borderline.
TICKETS = {
"Engineering": [
("API returns 500 on payloads over 10 MB; smaller calls work", F, 3),
("Dashboard data pipeline stalls after the weekly upgrade", F, 3),
("A serverless notebook fails to start after a library change", F, 2),
("SDK raises AttributeError on streaming writes to Delta", F, 3),
("Autoscaler does not release nodes after the job completes", F, 2),
("Job has been hung at the checkpoint stage for six hours and the morning report depends on it", T, 5),
("Production pipeline is down and the SLA breaches in two hours", T, 5),
("Batch job returned the wrong row count after a schema change", F, 3),
("MERGE statement fails with a concurrent modification error twice a week", F, 3),
("Python UDF is ten times slower since the runtime update", F, 3),
("Streaming query restarts every few minutes with an offset error", F, 3),
("Model serving endpoint times out on every request since the deploy and the customer app is down", T, 5),
("Unit tests for the ingestion library fail only on the CI cluster", F, 2),
("A VACUUM removed files that a downstream job still reads", F, 4),
("Need guidance on partitioning a 4 TB events table", F, 1),
("Query plan shows a broadcast join blowing up executor memory", F, 3),
("Schema evolution added a column with the wrong type to the silver table", F, 3),
("REST API pagination token stops working after 1,000 results", F, 2),
("Checkout service writes to the orders table are failing right now and no orders are landing", T, 5),
("Notebook widget values reset when the job is triggered from the scheduler", F, 2),
("dbt run fails on one model after upgrading the adapter", F, 2),
("Feature request: retry a single task instead of the whole job", F, 1),
("Spark job spills to disk on a small join; looking for tuning advice", F, 2),
("Auto Loader stream picks up the same files twice after a restart", F, 3),
("Timestamp columns shift by eight hours after moving to the new cluster", F, 3),
("Vector search index sync has failed since last night and the support chatbot returns nothing", T, 5),
("Request for a code review of our medallion pipeline before go-live", F, 1),
("JDBC driver drops connections after exactly 30 minutes", F, 2),
("Integration test environment shows stale data from last week", F, 2),
("Job that worked yesterday now fails with 'table not found' and nothing changed on our side", F, 3),
("Need help migrating a Scala notebook to Python", F, 1),
("Lakeflow pipeline update stuck in Initializing for an hour", F, 3),
("Duplicate rows appearing in the gold table after the backfill", F, 3),
("Query that took 20 seconds now takes 4 minutes on the same warehouse", F, 3),
("Library install fails with a dependency conflict on numpy", F, 2),
("How should we version our ML features?", F, 1),
("Git folder sync fails with an authentication error after the repo moved", F, 2),
("Payment processing job crashed mid-run and half of today's transactions are unposted", T, 5),
("Long-running notebook is killed with an out-of-memory error at 80 percent complete", F, 3),
("Change data feed returns no rows for a table we know changed", F, 3),
("Is liquid clustering a fit for our access pattern?", F, 1),
("Python wheel task cannot find its entry point after repackaging", F, 2),
# borderline
("Billing report shows a cluster we do not recognize running overnight", T, 4),
("Notebook job ran for 14 hours and cost 40 times the estimate", F, 3),
("Legacy pipeline broke after the workspace upgrade and is blocking the Friday financial close", T, 5),
("Invoice shows DBUs for a job that should have failed fast but kept retrying all weekend", F, 3),
("Workspace upgrade scheduled by ops broke our job's init script", F, 3),
("Jobs fail with permission denied after a library was moved to a new volume", F, 3),
("Cost of the nightly job doubled after we added one join", F, 2),
("Genie agent answers are wrong because a source table stopped refreshing", F, 3),
],
"Billing": [
("Invoice total does not match the usage report for September", F, 2),
("Charged the wrong tier for the past three months", F, 3),
("Credit applied to the wrong workspace", F, 2),
("Need a breakdown of DBU spend by team for Q3", F, 1),
("Charged for a cluster that was stopped during the maintenance window", F, 2),
("Overage notification sent in error; the account is not over its limit", F, 2),
("Need to dispute a line item from last month", F, 2),
("Prepaid credits are not showing on the current invoice", F, 3),
("Purchase order number missing from the invoice and our AP team will reject it", F, 3),
("Change the billing contact to our new finance lead", F, 1),
("Need a W-9 for vendor onboarding", F, 1),
("Card on file declined and the account shows a suspension warning for tomorrow", T, 4),
("Want to move from monthly to annual billing", F, 1),
("Tax was charged on an invoice for a tax-exempt entity", F, 2),
("Usage dashboard and invoice disagree by about 12 percent", F, 2),
("Need invoices re-issued with the new company legal name", F, 1),
("How is serverless usage billed compared with classic compute?", F, 1),
("Received two invoices for the same month", F, 2),
("Commitment drawdown report looks wrong for August", F, 2),
("Need a quote for three more workspaces next quarter", F, 1),
("Refund for a duplicate payment has not arrived after 30 days", F, 3),
("Invoice currency changed from USD to EUR without notice", F, 2),
("Budget alert emails go to someone who left the finance team", F, 1),
("Want cost tags as a column on the invoice export", F, 1),
("Account suspended for non-payment but the payment cleared yesterday, and all jobs are stopped", T, 5),
("Late fee applied even though we paid on the due date", F, 2),
("Need a statement of all payments this fiscal year for the audit", F, 2),
("Price on the renewal quote differs from the contract", F, 3),
("Marketplace charges on our invoice for listings we never bought", F, 3),
("Requesting net-60 payment terms", F, 1),
("Billing portal shows zero usage for the last five days", F, 2),
("Need to split the invoice across two cost centers", F, 1),
("Enterprise agreement discount is not applied", F, 3),
("Credit card receipt needed for an expense report", F, 1),
("Usage export CSV is missing the SKU column", F, 2),
("Does unused commit roll over to next year?", F, 1),
("Invoice due date falls before we received the invoice", F, 2),
("Help forecasting next quarter's spend from current usage", F, 1),
("Charged for model serving we turned off in July", F, 2),
("Wire transfer sent with the wrong reference number", F, 2),
("Want to confirm the price per DBU on our contract", F, 1),
("Pro-rated charge for a mid-month upgrade looks doubled", F, 2),
# borderline
("API call fails and the invoice shows usage for it anyway", F, 3),
("Workspace cost jumped after ops enabled a new region", F, 2),
("Billing shows charges from a workspace we deleted last month", F, 3),
("Need usage broken down per service principal for a chargeback model", F, 1),
("Our finance team cannot log in to the billing portal", F, 2),
("Spend spiked overnight and we want to know which jobs caused it", F, 3),
("Invoice lists a workspace owned by a contractor whose contract ended", F, 2),
("Overnight jobs ran on an expensive instance type after a policy change; we want a credit", F, 2),
],
"Security": [
("Access token committed to a public repository", T, 5),
("Login from an unknown IP address succeeded after two failed attempts", T, 5),
("Service principal permissions exceed what the job needs", F, 3),
("Audit log shows a data export by a former employee's account", T, 5),
("Group membership grant went to the wrong user", F, 3),
("Workspace permissions review requested before an external audit", F, 2),
("API key in a notebook that was shared with an external partner", T, 4),
("Account lockout after three failed MFA attempts while the user is travelling", F, 2),
("Need SSO configured with our new identity provider", F, 2),
("Enforce IP access lists on all workspaces", F, 2),
("Want column masking on the salary column before analysts get access", F, 2),
("Three users reported a phishing email imitating our workspace login page", T, 4),
("Need evidence of encryption at rest for a compliance questionnaire", F, 1),
("Row filters are not applied when the table is queried through a view", T, 4),
("Rotate all personal access tokens older than 90 days", F, 2),
("Customer PII found in a table tagged as public", T, 4),
("Need audit logs delivered to our SIEM", F, 2),
("Secret scope readable by a group that should not have it", F, 3),
("Want to block personal access tokens and require OAuth", F, 2),
("Ransomware alert on a laptop that has a stored workspace token", T, 5),
("How do I grant read-only access to a single schema?", F, 1),
("Penetration test scheduled; what are we allowed to test?", F, 1),
("Catalog with sensitive data is granted to all account users", F, 4),
("Unknown service principal with admin rights was created over the weekend", T, 5),
("Need to confirm data residency for EU customers", F, 2),
("User removed from the identity provider still has an active workspace session", F, 3),
("Need a list of everyone with admin rights", F, 1),
("Delta Sharing recipient still active for a partner we stopped working with", F, 3),
("SCIM sync stopped and new joiners get no group memberships", F, 3),
("Customer security questionnaire asks about our key management", F, 1),
("Want an alert when anyone downloads more than 1 GB of query results", F, 2),
("A notebook prints environment variables that include a database password", F, 3),
("SQL injection strings showing up in the app's query logs", F, 4),
("Need customer-managed keys on the new workspace", F, 2),
("Shared dashboard link opens without login for people outside the company", T, 4),
("Do audit logs capture Genie questions?", F, 1),
("Password reset emails arriving for users who did not request them", F, 3),
("Service principal secret was pasted into a public chat channel", T, 4),
("Restrict who can create clusters with public IPs", F, 2),
("Old storage credential still points at a bucket with broad access", F, 3),
("Two-factor enrollment is optional and the auditor wants it mandatory", F, 2),
("Disable the download button on query results", F, 2),
# borderline
("User reports the dashboard shows rows they should not see", T, 5),
("Former-employee account still shows as active in the member list", F, 3),
("Job fails with permission denied after the catalog owner changed", F, 3),
("Invoice shows usage from a cloud region we have never used", T, 4),
("CPU graphs suggest a cluster in our account is mining cryptocurrency", T, 5),
("Need a group for the analytics team with access to two schemas", F, 1),
("A contractor asks why they can see the HR schema", T, 4),
("Audit log delivery stopped three days ago", F, 3),
],
"Operations": [
("Provision a new workspace for the EU region", F, 2),
("Need to migrate tables from the old catalog to the new one", F, 2),
("New-user onboarding checklist not completed after two weeks", F, 1),
("Turn on a preview feature for the workspace", F, 1),
("Region failover test needs scheduling for next quarter", F, 2),
("Workspace storage is approaching the quarterly quota", F, 3),
("Add a second workspace admin", F, 1),
("Old cluster policy still in use after deprecation", F, 2),
("Need a SQL warehouse sized for 40 concurrent BI users", F, 2),
("Maintenance window clashes with month-end processing; can we move it?", F, 3),
("Raise the job concurrency quota", F, 2),
("Whole workspace unreachable for every user since 9 AM", T, 5),
("Need a sandbox workspace for the interns starting Monday", F, 2),
("Want a runbook for restoring a dropped table", F, 1),
("Cluster policies need updating for the new instance families", F, 1),
("Archive the workspace for a finished project", F, 1),
("SQL warehouses will not start and every dashboard is blank before the board meeting", T, 5),
("Enable serverless on the analytics workspace", F, 1),
("Disaster recovery plan review is due this month", F, 2),
("Set default tags on every new cluster", F, 1),
("Rename the production catalog to match our naming standard", F, 1),
("Storage behind the workspace hit its limit and writes are failing everywhere", T, 5),
("Need a second region added to our account", F, 2),
("When is the next runtime upgrade scheduled?", F, 1),
("Shared cluster restarts every night at 2 AM and we do not know why", F, 2),
("Set up instance pools to cut cluster start time", F, 1),
("Move ten jobs to a new service principal before the old one is retired", F, 2),
("Enforce a naming convention for new schemas", F, 1),
("Copy the production workspace configuration into staging", F, 2),
("Network team needs the workspace egress IP ranges", F, 2),
("Private link setup for the new VPC", F, 2),
("Monthly capacity report for leadership", F, 1),
("Two workspaces need to share one metastore", F, 2),
("Stop unused SQL warehouses after hours", F, 1),
("Need an inventory of every job and its owner", F, 1),
("Account console is down and nobody can provision anything", T, 4),
("Move a workspace to a different cloud subscription", F, 2),
("Need Terraform state for the workspaces we created by hand", F, 2),
("Volume is missing on the new workspace after the migration", F, 2),
("Retire the dev workspace at the end of the month", F, 1),
("Plan a runtime upgrade across 300 jobs", F, 2),
("Schedule a quarterly access recertification", F, 1),
# borderline
("Permissions change request following a team reorganization", F, 2),
("New hire cannot log in after onboarding and it is blocking their first sprint", F, 3),
("Need a budget policy for each team before the new fiscal year", F, 1),
("Workspace upgrade broke three jobs; please roll it back", F, 4),
("Expired service principal token failed every scheduled job overnight", F, 4),
("Need to know who deleted a table yesterday", F, 3),
("Cost alert fired because the dev cluster ran all weekend", F, 2),
("Rotate the storage credential before the cloud key expires on Friday", F, 3),
],
}

rows = []
for team, items in TICKETS.items():
    for desc, escalate, urgency in items:
        rows.append((len(rows) + 1, desc, team, escalate, urgency))

assert len(rows) == 200 and len({r[1] for r in rows}) == 200, "expected 200 distinct tickets"

schema = StructType([
    StructField("ticket_id", IntegerType()),
    StructField("description", StringType()),
    StructField("true_team", StringType()),
    StructField("true_escalate", BooleanType()),
    StructField("true_urgency", IntegerType()),
])
df = spark.createDataFrame(rows, schema)
df.write.mode("overwrite").saveAsTable(TABLE)

label_dist = spark.sql(
    f"SELECT true_team, COUNT(*) AS n, SUM(CAST(true_escalate AS INT)) AS escalated "
    f"FROM {TABLE} GROUP BY true_team ORDER BY true_team"
).toPandas()
total = spark.table(TABLE).count()
print(f"Ticket set: {total} rows")
print(label_dist.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 1: bare team names
# MAGIC My first runs gave each function only the four team names. All three functions missed roughly one ticket in five, and when I read the misses ticket by ticket, most of them sat on the line between Engineering and Operations, a line the names alone never drew. "Production pipeline is down" went to Operations from all three; I had labelled it Engineering. So this cell keeps that first pass, one untimed run per function, and the timed experiment below gives every function the same one-line description of each team.

# COMMAND ----------

QUERY_MODEL = "databricks-meta-llama-3-3-70b-instruct"

def first_team(reply):
    # The earliest team name in the reply, or None when it names none
    if reply is None:
        return None
    text = reply.lower()
    hits = [(text.find(t.lower()), t) for t in TEAMS if t.lower() in text]
    return min(hits)[1] if hits else None

BARE_DECIDE = json.dumps({"team": {
    "type": "choice",
    "instructions": "Which support team should handle this ticket?",
    "criteria": {team: None for team in TEAMS},
}})
BARE_PROMPT = (
    "You route support tickets. Which support team should handle this ticket? "
    "Reply with exactly one word: Engineering, Billing, Security or Operations. Ticket: "
)
R1_OUT = f"{CATALOG}.{SCHEMA}.round1_results"
spark.sql(
    f"CREATE OR REPLACE TABLE {R1_OUT} AS "
    f"SELECT ticket_id, true_team, "
    f"ai_classify(description, '{json.dumps(TEAMS)}', map('version', '2.1')):response[0].value::string AS classify_team, "
    f"ai_decide(description, '{BARE_DECIDE}'):response.answers.team.choice::string AS decide_team, "
    f"ai_query('{QUERY_MODEL}', concat('{BARE_PROMPT}', description)) AS query_raw "
    f"FROM {TABLE}"
)
r1 = spark.table(R1_OUT).toPandas()
r1["query_team"] = r1["query_raw"].map(first_team)
r1_correct = {
    name: int((r1["true_team"] == r1[col]).sum())
    for name, col in [("ai_classify", "classify_team"), ("ai_decide", "decide_team"), ("ai_query", "query_team")]
}
for name, n in r1_correct.items():
    print(f"Round 1, {name}: {n}/{len(r1)} right team")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Round 2: one line per team
# MAGIC These descriptions write down the rule I used when I labelled the tickets. I wrote them after round 1, and the labels didn't change. All three functions get the same text: the ai_classify function as label descriptions, the ai_decide function as choice criteria, and Llama 3.3 70B in its prompt. The escalation question gets the same treatment.

# COMMAND ----------

TEAM_DESCRIPTIONS = {
    "Engineering": "Code, jobs, pipelines, queries, notebooks, models and libraries that are failing, slow or returning wrong results",
    "Billing": "Invoices, charges, credits, payments, prices, contracts and spend reports",
    "Security": "Access, permissions, credentials, identity, audit logs and any sign of exposed data or a compromised account",
    "Operations": "Workspace and account administration: provisioning, regions, quotas, capacity, policies, upgrades, networking, migrations and onboarding",
}
ESCALATE_CRITERIA = {
    "true": "An outage, data loss or security breach happening now that stops the customer working or exposes data",
    "false": "Anything that can wait for the normal queue, including serious problems with a workaround and access cleanup with no sign of misuse",
}
for team, words in TEAM_DESCRIPTIONS.items():
    print(f"{team}: {words}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The ai_classify function run
# MAGIC One question, which team handles this ticket, with the team descriptions as labels. Each run writes its answers to a table, and each batch runs three times after a warm-up; the notebook keeps the middle time, so one slow or fast run can't skew it.

# COMMAND ----------

CLASSIFY_LABELS = json.dumps(TEAM_DESCRIPTIONS)

CLASSIFY_OUT = f"{CATALOG}.{SCHEMA}.classify_results"

def run_classify(table, labels_json):
    # Each run writes its answers to a table, so the timed work is the AI call and
    # the scoring reads exactly the answers that were timed.
    start = time.perf_counter()
    spark.sql(
        f"CREATE OR REPLACE TABLE {CLASSIFY_OUT} AS "
        f"SELECT ticket_id, true_team, true_escalate, "
        f"ai_classify(description, '{labels_json}', map('version', '2.1'))"
        f":response[0].value::string AS classify_team "
        f"FROM {table}"
    )
    elapsed = time.perf_counter() - start
    return spark.table(CLASSIFY_OUT), elapsed

# warm-up
_, _ = run_classify(TABLE, CLASSIFY_LABELS)

classify_times = []
for i in range(3):
    df_classify, t = run_classify(TABLE, CLASSIFY_LABELS)
    classify_times.append(t)
    print(f"  classify run {i+1}: {t:.1f}s")

classify_median = statistics.median(classify_times)
print(f"ai_classify typical batch (middle of 3): {classify_median:.1f}s")

classify_rows = df_classify.toPandas()
classify_correct = (classify_rows["true_team"] == classify_rows["classify_team"]).sum()
classify_accuracy = 100 * classify_correct / len(classify_rows)
print(f"ai_classify accuracy: {classify_correct}/{len(classify_rows)} = {classify_accuracy:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The ai_decide function run
# MAGIC First the same single team question, timed the same way, so the speed comparison is like for like. Then one call that also asks whether the ticket needs escalating and how urgent it is.

# COMMAND ----------

# The same team descriptions the ai_classify function got, as choice criteria
TEAM_QUESTION = {
    "type": "choice",
    "instructions": "Which support team should handle this ticket?",
    "criteria": TEAM_DESCRIPTIONS,
}

DECIDE_QUESTIONS_SINGLE = json.dumps({"team": TEAM_QUESTION})

DECIDE_QUESTIONS_THREE = json.dumps({
    "team": TEAM_QUESTION,
    "escalate": {
        "type": "noul",
        "instructions": "Does this ticket need immediate escalation?",
        "criteria": ESCALATE_CRITERIA,
    },
    "urgency": {
        "type": "score",
        "instructions": "How urgent is this ticket?",
        # Ordered lowest to highest; index 0-4 maps to the 1-5 urgency label
        "criteria": [
            "Routine request, no time pressure",
            "Minor issue, can wait for the normal queue",
            "Real problem with a workaround",
            "Serious problem, work is degraded",
            "Critical: production down or an active security incident",
        ],
    },
})

DECIDE_SINGLE_OUT = f"{CATALOG}.{SCHEMA}.decide_single_results"
DECIDE_THREE_OUT = f"{CATALOG}.{SCHEMA}.decide_three_results"

def run_decide_single(table, questions_json):
    start = time.perf_counter()
    spark.sql(
        f"CREATE OR REPLACE TABLE {DECIDE_SINGLE_OUT} AS "
        f"SELECT ticket_id, true_team, true_escalate, "
        f"ai_decide(description, '{questions_json}') AS decide_result "
        f"FROM {table}"
    )
    elapsed = time.perf_counter() - start
    return spark.table(DECIDE_SINGLE_OUT), elapsed

def run_decide_three(table, questions_json):
    start = time.perf_counter()
    spark.sql(
        f"CREATE OR REPLACE TABLE {DECIDE_THREE_OUT} AS "
        f"SELECT ticket_id, true_team, true_escalate, true_urgency, "
        f"ai_decide(description, '{questions_json}') AS decide_result "
        f"FROM {table}"
    )
    elapsed = time.perf_counter() - start
    return spark.table(DECIDE_THREE_OUT), elapsed

# warm-up (single question)
_, _ = run_decide_single(TABLE, DECIDE_QUESTIONS_SINGLE)

decide_single_times = []
for i in range(3):
    df_decide_single, t = run_decide_single(TABLE, DECIDE_QUESTIONS_SINGLE)
    decide_single_times.append(t)
    print(f"  decide (single q) run {i+1}: {t:.1f}s")

decide_single_median = statistics.median(decide_single_times)
print(f"ai_decide (single q) typical batch (middle of 3): {decide_single_median:.1f}s")

# Three-question run (one warm-up, three timed)
_, _ = run_decide_three(TABLE, DECIDE_QUESTIONS_THREE)
decide_three_times = []
for i in range(3):
    df_decide_three, t = run_decide_three(TABLE, DECIDE_QUESTIONS_THREE)
    decide_three_times.append(t)
    print(f"  decide (3-q) run {i+1}: {t:.1f}s")

decide_three_median = statistics.median(decide_three_times)
print(f"ai_decide (3-q) typical batch (middle of 3): {decide_three_median:.1f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Llama 3.3 70B, same question
# MAGIC The blog's comparison is with a large language model (LLM), so this sends the same team question to Meta's Llama 3.3 70B Instruct, one of the foundation models Databricks hosts, through Databricks' [ai_query function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_query), timed the same way, with the team descriptions in its prompt. I added this arm after the first runs, to answer the blog's claim directly; it wasn't one of the validation conditions above. The model replies in free text, so the notebook takes the first team name in the reply.

# COMMAND ----------

QUERY_PROMPT = (
    "You route support tickets to one of four teams. "
    + " ".join(f"{team}: {words}." for team, words in TEAM_DESCRIPTIONS.items())
    + " Which team should handle this ticket? Reply with exactly one word: the team name. Ticket: "
)
QUERY_OUT = f"{CATALOG}.{SCHEMA}.query_results"

def run_query(table):
    start = time.perf_counter()
    spark.sql(
        f"CREATE OR REPLACE TABLE {QUERY_OUT} AS "
        f"SELECT ticket_id, true_team, "
        f"ai_query('{QUERY_MODEL}', concat('{QUERY_PROMPT}', description)) AS query_raw "
        f"FROM {table}"
    )
    elapsed = time.perf_counter() - start
    return spark.table(QUERY_OUT), elapsed

# warm-up
_, _ = run_query(TABLE)

query_times = []
for i in range(3):
    df_query, t = run_query(TABLE)
    query_times.append(t)
    print(f"  ai_query run {i+1}: {t:.1f}s")

query_median = statistics.median(query_times)
print(f"ai_query ({QUERY_MODEL}) typical batch (middle of 3): {query_median:.1f}s")

query_rows = df_query.toPandas()
query_rows["query_team"] = query_rows["query_raw"].map(first_team)
query_unparsed = int(query_rows["query_team"].isna().sum())
query_correct = int((query_rows["true_team"] == query_rows["query_team"]).sum())
query_accuracy = 100 * query_correct / len(query_rows)
print(f"ai_query accuracy: {query_correct}/{len(query_rows)} = {query_accuracy:.1f}% (no team named: {query_unparsed})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scoring
# MAGIC The ai_decide function returns a VARIANT, so this cell pulls out the fields it needs before scoring. The escalation numbers come from the three-question call.

# COMMAND ----------

from pyspark.sql.functions import col, expr

# Flatten ai_decide single-question result
df_decide_flat = df_decide_single.withColumn(
    "decide_team",
    expr("decide_result:response.answers.team.choice::string")
).withColumn(
    "decide_confidence",
    expr("decide_result:response.answers.team.confidence::double")
).withColumn(
    "decide_error",
    expr("decide_result:error_message::string")
)

# Flatten ai_decide three-question result for escalation scoring
# answers is an object keyed by question id: team, escalate, urgency
df_three_flat = df_decide_three.withColumn(
    "decide_team_3q",
    expr("decide_result:response.answers.team.choice::string")
).withColumn(
    "escalate_prob",
    expr("decide_result:response.answers.escalate.probability::double")
).withColumn(
    "urgency_score",
    expr("decide_result:response.answers.urgency.score::double")
).withColumn(
    "decide_error",
    expr("decide_result:error_message::string")
)

decide_rows = df_decide_flat.drop("decide_result").toPandas()
three_rows = df_three_flat.drop("decide_result").toPandas()

# ai_decide accuracy (single question, same comparison as ai_classify)
decide_correct = (decide_rows["true_team"] == decide_rows["decide_team"]).sum()
decide_accuracy = 100 * decide_correct / len(decide_rows)
print(f"ai_decide accuracy: {decide_correct}/{len(decide_rows)} = {decide_accuracy:.1f}%")

# Where the two functions agree and disagree, ticket by ticket
agree = classify_rows[["ticket_id", "true_team", "classify_team"]].merge(
    decide_rows[["ticket_id", "decide_team"]], on="ticket_id"
)
c_ok = agree["true_team"] == agree["classify_team"]
d_ok = agree["true_team"] == agree["decide_team"]
both_wrong = int((~c_ok & ~d_ok).sum())
only_classify_right = int((c_ok & ~d_ok).sum())
only_decide_right = int((~c_ok & d_ok).sum())
print(f"Both wrong: {both_wrong}; only ai_classify right: {only_classify_right}; only ai_decide right: {only_decide_right}")

# Error rows
decide_errors = decide_rows["decide_error"].notna().sum()
print(f"ai_decide error_message rows: {decide_errors}")

# Confidence on right vs wrong answers
right_conf = decide_rows[decide_rows["true_team"] == decide_rows["decide_team"]]["decide_confidence"]
wrong_conf = decide_rows[decide_rows["true_team"] != decide_rows["decide_team"]]["decide_confidence"]
conf_right_med = float(right_conf.median()) if len(right_conf) else None
conf_wrong_med = float(wrong_conf.median()) if len(wrong_conf) else None
print(f"Typical confidence, right answers: {conf_right_med:.3f}" if conf_right_med else "Confidence correct: n/a")
print(f"Typical confidence, wrong answers: {conf_wrong_med:.3f}" if conf_wrong_med else "Confidence wrong: n/a")

# Escalation scoring at 0.5 threshold
escalate_pred = (three_rows["escalate_prob"] >= 0.5)
escalate_true = three_rows["true_escalate"]

tp = (escalate_pred & escalate_true).sum()
fp = (escalate_pred & ~escalate_true).sum()
fn = (~escalate_pred & escalate_true).sum()

recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
print(f"\nEscalation at 0.5 threshold:")
print(f"  TP={tp}, FP={fp}, FN={fn}")
print(f"  Recall:    {100*recall:.1f}%  (target: >=90%)")
print(f"  Precision: {100*precision:.1f}%  (target: >=80%)")

# Verdict check
acc_gap = classify_accuracy - decide_accuracy
speed_faster = decide_single_median < classify_median
esc_recall_ok = recall >= 0.90
esc_precision_ok = precision >= 0.80

conditions_met = sum([acc_gap <= 2.0, speed_faster, esc_recall_ok and esc_precision_ok])

if conditions_met == 3:
    verdict_label = "supported"
elif conditions_met == 2:
    verdict_label = "mixed-signal"
else:
    verdict_label = "not_supported"

print(f"\nVerdicts per condition:")
print(f"  Accuracy gap {acc_gap:.1f}pp <= 2pp: {'met' if acc_gap <= 2.0 else 'not met'}")
print(f"  Speed faster:                      {'met' if speed_faster else 'not met'}")
print(f"  Escalation recall+precision:       {'met' if (esc_recall_ok and esc_precision_ok) else 'not met'}")
print(f"\nVerdict: {verdict_label}")

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
# Starter: _lib/notebook-kit/results_panel_starter.py
import html

OAT_LIGHT, OAT, WHITE = "#F9F7F4", "#EEEDE9", "#FFFFFF"
NAVY, NAVY_600, GRAY_TEXT, GRAY_LINES = "#1B3139", "#1B5162", "#5A6F77", "#DCE0E2"
LAVA = "#FF3621"
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"
FONTS = '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'

def esc(v):
    return html.escape(str(v))

def section(title, dek, body):
    return (f"<div style='padding:24px 0 20px;border-top:1px solid {GRAY_LINES}'>"
            f"<div style='font-size:22px;font-weight:700;color:{NAVY}'>{esc(title)}</div>"
            f"<div style='font-size:14px;color:{GRAY_TEXT};margin:4px 0 16px;max-width:680px'>{esc(dek)}</div>"
            f"{body}</div>")

def squares(cells, size=9, gap=2, per_row=50):
    # cells: list of (fill, outline) pairs, one square per ticket
    out = []
    for fill, outline in cells:
        border = f"1.5px solid {outline}" if outline else "none"
        out.append(f"<span style='display:inline-block;width:{size}px;height:{size}px;margin:0 {gap}px {gap}px 0;"
                   f"background:{fill};border:{border};box-sizing:border-box'></span>")
    return f"<div style='max-width:{per_row * (size + gap)}px;line-height:0'>{''.join(out)}</div>"

def labelled(name, figure, body, figure_pop=False):
    return (f"<div style='display:flex;align-items:center;gap:18px;margin:10px 0'>"
            f"<div style='width:190px;font-size:15px;font-weight:700;color:{NAVY}'>{esc(name)}</div>"
            f"<div style='flex:1'>{body}</div>"
            f"<div style='width:150px;text-align:right;font-size:20px;font-weight:700;color:{LAVA if figure_pop else NAVY}'>{figure}</div></div>")

def bar(value, largest, pop=False, height=14):
    width = max(1, 100 * value / largest) if largest else 1
    return f"<div style='height:{height}px;background:{LAVA if pop else NAVY};width:{width:.1f}%'></div>"

def key(*pairs):
    items = "".join(
        f"<span style='display:inline-flex;align-items:center;gap:6px;margin-right:18px'>"
        f"<span style='display:inline-block;width:10px;height:10px;background:{fill};"
        f"border:{'1.5px solid ' + outline if outline else 'none'};box-sizing:border-box'></span>{esc(words)}</span>"
        for fill, outline, words in pairs)
    return f"<div style='font-size:13px;color:{GRAY_TEXT};margin-top:10px'>{items}</div>"

def callout(text):
    return (f"<div style='padding:14px 18px;margin:6px 0 4px;background:{OAT};border-left:4px solid {LAVA};"
            f"font-size:15px;color:{NAVY}'>{esc(text)}</div>")

def panel(headline, dek, body):
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 26px;font-family:{SANS};"
            f"border-top:6px solid {NAVY}'>"
            f"<div style='font-size:36px;font-weight:700;line-height:1.1;max-width:760px'>{esc(headline)}</div>"
            f"<div style='font-size:17px;color:{GRAY_TEXT};line-height:1.45;max-width:680px;margin:12px 0 22px'>{esc(dek)}</div>"
            f"{body}</div>")

# 1. Team routing: one square per ticket, in ticket order, for each function
classify_by_id = dict(zip(classify_rows["ticket_id"], classify_rows["true_team"] == classify_rows["classify_team"]))
decide_by_id = dict(zip(decide_rows["ticket_id"], decide_rows["true_team"] == decide_rows["decide_team"]))
query_by_id = dict(zip(query_rows["ticket_id"], query_rows["true_team"] == query_rows["query_team"]))
ids = sorted(classify_by_id)
routing = section(
    "Team routing",
    f"Round 2, with the team descriptions. One square per ticket, same order on every line. {both_wrong} tickets fooled both ai_classify and ai_decide; "
    f"those two disagreed on {only_classify_right + only_decide_right}.",
    labelled("ai_classify", f"{classify_correct} / {total}",
             squares([(NAVY, None) if classify_by_id[i] else (GRAY_LINES, None) for i in ids]))
    + labelled("ai_decide", f"{decide_correct} / {total}",
               squares([(NAVY, None) if decide_by_id.get(i) else (GRAY_LINES, None) for i in ids]))
    + labelled("ai_query (Llama 3.3 70B)", f"{query_correct} / {total}",
               squares([(NAVY, None) if query_by_id.get(i) else (GRAY_LINES, None) for i in ids]))
    + key((NAVY, None, "right team"), (GRAY_LINES, None, "wrong team")),
)

# 1b. Round 1 against round 2: right team with bare names, then with descriptions
round2_correct = {"ai_classify": int(classify_correct), "ai_decide": int(decide_correct), "ai_query": int(query_correct)}
change_rows = ""
for name in ["ai_classify", "ai_decide", "ai_query"]:
    before, after = r1_correct[name], round2_correct[name]
    change_rows += labelled(
        name, f"{before} → {after}",
        f"<div style='height:8px;background:{GRAY_LINES};width:{100 * before / total:.1f}%;margin-bottom:3px'></div>"
        f"<div style='height:8px;background:{NAVY};width:{100 * after / total:.1f}%'></div>",
    )
descriptions = section(
    "What one line per team changed",
    f"Right team out of {total}: round 1 with bare team names (gray), round 2 with the descriptions (navy).",
    change_rows + key((GRAY_LINES, None, "bare names"), (NAVY, None, "with descriptions")),
)

# 2. Batch time: median of three timed runs; lava marks ai_decide when it is the slower one
largest_time = max(classify_median, decide_single_median, decide_three_median, query_median)
decide_slower = decide_single_median > classify_median
timing = section(
    "Batch time for all tickets",
    "Each batch ran three times after a warm-up; this is the middle time. Each run sends every ticket through the function once.",
    labelled("ai_classify (team)", f"{classify_median:.0f}s", bar(classify_median, largest_time))
    + labelled("ai_decide (team)", f"{decide_single_median:.0f}s",
               bar(decide_single_median, largest_time, pop=decide_slower), figure_pop=decide_slower)
    + labelled("ai_decide (team, escalation, urgency)", f"{decide_three_median:.0f}s",
               bar(decide_three_median, largest_time))
    + labelled("ai_query, Llama 3.3 70B (team)", f"{query_median:.0f}s", bar(query_median, largest_time)),
)

# 3. Escalation: the tickets labelled for escalation, caught or missed, then the false alarms
escalation = section(
    "Escalation at a 0.5 threshold",
    f"{int(tp + fn)} tickets were labelled for escalation. Recall {100 * recall:.0f}%, precision {100 * precision:.0f}%.",
    labelled("Needed escalating", f"{int(tp)} caught",
             squares([(NAVY, None)] * int(tp) + [(LAVA, None)] * int(fn), size=14, gap=3))
    + labelled("Flagged, didn't need it", f"{int(fp)}",
               squares([(OAT_LIGHT, NAVY)] * int(fp), size=14, gap=3))
    + key((NAVY, None, "caught"), (LAVA, None, f"missed ({int(fn)})"), (OAT_LIGHT, NAVY, "false alarm")),
)

# 4. Confidence on the team answer, split by whether the answer was right
def conf_bins(values, edges=(0.0, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0001)):
    counts = [0] * (len(edges) - 1)
    for v in values:
        for b in range(len(edges) - 1):
            if edges[b] <= v < edges[b + 1]:
                counts[b] += 1
                break
    labels = ["under 0.5"] + [f"{edges[b]:.1f} to {min(edges[b + 1], 1.0):.1f}" for b in range(1, len(edges) - 1)]
    return labels, counts

right_vals = [float(v) for v in right_conf.dropna()]
wrong_vals = [float(v) for v in wrong_conf.dropna()]
labels, right_counts = conf_bins(right_vals)
_, wrong_counts = conf_bins(wrong_vals)
rows_html = (
    f"<div style='display:flex;gap:12px;font-size:12px;font-weight:700;color:{NAVY_600};margin-bottom:6px'>"
    f"<div style='width:90px'>confidence</div><div style='flex:1;text-align:right'>right answers</div>"
    f"<div style='width:72px'></div><div style='flex:1'>wrong answers</div></div>"
)
for label, rc, wc in zip(labels, right_counts, wrong_counts):
    if rc == 0 and wc == 0:
        continue  # skip empty bands
    r_share = rc / len(right_vals) if right_vals else 0
    w_share = wc / len(wrong_vals) if wrong_vals else 0
    rows_html += (
        f"<div style='display:flex;align-items:center;gap:12px;margin:4px 0;font-size:13px;color:{GRAY_TEXT}'>"
        f"<div style='width:90px'>{esc(label)}</div>"
        f"<div style='flex:1;display:flex;justify-content:flex-end'>"
        f"<div style='height:12px;background:{NAVY};width:{100 * r_share:.1f}%'></div></div>"
        f"<div style='width:72px;text-align:center;white-space:nowrap'>{rc} | {wc}</div>"
        f"<div style='flex:1'><div style='height:12px;background:{LAVA};width:{100 * w_share:.1f}%'></div></div></div>"
    )
confidence = section(
    "How sure ai_decide said it was",
    f"The confidence field on each team answer, as a share of its {len(right_vals)} right answers "
    f"and {len(wrong_vals)} wrong ones. Counts in the middle; empty bands left out.",
    rows_html,
)

body = routing + descriptions + timing + escalation + confidence
if conf_right_med is not None and conf_wrong_med is not None:
    body += callout(f"Typical confidence: {conf_right_med:.2f} on right answers, {conf_wrong_med:.2f} on wrong ones.")

displayHTML(panel(
    f"ai_decide, ai_classify and Llama 3.3 70B on {total} support tickets",
    "The same Stark Industries tickets went through each function. Each section draws one validation "
    "condition, and the last shows the confidence ai_decide reported on its team answers.",
    body,
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

print("RESULTS_JSON " + json.dumps({
    "ticket_count": total,
    "gate.ai_decide_resolves": gate_results["ai_decide_resolves"],
    "gate.ai_classify_resolves": gate_results["ai_classify_resolves"],
    "classify.accuracy_pct": round(classify_accuracy, 1),
    "classify.batch_sec_median": round(classify_median, 1),
    "classify.batch_repeat_count": 3,
    "decide.accuracy_pct": round(decide_accuracy, 1),
    "decide.batch_sec_median": round(decide_single_median, 1),
    "decide.batch_repeat_count": 3,
    "decide.escalation_recall": round(recall, 3),
    "decide.escalation_precision": round(precision, 3),
    "agree.both_wrong": both_wrong,
    "agree.only_classify_right": only_classify_right,
    "agree.only_decide_right": only_decide_right,
    "round1.classify_correct": r1_correct["ai_classify"],
    "round1.decide_correct": r1_correct["ai_decide"],
    "round1.query_correct": r1_correct["ai_query"],
    "query.model": QUERY_MODEL,
    "query.accuracy_pct": round(query_accuracy, 1),
    "query.batch_sec_median": round(query_median, 1),
    "query.batch_repeat_count": 3,
    "query.unparsed_rows": query_unparsed,
    "decide.escalation_tp": int(tp),
    "decide.escalation_fp": int(fp),
    "decide.escalation_fn": int(fn),
    "decide.three_q_sec_median": round(decide_three_median, 1),
    "decide.error_message_rows": int(decide_errors),
    "decide.confidence_right_median": round(conf_right_med, 3) if conf_right_med else None,
    "decide.confidence_wrong_median": round(conf_wrong_med, 3) if conf_wrong_med else None,
    "verdict.accuracy_gap_pp": round(acc_gap, 1),
    "verdict.speed_faster": speed_faster,
    "verdict.escalation_recall_ok": bool(esc_recall_ok),
    "verdict.escalation_precision_ok": bool(esc_precision_ok),
    "verdict.conditions_met": conditions_met,
    "verdict.label": verdict_label,
}, default=lambda o: o.item() if hasattr(o, "item") else str(o)))  # numpy scalars to plain JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC One line per team did more for accuracy than the choice of function. With bare team names, the ai_classify function routed 151 of 200 tickets to the right team, the ai_decide function 147 and Llama 3.3 70B 167. With the descriptions in place those became 181, 175 and 176, so all three ended up within 6 tickets of each other.
# MAGIC
# MAGIC None of the three validation conditions held on round 2. On accuracy, 175 against 181 is a gap of 3.0 points (6 tickets), outside the 2-point condition. Its single-question batch typically took 154.2 seconds against 60.4 for the ai_classify function, and asking three questions per ticket took 194.3. Llama 3.3 70B's typical batch took 38.2 seconds, but its timed runs were 3.8, 38.2 and 71.2 seconds, so read that one loosely. This is one run on shared Free Edition compute, on one routing task, and the ai_decide function is in Beta.
# MAGIC
# MAGIC At a 0.5 threshold, the escalation question caught 23 of the 29 tickets labelled for escalation (79.3% recall) and raised 8 false alarms (74.2% precision), short of the 90% and 80% you'd want before trusting it to page someone.
# MAGIC
# MAGIC What the ai_decide function is good for today: one call answered team, escalation and urgency together, with no error rows across 200 tickets, and you get a confidence score on every answer. Its typical confidence was 0.95 on right answers and 0.90 on wrong ones, so a lower score is a fair reason to send a ticket to a person. And if your routing is off, write down what each team owns before you swap functions.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

if KEEP_TABLES:
    print(f"Kept {CATALOG}.{SCHEMA}; drop it with DROP SCHEMA {CATALOG}.{SCHEMA} CASCADE when you're done")
else:
    spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
    print(f"Dropped {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [ai_decide function reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_decide): question types and what comes back for each.
# MAGIC - [ai_classify function reference](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_classify): label formats and confidence scores.
# MAGIC - [AI Functions overview](https://docs.databricks.com/aws/en/large-language-models/ai-functions.html): every built-in AI Function on Databricks.
# MAGIC - [Unity Catalog row filters](https://docs.databricks.com/aws/en/data-governance/unity-catalog/row-and-column-filters.html): limit which rows a query sees by who runs it.
# MAGIC - [system.ai_gateway.usage](https://docs.databricks.com/aws/en/ai-gateway/usage-tracking): usage records for AI Functions calls.
