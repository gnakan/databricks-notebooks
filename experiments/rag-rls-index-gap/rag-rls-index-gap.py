# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Row-Level Security for a RAG Agent: What Unity Catalog Enforces, and What You Have to Build
# MAGIC
# MAGIC > "Two colleagues ask the same chatbot the same question. Should they get the same answer, or different ones because they are allowed to see different things?"
# MAGIC
# MAGIC That question comes from a [Databricks Community article on row-level security for a retrieval-augmented generation (RAG) agent](https://community.databricks.com/t5/community-articles/row-level-security-for-a-rag-agent-what-unity-catalog-enforces/m-p/170272#M1619). The article's point: [Unity Catalog row filters and column masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/) give every reader their own view of a table, but an AI Search index "has no row filters and no column masks. Filtering an index is a parameter you pass in the query, from your own application code."
# MAGIC
# MAGIC Found this interesting, so I put together an experiment. One thing changes the setup before it starts: the docs say you can't [create an AI Search index](https://docs.databricks.com/aws/en/ai-search/create-ai-search) from a table that already has a row filter or column mask. So in practice the index gets built from an unfiltered copy, and the governance has to come back somewhere else.
# MAGIC
# MAGIC The notebook builds a Stark Industries account-briefs table with a row filter and a column mask, plus a staging copy without them that feeds an [AI Search](https://docs.databricks.com/aws/en/ai-search/ai-search) index. Then it asks the same question three ways. Path A is plain SQL against the governed table, Path B searches the index with no filter, and Path C runs the same search with a filter the application works out from who's asking.
# MAGIC
# MAGIC **Platform:** Databricks Free Edition, serverless notebook compute. About 15 minutes, most of it the index sync.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Before you run it: two account groups
# MAGIC
# MAGIC The row filter checks group membership with [`is_account_group_member`](https://docs.databricks.com/aws/en/sql/language-manual/functions/is_account_group_member), the function the Unity Catalog docs use for this. It only sees **account** groups. A group created inside a workspace is a workspace-local group, and `is_account_group_member` returns false for it even when `is_member` returns true. (I hit exactly that on my first try.)
# MAGIC
# MAGIC So, before running:
# MAGIC
# MAGIC 1. Create two account groups, `si-alpha` and `si-bravo`, in the account console under **User management > Groups**. You need to be an account admin.
# MAGIC 2. Add yourself to `si-alpha`.
# MAGIC 3. Give it a few minutes. On my Free Edition workspace, a new membership took about four minutes to show up in `is_account_group_member`.
# MAGIC
# MAGIC The gate cell below checks this and stops if it isn't true yet.

# COMMAND ----------

# DBTITLE 1,Install the AI Search client
%pip install --quiet databricks-vectorsearch
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import time
import json
from databricks.vector_search.client import VectorSearchClient

CATALOG        = "workspace"
SCHEMA         = "rag_rls_demo"
TABLE_NAME     = f"{CATALOG}.{SCHEMA}.si_account_briefs"
STAGING_TABLE  = f"{CATALOG}.{SCHEMA}.si_account_briefs_staging"
FILTER_FN      = f"{CATALOG}.{SCHEMA}.si_region_filter"
MASK_FN        = f"{CATALOG}.{SCHEMA}.si_clearance_mask"

ENDPOINT_NAME   = "rag-rls-demo-ep"
INDEX_NAME      = f"{CATALOG}.{SCHEMA}.si_briefs_index"
BLOCKED_INDEX   = f"{CATALOG}.{SCHEMA}.si_briefs_index_on_governed_table"
EMBEDDING_MODEL = "databricks-bge-large-en"

GROUPS = {"ALPHA": "si-alpha", "BRAVO": "si-bravo"}

CURRENT_USER = spark.sql("SELECT current_user()").first()[0]
print(f"Current user: {CURRENT_USER}")
print(f"Source table: {TABLE_NAME}")
print(f"Staging copy: {STAGING_TABLE}")
print(f"Index:        {INDEX_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gate: groups, endpoint and embedding model
# MAGIC
# MAGIC Three checks before anything is built: you're in exactly one of the two account groups, an AI Search endpoint is online, and the embedding model answers.

# COMMAND ----------

# DBTITLE 1,Gate: account group membership
membership = {
    region: spark.sql(f"SELECT is_account_group_member('{group}')").first()[0]
    for region, group in GROUPS.items()
}
print(f"is_account_group_member: {membership}")

in_groups = [region for region, is_in in membership.items() if is_in]
if len(in_groups) != 1:
    print("[gate groups] exactly one of si-alpha / si-bravo -> FAIL")
    raise RuntimeError(
        f"Expected membership in exactly one of {list(GROUPS.values())}, found {in_groups}. "
        "Create both as account groups, add yourself to one, and wait a few minutes."
    )
CALLER_REGION = in_groups[0]
print(f"[gate groups] exactly one of si-alpha / si-bravo -> OK ({GROUPS[CALLER_REGION]})")

# COMMAND ----------

# DBTITLE 1,Gate: endpoint and embedding model
vsc = VectorSearchClient(disable_notice=True)

try:
    ep = vsc.get_endpoint(ENDPOINT_NAME)
except Exception:
    print(f"Creating endpoint '{ENDPOINT_NAME}' (3 to 5 minutes the first time)...")
    vsc.create_endpoint(name=ENDPOINT_NAME, endpoint_type="STANDARD")
    ep = vsc.get_endpoint(ENDPOINT_NAME)

for _ in range(60):
    state = vsc.get_endpoint(ENDPOINT_NAME).get("endpoint_status", {}).get("state", "")
    if state == "ONLINE":
        break
    time.sleep(10)
print(f"[gate endpoint] {ENDPOINT_NAME} online -> {'OK' if state == 'ONLINE' else 'FAIL'} ({state})")
assert state == "ONLINE", f"Endpoint state is {state}"

embed_ok = spark.sql(f"SELECT ai_query('{EMBEDDING_MODEL}', 'gate check') IS NOT NULL").first()[0]
print(f"[gate embedding] {EMBEDDING_MODEL} answers -> {'OK' if embed_ok else 'FAIL'}")
assert embed_ok

# COMMAND ----------

# MAGIC %md
# MAGIC ## The source table
# MAGIC
# MAGIC Ten Stark Industries account briefs: five in the ALPHA region, five in BRAVO. `summary` is the text that gets embedded, `region` is what the row filter keys on, and `clearance_level` (`RESTRICTED` or `OPEN`) is what the column mask hides. Change Data Feed is on here as well as on the staging copy, so the indexing attempt later can only fail because of the filter and mask.

# COMMAND ----------

# DBTITLE 1,Schema and source table
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

rows = [
    (1,  "ALPHA", "Stark Avionics",          "Develops next-generation propulsion systems for the Stark Defense division.",   "RESTRICTED"),
    (2,  "ALPHA", "Stark Energy Logistics",  "Manages distribution contracts for the arc-reactor field-deployment program.",  "OPEN"),
    (3,  "ALPHA", "Stark Ventures",          "Internal investment arm for philanthropic technology initiatives.",             "RESTRICTED"),
    (4,  "ALPHA", "Stark Medtech",           "Clinical AI diagnostic tools built on the patient-outcomes data warehouse.",     "OPEN"),
    (5,  "ALPHA", "Stark Robotics Lab",      "Prototype exo-frame manufacturing and materials-science research pipeline.",    "RESTRICTED"),
    (6,  "BRAVO", "Stark Defense Integration","Acquired defense contractor; systems integration in progress.",                "RESTRICTED"),
    (7,  "BRAVO", "Stark Retail Ops",        "Supply-chain and point-of-sale data for the consumer product line.",            "OPEN"),
    (8,  "BRAVO", "Stark Materials Research","Joint research initiative on sustainable materials processing.",                 "RESTRICTED"),
    (9,  "BRAVO", "Stark Legal Holdings",    "Intellectual property portfolio and patent-defense account tracking.",          "OPEN"),
    (10, "BRAVO", "Stark Global Facilities", "Facilities management and energy-cost optimization across 14 sites.",           "OPEN"),
]
(spark.createDataFrame(rows, "id BIGINT, region STRING, account_name STRING, summary STRING, clearance_level STRING")
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE_NAME))
# Change Data Feed on the governed table too, so that when the index attempt below
# fails, the row filter and mask are the only reason it can fail.
spark.sql(f"ALTER TABLE {TABLE_NAME} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(f"Wrote {len(rows)} rows to {TABLE_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The staging copy for AI Search
# MAGIC
# MAGIC The copy is taken before the filter and mask go on, so it holds all ten rows with their real `clearance_level`. [Change Data Feed](https://docs.databricks.com/aws/en/delta/delta-change-data-feed) is turned on because a Delta Sync index needs it. This copy is the table the index reads, and nothing in Unity Catalog filters it per user.

# COMMAND ----------

# DBTITLE 1,Staging copy without the filter or mask
spark.sql(f"""
CREATE OR REPLACE TABLE {STAGING_TABLE}
TBLPROPERTIES (delta.enableChangeDataFeed = true)
AS SELECT id, region, account_name, summary, clearance_level FROM {TABLE_NAME}
""")
staging_count = spark.sql(f"SELECT count(*) FROM {STAGING_TABLE}").first()[0]
print(f"Staging copy: {staging_count} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The row filter and the column mask
# MAGIC
# MAGIC The row filter lets a row through when the reader is in the account group for its region: `si-alpha` sees ALPHA, `si-bravo` sees BRAVO. The column mask shows the real `clearance_level` only to members of `si-admins`, a group nobody in this run belongs to, so every reader here sees `OPEN`.

# COMMAND ----------

# DBTITLE 1,Row filter and column mask
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FILTER_FN}(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member(CONCAT('si-', LOWER(region)))
""")
spark.sql(f"""
CREATE OR REPLACE FUNCTION {MASK_FN}(clearance_level STRING)
RETURNS STRING
RETURN CASE WHEN is_account_group_member('si-admins') THEN clearance_level ELSE 'OPEN' END
""")
spark.sql(f"ALTER TABLE {TABLE_NAME} SET ROW FILTER {FILTER_FN} ON (region)")
spark.sql(f"ALTER TABLE {TABLE_NAME} ALTER COLUMN clearance_level SET MASK {MASK_FN}")

for region, group in GROUPS.items():
    spark.sql(f"GRANT SELECT ON TABLE {TABLE_NAME} TO `{group}`")
print(f"Row filter and column mask set on {TABLE_NAME}; SELECT granted to {list(GROUPS.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Indexing the governed table directly
# MAGIC
# MAGIC The docs say this is blocked. The cell tries it anyway and records what comes back.

# COMMAND ----------

# DBTITLE 1,Try to index the governed table
try:
    vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        index_name=BLOCKED_INDEX,
        source_table_name=TABLE_NAME,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="summary",
        embedding_model_endpoint_name=EMBEDDING_MODEL,
    )
    governed_index_error = None
    print("Index creation on the governed table did not raise an error.")
except Exception as ex:
    governed_index_error = str(ex)
    print(f"Error: {governed_index_error[:400]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path A: SQL against the governed table
# MAGIC
# MAGIC A plain `SELECT` as you. Unity Catalog applies the row filter and the column mask before anything comes back.

# COMMAND ----------

# DBTITLE 1,Path A: SQL on the governed table
path_a = spark.sql(f"SELECT id, region, account_name, clearance_level FROM {TABLE_NAME} ORDER BY id").collect()
path_a_count = len(path_a)
path_a_regions = sorted({r.region for r in path_a})
path_a_restricted = sum(1 for r in path_a if r.clearance_level == "RESTRICTED")

print(f"Path A rows: {path_a_count}   regions: {path_a_regions}   RESTRICTED shown: {path_a_restricted}")
for r in path_a:
    print(f"  [{r.id:2d}] {r.account_name:<26s} region={r.region}  clearance={r.clearance_level}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The index on the staging copy
# MAGIC
# MAGIC A [Delta Sync index](https://docs.databricks.com/aws/en/ai-search/create-ai-search) on the staging copy. It embeds `summary` and keeps `region` and `clearance_level` as columns, so a query can filter on them. The sync is `TRIGGERED`, and the cell waits for it to finish.

# COMMAND ----------

# DBTITLE 1,Create and sync the index
try:
    vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
    print(f"Index '{INDEX_NAME}' already exists.")
except Exception:
    print(f"Creating '{INDEX_NAME}' (first sync takes a few minutes)...")
    vsc.create_delta_sync_index(
        endpoint_name=ENDPOINT_NAME,
        index_name=INDEX_NAME,
        source_table_name=STAGING_TABLE,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="summary",
        embedding_model_endpoint_name=EMBEDDING_MODEL,
        columns_to_sync=["id", "region", "account_name", "summary", "clearance_level"],
    )

# Wait until every staging row is in the index, not just until the index is online:
# an online index can still be part-way through its first sync.
index = vsc.get_index(ENDPOINT_NAME, INDEX_NAME)
for attempt in range(80):
    status = index.describe().get("status", {})
    detailed, indexed = status.get("detailed_state", "UNKNOWN"), status.get("indexed_row_count", 0)
    if detailed == "ONLINE_NO_PENDING_UPDATE" and indexed == staging_count:
        break
    time.sleep(15)
synced = detailed == "ONLINE_NO_PENDING_UPDATE" and indexed == staging_count
print(f"[gate index] {INDEX_NAME} holds all {staging_count} rows -> {'OK' if synced else 'FAIL'} ({detailed}, {indexed} indexed)")
assert synced, f"{detailed}, {indexed} of {staging_count} rows indexed"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path B: similarity search with no filter
# MAGIC
# MAGIC The question a RAG agent would send, top five matches, no filter. This is what the agent retrieves if the application passes nothing about the caller.

# COMMAND ----------

# DBTITLE 1,Path B: unfiltered similarity search
QUERY_TEXT = "Which divisions work on defense technology and proprietary research?"
NUM_RESULTS = 5
COLUMNS = ["id", "region", "account_name", "summary", "clearance_level"]

def search(filters=None):
    """Run the similarity search and return the result rows as dicts."""
    res = index.similarity_search(query_text=QUERY_TEXT, columns=COLUMNS, num_results=NUM_RESULTS, filters=filters)
    return [dict(zip(COLUMNS, r)) for r in res.get("result", {}).get("data_array", [])]

path_b = search()
path_b_by_region = {reg: sum(1 for r in path_b if r["region"] == reg) for reg in GROUPS}
path_b_restricted = sum(1 for r in path_b if r["clearance_level"] == "RESTRICTED")
path_b_other_region = sum(n for reg, n in path_b_by_region.items() if reg != CALLER_REGION)

print(f"Query: {QUERY_TEXT}")
print(f"Path B rows: {len(path_b)}   by region: {path_b_by_region}   RESTRICTED shown: {path_b_restricted}")
for r in path_b:
    print(f"  [{int(r['id']):2d}] {r['account_name']:<26s} region={r['region']}  clearance={r['clearance_level']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Path C: similarity search with a filter from the caller's identity
# MAGIC
# MAGIC The application works out the caller's region from the same account groups the row filter checks, then passes it as a [query filter](https://docs.databricks.com/aws/en/ai-search/query-ai-search). Unity Catalog doesn't add this filter for you; it's a parameter the application owns. If the caller's region can't be worked out, the cell stops rather than searching unfiltered, because an unfiltered search is Path B.

# COMMAND ----------

# DBTITLE 1,Path C: identity-derived filter
caller_regions = [reg for reg, grp in GROUPS.items()
                  if spark.sql(f"SELECT is_account_group_member('{grp}')").first()[0]]
if len(caller_regions) != 1:
    raise RuntimeError(f"Can't resolve one region for {CURRENT_USER}: {caller_regions}. Not searching unfiltered.")

path_c_filter = {"region": caller_regions}
path_c = search(filters=path_c_filter)
path_c_by_region = {reg: sum(1 for r in path_c if r["region"] == reg) for reg in GROUPS}
path_c_restricted = sum(1 for r in path_c if r["clearance_level"] == "RESTRICTED")

print(f"Filter: {path_c_filter}")
print(f"Path C rows: {len(path_c)}   by region: {path_c_by_region}   RESTRICTED shown: {path_c_restricted}")
for r in path_c:
    print(f"  [{int(r['id']):2d}] {r['account_name']:<26s} region={r['region']}  clearance={r['clearance_level']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step four: who can query the index at all
# MAGIC
# MAGIC Paths B and C are about what comes back from the index. This step is about who can search it in the first place. The [query docs](https://docs.databricks.com/aws/en/ai-search/query-ai-search) say a user who doesn't own the index needs `USE CATALOG`, `USE SCHEMA` and `SELECT` on it, the same privileges as a table.
# MAGIC
# MAGIC You own the index right now, so the cell hands the index and its schema to the other region's group, the one you're not in. It searches with no grant, then grants `SELECT` to your group and searches again, then revokes it and searches a third time. At the end it hands ownership back to you, which works on Free Edition because workspace admins own the `workspace` catalog.

# COMMAND ----------

# DBTITLE 1,Step four: SELECT on the index
OTHER_GROUP, MY_GROUP = GROUPS[[r for r in GROUPS if r != CALLER_REGION][0]], GROUPS[CALLER_REGION]

def try_search():
    """Search the index with no filter; return (ok, rows or error text)."""
    try:
        return True, search()
    except Exception as ex:
        return False, str(ex)

def show(label, outcome):
    ok, detail = outcome
    if ok:
        print(f"{label}: allowed, {len(detail)} rows, regions {sorted({r['region'] for r in detail})}")
    else:
        print(f"{label}: denied: {detail[:220]}")

as_owner = try_search()
show("1. As the index owner", as_owner)

spark.sql(f"ALTER TABLE {INDEX_NAME} OWNER TO `{OTHER_GROUP}`")
spark.sql(f"ALTER SCHEMA {CATALOG}.{SCHEMA} OWNER TO `{OTHER_GROUP}`")
time.sleep(20)
no_grant = try_search()
show(f"2. Index owned by {OTHER_GROUP}, no grant to {MY_GROUP}", no_grant)

spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `{MY_GROUP}`")
spark.sql(f"GRANT SELECT ON TABLE {INDEX_NAME} TO `{MY_GROUP}`")
time.sleep(20)
with_grant = try_search()
show(f"3. SELECT granted to {MY_GROUP}", with_grant)

spark.sql(f"REVOKE SELECT ON TABLE {INDEX_NAME} FROM `{MY_GROUP}`")
time.sleep(20)
after_revoke = try_search()
show(f"4. SELECT revoked", after_revoke)

spark.sql(f"ALTER SCHEMA {CATALOG}.{SCHEMA} OWNER TO `{CURRENT_USER}`")
spark.sql(f"ALTER TABLE {INDEX_NAME} OWNER TO `{CURRENT_USER}`")
print(f"Ownership handed back to {CURRENT_USER}")

with_grant_other = sum(1 for r in with_grant[1] if r["region"] != CALLER_REGION) if with_grant[0] else None
index_access = {
    "owner_allowed": as_owner[0],
    "no_grant_allowed": no_grant[0],
    "with_grant_allowed": with_grant[0],
    "with_grant_other_region_rows": with_grant_other,
    "after_revoke_allowed": after_revoke[0],
}
print(index_access)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Side by side

# COMMAND ----------

# DBTITLE 1,Side-by-side comparison
print(f"Caller: {CURRENT_USER} in {GROUPS[CALLER_REGION]}")
print(f"{'Path':<6}{'How':<36}{'Rows':<6}{'ALPHA':<7}{'BRAVO':<7}{'RESTRICTED shown'}")
print("-" * 80)
print(f"{'A':<6}{'SQL on the governed table':<36}{path_a_count:<6}"
      f"{sum(1 for r in path_a if r.region == 'ALPHA'):<7}{sum(1 for r in path_a if r.region == 'BRAVO'):<7}{path_a_restricted}")
print(f"{'B':<6}{'AI Search, no filter':<36}{len(path_b):<6}{path_b_by_region['ALPHA']:<7}{path_b_by_region['BRAVO']:<7}{path_b_restricted}")
print(f"{'C':<6}{'AI Search, identity filter':<36}{len(path_c):<6}{path_c_by_region['ALPHA']:<7}{path_c_by_region['BRAVO']:<7}{path_c_restricted}")

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
import html

OAT_LIGHT, OAT, WHITE = "#F9F7F4", "#EEEDE9", "#FFFFFF"
NAVY, NAVY_600, GRAY_TEXT, GRAY_LINES = "#1B3139", "#1B5162", "#5A6F77", "#DCE0E2"
LAVA = "#FF3621"
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"
FONTS = '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'

def esc(v) -> str: return html.escape(str(v))

def stat(number, words, out_of=None):
    of = f"<span style='color:{GRAY_TEXT};font-size:18px'> / {esc(out_of)}</span>" if out_of is not None else ""
    return (f"<div style='min-width:150px'><div style='font-size:30px;font-weight:700;color:{NAVY}'>"
            f"{esc(number)}{of}</div>"
            f"<div style='font-size:13px;color:{GRAY_TEXT};line-height:1.35'>{esc(words)}</div></div>")

def stats(*figures):
    return f"<div style='display:flex;gap:34px;flex-wrap:wrap;padding-bottom:26px'>{''.join(figures)}</div>"

def row_block(name, tag="", tag_pop=False, body=""):
    tag_html = f"<span style='font-size:12px;font-weight:700;color:{LAVA if tag_pop else NAVY_600}'>{esc(tag)}</span>" if tag else ""
    return (f"<div style='padding:22px 0 18px;border-top:1px solid {GRAY_LINES}'>"
            f"<div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap'>"
            f"<span style='font-size:22px;font-weight:700;color:{NAVY}'>{esc(name)}</span>"
            f"{tag_html}</div>{body}</div>")

def bar_row(label, count, total, pop=False):
    width = max(4, int(100 * count / total)) if total else 4
    color = LAVA if pop else NAVY
    return (f"<div style='display:flex;align-items:center;gap:14px;padding:7px 0'>"
            f"<span style='font-size:13px;color:{GRAY_TEXT};width:220px;flex-shrink:0'>{esc(label)}</span>"
            f"<div style='height:10px;background:{color};width:{width}%;min-width:4px'></div>"
            f"<span style='font-size:13px;font-weight:700;color:{NAVY}'>{esc(count)}</span></div>")

def panel(headline, dek, body):
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 30px;"
            f"font-family:{SANS};border-top:6px solid {NAVY}'>"
            f"<div style='font-size:36px;font-weight:700;line-height:1.1;max-width:760px'>{esc(headline)}</div>"
            f"<div style='font-size:16px;color:{GRAY_TEXT};line-height:1.45;max-width:680px;margin:12px 0 26px'>{esc(dek)}</div>"
            f"{body}</div>")

other = [reg for reg in GROUPS if reg != CALLER_REGION][0]
path_a_other = sum(1 for r in path_a if r.region == other)
path_a_own = path_a_count - path_a_other
scale = max(path_a_count, len(path_b), len(path_c), 1)

body = stats(
    stat(path_a_other, f"{other} rows through SQL"),
    stat(path_b_other_region, f"{other} rows from the index, no filter"),
    stat(path_c_by_region[other], f"{other} rows from the index, group filter"),
    stat(path_c_restricted, "hidden clearance values still shown with the group filter"),
)
for name, own, oth, hidden in [
    ("Path A: SQL on the governed table", path_a_own, path_a_other, path_a_restricted),
    ("Path B: index, no filter", path_b_by_region[CALLER_REGION], path_b_other_region, path_b_restricted),
    ("Path C: index, group filter", path_c_by_region[CALLER_REGION], path_c_by_region[other], path_c_restricted),
]:
    body += row_block(name, body=bar_row(f"{CALLER_REGION} rows (yours)", own, scale) +
                                 bar_row(f"{other} rows (not yours)", oth, scale, pop=oth > 0) +
                                 bar_row("Hidden clearance values shown", hidden, scale, pop=hidden > 0))

word = lambda allowed: "allowed" if allowed else "denied"
access_line = (f"No grant: {word(index_access['no_grant_allowed'])}. "
               f"SELECT granted: {word(index_access['with_grant_allowed'])}"
               + (f", with {index_access['with_grant_other_region_rows']} {other} rows in the result" if index_access['with_grant_allowed'] else "")
               + f". Revoked: {word(index_access['after_revoke_allowed'])}.")
body += row_block("Step four: SELECT on the index", tag="UNITY CATALOG GRANT",
                  body=f"<div style='font-size:15px;color:{NAVY};padding-top:8px'>{esc(access_line)}</div>")

displayHTML(panel(
    "Where your governance reaches when your agent searches",
    f"One question, asked as a member of {GROUPS[CALLER_REGION]}: through SQL, through the index with no filter, and through the index with a filter from the caller's group. The filter covers the rows; the columns you sync are yours to decide.",
    body,
))

# COMMAND ----------

# DBTITLE 1,RESULTS_JSON
results = {
    "caller_region": CALLER_REGION,
    "gate": {"index_creation_on_filtered_table_error": governed_index_error[:300] if governed_index_error else None},
    "staging_rows": staging_count,
    "query_text": QUERY_TEXT,
    "num_results": NUM_RESULTS,
    "path_a": {"rows_returned": path_a_count, "other_region_rows": path_a_other, "restricted_shown": path_a_restricted},
    "path_b": {"rows_returned": len(path_b), "other_region_rows": path_b_other_region, "restricted_shown": path_b_restricted},
    "path_c": {"rows_returned": len(path_c), "other_region_rows": path_c_by_region[other], "restricted_shown": path_c_restricted},
    "index_access": index_access,
}
print("RESULTS_JSON " + json.dumps(results))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC Run as a member of `si-alpha`, plain SQL on the governed table came back with 5 rows from ALPHA and none from BRAVO. The column mask showed every clearance level as `OPEN`. The index was a different story, because it was built from the staging copy. Indexing the governed table directly failed with "cannot have both row/column security and online materialized views", which is the docs' limit showing up as an error.
# MAGIC
# MAGIC With no filter, the same question against the index returned 5 results and 3 of them were BRAVO rows, 4 with a `RESTRICTED` clearance. With a region filter worked out from your account group, BRAVO dropped to 0. The rows were fixed. The column wasn't: 3 of the 5 filtered results still showed `RESTRICTED`, a value SQL hides from you, because the index holds whatever columns you sync into it.
# MAGIC
# MAGIC Step four is the layer Unity Catalog does govern on the index. With no grant the search was denied, with `SELECT` it was allowed, and after the revoke it was denied again. With `SELECT` in place, the unfiltered search still returned 3 BRAVO rows. The grant decides whether you can search the index, not which rows come back.
# MAGIC
# MAGIC Run again as a member of `si-bravo`, the second colleague got their own answer. SQL returned 5 BRAVO rows and the filtered search returned no ALPHA results, while 2 `RESTRICTED` values still showed up in it.
# MAGIC
# MAGIC So governance has a few layers once a retrieval-augmented generation (RAG) agent is in the picture, and each one has a different owner:
# MAGIC
# MAGIC | Where the data lives | What governs it | Who owns it |
# MAGIC |---|---|---|
# MAGIC | Catalog, schema, table | Unity Catalog grants | Unity Catalog |
# MAGIC | Rows and columns of the table | Row filter and column mask | Unity Catalog |
# MAGIC | The index, as an object | `SELECT` on the index | Unity Catalog |
# MAGIC | Rows inside the index | The query filter, tied to the same account groups | Your application |
# MAGIC | Columns inside the index | What you choose to sync | You, when you build the index |
# MAGIC
# MAGIC The bottom two rows are design decisions your team makes today. If a column needs a mask in the table, the simplest move is to leave it out of the index, or sync a version of it that's already safe to show anyone who can search.
# MAGIC
# MAGIC One question, the top 5 results and two runs, with one user moved between two groups, so read the counts as what this run returned rather than a rate.

# COMMAND ----------

# MAGIC %md
# MAGIC ## The other colleague
# MAGIC
# MAGIC This workspace has one user, so the second colleague is the same person in the other group. To see what they get, move yourself from `si-alpha` to `si-bravo` in the account console, wait a few minutes for the membership to show up, and run the notebook again from the top. The gate cell picks up the new group.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops both indexes, the endpoint, the tables, the functions and the schema. The account groups stay: delete `si-alpha` and `si-bravo` in the account console when you're done with them.

# COMMAND ----------

# DBTITLE 1,Cleanup
for name in [INDEX_NAME, BLOCKED_INDEX]:
    try:
        vsc.delete_index(ENDPOINT_NAME, name)
        print(f"Deleted index {name}")
    except Exception as e:
        print(f"Index {name}: {str(e)[:120]}")

try:
    vsc.delete_endpoint(ENDPOINT_NAME)
    print(f"Deleted endpoint {ENDPOINT_NAME}")
except Exception as e:
    print(f"Endpoint {ENDPOINT_NAME}: {str(e)[:120]}")

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
print(f"Dropped schema {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Row filters and column masks](https://docs.databricks.com/aws/en/data-governance/unity-catalog/filters-and-masks/): filter and mask functions, `SET ROW FILTER`, and the limit on building an AI Search index from a filtered table.
# MAGIC - [is_account_group_member function](https://docs.databricks.com/aws/en/sql/language-manual/functions/is_account_group_member): the account-level group check the filter uses.
# MAGIC - [Create AI Search endpoints and indexes](https://docs.databricks.com/aws/en/ai-search/create-ai-search): Delta Sync indexes, sync modes and the columns an index keeps.
# MAGIC - [Query an AI Search index](https://docs.databricks.com/aws/en/ai-search/query-ai-search): the `filters` parameter Path C uses.
# MAGIC - [Attribute-based access control (ABAC) policies](https://docs.databricks.com/aws/en/data-governance/unity-catalog/abac/policies): tag-driven filters and masks that apply across many tables at once.
