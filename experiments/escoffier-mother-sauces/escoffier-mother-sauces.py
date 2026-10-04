# Databricks notebook source

# MAGIC %md
# MAGIC In a recent release note about Databricks' [`ai_extract` function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract):
# MAGIC
# MAGIC > "The standard (default) mode for ai_extract now uses a new model checkpoint that is 2.3x faster while improving overall quality"
# MAGIC
# MAGIC Found "improving overall quality" interesting, so I put together an experiment.
# MAGIC
# MAGIC The notebook downloads the two sauce chapters of Auguste Escoffier's *A Guide to Modern Cookery* (1907 English edition) as page scans, reads them with Databricks' [`ai_parse_document` function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document), and uses the `ai_extract` function to rebuild Escoffier's sauce family tree: which sauces he calls the leading sauces, and which leading sauce each of his small sauces is built on. Then it lines that tree up against the five mother sauces culinary schools teach today.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) serverless compute in about 15 minutes.
# MAGIC
# MAGIC **What you need:**
# MAGIC - Serverless compute. The `ai_parse_document` and `ai_extract` functions are both generally available, so there's no preview to turn on.
# MAGIC - Outbound HTTPS to `archive.org`, where the page scans live.
# MAGIC - `matplotlib` for the one chart. It ships with the serverless environment, so there's no install step.
# MAGIC
# MAGIC **The book.** *A Guide to Modern Cookery* by A. Escoffier, published by W. Heinemann in London in 1907, scanned by Cornell University Library and hosted on the [Internet Archive](https://archive.org/details/cu31924000610117). Published in 1907, it's in the public domain in the United States. The notebook reads printed pages 15 to 47: Chapter II, "The Leading Warm Sauces", and Chapter III, "The Small Compound Sauces".

# COMMAND ----------

import json
import re
import urllib.request
from collections import Counter

import matplotlib.pyplot as plt

# workspace is the writable catalog on Free Edition. The schema and volume are
# created below if they don't exist.
CATALOG = "workspace"
SCHEMA = "escoffier_sauces"
VOLUME = "pages"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
PARSED_TABLE = f"{CATALOG}.{SCHEMA}.parsed_pages"

ARCHIVE_ID = "cu31924000610117"
FIRST_PAGE, LAST_PAGE = 15, 47      # printed page numbers
CHAPTER_III_FIRST_PAGE = 24         # "The Small Compound Sauces" starts here
LEAF_OFFSET = 25                    # scan leaf = printed page + 25 in this scan
PAGE_WIDTH = 1400                   # pixels; wide enough for clean small type

EXTRACT_OPTIONS = ("map('version', '2.1', 'instructions', "
                   "'A sauce recipe from Auguste Escoffier, A Guide to Modern Cookery, 1907.')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Land the page scans in a volume
# MAGIC
# MAGIC The Internet Archive serves each scanned page as a JPEG. I needed them somewhere `READ_FILES` can see, so each one streams straight into a [Unity Catalog volume](https://docs.databricks.com/aws/en/volumes/), named by its printed page number so the pages sort in reading order.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")

for page in range(FIRST_PAGE, LAST_PAGE + 1):
    url = f"https://archive.org/download/{ARCHIVE_ID}/page/n{page + LEAF_OFFSET}_w{PAGE_WIDTH}.jpg"
    request = urllib.request.Request(url, headers={"User-Agent": "databricks-notebook"})
    with urllib.request.urlopen(request, timeout=120) as response:
        data = response.read()
    with open(f"{VOLUME_PATH}/p{page:03d}.jpg", "wb") as out:
        out.write(data)

pages_downloaded = len(dbutils.fs.ls(VOLUME_PATH))
print(f"{pages_downloaded} page scans in {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse every page once
# MAGIC
# MAGIC The [`ai_parse_document` function page](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document) lists JPG among the supported formats and says the function "identifies and extracts layout information from a document, like page numbers, headers, tables, and footers, and returns them as structured elements." I write the result to a table so the rest of the notebook reads the same parse.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {PARSED_TABLE} AS
SELECT
  path,
  ai_parse_document(content, map('version', '2.0')) AS parsed
FROM READ_FILES('{VOLUME_PATH}', format => 'binaryFile')
""")

elements = spark.sql(f"""
SELECT
  CAST(regexp_extract(path, 'p([0-9]+)\\\\.jpg', 1) AS INT) AS page,
  el.pos                                  AS pos,
  el.value:type::STRING                   AS type,
  el.value:content::STRING                AS content
FROM {PARSED_TABLE},
LATERAL variant_explode(parsed:document:elements) AS el
ORDER BY page, pos
""").collect()

parse_errors = spark.sql(f"""
SELECT count(*) AS n FROM {PARSED_TABLE}
WHERE parsed:error_status IS NOT NULL AND array_size(parsed:error_status::ARRAY<VARIANT>) > 0
""").first()["n"]

print(f"{len(elements)} elements across {pages_downloaded} pages; pages reporting errors: {parse_errors}")
print(Counter(e["type"] for e in elements).most_common())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stitch the elements back into recipes
# MAGIC
# MAGIC Escoffier numbers every recipe (number 36 is Devilled Sauce), and those headings come back as `section_header` elements. A recipe often runs onto the next page, so I walk the elements in page order and attach every text block to the last numbered heading before it.
# MAGIC
# MAGIC My first version closed a recipe at any heading without a number, and lost Béchamel (No. 28) entirely: the line right under its title, "Quantities Required for Four Quarts.", comes back as a `section_header` too. Headings like that end in a full stop, and chapter titles don't, so now a heading ending in a full stop stays inside the recipe as text and any other unnumbered heading closes it. Escoffier also numbers a few recipes with a letter ("26a"), which the pattern below allows.

# COMMAND ----------

HEADING = re.compile("^\\s*[\"\u201c]?(\\d{1,3}[a-z]?)\\s*[\u2014\u2013\\-=]+\\s*(.+?)\\s*$")

recipes, current = [], None
for e in elements:
    if e["type"] == "section_header":
        m = HEADING.match(e["content"] or "")
        content = (e["content"] or "").strip()
        if m:
            current = {"number": m.group(1), "name": m.group(2).strip(" .,\"\u201d\u201c"),
                       "page": e["page"], "text": []}
            recipes.append(current)
        elif content.endswith(".") and current is not None:
            current["text"].append(content)
        else:
            current = None
    elif e["type"] in ("text", "table", "list_item") and current is not None:
        current["text"].append(e["content"] or "")

for r in recipes:
    r["chapter"] = "II" if r["page"] < CHAPTER_III_FIRST_PAGE else "III"
    r["text"] = "\n".join(r["text"]).strip()

recipes_df = spark.createDataFrame(
    [(r["number"], r["name"], r["page"], r["chapter"], r["text"]) for r in recipes if r["text"]],
    "number STRING, name STRING, page INT, chapter STRING, text STRING",
)
recipes_df.createOrReplaceTempView("recipes")

print("Recipes stitched from the parse, by chapter")
display(spark.sql("SELECT chapter, count(*) AS recipes, min(number) AS first_no, max(number) AS last_no FROM recipes GROUP BY chapter ORDER BY chapter"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## First pass: which sauces does Escoffier call leading sauces?
# MAGIC
# MAGIC Chapter II opens: "Warm sauces are of two kinds: the leading sauces, also called 'mother sauces,' and the small sauces, which are usually derived from the first-named." It also holds the roux and a few variants. The [`ai_extract` function page](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract) says the schema "Supports string , integer , number , boolean , and enum types", so the first pass asks for one enum per Chapter II recipe: what role it plays.
# MAGIC
# MAGIC The first runs of this notebook didn't agree with each other on which recipes are leading sauces, so the pass runs five times. A recipe is a leading sauce when most of the five runs call it one, and the table records how many did.

# COMMAND ----------

ROLE_SCHEMA = json.dumps({
    "role": {
        "type": "enum",
        "labels": ["leading sauce", "roux or thickening", "variant of another sauce in this chapter", "stock or other preparation"],
        "description": "The role this preparation plays in Escoffier's chapter on the leading warm sauces",
    }
}).replace("'", "''")

ROLE_RUNS = 5

roles = spark.sql(f"""
SELECT page, number, name, extracted:response.role.value::STRING AS role
FROM (
  SELECT page, number, name, ai_extract(concat(name, '\\n', text), '{ROLE_SCHEMA}', {EXTRACT_OPTIONS}) AS extracted
  FROM recipes
  WHERE chapter = 'II'
)
ORDER BY page, number
""")

# Each collect() runs ai_extract again, so every run is an independent answer.
runs = [{r["name"]: r for r in roles.collect()} for _ in range(ROLE_RUNS)]
names = list(runs[0])
leading_votes = {n: sum(1 for run in runs if run[n]["role"] == "leading sauce") for n in names}
role_rows = [{"page": runs[0][n]["page"], "number": runs[0][n]["number"], "name": n,
              "roles": [run[n]["role"] for run in runs], "leading_votes": leading_votes[n]} for n in names]
leading = [n for n in names if leading_votes[n] * 2 > ROLE_RUNS]
split_calls = {n: v for n, v in leading_votes.items() if 0 < v < ROLE_RUNS}

print(f"Chapter II, the role each of {ROLE_RUNS} runs gave every recipe")
display(spark.createDataFrame(
    [(r["number"], r["name"], " / ".join(r["roles"]), r["leading_votes"]) for r in role_rows],
    f"number STRING, name STRING, roles STRING, leading_in_{ROLE_RUNS}_runs INT"))
print(f"Leading sauces (a majority of {ROLE_RUNS} runs): {leading}")
print(f"Calls the runs split on: {split_calls}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Second pass: what is each small sauce built on?
# MAGIC
# MAGIC Every Chapter III sauce gets asked the same question twice. The open version is a plain string field. The enum version lists the leading sauces from the first pass, plus "none of these", so the answer has to be one of Escoffier's own leading sauces. The [`ai_extract` function page](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract) says "Values that are not valid result in an error", so a bad enum value would show up as an error rather than as a made-up sauce.

# COMMAND ----------

BASE_DESCRIPTION = "The leading (mother) sauce this sauce is built on"
OPEN_SCHEMA = json.dumps({"base_sauce": {"type": "string", "description": BASE_DESCRIPTION}}).replace("'", "''")
ENUM_SCHEMA = json.dumps({"base_sauce": {"type": "enum", "labels": leading + ["none of these"],
                                         "description": BASE_DESCRIPTION}}).replace("'", "''")

parents = spark.sql(f"""
WITH small AS (SELECT page, number, name, concat(name, '\\n', text) AS recipe FROM recipes WHERE chapter = 'III')
SELECT page, number, name,
       ai_extract(recipe, '{OPEN_SCHEMA}', {EXTRACT_OPTIONS})                AS open_result,
       ai_extract(recipe, '{ENUM_SCHEMA}', {EXTRACT_OPTIONS})                AS enum_result
FROM small
""")
parents.createOrReplaceTempView("parents_raw")
spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW parents AS
SELECT page, number, name,
       open_result:response.base_sauce.value::STRING AS open_base,
       enum_result:response.base_sauce.value::STRING AS enum_base,
       coalesce(open_result:error_message::STRING, enum_result:error_message::STRING) AS error
FROM parents_raw
""")
parent_rows = spark.sql("SELECT * FROM parents ORDER BY page, number").collect()

print("Chapter III, the same question asked open and as an enum")
display(spark.sql("SELECT number, name, open_base, enum_base, error FROM parents ORDER BY page, number"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escoffier's family tree
# MAGIC
# MAGIC Children per leading sauce, from the enum pass.

# COMMAND ----------

# DBTITLE 1,Children per leading sauce
# hide-code
children = Counter(r["enum_base"] for r in parent_rows if r["enum_base"] and r["enum_base"] != "none of these")
children_by_mother = {name: children.get(name, 0) for name in leading}

print("Children per leading sauce, from the enum pass")
fig, ax = plt.subplots(figsize=(8, 4))
names = sorted(children_by_mother, key=children_by_mother.get, reverse=True)
ax.barh([n.title() for n in names], [children_by_mother[n] for n in names], color="#1B3139")  # Databricks Navy 800
ax.invert_yaxis()
ax.set_xlabel("Small sauces in Chapter III built on it")
ax.set_title("Escoffier, A Guide to Modern Cookery (1907)")
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Against the five taught today
# MAGIC
# MAGIC The Auguste Escoffier School of Culinary Arts teaches five mother sauces: "Béchamel Sauce, Velouté Sauce, Espagnole Sauce, Hollandaise Sauce, Sauce Tomate", and says they were "first classified by French Chef Marie-Antoine Carême and later codified by Auguste Escoffier" ([How to Make the 5 Mother Sauces](https://www.escoffier.edu/blog/recipes/how-to-make-the-five-mother-sauces/)). This cell matches each of the five to the leading sauces the first pass found, by name, and lists any leading sauce that matches none of them.

# COMMAND ----------

SCHOOL_FIVE = {
    "Béchamel": ["bechamel", "béchamel"],
    "Velouté": ["velout"],
    "Espagnole": ["espagnol"],
    "Hollandaise": ["hollandaise"],
    "Sauce Tomate": ["tomato", "tomate"],
}

def matches(name, keys):
    """True when any keyword for a school sauce appears in an Escoffier sauce name."""
    return any(k in name.lower() for k in keys)

school_five_in_leading = {s: [n for n in leading if matches(n, keys)] for s, keys in SCHOOL_FIVE.items()}
leading_outside_school_five = [n for n in leading if not any(matches(n, k) for k in SCHOOL_FIVE.values())]

for school, found in school_five_in_leading.items():
    print(f"{school:<13} -> {found if found else 'not among the leading sauces'}")
print(f"Leading sauces outside the five: {leading_outside_school_five}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The run at a glance
# MAGIC
# MAGIC Escoffier's sauce family drawn from this run's own variables. Each leading sauce gets a row, with a rule as long as the number of small sauces the enum pass put under it. A small sauce the enum couldn't place hangs under the sauce the open field named for it, when that sauce is on the page.

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
import html
import unicodedata

def label(name):
    """Title-case an all-caps heading from the scan and escape it for HTML."""
    caps = sum(c.isupper() for c in name or "") > sum(c.islower() for c in name or "")
    text = re.sub(r"\w+('\w+)?", lambda m: m.group(0).capitalize(), name.lower()) if caps else (name or "")
    return html.escape(text)

def key(name):
    """A loose matching key for a sauce name: no accents, case, or filler words."""
    plain = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    words = [w for w in re.findall(r"[a-z]+", plain) if w not in {"sauce", "ordinary", "the", "or", "a", "la", "l"}]
    return " ".join(words)

mother_no = {r["name"]: r["number"] for r in role_rows}
placed = {m: sorted((r for r in parent_rows if r["enum_base"] == m), key=lambda r: label(r["name"])) for m in leading}
unplaced = [r for r in parent_rows if not r["enum_base"] or r["enum_base"] == "none of these"]
school_of = {m: next((s for s, keys in SCHOOL_FIVE.items() if matches(m, keys)), None) for m in leading}

def find(answer, names):
    """The name the open-field answer refers to: an exact key match first, then a containment match."""
    k = key(answer)
    if not k:
        return None
    keyed = {key(n): n for n in names}
    return keyed.get(k) or next((n for nk, n in keyed.items() if len(k) > 4 and (k in nk or nk in k)), None)

# For each sauce the enum couldn't place, follow what the open field named: a leading sauce puts it
# directly under that sauce; a small sauce on the page hangs it one level down; anything else stays unplaced.
by_open, grandkids, still_unplaced = {}, {}, []
rest = []
for r in unplaced:
    mother = find(r["open_base"], leading)
    if mother:
        by_open.setdefault(mother, []).append(r)
    else:
        rest.append(r)
child_names = [r["name"] for rows in list(placed.values()) + list(by_open.values()) for r in rows]
for r in rest:
    child = find(r["open_base"], child_names)
    if child:
        grandkids.setdefault(child, []).append(r)
    else:
        still_unplaced.append(r)

# Databricks brand palette (Extended Brand Guidelines): navy and oat carry the page, lava is the one pop.
OAT_LIGHT, OAT, NAVY, NAVY_600, GRAY_TEXT, GRAY_LINES, LAVA = "#F9F7F4", "#EEEDE9", "#1B3139", "#1B5162", "#5A6F77", "#DCE0E2", "#FF3621"
GROUND, INK, DEK, LIT, GOLD, RED, RULE = OAT_LIGHT, NAVY, GRAY_TEXT, NAVY, NAVY_600, LAVA, GRAY_LINES
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"  # unquoted: it sits inside quoted style attributes
MONO = SANS + ";font-variant-numeric:tabular-nums;font-weight:700"
most = max([len(v) for v in placed.values()] + [1])

def child_item(r, via_open=False):
    """One small sauce, with any grandchildren the open field hung under it. via_open marks a sauce only the open field placed."""
    under = grandkids.get(r["name"], [])
    sub = (f"<div style='font-size:12px;color:{DEK};margin-top:2px;line-height:1.5'>"
           + "<br>".join(f"<span style='color:{RULE}'>&#9492;</span> {label(g['name'])}" for g in under) + "</div>") if under else ""
    name = (f"<span style='color:{DEK};font-style:italic'>{label(r['name'])}</span> <span style='color:{DEK};font-size:11px'>open field</span>"
            if via_open else f"<span style='color:{LIT}'>{label(r['name'])}</span>")
    return (f"<div style='break-inside:avoid;padding:3px 0;display:flex;gap:8px'>"
            f"<span style='font-family:{MONO};font-size:11px;color:{GOLD};flex:0 0 34px;padding-top:2px'>{html.escape(r['number'])}</span>"
            f"<div>{name}{sub}</div></div>")

def mother_row(m):
    """A leading sauce: number, name, how it maps to the five taught today, a rule as long as its child count, and its children."""
    n = len(placed[m])
    extra = (f"<span style='font-size:16px;color:{DEK};font-weight:400'> +{len(by_open[m])} open field</span>" if by_open.get(m) else "")
    taught = (f"<span style='color:{GOLD}'>taught today as {html.escape(school_of[m])}</span>" if school_of[m]
              else f"<span style='color:{RED}'>not one of the five taught today</span>")
    return f"""
    <div style='padding:22px 0 18px;border-top:1px solid {RULE}'>
      <div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap'>
        <span style='font-family:{MONO};font-size:12px;color:{GOLD}'>No. {html.escape(str(mother_no.get(m, '')))}</span>
        <span style='font-family:{SANS};font-size:26px;font-weight:700;color:{INK}'>{label(m)}</span>
        <span style='font-family:{MONO};font-size:12px'>{taught}</span>
        <span style='font-family:{SANS};font-size:12px;color:{DEK}'>leading in {leading_votes.get(m, 0)} of {ROLE_RUNS} runs</span>
        <span style='margin-left:auto;font-family:{SANS};font-size:30px;font-weight:700;color:{INK}'>{n}{extra}</span>
      </div>
      <div style='height:3px;background:{GOLD if school_of[m] else RED};width:{max(4, 100 * n / most):.0f}%;margin:10px 0 12px'></div>
      <div style='column-width:220px;column-gap:28px;font-family:{SANS};font-size:14px'>{''.join(child_item(r) for r in placed[m])}{''.join(child_item(r, True) for r in by_open.get(m, []))}</div>
    </div>"""

def stat(number, words):
    """A scoreboard number with its plain-language caption."""
    return (f"<div style='min-width:150px'><div style='font-family:{SANS};font-size:30px;font-weight:700;color:{INK}'>{number}</div>"
            f"<div style='font-family:{SANS};font-size:13px;color:{DEK};line-height:1.35'>{words}</div></div>")

total = len(parent_rows)
open_blank = sum(1 for r in parent_rows if not r["open_base"])
enum_none = len(unplaced)
hung = sum(len(v) for v in grandkids.values())

unplaced_list = "".join(
    f"<div style='break-inside:avoid;padding:3px 0;color:{LIT}'>{label(r['name'])}"
    + (f" <span style='color:{DEK}'>&larr; {html.escape(r['open_base'])}</span>" if r["open_base"] else f" <span style='color:{DEK}'>&larr; no parent named</span>")
    + "</div>"
    for r in still_unplaced
)

split_block = "".join(
    f"<div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap;padding:14px 18px;margin-bottom:22px;background:{OAT};border-left:4px solid {RED}'>"
    f"<span style='font-family:{MONO};font-size:12px;color:{GOLD}'>No. {html.escape(str(mother_no.get(n, '')))}</span>"
    f"<span style='font-size:20px;font-weight:700'>{label(n)}</span>"
    f"<span style='font-size:14px;color:{DEK}'>called a leading sauce in {v} of {ROLE_RUNS} runs, so it "
    f"{'counts as one here' if n in leading else 'is treated as a variant here'}</span></div>"
    for n, v in split_calls.items()
)

displayHTML(f"""
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">
<div style="background:{GROUND};color:{INK};padding:34px 38px 30px;font-family:{SANS};border-top:6px solid {NAVY}">
  <div style="font-size:40px;font-weight:700;line-height:1.1;max-width:760px">Escoffier's sauce family, rebuilt from the 1907 scans</div>
  <div style="font-size:17px;color:{DEK};line-height:1.45;max-width:680px;margin:12px 0 26px">
    <i>A Guide to Modern Cookery</i>, pages {FIRST_PAGE} to {LAST_PAGE}. Chapter II gave {len(leading)} leading sauces. Of the {total} small sauces in Chapter III, the enum placed {total - enum_none} under one, the open field placed {sum(len(v) for v in by_open.values())} more under a leading sauce and hung {hung} one level down, and {len(still_unplaced)} are still unplaced.
  </div>
  <div style="display:flex;gap:34px;flex-wrap:wrap;padding-bottom:26px">
    {stat(pages_downloaded, "page scans parsed, " + str(parse_errors) + " with errors")}
    {stat(f"{open_blank}<span style='color:{DEK};font-size:18px'> / {total}</span>", "parents the open field left blank")}
    {stat(f"{enum_none}<span style='color:{DEK};font-size:18px'> / {total}</span>", "small sauces the enum couldn't place under a leading sauce")}
  </div>
  {split_block}
  {''.join(mother_row(m) for m in sorted(leading, key=lambda m: len(placed[m]), reverse=True))}
  <div style="padding:22px 0 0;border-top:1px solid {RULE}">
    <div style="display:flex;align-items:baseline;gap:14px">
      <span style="font-family:{SANS};font-size:22px;font-weight:700">Still unplaced</span>
      <span style="font-family:{MONO};font-size:12px;color:{RED}">what the open field named isn't on these pages either</span>
      <span style="margin-left:auto;font-size:30px;font-weight:700">{len(still_unplaced)}</span>
    </div>
    <div style="height:3px;background:{RED};width:{max(4, 100 * len(still_unplaced) / max(most, len(still_unplaced))):.0f}%;margin:10px 0 12px"></div>
    <div style="column-width:260px;column-gap:28px;font-size:14px">{unplaced_list}</div>
  </div>
</div>
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

open_blank = sum(1 for r in parent_rows if not r["open_base"])
enum_none = sum(1 for r in parent_rows if not r["enum_base"] or r["enum_base"] == "none of these")

results = {
    "pages_parsed": pages_downloaded,
    "parse_error_pages": parse_errors,
    "elements": len(elements),
    "recipes_chapter_ii": sum(1 for r in recipes if r["chapter"] == "II" and r["text"]),
    "recipes_chapter_iii": len(parent_rows),
    "leading_sauces": leading,
    "leading_count": len(leading),
    "role_runs": ROLE_RUNS,
    "leading_votes": leading_votes,
    "split_calls": split_calls,
    "open_base_blank": open_blank,
    "enum_base_none": enum_none,
    "extract_errors": sum(1 for r in parent_rows if r["error"]),
    "open_base_top": Counter(r["open_base"] for r in parent_rows if r["open_base"]).most_common(8),
    "children_by_mother": children_by_mother,
    "school_five_in_leading": {s: bool(found) for s, found in school_five_in_leading.items()},
    "leading_outside_school_five": leading_outside_school_five,
}
print("RESULTS_JSON " + json.dumps(results, default=str, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC The `ai_parse_document` function read all 33 page scans with no page reporting an error, and stitching its 317 elements back together gave 13 Chapter II recipes and 92 small sauces in Chapter III. Béchamel is only in that count because of the fix in the stitching cell: the line under its title, "Quantities Required for Four Quarts.", comes back as a `section_header` too.
# MAGIC
# MAGIC Across five passes, the `ai_extract` function called Espagnole, Ordinary Velouté, Béchamel, Tomato and Hollandaise a leading sauce every time, so all five mother sauces taught today are on Escoffier's list. The passes split on two recipes, and both are children of the five: Half Glaze (3 of 5, so it counts as a leading sauce here) and Fish Velouté (2 of 5, so it doesn't). If you rerun the notebook, those are the two that can flip.
# MAGIC
# MAGIC Asking for each small sauce's parent two ways showed how deep the tree goes. The enum put 58 of the 92 under a leading sauce, Ordinary Velouté the most at 18, and couldn't place 34. The open field left 19 blank, and for a lot of the rest it named a parent that's a small sauce itself: Allemande Sauce 5 times, Normande Sauce 4. Escoffier's tree is three levels deep, and an enum of the leading sauces only has room for two. Neither version returned an error on any of the 92.
# MAGIC
# MAGIC Where the results point: for a book built as a family tree, ask for the immediate parent as an open field and walk it up to a leading sauce in a second step, the way the panel above hangs the open-field answers one level down.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops the schema, which removes the parsed-pages table and the volume holding the scans.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [ai_parse_document function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document): supported formats, the element types and the version 2.0 output.
# MAGIC - [ai_extract function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_extract): typed schemas, enums, citations and precision mode.
# MAGIC - [Transform unstructured data using AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions): every task-specific AI Function on one page.
# MAGIC - [read_files table-valued function](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files): the `binaryFile` format that feeds scans to the `ai_parse_document` function.
# MAGIC - [A Guide to Modern Cookery on the Internet Archive](https://archive.org/details/cu31924000610117): the rest of Escoffier's recipes, if you want to keep going.
