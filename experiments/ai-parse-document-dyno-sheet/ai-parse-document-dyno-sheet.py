# Databricks notebook source

# MAGIC %md
# MAGIC In a recent release note about Databricks' [`ai_parse_document` function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document):
# MAGIC
# MAGIC > "ai_parse_document now extracts chart values as structured JSON and describes their series, legends, and axes."
# MAGIC
# MAGIC Found "chart values as structured JSON" interesting, so I put together an experiment.
# MAGIC
# MAGIC I drive a Challenger Hellcat, and the chart I read more than any other is a dyno sheet: two pulls laid over each other, power and torque sharing one plot on two axes, an air/fuel trace underneath, and the peak numbers printed in the legend. That's a much busier chart than a bar chart of quarterly totals, so it's a good test of the release note's claim. The notebook draws a dyno sheet from numbers it knows, hands the image to the `ai_parse_document` function three times, and scores what comes back against the numbers it drew. A second section runs the same scoring on any dyno sheet you bring.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition) serverless compute in about 10 minutes, and you don't install anything. The `ai_parse_document` function is generally available, so there's no preview to turn on, and `matplotlib`, `numpy` and `pandas` already ship with the serverless environment. It doesn't reach the internet either: the dyno sheet is drawn inside the notebook.
# MAGIC
# MAGIC The sheet belongs to a made-up Stark Industries test car, and every number on it comes from the curves defined in the next cells. That's what makes it scoreable. The notebook knows the true horsepower at every engine speed, so it can measure how far each value the function returns sits from the truth.

# COMMAND ----------

import json
import re
import statistics
from typing import Optional

import matplotlib.image as mpimg
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# workspace is the writable catalog on Free Edition. The schema and volume are
# created below and dropped by the cleanup cell at the end.
CATALOG = "workspace"
SCHEMA = "dyno_sheet_parse"
VOLUME = "sheets"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
SHEET_FILE = f"{VOLUME_PATH}/stark-test-car-dyno.png"

REPEATS = 3          # separate calls on the same image, to see whether the answer moves
CURSOR_RPM = 4715    # where the sheet's cursor line sits, like the readout on a real dyno sheet

# The same options for every call: output schema 2.0, with figure descriptions on.
PARSE_OPTIONS = "map('version', '2.0', 'descriptionElementTypes', '*')"

# Colours from the Databricks brand palette: Blue 600 and Lava 600 for the two pulls,
# their 400 shades for the torque lines, Navy for text, Gray Lines for the grid.
BLUE, BLUE_LIGHT = "#2272B4", "#8ACAFF"
LAVA, LAVA_LIGHT = "#FF3621", "#FF9E94"
NAVY, GRAY_TEXT, GRAY_LINES, OAT_LIGHT = "#1B3139", "#5A6F77", "#DCE0E2", "#F9F7F4"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Writing to {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Draw a dyno sheet with known numbers
# MAGIC
# MAGIC A chassis dyno reports torque and horsepower at the wheels across a sweep of engine speeds in revolutions per minute (RPM), and horsepower is torque times RPM divided by 5,252. So I defined a torque curve for each of two pulls (a baseline and a second pull with a few percent more torque up top, the way a cold-air intake run usually looks), added a little ripple so the lines wobble like real dyno traces, and computed horsepower from them.
# MAGIC
# MAGIC The layout copies what a real sheet does, because that's the hard part for a reader:
# MAGIC - Power on the left axis and torque on the right, both from 100 to 700, so the two families of lines cross.
# MAGIC - One colour per pull, with torque drawn thinner and lighter than power in the same colour.
# MAGIC - One legend entry per pull carrying its printed peaks, `Max Power = ... Max Torque = ...`.
# MAGIC - A cursor line at 4,715 RPM with the four values labelled where it crosses each line.
# MAGIC - A smaller air/fuel ratio panel underneath on the same RPM axis.

# COMMAND ----------

RPM = np.arange(2000, 6201, 25)

def torque_curve(points: list[tuple[int, float]], ripple_phase: float) -> np.ndarray:
    """Interpolate a torque curve through control points, smooth it, and add a small dyno ripple."""
    xs, ys = zip(*points)
    base = np.interp(RPM, xs, ys)
    smooth = np.convolve(np.pad(base, 6, mode="edge"), np.ones(13) / 13, mode="valid")
    return smooth + 2.5 * np.sin(RPM / 97.0 + ripple_phase)

TORQUE = {
    "baseline": torque_curve([(2000, 452), (2500, 515), (3000, 560), (3500, 590), (4000, 603),
                              (4500, 610), (5000, 598), (5500, 584), (6000, 572), (6200, 561)], 0.0),
    "intake":   torque_curve([(2000, 460), (2500, 521), (3000, 568), (3500, 600), (4000, 614),
                              (4500, 622), (5000, 612), (5500, 598), (6000, 588), (6200, 578)], 1.3),
}
POWER = {run: TORQUE[run] * RPM / 5252.0 for run in TORQUE}
AIR_FUEL = {
    "baseline": np.where(RPM < 2600, 13.9 - (RPM - 2000) / 600 * 0.9, 13.0) + 0.06 * np.sin(RPM / 61.0),
    "intake":   np.where(RPM < 2600, 13.8 - (RPM - 2000) / 600 * 0.9, 12.9) + 0.06 * np.sin(RPM / 53.0 + 0.7),
}

# The truth the scoring reads: four main-panel curves and two air/fuel curves.
TRUTH_MAIN = {
    "baseline power": POWER["baseline"], "baseline torque": TORQUE["baseline"],
    "intake power": POWER["intake"], "intake torque": TORQUE["intake"],
}
TRUTH_AF = {"baseline air/fuel": AIR_FUEL["baseline"], "intake air/fuel": AIR_FUEL["intake"]}

def at(curve: np.ndarray, rpm: float) -> float:
    """A curve's value at any RPM inside the sweep."""
    return float(np.interp(rpm, RPM, curve))

PEAKS = {run: {"power": round(float(POWER[run].max()), 2), "torque": round(float(TORQUE[run].max()), 2)}
         for run in TORQUE}
CURSOR = {f"{run} {kind}": round(at(TRUTH_MAIN[f"{run} {kind}"], CURSOR_RPM), 2)
          for run in TORQUE for kind in ("power", "torque")}

# Every number printed on the sheet, as it's printed: four peaks in the legend, four cursor labels.
PRINTED = {f"{run} max {kind}": PEAKS[run][kind] for run in PEAKS for kind in ("power", "torque")}
PRINTED.update({f"{name} at cursor": value for name, value in CURSOR.items()})
print(f"{len(PRINTED)} numbers printed on the sheet:")
for name, value in PRINTED.items():
    print(f"  {name:<26} {value:.2f}")

# COMMAND ----------

def draw_sheet(path: str) -> plt.Figure:
    """Draw the dyno sheet to a PNG at path and return the figure."""
    fig = plt.figure(figsize=(11.5, 7.2), dpi=110)
    grid = fig.add_gridspec(2, 1, height_ratios=[3.2, 1], hspace=0.08, left=0.08, right=0.92, top=0.9, bottom=0.2)
    ax = fig.add_subplot(grid[0])
    ax_t = ax.twinx()
    ax_af = fig.add_subplot(grid[1], sharex=ax)
    x = RPM / 1000
    styles = {"baseline": (BLUE, BLUE_LIGHT), "intake": (LAVA, LAVA_LIGHT)}
    for run, (dark, light) in styles.items():
        label = f"stark_test_car_{run}_001.drf  Max Power = {PEAKS[run]['power']:.2f}  Max Torque = {PEAKS[run]['torque']:.2f}"
        ax.plot(x, POWER[run], color=dark, linewidth=1.6, label=label)
        ax_t.plot(x, TORQUE[run], color=light, linewidth=1.0)
        ax_af.plot(x, AIR_FUEL[run], color=dark, linewidth=1.2)
    for axis in (ax, ax_t):
        axis.set_ylim(100, 700)
    ax.set_ylabel("Power (hp)", color=NAVY)
    ax_t.set_ylabel("Torque (ft-lbs)", color=NAVY)
    ax_af.set_ylim(10, 20)
    ax_af.set_ylabel("Air/Fuel", color=NAVY)
    ax_af.set_xlabel("Engine Speed (RPM x1000)", color=NAVY)
    ax.set_xlim(1.95, 6.25)
    for axis in (ax, ax_af):
        axis.grid(True, color=GRAY_LINES, linewidth=0.6)
        axis.axvline(CURSOR_RPM / 1000, color=NAVY, linewidth=0.9)
    for run, (dark, light) in styles.items():
        hp, tq = CURSOR[f"{run} power"], CURSOR[f"{run} torque"]
        ax.plot([CURSOR_RPM / 1000], [hp], "o", color=dark, markersize=4)
        ax_t.plot([CURSOR_RPM / 1000], [tq], "o", color=dark, markersize=4)
        ax.annotate(f"{hp:.2f}", (CURSOR_RPM / 1000, hp), xytext=(-42, 4), textcoords="offset points", fontsize=7, color=NAVY)
        ax_t.annotate(f"{tq:.2f}", (CURSOR_RPM / 1000, tq), xytext=(6, 4), textcoords="offset points", fontsize=7, color=NAVY)
    ax_af.text(CURSOR_RPM / 1000 + 0.03, 11, f"X = {CURSOR_RPM / 1000:.3f}", fontsize=7, color=NAVY)
    ax.legend(loc="lower right", fontsize=7, frameon=False)
    plt.setp(ax.get_xticklabels(), visible=False)
    fig.suptitle("DYNO SHEET  |  Stark Industries test car  |  SAE  Smoothing: 5", fontsize=10, color=NAVY)
    footer = "\n".join(
        f"stark_test_car_{run}_001.drf   Run Type: RO   5th Gear, sport mode   "
        f"Max Power = {PEAKS[run]['power']:.2f}   Max Torque = {PEAKS[run]['torque']:.2f}"
        for run in styles)
    fig.text(0.08, 0.05, footer, fontsize=8, color=GRAY_TEXT, va="bottom")
    fig.savefig(path, facecolor="white")
    return fig

fig = draw_sheet(SHEET_FILE)
print(f"The dyno sheet I drew, saved to {SHEET_FILE}")
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the docs say
# MAGIC
# MAGIC The [`ai_parse_document` function page](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document) says "Charts are returned as figure elements" and, under chart data in figure content, "When chart data can be read, their content string contains JSON with series values, legend labels, and, where available, axis information." Each element also carries a confidence field, "A confidence score from 0 to 1, indicating how reliably the content field was extracted from the element."
# MAGIC
# MAGIC So the notebook reads the figure element's content as JSON and checks it against the drawn curves.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parse the sheet, three times
# MAGIC
# MAGIC [`read_files`](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files) with `format => 'binaryFile'` hands the PNG's bytes to the function, which returns a [`VARIANT`](https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type). I call it three separate times on the same image, because one answer can't tell you whether the next one would match it.
# MAGIC
# MAGIC The scoring reads each chart the function returns and does five things:
# MAGIC - Counts the panels and the series in each one. The main panel has four drawn curves, two power and two torque.
# MAGIC - Matches every returned series to the drawn curve it sits closest to, then measures how far each value is from the truth at that RPM, in horsepower or pound-feet.
# MAGIC - Checks whether the two pulls come back as different lines, since they're different on the sheet.
# MAGIC - Flags a series whose values rise in perfectly even steps. A dyno trace never does that, so it marks values filled in along a straight line rather than read off the curve.
# MAGIC - Looks for each of the eight printed numbers, written exactly as printed, anywhere in the output.

# COMMAND ----------

def parse_once(path: str) -> dict:
    """Run ai_parse_document on one file and return its output as a Python dict."""
    row = spark.sql(f"""
        SELECT ai_parse_document(content, {PARSE_OPTIONS}) AS parsed
        FROM read_files('{path}', format => 'binaryFile')
    """).first()
    return json.loads(row["parsed"].toJson())


def chart_panels(parsed: dict) -> tuple[dict, list]:
    """Return ({panel name: {series name: {x: y}}}, figure confidences) from every chart in the output."""
    panels, confidences = {}, []
    for el in (parsed.get("document") or {}).get("elements") or []:
        if el.get("type") != "figure":
            continue
        try:
            chart = json.loads(el.get("content") or "")
        except (TypeError, ValueError):
            continue
        if not isinstance(chart, dict):
            continue
        confidences.append(el.get("confidence"))
        found = chart.get("panels") if isinstance(chart.get("panels"), dict) else {chart.get("title") or "chart": chart}
        for name, panel in found.items():
            values = (panel or {}).get("values") or {}
            if values and all(not isinstance(v, dict) for v in values.values()):
                values = {name: values}
            panels[name] = {s: pts for s, pts in values.items() if isinstance(pts, dict)}
    return panels, confidences


def to_rpm(key: str) -> Optional[float]:
    """Turn an x label such as '4.75' (thousands) or '4750' into RPM."""
    m = re.search(r"\d+(\.\d+)?", str(key))
    if not m:
        return None
    x = float(m.group(0))
    return x * 1000 if x < 100 else x


def evenly_stepped(values: list[float]) -> bool:
    """True when a series of three or more values rises or falls by exactly the same amount every step."""
    steps = [round(b - a, 6) for a, b in zip(values, values[1:])]
    return len(steps) >= 2 and len(set(steps)) == 1


def score_series(series: dict, truths: dict) -> list[dict]:
    """Match each returned series to its closest drawn curve and measure the error at every point."""
    rows = []
    for name, points in series.items():
        pts = [(to_rpm(k), float(v)) for k, v in points.items()
               if to_rpm(k) is not None and isinstance(v, (int, float)) and RPM[0] <= to_rpm(k) <= RPM[-1]]
        if not pts:
            rows.append({"series": name, "points": 0, "matched": None})
            continue
        errors = {t: [abs(y - at(curve, x)) for x, y in pts] for t, curve in truths.items()}
        best = min(errors, key=lambda t: statistics.mean(errors[t]))
        rows.append({"series": name, "points": len(pts), "matched": best,
                     "typical_error": round(statistics.median(errors[best]), 1),
                     "worst_error": round(max(errors[best]), 1),
                     "even_steps": evenly_stepped([y for _, y in sorted(pts)])})
    return rows


def score(parsed: dict, truth_main: dict, truth_af: Optional[dict], printed: dict) -> dict:
    """Score one parse of a sheet against the drawn truth (or, for a reader's sheet, only its printed numbers)."""
    panels, confidences = chart_panels(parsed)
    text = json.dumps(parsed)
    main = af = {}
    for series in panels.values():
        flat = [float(v) for pts in series.values() for v in pts.values() if isinstance(v, (int, float))]
        if flat and statistics.median(flat) > 50:
            main = {**main, **series}
        elif flat:
            af = {**af, **series}
    main_rows = score_series(main, truth_main) if truth_main else []
    af_rows = score_series(af, truth_af) if truth_af else []
    distinct_lines = len({json.dumps(pts, sort_keys=True) for pts in main.values()})
    found = {name: f"{value:.2f}" in text for name, value in printed.items()}
    return {
        "panels": len(panels),
        "main_series": len(main),
        "main_distinct_lines": distinct_lines,
        "af_series": len(af),
        "curves_matched": len({r["matched"] for r in main_rows if r.get("matched")}),
        "main_rows": main_rows,
        "af_rows": af_rows,
        "even_step_series": sum(1 for r in main_rows + af_rows if r.get("even_steps")),
        "printed_found": found,
        "figure_confidence": confidences,
        "errors": parsed.get("error_status"),
        "chart_json": json.dumps(panels, sort_keys=True),
    }


drawn_runs = [score(parse_once(SHEET_FILE), TRUTH_MAIN, TRUTH_AF, PRINTED) for _ in range(REPEATS)]

for i, s in enumerate(drawn_runs, 1):
    print(f"Call {i}: {s['panels']} panels, {s['main_series']} series on the power and torque panel "
          f"({s['main_distinct_lines']} distinct), {s['curves_matched']} of 4 drawn curves matched, "
          f"{s['even_step_series']} evenly stepped series, "
          f"{sum(s['printed_found'].values())} of {len(PRINTED)} printed numbers found, errors: {s['errors']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Every returned series against the curve it matched
# MAGIC
# MAGIC One row per series per call. `typical error` is the middle of the per-point misses for that series and `worst error` is the largest, both in the series' own units (horsepower, pound-feet, or air/fuel ratio). `even steps` is true when every step between neighbouring values is identical.

# COMMAND ----------

series_table = pd.DataFrame([
    {"call": i, "panel": panel, "returned series": r["series"], "matched drawn curve": r.get("matched"),
     "points": r["points"], "typical error": r.get("typical_error"), "worst error": r.get("worst_error"),
     "even steps": r.get("even_steps")}
    for i, s in enumerate(drawn_runs, 1)
    for panel, rows in (("power and torque", s["main_rows"]), ("air/fuel", s["af_rows"]))
    for r in rows
])
print("Every series the function returned for the drawn sheet, scored against the curves I drew")
display(series_table)

printed_table = pd.DataFrame([
    {"printed number": name, "as printed": f"{value:.2f}",
     **{f"call {i}": "found" if s["printed_found"][name] else "missing" for i, s in enumerate(drawn_runs, 1)}}
    for name, value in PRINTED.items()
])
print("The eight numbers printed on the sheet, and whether each call returned them exactly")
display(printed_table)

first_chart = json.loads(drawn_runs[0]["chart_json"])
print("The chart JSON from call 1, as the function returned it")
print(json.dumps(first_chart, indent=1)[:6000])

# COMMAND ----------

# DBTITLE 1,The drawn sheet next to the curves the function returned
# hide-code
def side_by_side(image_path: str, panels: dict, truths: Optional[dict], title: str) -> plt.Figure:
    """The sheet on the left; the returned series re-plotted on the right, with the drawn curves dashed when known."""
    fig, (left, right) = plt.subplots(1, 2, figsize=(15, 5.2), dpi=100, gridspec_kw={"width_ratios": [1.15, 1]})
    left.imshow(mpimg.imread(image_path))
    left.axis("off")
    left.set_title("The sheet", color=NAVY, fontsize=11)
    if truths:
        for name, curve in truths.items():
            right.plot(RPM, curve, color=GRAY_LINES, linewidth=2.2, linestyle="--")
    shades = [BLUE, LAVA, BLUE_LIGHT, LAVA_LIGHT, NAVY, GRAY_TEXT]
    n = 0
    for panel in panels.values():
        for name, pts in panel.items():
            xy = sorted((to_rpm(k), float(v)) for k, v in pts.items()
                        if to_rpm(k) is not None and isinstance(v, (int, float)) and float(v) > 50)
            if xy:
                right.plot(*zip(*xy), marker="o", markersize=3, linewidth=1.3, color=shades[n % len(shades)],
                           label=(name[:48] + "...") if len(name) > 48 else name)
                n += 1
    right.set_xlabel("Engine speed (RPM)", color=NAVY)
    right.set_ylabel("Power or torque, as returned", color=NAVY)
    right.grid(True, color=GRAY_LINES, linewidth=0.6)
    right.legend(fontsize=7, frameon=False, loc="lower right")
    right.set_title(title, color=NAVY, fontsize=11)
    fig.tight_layout()
    return fig

print("The sheet I drew, next to the curves ai_parse_document returned on call 1")
fig = side_by_side(SHEET_FILE, first_chart, TRUTH_MAIN, "Returned on call 1 (dashed: what I drew)")
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bring your own dyno sheet
# MAGIC
# MAGIC A drawn sheet is clean by construction. A real one has a shop logo, a scanned or screenshotted image, and type that's a little soft. So this section takes any dyno sheet image you put in a [Unity Catalog volume](https://docs.databricks.com/aws/en/volumes/) and runs the same three calls on it.
# MAGIC
# MAGIC A real sheet comes with no curve truth, only what's printed on it, so the scoring here uses the numbers you type in from the sheet (its peaks and any cursor readouts). It also reports the series count, whether the pulls come back as different lines, and whether any series rises in perfectly even steps.
# MAGIC
# MAGIC The one I used is a dyno sheet for a 2015 Challenger Hellcat that [aFe Power](https://afepower.com/) measured on a [Dynojet](https://dynojet.com/) dyno, a baseline pull against a pull with their 54-72204 intake, posted in the [Hellcat dyno sheets thread on hellcat.org](https://www.hellcat.org/threads/hellcat-dyno-sheets.6067/). The forum sits behind a browser check, so the notebook doesn't download it: save the image, upload it to the volume the setup cell created, and set the two [widgets](https://docs.databricks.com/aws/en/notebooks/widgets) at the top of the notebook. For that sheet, `printed_values` is `665.78, 678.57, 612.44, 623.03, 541.09, 557.50, 602.68, 620.97`.
# MAGIC
# MAGIC With `sheet_path` left empty, this section skips.

# COMMAND ----------

dbutils.widgets.text("sheet_path", "")
dbutils.widgets.text("printed_values", "")
SHEET_PATH = dbutils.widgets.get("sheet_path").strip()
PRINTED_INPUT = [v.strip() for v in dbutils.widgets.get("printed_values").split(",") if v.strip()]

byo_runs = []
if SHEET_PATH:
    byo_printed = {f"printed {v}": float(v) for v in PRINTED_INPUT}
    byo_runs = [score(parse_once(SHEET_PATH), {}, None, byo_printed) for _ in range(REPEATS)]
    for i, s in enumerate(byo_runs, 1):
        print(f"Call {i}: {s['panels']} panels, {s['main_series']} series on the main panel "
              f"({s['main_distinct_lines']} distinct), "
              f"{sum(s['printed_found'].values())} of {len(byo_printed)} printed numbers found, errors: {s['errors']}")
    for i, s in enumerate(byo_runs, 1):
        steps = sum(1 for pts in json.loads(s["chart_json"]).values() for series in pts.values()
                    if evenly_stepped([float(v) for _, v in sorted((to_rpm(k), v) for k, v in series.items()
                                       if to_rpm(k) is not None and isinstance(v, (int, float)))]))
        s["even_step_series"] = steps
        print(f"Call {i}: {steps} series rise or fall in perfectly even steps")
    print("The chart JSON from call 1 on your sheet, as the function returned it")
    print(json.dumps(json.loads(byo_runs[0]["chart_json"]), indent=1)[:6000])
else:
    print("No sheet_path set, so the bring-your-own section skipped.")

# COMMAND ----------

# DBTITLE 1,Your sheet next to the curves the function returned
# hide-code
if byo_runs:
    print("Your sheet, next to the curves ai_parse_document returned on call 1")
    fig = side_by_side(SHEET_PATH, json.loads(byo_runs[0]["chart_json"]), None, "Returned on call 1")
    display(fig)
    plt.close(fig)
else:
    print("No sheet_path set, so there's nothing to draw here.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The run at a glance

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
import html

OAT, WHITE, NAVY_600 = "#EEEDE9", "#FFFFFF", "#1B5162"
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


def row(name: str, count=None, note: str = "", rule: str = "", body: str = "") -> str:
    """One group: a hairline, its name, a note, a count, a measured rule, then its body."""
    note_html = f"<span style='font-size:12px;color:{GRAY_TEXT}'>{esc(note)}</span>" if note else ""
    count_html = f"<span style='margin-left:auto;font-size:30px;font-weight:700;color:{NAVY}'>{esc(count)}</span>" if count is not None else ""
    return (f"<div style='padding:22px 0 18px;border-top:1px solid {GRAY_LINES}'>"
            f"<div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap'>"
            f"<span style='font-size:22px;font-weight:700;color:{NAVY}'>{esc(name)}</span>{note_html}{count_html}</div>"
            f"{rule}{body}</div>")


def callout(text: str) -> str:
    """The one lava-edged line the reader should not miss."""
    return (f"<div style='padding:14px 18px;margin-bottom:22px;background:{OAT};border-left:4px solid {LAVA};"
            f"font-size:15px;color:{NAVY}'>{text}</div>")


def panel(headline: str, dek: str, body: str) -> str:
    """The whole display: oat ground, navy top edge, DM Sans, a headline, a dek, then the body."""
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 30px;font-family:{SANS};"
            f"border-top:6px solid {NAVY}'>"
            f"<div style='font-size:40px;font-weight:700;line-height:1.1;max-width:760px'>{esc(headline)}</div>"
            f"<div style='font-size:17px;color:{GRAY_TEXT};line-height:1.45;max-width:680px;margin:12px 0 26px'>{dek}</div>"
            f"{body}</div>")


def curve_line(name: str) -> str:
    """One drawn curve: how many calls matched it, and its typical miss on each call that did."""
    # Count calls, not returned series: two series on one call can match the same drawn curve.
    hits = []
    for s in drawn_runs:
        call_hits = [r for r in s["main_rows"] + s["af_rows"] if r.get("matched") == name]
        if call_hits:
            hits.append(min(call_hits, key=lambda r: r["typical_error"]))
    misses = ", ".join(f"{r['typical_error']}" for r in hits) or "none"
    return (f"<div style='font-size:14px;padding:3px 0;color:{NAVY}'>{esc(name)}"
            f"<span style='color:{GRAY_TEXT}'> &middot; matched on {len(hits)} of {REPEATS} calls"
            f" &middot; typical miss per call: {esc(misses)}</span></div>")


matched = [s["curves_matched"] for s in drawn_runs]
found = [sum(s["printed_found"].values()) for s in drawn_runs]
stepped = [s["even_step_series"] for s in drawn_runs]
identical = len({s["chart_json"] for s in drawn_runs}) == 1

body = stats(
    stat(min(matched), "drawn power and torque curves matched, fewest on any call", out_of=4),
    stat(min(found), "printed numbers returned exactly, fewest on any call", out_of=len(PRINTED)),
    stat(max(stepped), "returned series rising in perfectly even steps, most on any call"),
    stat("yes" if identical else "no", f"all {REPEATS} calls returned the same chart JSON"),
)
if max(stepped):
    body = callout(f"On at least one call, {max(stepped)} returned series rose in perfectly even steps, "
                   "which no dyno trace does.") + body
body += row("Power and torque panel", count=f"{min(matched)}/4",
            rule=measured_rule(min(matched), 4, pop=min(matched) < 4),
            body="".join(curve_line(n) for n in TRUTH_MAIN))
body += row("Air/fuel panel", count=f"{min(s['af_series'] for s in drawn_runs)}",
            note="series returned, fewest on any call",
            body="".join(curve_line(n) for n in TRUTH_AF))
if byo_runs:
    byo_found = [sum(s["printed_found"].values()) for s in byo_runs]
    body += row("Your sheet", count=f"{min(byo_found)}/{len(PRINTED_INPUT)}",
                note="printed numbers returned exactly, fewest on any call",
                body=(f"<div style='font-size:14px;color:{NAVY}'>"
                      f"series per call: {esc([s['main_series'] for s in byo_runs])} &middot; "
                      f"distinct lines per call: {esc([s['main_distinct_lines'] for s in byo_runs])} &middot; "
                      f"evenly stepped series per call: {esc([s['even_step_series'] for s in byo_runs])}</div>"))

displayHTML(panel("How the function read a dyno sheet",
                  f"Each drawn curve, how many of the {REPEATS} calls found it, and how far the returned values "
                  "sat from the numbers I drew (horsepower, pound-feet, or air/fuel ratio).", body))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

def summary(runs: list[dict]) -> dict:
    """The per-call numbers a finding can cite, one list entry per call."""
    return {
        "panels_found": [s["panels"] for s in runs],
        "series_returned_main": [s["main_series"] for s in runs],
        "distinct_lines_main": [s["main_distinct_lines"] for s in runs],
        "series_returned_af": [s["af_series"] for s in runs],
        "even_step_series": [s["even_step_series"] for s in runs],
        "printed_values_found": [sum(s["printed_found"].values()) for s in runs],
        "printed_values_missing": [[k for k, v in s["printed_found"].items() if not v] for s in runs],
        "figure_confidence": [s["figure_confidence"] for s in runs],
        "parse_errors": [s["errors"] for s in runs],
        "distinct_outputs": len({s["chart_json"] for s in runs}),
    }


results = {
    "repeats": REPEATS,
    "drawn": {
        **summary(drawn_runs),
        "series_drawn_main": len(TRUTH_MAIN),
        "series_drawn_af": len(TRUTH_AF),
        "printed_values_total": len(PRINTED),
        "printed_values": {k: round(v, 2) for k, v in PRINTED.items()},
        "curves_matched": [s["curves_matched"] for s in drawn_runs],
        "series_scores": [[{k: r.get(k) for k in ("series", "matched", "points", "typical_error", "worst_error", "even_steps")}
                           for r in s["main_rows"] + s["af_rows"]] for s in drawn_runs],
    },
    "byo": ({"ran": True, "sheet": SHEET_PATH.rsplit("/", 1)[-1], "printed_values_total": len(PRINTED_INPUT),
             **summary(byo_runs), "chart_json_call_1": json.loads(byo_runs[0]["chart_json"])}
            if byo_runs else {"ran": False}),
}
print("RESULTS_JSON " + json.dumps(results, default=str, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC On the sheet I drew, the `ai_parse_document` function found both panels on all three calls and returned the two power curves close to the numbers I drew: a typical miss of 1.5 to 4.8 horsepower per point, and 15.4 horsepower at worst. The air/fuel trace came back within 0.1 of the drawn ratio. Neither torque curve came back on any call, so the twin-axis panel gave back two of its four lines. Six of the eight printed numbers came back exactly, and the two missing were the torque readouts at the cursor.
# MAGIC
# MAGIC The real sheet from hellcat.org is where it got interesting. Every call returned the four printed peaks exactly, the numbers in the legend and the footnotes, and no call returned the four readouts printed at the cursor. The curves changed from call to call. Call 1 returned two lines shaped like the torque traces, with the max torque from each legend placed on the line as a point (612.44 at 4,500 RPM on the baseline pull). Calls 2 and 3 returned both pulls as one line, moving in perfectly even steps. Call 1's air/fuel panel slid from 14.0 down to 12.0 with the cursor values, 13.14 and 12.92, dropped in at 4,750 RPM. The chart's confidence score sat at about 0.95 on every call, including the ones that disagreed.
# MAGIC
# MAGIC So take the printed numbers from the JSON, and treat the curves as a sketch you check against the image. If a value read off a curve matters, parse the chart more than once and compare: on this sheet, three calls gave three different answers, and the confidence score didn't tell them apart.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Drops the schema, which removes the volume and every image in it, including a sheet you uploaded there. Copy anything you want to keep first.

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")
print(f"Dropped {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [ai_parse_document function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_parse_document): the output schema, the figure element and the chart JSON it carries.
# MAGIC - [Transform unstructured data using AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions): where the `ai_parse_document` function sits among the other AI Functions.
# MAGIC - [Databricks' ai_prep_search function](https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_prep_search): the next step when parsed documents feed search.
# MAGIC - [read_files table-valued function](https://docs.databricks.com/aws/en/sql/language-manual/functions/read_files): the `binaryFile` format used to read the image.
# MAGIC - [Databricks widgets](https://docs.databricks.com/aws/en/notebooks/widgets): how the bring-your-own section takes its inputs.
