# Experiment: Genie Code — Bronze to Silver Pipeline

**What this is:** The setup notebook and Genie Code output from a structured practitioner experiment testing whether Genie Code's Bronze-to-Silver pipeline output meets a production bar.

The companion Medium journal covers the full methodology, findings, and what the results suggest for teams evaluating Genie Code today.

## Files

- `setup.py` — creates the Unity Catalog schema and volume, generates ~50,000 synthetic click event records with deliberate quality violations seeded in
- `generated/` — the unmodified Genie Code output: pipeline spec and transformation files

## Running the setup notebook

1. Import `setup.py` into your Databricks workspace (**Workspace → Import**)
2. Update the configuration block at the top of the notebook:

```python
CATALOG = "your_catalog"
SCHEMA  = "your_schema"
VOLUME  = "click_events_raw"   # rename to match your conventions
```

3. Run all cells — the notebook creates the schema and volume if they don't exist, generates the synthetic data, and verifies the output with a Spark read

## About the generated files

The files in `generated/` are the raw Genie Code output — unmodified. The companion article reviews these files against a production bar. The volume path in `bronze_click_events.py` is hardcoded to the experiment catalog and schema; update it to match your environment if you want to run the pipeline yourself.
