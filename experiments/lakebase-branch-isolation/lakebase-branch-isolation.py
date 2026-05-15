# Databricks notebook source
# MAGIC %md
# MAGIC In a recent Databricks blog post about Lakebase database branching:
# MAGIC
# MAGIC > "When you create a branch in Lakebase, you get a new, fully isolated Postgres environment that: Starts from the exact schema and data of its parent at a specific point in time; Shares the same underlying storage instead of duplicating it; Only writes new data when you actually make changes."
# MAGIC
# MAGIC Branching is the workflow I know from code. Applying it to a database is the same idea: same point-in-time fork, same isolated modifications, same convergence model. Found this interesting, so I put together an experiment to see the branching concept in action on [Lakebase](https://docs.databricks.com/aws/en/oltp/projects/).
# MAGIC
# MAGIC The notebook stands up a Lakebase project, seeds a real Postgres table on the parent, branches the database, modifies the data on the branch, and shows the parent unchanged. The artifact is the side-by-side view: same table, same query, two different results because the writes diverged.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC What you need before running this:
# MAGIC
# MAGIC - **A Databricks workspace with Lakebase available.** Lakebase is not available on Free Edition. The notebook was demonstrated on a Premium Azure workspace. On Free Edition the project-create call fails at the authorization layer; that failure is the architectural confirmation that the feature is Premium and Enterprise only.
# MAGIC - **Compute:** the default Serverless runtime. No cluster attach required.
# MAGIC - **Libraries:** `databricks-sdk` and [`psycopg[binary]`](https://www.psycopg.org/psycopg3/docs/). The next cell installs both and restarts the kernel so the Lakebase `postgres` namespace and the Postgres driver are reachable in the same session.
# MAGIC - **Identity:** the workspace user runs as their Databricks email; Lakebase maps that to a [Postgres role](https://docs.databricks.com/aws/en/oltp/projects/authentication) with the same identity. No PAT or external secret to manage.

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk "psycopg[binary]>=3.2" --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import time
import uuid

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.postgres import (
    Branch,
    BranchSpec,
    Duration,
    Endpoint,
    EndpointSpec,
    EndpointType,
    Project,
    ProjectSpec,
)

w = WorkspaceClient()

# Dynamic naming so reruns do not collide with prior probe resources on the
# same workspace. Lakebase project IDs accept kebab-case; the UUID4 suffix
# keeps the namespace unique across runs and across collaborators.
PROBE_ID = uuid.uuid4().hex[:8]
PROJECT_ID = f"tl-probe-{PROBE_ID}"
BRANCH_ID = "tl-probe-branch"

# Workspace user becomes the Postgres role automatically. No hardcoding.
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
current_user = ctx.userName().get()

print(f"Workspace user   : {current_user}")
print(f"Probe project id : {PROJECT_ID}")
print(f"Probe branch id  : {BRANCH_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the project (the parent database)
# MAGIC
# MAGIC [`w.postgres.create_project`](https://docs.databricks.com/aws/en/oltp/projects/manage-projects) returns a long-running operation. `.wait()` blocks until the project reaches a terminal state. A project ships with a [default branch](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) named `production` and a default read-write compute endpoint attached to that branch.

# COMMAND ----------

project_spec = ProjectSpec(
    display_name=f"TL Probe {PROBE_ID}",
    pg_version=17,
)
project = Project(spec=project_spec)

t_project_start = time.perf_counter()
created_project = w.postgres.create_project(project=project, project_id=PROJECT_ID).wait()
t_project_elapsed = time.perf_counter() - t_project_start

PROJECT_NAME = created_project.name           # projects/<PROJECT_ID>
DEFAULT_BRANCH_NAME = f"{PROJECT_NAME}/branches/production"

print(f"Project resource name : {PROJECT_NAME}")
print(f"Default branch        : {DEFAULT_BRANCH_NAME}")
print(f"Project create + wait : {t_project_elapsed:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the default endpoint on the production branch
# MAGIC
# MAGIC Endpoints are a separate sub-resource under a branch. The project creation provisions one read-write endpoint on the default branch, which is what makes the database immediately reachable. `list_endpoints(parent=branch_name)` returns the endpoints attached to that branch; the response carries the connection hostname at `endpoint.status.hosts.host`.

# COMMAND ----------

parent_endpoints = list(w.postgres.list_endpoints(parent=DEFAULT_BRANCH_NAME))
print(f"Endpoints on default branch : {len(parent_endpoints)}")

parent_endpoint = parent_endpoints[0]
PARENT_ENDPOINT_NAME = parent_endpoint.name
PARENT_HOST = parent_endpoint.status.hosts.host

print(f"Parent endpoint name  : {PARENT_ENDPOINT_NAME}")
print(f"Parent endpoint host  : {PARENT_HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Seed the parent with a real table
# MAGIC
# MAGIC The demonstration only lands if the parent has data to start with. [`generate_database_credential`](https://docs.databricks.com/aws/en/oltp/projects/authentication) returns a one-hour OAuth token that psycopg uses as the password. The user is the Databricks email; the database name is [`databricks_postgres`](https://docs.databricks.com/aws/en/oltp/projects/connection-strings); the SSL mode is required. I created a small `products` table on the parent and inserted three rows.

# COMMAND ----------

def connect(endpoint_name: str, host: str):
    """Return a psycopg connection to the given Lakebase endpoint."""
    cred = w.postgres.generate_database_credential(endpoint=endpoint_name)
    return psycopg.connect(
        host=host,
        port=5432,
        dbname="databricks_postgres",
        user=current_user,
        password=cred.token,
        sslmode="require",
    )

with connect(PARENT_ENDPOINT_NAME, PARENT_HOST) as parent_conn:
    with parent_conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS products")
        cur.execute("""
            CREATE TABLE products (
                id    INTEGER PRIMARY KEY,
                name  TEXT NOT NULL,
                price NUMERIC(10, 2) NOT NULL
            )
        """)
        cur.executemany(
            "INSERT INTO products (id, name, price) VALUES (%s, %s, %s)",
            [(1, "widget-a", 10.00), (2, "widget-b", 20.00), (3, "widget-c", 30.00)],
        )
        parent_conn.commit()

        cur.execute("SELECT id, name, price FROM products ORDER BY id")
        parent_state_before = cur.fetchall()

print("Parent state before branching:")
for row in parent_state_before:
    print(f"  {row}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Branch the database
# MAGIC
# MAGIC The git-branch moment. [`create_branch`](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) returns the branch resource immediately; [copy-on-write](https://docs.databricks.com/aws/en/oltp/projects/) storage means the new branch points at the parent's data at the current [LSN](https://www.postgresql.org/docs/current/datatype-pg-lsn.html) without copying it.

# COMMAND ----------

branch_spec = BranchSpec(
    source_branch=DEFAULT_BRANCH_NAME,
    ttl=Duration(seconds=86400),  # 1 day so a forgotten branch does not linger
)

t_branch_start = time.perf_counter()
created_branch = w.postgres.create_branch(
    parent=PROJECT_NAME,
    branch=Branch(spec=branch_spec),
    branch_id=BRANCH_ID,
).wait()
t_branch_elapsed = time.perf_counter() - t_branch_start

BRANCH_NAME = created_branch.name
print(f"Branch resource name : {BRANCH_NAME}")
print(f"Branch create + wait : {t_branch_elapsed:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find the branch's endpoint
# MAGIC
# MAGIC A new branch inherits a read-write compute endpoint from its source. The docs and SDK source did not state this; the first run of this notebook surfaced it as a `BadRequest: read_write endpoint already exists` when I tried `create_endpoint` against the new branch. Listing the branch's endpoints returns the one that came with it. If the endpoint is still in `INIT` state, the cell polls until it reaches `ACTIVE` or `IDLE` so the connection in the next cell does not race the provisioner.

# COMMAND ----------

from databricks.sdk.service.postgres import EndpointStatusState

t_endpoint_start = time.perf_counter()
branch_endpoints = list(w.postgres.list_endpoints(parent=BRANCH_NAME))
print(f"Endpoints on new branch : {len(branch_endpoints)}")

branch_endpoint = branch_endpoints[0]
BRANCH_ENDPOINT_NAME = branch_endpoint.name

# Poll until the inherited endpoint is reachable. ACTIVE and IDLE both accept
# connections; IDLE just means the compute has auto-suspended and will wake.
while True:
    state = branch_endpoint.status.current_state
    if state in (EndpointStatusState.ACTIVE, EndpointStatusState.IDLE):
        break
    if state == EndpointStatusState.DEGRADED:
        raise RuntimeError(
            f"Branch endpoint {BRANCH_ENDPOINT_NAME} entered DEGRADED state; aborting"
        )
    time.sleep(2)
    branch_endpoint = w.postgres.get_endpoint(name=BRANCH_ENDPOINT_NAME)

t_endpoint_elapsed = time.perf_counter() - t_endpoint_start
BRANCH_HOST = branch_endpoint.status.hosts.host

print(f"Branch endpoint name   : {BRANCH_ENDPOINT_NAME}")
print(f"Branch endpoint host   : {BRANCH_HOST}")
print(f"Branch endpoint state  : {branch_endpoint.status.current_state.value}")
print(f"List + wait-for-ready  : {t_endpoint_elapsed:.2f} seconds")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Modify the data on the branch
# MAGIC
# MAGIC Same `products` table, same query surface, separate compute. I updated one row, inserted a new one, and deleted another. Three classic write operations.

# COMMAND ----------

with connect(BRANCH_ENDPOINT_NAME, BRANCH_HOST) as branch_conn:
    with branch_conn.cursor() as cur:
        cur.execute("UPDATE products SET price = 99.99 WHERE id = 1")
        cur.execute(
            "INSERT INTO products (id, name, price) VALUES (%s, %s, %s)",
            (4, "widget-d", 40.00),
        )
        cur.execute("DELETE FROM products WHERE id = 3")
        branch_conn.commit()

        cur.execute("SELECT id, name, price FROM products ORDER BY id")
        branch_state_after = cur.fetchall()

print("Branch state after modifications:")
for row in branch_state_after:
    print(f"  {row}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Re-query the parent
# MAGIC
# MAGIC The demonstration. Same table name, same query, separate compute. The parent should be untouched: widget-a still at 10.00, widget-c still present, no widget-d. If the copy-on-write isolation guarantee holds end to end, the parent reads back exactly as we left it before the branch.

# COMMAND ----------

with connect(PARENT_ENDPOINT_NAME, PARENT_HOST) as parent_conn:
    with parent_conn.cursor() as cur:
        cur.execute("SELECT id, name, price FROM products ORDER BY id")
        parent_state_after = cur.fetchall()

print("Parent state after the branch's writes:")
for row in parent_state_after:
    print(f"  {row}")

isolation_holds = parent_state_before == parent_state_after
print(f"\nParent state matches pre-branch state: {isolation_holds}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the side-by-side shows

# COMMAND ----------

print(f"{'PARENT':<32s}    BRANCH")
print("-" * 70)
parent_lines = [f"{row[0]}: {row[1]:10s} {float(row[2]):>7.2f}" for row in parent_state_after]
branch_lines = [f"{row[0]}: {row[1]:10s} {float(row[2]):>7.2f}" for row in branch_state_after]
max_rows = max(len(parent_lines), len(branch_lines))
for i in range(max_rows):
    left = parent_lines[i] if i < len(parent_lines) else " " * 25
    right = branch_lines[i] if i < len(branch_lines) else ""
    print(f"  {left:30s}    {right}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Result
# MAGIC
# MAGIC Same table name, same query, two different results. The branch shows widget-a at 99.99, widget-d added, widget-c gone; the parent shows widget-a at 10.00, widget-c present, no widget-d. The writes landed on the branch's copy-on-write view of the storage and never touched the parent's view. That is database branching working the way git branching works on code: a point-in-time fork, isolated modifications, no shared state on writes.
# MAGIC
# MAGIC Lakebase shipped this as a managed primitive. The total state on the workspace at this point is one project, two branches (production and the probe), and two compute endpoints (one on each branch). The cleanup cell below tears all of it down.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC The notebook always attempts to delete the branch's endpoint, the branch, and the project, in that order. Lakebase caps [unarchived branches at 10 per project](https://docs.databricks.com/aws/en/oltp/projects/manage-branches) and [concurrent active computes at 20 per project](https://docs.databricks.com/aws/en/oltp/projects/manage-projects); leaving probe resources behind eats into a real project's headroom. Each delete is wrapped in try/except so a partial state does not stop cleanup.

# COMMAND ----------

try:
    w.postgres.delete_endpoint(name=BRANCH_ENDPOINT_NAME).wait()
    print(f"Deleted endpoint : {BRANCH_ENDPOINT_NAME}")
except Exception as e:
    print(f"Endpoint cleanup error (continuing): {e}")

try:
    w.postgres.delete_branch(name=BRANCH_NAME).wait()
    print(f"Deleted branch   : {BRANCH_NAME}")
except Exception as e:
    print(f"Branch cleanup error (continuing): {e}")

try:
    w.postgres.delete_project(name=PROJECT_NAME).wait()
    print(f"Deleted project  : {PROJECT_NAME}")
except Exception as e:
    print(f"Project cleanup error (continuing): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Lakebase Autoscaling overview](https://docs.databricks.com/aws/en/oltp/projects/): the parent docs page for the Lakebase feature surface this notebook uses.
# MAGIC - [Manage Lakebase projects](https://docs.databricks.com/aws/en/oltp/projects/manage-projects): canonical reference for `databricks postgres create-project`, the REST API, and the Python SDK call surface.
# MAGIC - [Manage Lakebase branches](https://docs.databricks.com/aws/en/oltp/projects/manage-branches): canonical reference for `create-branch`, TTL semantics, expiration cap, and the 10-unarchived-branch limit.
# MAGIC - [Lakebase authentication](https://docs.databricks.com/aws/en/oltp/projects/authentication): documents the OAuth credential flow, `generate_database_credential`, and the one-hour token lifetime that drives token rotation.
# MAGIC - [Database branching in Postgres: git-style workflows with Databricks Lakebase](https://www.databricks.com/blog/database-branching-postgres-git-style-workflows-databricks-lakebase): the source article that triggered this experiment.

# COMMAND ----------

# Final summary. Emits structured results for automated runs via dbutils.notebook.exit.
# Interactive runs see this as a JSON payload printed by the workspace UI; automated
# runs see it in jobs/runs/get-output.result. Defensive against missing variables so
# a partial run still returns whatever did succeed.
import json as _json

_summary = {}
for _name in [
    "PROJECT_NAME", "DEFAULT_BRANCH_NAME", "BRANCH_NAME",
    "PARENT_ENDPOINT_NAME", "PARENT_HOST",
    "BRANCH_ENDPOINT_NAME", "BRANCH_HOST",
    "t_project_elapsed", "t_branch_elapsed", "t_endpoint_elapsed",
    "isolation_holds",
]:
    _val = globals().get(_name)
    if _val is not None:
        _summary[_name] = _val

if "parent_state_before" in globals():
    _summary["parent_state_before"] = [list(r) for r in parent_state_before]
if "parent_state_after" in globals():
    _summary["parent_state_after"] = [list(r) for r in parent_state_after]
if "branch_state_after" in globals():
    _summary["branch_state_after"] = [list(r) for r in branch_state_after]

dbutils.notebook.exit(_json.dumps(_summary, default=str))
