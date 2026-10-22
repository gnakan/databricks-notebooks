# Databricks notebook source

# MAGIC %md
# MAGIC In a recent Databricks [release note about Lakebase](https://docs.databricks.com/aws/en/release-notes/lakebase#replicate-data-in-and-out-of-lakebase-with-postgres-logical-replication):
# MAGIC
# MAGIC > "Migrate an existing Postgres database into Lakebase and keep it in sync until you cut over, or publish changes from Lakebase to external Postgres databases."
# MAGIC
# MAGIC Found "external Postgres databases" interesting, so I put together an experiment. The release note covers both directions of native Postgres [logical replication](https://docs.databricks.com/aws/en/oltp/projects/logical-replication); this notebook tests the second one, publishing out. It creates a [Lakebase](https://docs.databricks.com/aws/en/oltp/projects/) project, publishes a Stark Industries parts order ledger from it, starts a Postgres 17 server inside the notebook's own container to act as the outside subscriber, and checks what arrives.
# MAGIC
# MAGIC **Hypothesis:** If Lakebase publishes to an outside Postgres the way the docs describe, the subscriber should get an exact copy of the ledger and then every change within a few seconds. The two rules the docs spell out should hold too: a published table needs a primary key, and a column added after subscribing doesn't follow the data.
# MAGIC
# MAGIC **Validation conditions** (set before the run):
# MAGIC
# MAGIC 1. **Initial copy.** Every one of the 500 seeded orders is on the subscriber with identical values, and the table's sync status reads Ready within 120 seconds of subscribing.
# MAGIC 2. **Changes arrive.** All 15 timed changes (five inserts, five updates, five deletes, after one warm-up of each) show up on the subscriber, and the typical time for each kind, the middle of its five, is 5 seconds or less.
# MAGIC 3. **No primary key.** On a published table with no primary key, an insert reaches the subscriber within 30 seconds, and Lakebase refuses an update and a delete on that table with an error.
# MAGIC 4. **A new column.** After a column is added on Lakebase only, the column isn't on the subscriber, a row written after the change still hasn't arrived 20 seconds later, and once the same column is added on the subscriber that row arrives within 30 seconds.
# MAGIC
# MAGIC All four holding supports the hypothesis; two or three is a mixed result; none or one doesn't support it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC The notebook runs on Databricks Free Edition in a serverless notebook, in about ten minutes. You don't need a Lakebase project first: it creates its own, with password connections switched on, and deletes it at the end unless you set the `keep_project` widget to true.
# MAGIC
# MAGIC Two things come from outside the workspace. The next cell pip installs [pg8000](https://codeberg.org/tlocke/pg8000), a Postgres driver written in pure Python, and the subscriber cell downloads [micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html) and the [conda-forge Postgres 17 package](https://anaconda.org/conda-forge/postgresql) into `/tmp`. The subscriber has to be a Postgres outside Lakebase, because the docs say "Replication between Lakebase databases in the same region is not supported." A server in the notebook's container is the shortest way to get one, and it disappears with the session.
# MAGIC
# MAGIC Identity: you connect to Lakebase as yourself, with a one-hour OAuth token the notebook requests for its [Postgres role](https://docs.databricks.com/aws/en/oltp/projects/authentication). The subscriber connects as a separate password role with the `REPLICATION` attribute. The notebook generates that password inside the run and never prints it.

# COMMAND ----------

# MAGIC %pip install --quiet pg8000

# COMMAND ----------

# DBTITLE 1,Imports and configuration
import json
import os
import secrets
import shutil
import ssl
import statistics
import subprocess
import time
import uuid

import pg8000.native
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
USER = w.current_user.me().user_name

# A fresh project per run, named after the experiment so it's easy to spot in the Lakebase App
PROJECT_ID = f"lakebase-logical-replication-{uuid.uuid4().hex[:6]}"
BRANCH = f"projects/{PROJECT_ID}/branches/production"
DATABASE = "databricks_postgres"
REPL_ROLE = "stark_ledger_replica"

# The subscriber: a Postgres 17 server in this container, reached over its Unix socket
PG_PREFIX = "/tmp/pg"
PG_DATA = "/tmp/pgsub"
PG_PORT = 5433

SEED_ORDERS = 500
TIMED_REPEATS = 5   # timed changes per kind, after one warm-up
POLL_SEC = 0.02     # how often the subscriber is checked while a change is in flight

dbutils.widgets.text("keep_project", "false")
KEEP_PROJECT = dbutils.widgets.get("keep_project").strip().lower() == "true"

def api(method, path, query=None, body=None):
    """Call the Lakebase REST API through the SDK's authenticated client."""
    return w.api_client.do(method, path, query=query, body=body)

def wait_operation(op, timeout=600):
    """Poll a long-running Lakebase operation until it's done; raise if it reports an error."""
    t0 = time.time()
    while not op.get("done"):
        if time.time() - t0 > timeout:
            raise TimeoutError(f"operation {op.get('name')} still running after {timeout}s")
        time.sleep(2)
        op = api("GET", f"/api/2.0/postgres/{op['name']}")
    if op.get("error"):
        raise RuntimeError(f"operation failed: {op['error']}")
    return op

print(f"User: {USER}\nProject: {PROJECT_ID}\nKeep project: {KEEP_PROJECT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gates
# MAGIC
# MAGIC Four things have to work before anything is measured, and each prints a `[gate N] ... -> OK / FAIL` line. The project is created through the [Lakebase projects API](https://docs.databricks.com/aws/en/oltp/projects/manage-projects) with `enable_pg_native_login` set, because the docs say "password connections are disabled by default for new projects." Then the compute endpoint has to come up, and the notebook checks `wal_level`, which the [publishing guide](https://docs.databricks.com/aws/en/oltp/projects/logical-replication-publication) says Lakebase runs at "logical by default". The last gate installs and starts the subscriber.

# COMMAND ----------

# DBTITLE 1,Gates: project, endpoint, wal_level, subscriber
gate = {"project_created": False, "endpoint_active": False, "wal_level": None, "subscriber_started": False,
        "replication_role": False}

# Gate 1: a new project with password connections on
try:
    op = api("POST", "/api/2.0/postgres/projects", query={"project_id": PROJECT_ID},
             body={"spec": {"display_name": "Stark ledger replication", "pg_version": 17,
                            "enable_pg_native_login": True}})
    wait_operation(op)
    project = api("GET", f"/api/2.0/postgres/projects/{PROJECT_ID}")
    gate["project_created"] = bool(project.get("status", {}).get("enable_pg_native_login"))
    print(f"[gate 1] project {PROJECT_ID} with password connections -> {'OK' if gate['project_created'] else 'FAIL'}")
except Exception as e:
    print(f"[gate 1] project create -> FAIL: {e}")

# Gate 2: the default endpoint is up; its host is the direct (not pooler) hostname the subscriber needs
HOST, ENDPOINT = None, None
try:
    t0 = time.time()
    while time.time() - t0 < 300:
        eps = api("GET", f"/api/2.0/postgres/{BRANCH}/endpoints").get("endpoints", [])
        if eps and eps[0].get("status", {}).get("current_state") == "ACTIVE":
            ENDPOINT, HOST = eps[0]["name"], eps[0]["status"]["hosts"]["host"]
            break
        time.sleep(3)
    gate["endpoint_active"] = HOST is not None
    print(f"[gate 2] endpoint active -> {'OK ' + HOST if HOST else 'FAIL'}")
except Exception as e:
    print(f"[gate 2] endpoint -> FAIL: {e}")

def lakebase_conn():
    """A connection to Lakebase as the current user, with a fresh OAuth token as the password."""
    token = api("POST", "/api/2.0/postgres/credentials", body={"endpoint": ENDPOINT})["token"]
    return pg8000.native.Connection(user=USER, password=token, host=HOST, database=DATABASE,
                                    ssl_context=ssl.create_default_context())

# Gate 3: wal_level on the publisher
pub = None
try:
    pub = lakebase_conn()
    gate["wal_level"] = pub.run("SHOW wal_level")[0][0]
    PUB_VERSION = pub.run("SHOW server_version")[0][0]
    print(f"[gate 3] wal_level = {gate['wal_level']} (Postgres {PUB_VERSION}) -> {'OK' if gate['wal_level'] == 'logical' else 'FAIL'}")
except Exception as e:
    print(f"[gate 3] connect to Lakebase -> FAIL: {e}")

# Gate 4: Postgres 17 in this container. micromamba installs it as a normal user, no root needed.
def sh(cmd, check=True):
    r = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True, text=True)
    if check and r.returncode != 0:
        raise RuntimeError(f"{cmd if isinstance(cmd, str) else ' '.join(cmd)} failed: {(r.stdout + r.stderr)[-600:]}")
    return r

try:
    t_install = time.time()
    if not os.path.exists(f"{PG_PREFIX}/bin/postgres"):
        sh("curl -Ls https://micro.mamba.pm/api/micromamba/linux-aarch64/latest | tar -xj -C /tmp bin/micromamba"
           if os.uname().machine == "aarch64" else
           "curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xj -C /tmp bin/micromamba")
        os.environ["MAMBA_ROOT_PREFIX"] = "/tmp/mamba"
        sh(["/tmp/bin/micromamba", "create", "-y", "-q", "-p", PG_PREFIX, "-c", "conda-forge", "postgresql=17"])
    install_sec = time.time() - t_install
    sh([f"{PG_PREFIX}/bin/pg_ctl", "-D", PG_DATA, "-m", "immediate", "stop"], check=False)  # a rerun in the same session
    shutil.rmtree(PG_DATA, ignore_errors=True)
    sh([f"{PG_PREFIX}/bin/initdb", "-D", PG_DATA, "-U", "postgres", "--auth=trust"])
    with open(f"{PG_DATA}/postgresql.conf", "a") as f:
        f.write(f"port={PG_PORT}\nunix_socket_directories='/tmp'\nlisten_addresses=''\n")
    sh([f"{PG_PREFIX}/bin/pg_ctl", "-D", PG_DATA, "-l", "/tmp/pgsub.log", "-w", "start"])
    sub = pg8000.native.Connection(user="postgres", unix_sock=f"/tmp/.s.PGSQL.{PG_PORT}", database="postgres")
    SUB_VERSION = sub.run("SHOW server_version")[0][0]
    gate["subscriber_started"] = True
    print(f"[gate 4] subscriber Postgres {SUB_VERSION} started ({install_sec:.0f}s to install) -> OK")
except Exception as e:
    print(f"[gate 4] subscriber -> FAIL: {e}")

assert gate["project_created"] and gate["endpoint_active"], "No Lakebase project to publish from; see the gate lines above."
assert gate["wal_level"] == "logical", "The publisher isn't at wal_level = logical; see gate 3."
assert gate["subscriber_started"], "The subscriber Postgres didn't start; see gate 4."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the ledger and publish it
# MAGIC
# MAGIC Two tables go on Lakebase. `parts_orders` is the ledger: 500 Stark Industries orders with a primary key, a price in `numeric` and a timestamp, so the copy has more than integers to get right. `scan_log` is a dock scanner log with no primary key at all, which the publishing guide says every published table needs: a primary key or a replica identity. Both go in one publication, so condition 3 is tested on the same subscription as everything else.
# MAGIC
# MAGIC The replication role is created with SQL, so it needs `REPLICATION` spelled out; the docs say "roles created with SQL do not receive it automatically".

# COMMAND ----------

# DBTITLE 1,Tables, seed data, publication, replication role
PARTS = ["arc reactor housing", "repulsor coil", "palladium core", "servo actuator", "flight stabilizer",
         "HUD lens", "vibranium plate", "thruster nozzle", "power cell", "gyro module"]
PLANTS = ["Malibu", "Long Island", "Gulmira"]
STATUSES = ["ordered", "picked", "shipped", "received"]

pub.run("DROP PUBLICATION IF EXISTS stark_ledger")
pub.run("DROP TABLE IF EXISTS parts_orders, scan_log")
pub.run("""CREATE TABLE parts_orders (
    order_id integer PRIMARY KEY, part text NOT NULL, plant text NOT NULL, qty integer NOT NULL,
    unit_price numeric(10,2) NOT NULL, status text NOT NULL, ordered_at timestamptz NOT NULL)""")
pub.run("CREATE TABLE scan_log (dock text, part text, scanned_at timestamptz)")

for i in range(1, SEED_ORDERS + 1):
    pub.run("""INSERT INTO parts_orders VALUES (:i::integer, :part, :plant, :qty, :price, :status,
               timestamptz '2026-10-01 08:00:00+00' + :i::integer * interval '1 minute')""",
            i=i, part=PARTS[i % len(PARTS)], plant=PLANTS[i % len(PLANTS)], qty=1 + (i * 7) % 40,
            price=f"{(i * 37) % 9000 / 3 + 10:.2f}", status=STATUSES[i % len(STATUSES)])
for i in range(1, 21):
    pub.run("INSERT INTO scan_log VALUES (:dock, :part, now())", dock=f"dock-{1 + i % 4}", part=PARTS[i % len(PARTS)])

pub.run("CREATE PUBLICATION stark_ledger FOR TABLE parts_orders, scan_log")

REPL_PASSWORD = "S" + secrets.token_urlsafe(24)  # lives only in this session; never printed
pub.run(f"CREATE ROLE {REPL_ROLE} WITH LOGIN REPLICATION PASSWORD '{REPL_PASSWORD}'")
pub.run(f"GRANT USAGE ON SCHEMA public TO {REPL_ROLE}")
pub.run(f"GRANT SELECT ON parts_orders, scan_log TO {REPL_ROLE}")
role_row = pub.run("SELECT rolreplication, rolcanlogin FROM pg_catalog.pg_roles WHERE rolname = :r", r=REPL_ROLE)
gate["replication_role"] = bool(role_row and role_row[0][0] and role_row[0][1])

print(f"Seeded {pub.run('SELECT count(*) FROM parts_orders')[0][0]} orders and "
      f"{pub.run('SELECT count(*) FROM scan_log')[0][0]} scans on Lakebase")
print(f"Publication: {[r[0] for r in pub.run('SELECT pubname FROM pg_catalog.pg_publication')]}")
print(f"[gate 5] replication role {REPL_ROLE} has REPLICATION and LOGIN -> {'OK' if gate['replication_role'] else 'FAIL'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Subscribe and compare the initial copy
# MAGIC
# MAGIC The subscriber's tables have to exist and be empty first: the docs say "the initial copy fails on duplicate keys or constraint violations if a target table already contains rows." The connection string uses `sslmode=verify-full` with the system root store, the strongest option the publishing guide lists. Then the notebook waits for the per-table sync status from the [monitoring page](https://docs.databricks.com/aws/en/oltp/projects/logical-replication-manage) to read Ready on both tables, and compares every column of every order on both sides.

# COMMAND ----------

# DBTITLE 1,Create the subscription and wait for Ready
sub.run("DROP SUBSCRIPTION IF EXISTS stark_ledger_sub")
sub.run("DROP TABLE IF EXISTS parts_orders, scan_log")
sub.run("""CREATE TABLE parts_orders (
    order_id integer PRIMARY KEY, part text NOT NULL, plant text NOT NULL, qty integer NOT NULL,
    unit_price numeric(10,2) NOT NULL, status text NOT NULL, ordered_at timestamptz NOT NULL)""")
sub.run("CREATE TABLE scan_log (dock text, part text, scanned_at timestamptz)")

t_subscribe = time.time()
sub.run(f"""CREATE SUBSCRIPTION stark_ledger_sub
    CONNECTION 'host={HOST} port=5432 dbname={DATABASE} user={REPL_ROLE} password={REPL_PASSWORD} sslmode=verify-full sslrootcert=system'
    PUBLICATION stark_ledger""")

SYNC_SQL = """SELECT srrelid::regclass::text, srsubstate FROM pg_catalog.pg_subscription_rel"""
sync_ready_sec = None
while time.time() - t_subscribe < 120:
    states = dict(sub.run(SYNC_SQL))
    if states and all(s == "r" for s in states.values()):
        sync_ready_sec = time.time() - t_subscribe
        break
    time.sleep(0.25)
print(f"Sync status: {states}")
print(f"Both tables Ready after {sync_ready_sec:.1f}s" if sync_ready_sec is not None else "Not Ready after 120s")

# COMMAND ----------

# DBTITLE 1,Every order, both sides
ORDER_COLS = "order_id, part, plant, qty, unit_price::text, status, ordered_at"
pub_rows = pub.run(f"SELECT {ORDER_COLS} FROM parts_orders ORDER BY order_id")
sub_rows = sub.run(f"SELECT {ORDER_COLS} FROM parts_orders ORDER BY order_id")
mismatched = sum(1 for a, b in zip(pub_rows, sub_rows) if list(a) != list(b)) + abs(len(pub_rows) - len(sub_rows))

copy = {
    "rows_publisher": len(pub_rows),
    "rows_subscriber": len(sub_rows),
    "rows_mismatched": mismatched,
    "rows_match": len(pub_rows) == SEED_ORDERS and mismatched == 0,
    "sync_ready": sync_ready_sec is not None,
    "sync_ready_sec": round(sync_ready_sec, 2) if sync_ready_sec is not None else None,
    "scan_rows_subscriber": sub.run("SELECT count(*) FROM scan_log")[0][0],
}
print(f"The initial copy, order by order: {copy['rows_subscriber']} of {copy['rows_publisher']} orders on the subscriber, "
      f"{copy['rows_mismatched']} with any value different")
print("The first three orders as the subscriber has them:")
for r in sub_rows[:3]:
    print("  ", list(r))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Time each kind of change
# MAGIC
# MAGIC The publishing guide says "The change appears on the subscriber within a few seconds." So each kind of change gets one untimed warm-up, then five timed ones. The clock starts when the notebook sends the statement to Lakebase and stops when the subscriber can read the change, checked every 20 milliseconds. The commit's own round trip is recorded beside it, so you can see how much of the time is replication. The typical time is the middle of the five.

# COMMAND ----------

# DBTITLE 1,Inserts, updates and deletes, timed
def wait_until(sql, expect, timeout=30, since=None, **params):
    """Seconds from `since` (default: now) until the subscriber's query returns `expect`; None if not within `timeout`."""
    t0 = time.perf_counter() if since is None else since
    while time.perf_counter() - t0 < timeout:
        got = sub.run(sql, **params)
        if (got[0][0] if got else None) == expect:
            return time.perf_counter() - t0
        time.sleep(POLL_SEC)
    return None

next_id = SEED_ORDERS + 1
next_delete = 400  # each delete takes a different seeded order
lags = {"insert": [], "update": [], "delete": []}
commits = {"insert": [], "update": [], "delete": []}

def one_change(kind):
    """Make one change on Lakebase; return (seconds from send to visible on the subscriber, seconds for the commit)."""
    global next_id, next_delete
    if kind == "insert":
        oid = next_id; next_id += 1
        sql, params = "INSERT INTO parts_orders VALUES (:i, 'repulsor coil', 'Malibu', 3, 199.99, 'ordered', now())", {"i": oid}
        check, expect, check_params = "SELECT count(*) FROM parts_orders WHERE order_id = :i", 1, {"i": oid}
    elif kind == "update":
        oid = 1 + len(lags["update"]) * 7
        stamp = f"shipped-{uuid.uuid4().hex[:6]}"
        sql, params = "UPDATE parts_orders SET status = :s WHERE order_id = :i", {"s": stamp, "i": oid}
        check, expect, check_params = "SELECT status FROM parts_orders WHERE order_id = :i", stamp, {"i": oid}
    else:
        oid = next_delete; next_delete += 1
        sql, params = "DELETE FROM parts_orders WHERE order_id = :i", {"i": oid}
        check, expect, check_params = "SELECT count(*) FROM parts_orders WHERE order_id = :i", 0, {"i": oid}
    t_send = time.perf_counter()
    pub.run(sql, **params)
    commit_sec = time.perf_counter() - t_send
    return wait_until(check, expect, since=t_send, **check_params), commit_sec

warmups = {}
for kind in lags:
    warmups[kind] = one_change(kind)[0]  # untimed: first use of each statement path
for kind in lags:
    for _ in range(TIMED_REPEATS):
        visible, commit_sec = one_change(kind)
        lags[kind].append(visible)
        commits[kind].append(commit_sec)

def typical(values):
    got = [v for v in values if v is not None]
    return statistics.median(got) if got else None

arrived = sum(1 for k in lags for v in lags[k] if v is not None)
changes = {
    "timed": TIMED_REPEATS * len(lags),
    "arrived": arrived,
    "repeat_count": TIMED_REPEATS,
}
for kind in lags:
    t = typical(lags[kind])
    changes[f"{kind}_lag_typical_sec"] = round(t, 3) if t is not None else None
    changes[f"{kind}_lag_max_sec"] = round(max(v for v in lags[kind] if v is not None), 3) if any(v is not None for v in lags[kind]) else None
    changes[f"{kind}_commit_typical_sec"] = round(statistics.median(commits[kind]), 3)

import pandas as pd
lag_table = pd.DataFrame([{"Change": kind, "Run": n + 1, "Seconds, sent to visible on subscriber": None if v is None else round(v, 3),
                           "Seconds for the commit": round(commits[kind][n], 3)}
                          for kind in lags for n, v in enumerate(lags[kind])])
print(f"Time from sending each change to Lakebase to reading it on the subscriber, {changes['arrived']} of {changes['timed']} arrived")
display(lag_table)

# COMMAND ----------

# DBTITLE 1,Seconds from sending a change to reading it on the subscriber
# hide-code
import matplotlib.pyplot as plt

NAVY, LAVA, GRAY_TEXT, GRAY_LINES, OAT_LIGHT = "#1B3139", "#FF3621", "#5A6F77", "#DCE0E2", "#F9F7F4"
plt.rcParams["font.family"] = "DejaVu Sans"  # ships with matplotlib, so serverless has it

fig, ax = plt.subplots(figsize=(8, 3.6))
fig.patch.set_facecolor(OAT_LIGHT)
ax.set_facecolor(OAT_LIGHT)
kinds = list(lags)
for x, kind in enumerate(kinds):
    vals = [v for v in lags[kind] if v is not None]
    ax.scatter([x] * len(vals), vals, s=60, color=NAVY, zorder=3)
    if vals:
        mid = statistics.median(vals)
        ax.hlines(mid, x - 0.25, x + 0.25, color=LAVA, linewidth=2.5, zorder=4)
        ax.annotate(f"typical {mid:.2f}s", (x + 0.28, mid), va="center", fontsize=10, color=NAVY)
    missing = len(lags[kind]) - len(vals)
    if missing:
        ax.annotate(f"{missing} never arrived", (x, 0), ha="center", va="bottom", fontsize=10, color=LAVA)
ax.set_xticks(range(len(kinds)))
ax.set_xticklabels([f"{k} ({len(lags[k])} timed)" for k in kinds], color=NAVY)
ax.set_xlim(-0.6, len(kinds) - 0.1)
ax.set_ylim(bottom=0)
ax.set_ylabel("seconds", color=GRAY_TEXT)
ax.tick_params(colors=GRAY_TEXT)
for side in ["top", "right"]:
    ax.spines[side].set_visible(False)
for side in ["left", "bottom"]:
    ax.spines[side].set_color(GRAY_LINES)
ax.set_title("Sent to Lakebase, then visible on the subscriber", color=NAVY, fontsize=13, loc="left")
fig.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A published table with no primary key
# MAGIC
# MAGIC `scan_log` has no primary key and no [replica identity](https://www.postgresql.org/docs/17/sql-altertable.html). The Postgres docs say what to expect: "If a table without a replica identity is added to a publication that replicates `UPDATE` or `DELETE` operations then subsequent `UPDATE` or `DELETE` operations will cause an error on the publisher. `INSERT` operations can proceed regardless of any replica identity." The notebook tries all three on Lakebase and records the same fields for each.

# COMMAND ----------

# DBTITLE 1,Insert, update and delete on scan_log
def try_on_lakebase(sql, **params):
    """Run a statement on Lakebase; return (ok, error message)."""
    try:
        pub.run(sql, **params)
        return True, None
    except Exception as e:
        msg = e.args[0].get("M") if e.args and isinstance(e.args[0], dict) else str(e)
        return False, msg

scan_before = sub.run("SELECT count(*) FROM scan_log")[0][0]
nopk_rows = []
t_send = time.perf_counter()
ok, err = try_on_lakebase("INSERT INTO scan_log VALUES ('dock-9', 'palladium core', now())")
nopk_rows.append({"Statement": "INSERT", "Lakebase accepted it": ok, "Error": err,
                  "Seconds to subscriber": wait_until("SELECT count(*) FROM scan_log", scan_before + 1, since=t_send) if ok else None})
ok, err = try_on_lakebase("UPDATE scan_log SET dock = 'dock-0' WHERE dock = 'dock-1'")
moved = pub.run("SELECT count(*) FROM scan_log WHERE dock = 'dock-0'")[0][0] if ok else 0
nopk_rows.append({"Statement": "UPDATE", "Lakebase accepted it": ok, "Error": err,
                  "Seconds to subscriber": wait_until("SELECT count(*) FROM scan_log WHERE dock = 'dock-0'", moved) if ok else None})
ok, err = try_on_lakebase("DELETE FROM scan_log WHERE dock = 'dock-2'")
nopk_rows.append({"Statement": "DELETE", "Lakebase accepted it": ok, "Error": err,
                  "Seconds to subscriber": None})

nopk = {
    "insert_accepted": nopk_rows[0]["Lakebase accepted it"],
    "insert_arrived": nopk_rows[0]["Seconds to subscriber"] is not None,
    "insert_lag_sec": round(nopk_rows[0]["Seconds to subscriber"], 3) if nopk_rows[0]["Seconds to subscriber"] is not None else None,
    "update_refused": not nopk_rows[1]["Lakebase accepted it"],
    "update_error": nopk_rows[1]["Error"],
    "delete_refused": not nopk_rows[2]["Lakebase accepted it"],
    "delete_error": nopk_rows[2]["Error"],
}
print("What Lakebase did with each statement on a table with no primary key")
display(pd.DataFrame(nopk_rows))

# COMMAND ----------

# MAGIC %md
# MAGIC ## A column added on Lakebase only
# MAGIC
# MAGIC The overview says "Later schema changes are not replicated." The Postgres docs go further: "replication will error until the schema is updated. In many cases, intermittent errors can be avoided by applying additive schema changes to the subscriber first." So this runs both orders, and records the same fields for each.
# MAGIC
# MAGIC Lakebase first: the notebook adds `priority` on Lakebase and writes an order that sets it. After 20 seconds it looks at the subscriber. Then it adds `priority` there too and times how long that order takes to land. Subscriber first is the reverse: `inspector` goes on the subscriber before Lakebase, and one more order is timed the same way.

# COMMAND ----------

# DBTITLE 1,Lakebase first, then subscriber first
def column_on_subscriber(col):
    return sub.run("SELECT count(*) FROM information_schema.columns WHERE table_name = 'parts_orders' AND column_name = :c", c=col)[0][0] == 1

def apply_errors():
    got = sub.run("SELECT apply_error_count FROM pg_catalog.pg_stat_subscription_stats WHERE subname = 'stark_ledger_sub'")
    return got[0][0] if got else None

def log_errors_since(offset):
    with open("/tmp/pgsub.log") as f:
        f.seek(offset)
        return [line.strip() for line in f if " ERROR: " in line]

schema = {}

# Lakebase first
oid_a = next_id; next_id += 1
errors_before, log_offset = apply_errors(), os.path.getsize("/tmp/pgsub.log")
pub.run("ALTER TABLE parts_orders ADD COLUMN priority text")
pub.run("INSERT INTO parts_orders VALUES (:i, 'HUD lens', 'Gulmira', 1, 450.00, 'ordered', now(), 'rush')", i=oid_a)
before_fix = wait_until("SELECT count(*) FROM parts_orders WHERE order_id = :i", 1, timeout=20, i=oid_a)
col_before_fix = column_on_subscriber("priority")
errors_mid = apply_errors()
sub.run("ALTER TABLE parts_orders ADD COLUMN priority text")
after_fix = wait_until("SELECT count(*) FROM parts_orders WHERE order_id = :i", 1, timeout=30, i=oid_a) if before_fix is None else 0.0
landed = sub.run("SELECT priority FROM parts_orders WHERE order_id = :i", i=oid_a)
schema["publisher_first"] = {
    "column_on_subscriber_before_fix": col_before_fix,
    "arrived_before_fix": before_fix is not None,
    "apply_errors_while_waiting": (errors_mid - errors_before) if None not in (errors_mid, errors_before) else None,
    "seconds_after_fix": round(after_fix, 3) if after_fix is not None else None,
    "value_on_subscriber": landed[0][0] if landed else None,
    "subscriber_errors": log_errors_since(log_offset)[:3],
}

# Subscriber first
oid_b = next_id; next_id += 1
errors_before, log_offset = apply_errors(), os.path.getsize("/tmp/pgsub.log")
sub.run("ALTER TABLE parts_orders ADD COLUMN inspector text")
pub.run("ALTER TABLE parts_orders ADD COLUMN inspector text")
pub.run("INSERT INTO parts_orders VALUES (:i, 'power cell', 'Malibu', 2, 88.00, 'ordered', now(), NULL, 'Hogan')", i=oid_b)
before_fix_b = wait_until("SELECT count(*) FROM parts_orders WHERE order_id = :i", 1, timeout=20, i=oid_b)
errors_mid = apply_errors()
landed_b = sub.run("SELECT inspector FROM parts_orders WHERE order_id = :i", i=oid_b)
schema["subscriber_first"] = {
    "column_on_subscriber_before_fix": True,
    "arrived_before_fix": before_fix_b is not None,
    "apply_errors_while_waiting": (errors_mid - errors_before) if None not in (errors_mid, errors_before) else None,
    "seconds_after_fix": round(before_fix_b, 3) if before_fix_b is not None else None,
    "value_on_subscriber": landed_b[0][0] if landed_b else None,
    "subscriber_errors": log_errors_since(log_offset)[:3],
}
schema["column_on_subscriber"] = col_before_fix

print("What the subscriber did after each column change")
display(pd.DataFrame([{"Order": name.replace("_", " "), **{k: v for k, v in arm.items() if k != "subscriber_errors"}}
                      for name, arm in schema.items() if isinstance(arm, dict)]))
for name in ["publisher_first", "subscriber_first"]:
    for line in schema[name]["subscriber_errors"]:
        print(f"{name.replace('_', ' ')} | subscriber log: {line[-220:]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## The validation conditions
# MAGIC
# MAGIC Each condition is checked against the thresholds written at the top of the notebook, before any of this ran.

# COMMAND ----------

# DBTITLE 1,Check each condition
cond = {}
cond["copy"] = bool(copy["rows_match"] and copy["sync_ready"])
cond["changes"] = (changes["arrived"] == changes["timed"]
                   and all(changes[f"{k}_lag_typical_sec"] is not None and changes[f"{k}_lag_typical_sec"] <= 5.0 for k in lags))
cond["nopk"] = bool(nopk["insert_arrived"] and nopk["insert_lag_sec"] is not None and nopk["insert_lag_sec"] <= 30
                    and nopk["update_refused"] and nopk["delete_refused"])
pf = schema["publisher_first"]
cond["schema"] = bool((not pf["column_on_subscriber_before_fix"]) and (not pf["arrived_before_fix"])
                      and pf["seconds_after_fix"] is not None and pf["seconds_after_fix"] <= 30)
held = sum(cond.values())
verdict_label = "supported" if held == 4 else ("mixed-signal" if held >= 2 else "not supported")

def mark(ok):
    return "held" if ok else "did not hold"

print(f"Verdict: {verdict_label} ({held} of 4 conditions held)\n")
print(f"Condition 1 (initial copy): {copy['rows_subscriber']} of {SEED_ORDERS} orders arrived with "
      f"{copy['rows_mismatched']} differing, and the tables read Ready "
      + (f"after {copy['sync_ready_sec']:.1f} seconds." if copy['sync_ready'] else "never, inside 120 seconds.")
      + f" Result: {mark(cond['copy'])}.\n")
print(f"Condition 2 (changes arrive): {changes['arrived']} of {changes['timed']} timed changes arrived. Typical seconds: "
      + ", ".join(f"{k} {changes[f'{k}_lag_typical_sec']}" for k in lags) + f". Threshold: 5 seconds each. Result: {mark(cond['changes'])}.\n")
print(f"Condition 3 (no primary key): the insert {'arrived in ' + str(nopk['insert_lag_sec']) + ' seconds' if nopk['insert_arrived'] else 'never arrived'}; "
      f"Lakebase {'refused' if nopk['update_refused'] else 'accepted'} the update and "
      f"{'refused' if nopk['delete_refused'] else 'accepted'} the delete. Result: {mark(cond['nopk'])}.\n")
print(f"Condition 4 (a new column): the column was {'on' if pf['column_on_subscriber_before_fix'] else 'not on'} the subscriber, "
      f"the order {'had' if pf['arrived_before_fix'] else 'had not'} arrived after 20 seconds, and after the subscriber got the column it "
      + (f"landed in {pf['seconds_after_fix']} seconds." if pf['seconds_after_fix'] is not None else "still didn't land within 30 seconds.")
      + f" Result: {mark(cond['schema'])}.")

# COMMAND ----------

# DBTITLE 1,The run at a glance
# hide-code
import html

OAT_LIGHT, OAT, WHITE = "#F9F7F4", "#EEEDE9", "#FFFFFF"
NAVY, NAVY_600, GRAY_TEXT, GRAY_LINES = "#1B3139", "#1B5162", "#5A6F77", "#DCE0E2"
LAVA = "#FF3621"
SANS = "DM Sans, Helvetica Neue, Arial, sans-serif"
FONTS = '<link href="https://fonts.googleapis.com/css2?family=DM+Sans:ital,wght@0,400;0,700;1,400&display=swap" rel="stylesheet">'

def esc(value):
    return html.escape(str(value))

def stat(number, words, out_of=None):
    of = f"<span style='color:{GRAY_TEXT};font-size:18px'> / {esc(out_of)}</span>" if out_of is not None else ""
    return (f"<div style='min-width:150px'><div style='font-size:30px;font-weight:700;color:{NAVY}'>{esc(number)}{of}</div>"
            f"<div style='font-size:13px;color:{GRAY_TEXT};line-height:1.35'>{esc(words)}</div></div>")

def stats(*figures):
    return f"<div style='display:flex;gap:34px;flex-wrap:wrap;padding-bottom:26px'>{''.join(figures)}</div>"

def row(name, tag="", tag_pop=False, note="", body=""):
    tag_html = f"<span style='font-size:12px;font-weight:700;color:{LAVA if tag_pop else NAVY_600}'>{esc(tag)}</span>" if tag else ""
    note_html = f"<div style='font-size:14px;color:{GRAY_TEXT};margin-top:6px;line-height:1.45'>{esc(note)}</div>" if note else ""
    return (f"<div style='padding:18px 0 14px;border-top:1px solid {GRAY_LINES}'>"
            f"<div style='display:flex;align-items:baseline;gap:14px;flex-wrap:wrap'>"
            f"<span style='font-size:22px;font-weight:700;color:{NAVY}'>{esc(name)}</span>{tag_html}</div>"
            f"{note_html}{body}</div>")

def step(label, done, last=False):
    dot = f"<span style='display:inline-block;width:12px;height:12px;background:{NAVY if done else LAVA}'></span>"
    line = "" if last else f"<span style='display:inline-block;width:36px;height:2px;background:{GRAY_LINES};margin:0 8px'></span>"
    return (f"<span style='display:inline-flex;align-items:center;font-size:13px;color:{NAVY}'>{dot}"
            f"<span style='margin-left:6px'>{esc(label)}</span>{line}</span>")

def panel(headline, dek, body):
    return (f"{FONTS}<div style='background:{OAT_LIGHT};color:{NAVY};padding:34px 38px 30px;font-family:{SANS};"
            f"border-top:6px solid {NAVY}'>"
            f"<div style='font-size:36px;font-weight:700;line-height:1.1;max-width:760px'>{esc(headline)}</div>"
            f"<div style='font-size:17px;color:{GRAY_TEXT};line-height:1.45;max-width:680px;margin:12px 0 26px'>{esc(dek)}</div>"
            f"{body}</div>")

def secs(v):
    return "never" if v is None else f"{v:.2f}s"

body = stats(
    stat(copy["rows_subscriber"], "orders on the subscriber after the initial copy", out_of=copy["rows_publisher"]),
    stat(changes["arrived"], "timed changes that arrived", out_of=changes["timed"]),
    stat(secs(changes["insert_lag_typical_sec"]), "typical insert, sent to visible on the subscriber"),
    stat(f"{held} of 4", "validation conditions held"),
)
body += row("1. Initial copy", tag=mark(cond["copy"]), tag_pop=not cond["copy"],
            note=f"{copy['rows_mismatched']} orders with any value different; Ready after "
                 + (f"{copy['sync_ready_sec']:.1f}s" if copy["sync_ready"] else "more than 120s"))
body += row("2. Inserts, updates, deletes", tag=mark(cond["changes"]), tag_pop=not cond["changes"],
            note="Typical of five: " + ", ".join(f"{k} {secs(changes[f'{k}_lag_typical_sec'])}" for k in lags)
                 + f". Slowest of all {changes['timed']}: {secs(max([changes[f'{k}_lag_max_sec'] or 0 for k in lags]))}.")
body += row("3. Table with no primary key", tag=mark(cond["nopk"]), tag_pop=not cond["nopk"],
            note=f"Insert {'arrived in ' + secs(nopk['insert_lag_sec']) if nopk['insert_arrived'] else 'never arrived'}. "
                 f"Update {'refused' if nopk['update_refused'] else 'accepted'}, delete {'refused' if nopk['delete_refused'] else 'accepted'} by Lakebase.")
pf, sf = schema["publisher_first"], schema["subscriber_first"]
body += row("4. A column added on Lakebase only", tag=mark(cond["schema"]), tag_pop=not cond["schema"],
            note="The order written after the change, in each order of operations.",
            body=(f"<div style='margin-top:12px'>"
                  f"<div style='font-size:13px;font-weight:700;color:{NAVY_600};margin-bottom:6px'>Lakebase first</div>"
                  + step("column added on Lakebase", True)
                  + step("order arrived in 20s" if pf["arrived_before_fix"] else "order held back 20s", pf["arrived_before_fix"])
                  + step("column added on subscriber", True)
                  + step(f"order landed after {secs(pf['seconds_after_fix'])}", pf["seconds_after_fix"] is not None, last=True)
                  + f"<div style='font-size:13px;font-weight:700;color:{NAVY_600};margin:14px 0 6px'>Subscriber first</div>"
                  + step("column added on subscriber", True)
                  + step("column added on Lakebase", True)
                  + step(f"order landed after {secs(sf['seconds_after_fix'])}", sf["arrived_before_fix"], last=True)
                  + "</div>"))

displayHTML(panel(
    "Lakebase to an outside Postgres",
    f"A Stark Industries order ledger published from a new Lakebase project to a Postgres {SUB_VERSION} server "
    "started inside this notebook. Navy marks what went through; lava marks what didn't.",
    body,
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

# DBTITLE 1,Results
slots = pub.run("SELECT slot_name, active FROM pg_catalog.pg_replication_slots WHERE slot_type = 'logical'")
results = {
    "project_id": PROJECT_ID,
    "publisher_version": PUB_VERSION,
    "subscriber_version": SUB_VERSION,
    "gate.project_created": gate["project_created"],
    "gate.endpoint_active": gate["endpoint_active"],
    "gate.wal_level": gate["wal_level"],
    "gate.subscriber_started": gate["subscriber_started"],
    "gate.replication_role": gate["replication_role"],
    "subscriber_install_sec": round(install_sec, 1),
    **{f"copy.{k}": v for k, v in copy.items()},
    **{f"changes.{k}": v for k, v in changes.items()},
    "changes.warmup_sec": {k: (round(v, 3) if v is not None else None) for k, v in warmups.items()},
    "changes.lags_sec": {k: [round(v, 3) if v is not None else None for v in vals] for k, vals in lags.items()},
    "changes.commits_sec": {k: [round(v, 3) for v in vals] for k, vals in commits.items()},
    **{f"nopk.{k}": v for k, v in nopk.items()},
    "schema.column_on_subscriber": schema["column_on_subscriber"],
    **{f"schema.publisher_first.{k}": v for k, v in schema["publisher_first"].items()},
    **{f"schema.subscriber_first.{k}": v for k, v in schema["subscriber_first"].items()},
    "replication_slots_on_lakebase": [r[0] for r in slots],
    **{f"verdict.{k}": v for k, v in cond.items()},
    "verdict.conditions_held": held,
    "verdict.label": verdict_label,
}
print("RESULTS_JSON " + json.dumps(results, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ## What the run showed
# MAGIC
# MAGIC All four validation conditions held, so the run supports the hypothesis. The initial copy was exact: 500 of 500 orders on the subscriber, 0 with any value different, and both tables read Ready 0.36 seconds after subscribing.
# MAGIC
# MAGIC Every timed change made it across. All 15 arrived, and a typical insert, update and delete each took 0.007 seconds from sending the statement to reading it on the subscriber. Lakebase's own commit round trip for an insert was typically the same 0.007, so the change was on the subscriber about as soon as Lakebase said it was committed. Keep in mind where the subscriber sits: in this notebook's container, in the same region. Those are next-door numbers, not what you'd see across a network to a database somewhere else.
# MAGIC
# MAGIC The primary key rule shows up on Lakebase, the moment you write. On `scan_log`, the table with no primary key, the insert reached the subscriber in 0.004 seconds, and Lakebase refused the update and the delete outright: "cannot update table "scan_log" because it does not have a replica identity and publishes updates".
# MAGIC
# MAGIC Schema changes don't replicate, and the order you make them in matters. With `priority` added on Lakebase first, the next order still hadn't reached the subscriber 20 seconds later, and the subscriber logged 5 errors while it waited, each one ending `missing replicated column: "priority"`. Once the subscriber had the column too, the held order landed 0.106 seconds later, new value included. Adding `inspector` on the subscriber first went through with no errors at all.
# MAGIC
# MAGIC If you publish Lakebase changes to another Postgres, add a new column on the subscriber before you add it on Lakebase. The other way round, the held order still landed, but replication waited for the subscriber to catch up.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Order matters here. The [monitoring page](https://docs.databricks.com/aws/en/oltp/projects/logical-replication-manage) says "When the publisher is reachable, dropping the subscription disconnects from the publisher and removes the replication slot on the publisher." A slot left behind keeps Lakebase holding on to its write-ahead log, so the subscription goes first, then the local server, then the project. Set `keep_project` to true to keep the Lakebase side around and look at it in the Lakebase App.

# COMMAND ----------

# DBTITLE 1,Drop the subscription, stop the server, delete the project
sub.run("DROP SUBSCRIPTION IF EXISTS stark_ledger_sub")
slots_left = [r[0] for r in pub.run("SELECT slot_name FROM pg_catalog.pg_replication_slots WHERE slot_type = 'logical'")]
print(f"Logical replication slots left on Lakebase after DROP SUBSCRIPTION: {slots_left or 'none'}")
sub.close()
sh([f"{PG_PREFIX}/bin/pg_ctl", "-D", PG_DATA, "-m", "fast", "stop"], check=False)
pub.close()
if KEEP_PROJECT:
    print(f"Kept Lakebase project {PROJECT_ID}; delete it from the Lakebase App when you're done")
else:
    wait_operation(api("DELETE", f"/api/2.0/postgres/projects/{PROJECT_ID}"))
    print(f"Deleted Lakebase project {PROJECT_ID}")
print("CLEANUP_JSON " + json.dumps({"cleanup.slots_left": len(slots_left)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Where to go next
# MAGIC
# MAGIC - [Logical replication](https://docs.databricks.com/aws/en/oltp/projects/logical-replication): both directions, plus the limits on networking and schema.
# MAGIC - [Replicate data from Lakebase](https://docs.databricks.com/aws/en/oltp/projects/logical-replication-publication): the publishing steps this notebook follows.
# MAGIC - [Manage and monitor logical replication](https://docs.databricks.com/aws/en/oltp/projects/logical-replication-manage): sync status and the 40-hour removal of inactive slots.
# MAGIC - [Manage roles](https://docs.databricks.com/aws/en/oltp/projects/manage-roles): password roles and switching password connections on for a project.
# MAGIC - [Postgres logical replication restrictions](https://www.postgresql.org/docs/17/logical-replication-restrictions.html): what Postgres itself doesn't replicate, schema changes included.
