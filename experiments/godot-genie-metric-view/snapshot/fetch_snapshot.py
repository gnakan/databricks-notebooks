#!/usr/bin/env python3
"""Pull a pinned twelve-month delivery snapshot of godotengine/godot.

Writes CSV files under deliverables/snapshot/ that the notebook loads into Delta
tables. Every row describes code areas, release lines and author types; no
contributor login, name, branch name or PR title is written, because the piece
is about the Genie Agent, not a grade for Godot's maintainers.

Sources (all REST, authenticated with the `gh` CLI token):
  pulls?state=closed&sort=updated      merged PRs (the search API caps at 1,000)
  releases                             stable release tags and publish times
  compare/{base}...{head}              commits per release on the 4.6, 4.7, 3.6 lines
  commits/{sha}/pulls                  master PR behind each cherry-pick trailer
  actions/workflows/runner.yml/runs    CI runs with their attempt counts

Usage:
  python3 fetch_snapshot.py --as-of 2026-10-05 [--out DIR] [--cache DIR]

Auth: GITHUB_TOKEN when set, otherwise `gh auth token`.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import http.client
import datetime as dt
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = "godotengine/godot"
API = "https://api.github.com"
RUNNER_WORKFLOW = "runner.yml"  # the umbrella "GHA" workflow that calls every build
# Release lines the compares cover, each as (previous tag, tag) pairs in order.
COMPARES = [
    ("4.5-stable", "4.6-stable"),
    ("4.6-stable", "4.6.1-stable"),
    ("4.6.1-stable", "4.6.2-stable"),
    ("4.6.2-stable", "4.6.3-stable"),
    ("4.6-stable", "4.7-stable"),
    ("4.7-stable", "4.7.1-stable"),
    ("4.7.1-stable", "4.7.2-stable"),
    ("3.6.1-stable", "3.6.2-stable"),
    ("3.6.2-stable", "3.6.3-stable"),
]
CHERRY_RE = re.compile(r"\(cherry picked from commit ([0-9a-f]{7,40})\)")
KEPT_BRANCH_RE = re.compile(r"^(master|\d+\.(\d+|x)(\.\d+)?(-stable)?)$")
MERGE_PR_RE = re.compile(r"^Merge pull request #(\d+)")
TOPIC_PREFIXES = ("topic:", "platform:")
KIND_LABELS = {"bug", "enhancement", "regression", "crash", "performance", "documentation",
               "usability", "feature proposal", "discussion"}

log = logging.getLogger("fetch_snapshot")


def gh_token() -> str:
    """Return GITHUB_TOKEN when set (the notebook's refresh cell), else the gh CLI's token."""
    if os.environ.get("GITHUB_TOKEN"):
        return os.environ["GITHUB_TOKEN"]
    try:
        return subprocess.run(["gh", "auth", "token"], check=True, capture_output=True,
                              text=True).stdout.strip()
    except subprocess.CalledProcessError as exc:
        log.error("gh auth token failed: %s", exc.stderr.strip())
        raise


class GitHub:
    """Minimal REST client with on-disk caching and rate-limit backoff."""

    def __init__(self, token: str, cache: Path):
        """Store the token and create the cache directory."""
        self.token = token
        self.cache = cache
        self.cache.mkdir(parents=True, exist_ok=True)
        self.calls = 0

    def _cache_path(self, url: str) -> Path:
        """Map a URL to a cache file name."""
        safe = re.sub(r"[^A-Za-z0-9]+", "_", url.replace(API, ""))[:200]
        return self.cache / f"{safe}.json"

    def get(self, path: str, use_cache: bool = True) -> object:
        """GET one API path, returning parsed JSON; retries on rate limits and 5xx."""
        url = path if path.startswith("http") else f"{API}{path}"
        cp = self._cache_path(url)
        if use_cache and cp.exists():
            return json.loads(cp.read_text())
        for attempt in range(6):
            req = urllib.request.Request(url, headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            })
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    body = json.loads(resp.read())
                    self.calls += 1
                    if use_cache:
                        cp.write_text(json.dumps(body))
                    return body
            except urllib.error.HTTPError as exc:
                if exc.code in (403, 429):
                    reset = exc.headers.get("x-ratelimit-reset")
                    retry = exc.headers.get("retry-after")
                    wait = int(retry) if retry else max(5, int(reset or 0) - int(time.time()) + 2)
                    wait = min(wait, 900)
                    log.warning("rate limited on %s, sleeping %ss", url, wait)
                    time.sleep(wait)
                    continue
                if exc.code >= 500:
                    log.warning("HTTP %s on %s, retry %s", exc.code, url, attempt + 1)
                    time.sleep(2 ** attempt)
                    continue
                log.error("HTTP %s on %s", exc.code, url)
                raise
            except (urllib.error.URLError, TimeoutError, http.client.IncompleteRead, ConnectionError) as exc:
                log.warning("network error on %s: %s", url, exc)
                time.sleep(2 ** attempt)
        raise RuntimeError(f"giving up on {url}")


def iso(s: str | None) -> dt.datetime | None:
    """Parse a GitHub timestamp."""
    return dt.datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def author_type(user: dict | None) -> str:
    """Classify an account as human or bot without recording who it is."""
    if not user:
        return "unknown"
    return "bot" if user.get("type") == "Bot" or user.get("login", "").endswith("[bot]") else "human"


def fetch_prs(gh: GitHub, start: dt.datetime, end: dt.datetime) -> list[dict]:
    """Page closed PRs by update time until updates fall before the window."""
    rows: dict[int, dict] = {}
    page = 1
    while True:
        batch = gh.get(f"/repos/{REPO}/pulls?state=closed&sort=updated&direction=desc"
                       f"&per_page=100&page={page}")
        if not batch:
            break
        for pr in batch:
            merged = iso(pr.get("merged_at"))
            if merged and start <= merged < end:
                labels = [lb["name"] for lb in pr.get("labels", [])]
                areas = sorted(lb for lb in labels if lb.startswith(TOPIC_PREFIXES))
                rows[pr["number"]] = {
                    "pr_number": pr["number"],
                    "created_at": pr["created_at"],
                    "merged_at": pr["merged_at"],
                    "base_branch": pr["base"]["ref"],
                    "author_type": author_type(pr.get("user")),
                    "author_association": pr.get("author_association"),
                    "milestone": (pr.get("milestone") or {}).get("title"),
                    "areas": areas,
                    "kinds": sorted(lb for lb in labels if lb in KIND_LABELS),
                    "cherrypick_labels": sorted(lb for lb in labels if lb.startswith("cherrypick:")),
                    "merge_commit_sha": pr.get("merge_commit_sha"),
                }
        oldest_update = iso(batch[-1]["updated_at"])
        log.info("pulls page %s: %s merged in window so far (oldest update %s)",
                 page, len(rows), oldest_update.date())
        if oldest_update < start:
            break
        page += 1
    return sorted(rows.values(), key=lambda r: r["pr_number"])


def fetch_releases(gh: GitHub) -> list[dict]:
    """Every stable GitHub release with its line, type and publish time."""
    out = []
    for page in (1, 2):
        for rel in gh.get(f"/repos/{REPO}/releases?per_page=100&page={page}"):
            tag = rel["tag_name"]
            m = re.match(r"^(\d+)\.(\d+)(?:\.(\d+))?-stable$", tag)
            if not m or rel.get("prerelease") or rel.get("draft"):
                continue
            out.append({
                "release_tag": tag,
                "release_line": f"{m.group(1)}.{m.group(2)}",
                "version": tag.replace("-stable", ""),
                "release_type": "patch" if m.group(3) else "feature",
                "published_at": rel["published_at"],
            })
    return sorted(out, key=lambda r: r["published_at"])


def fetch_compare(gh: GitHub, base: str, head: str) -> list[dict]:
    """All commits reachable from head and not from base, paginated."""
    commits, page = [], 1
    while True:
        body = gh.get(f"/repos/{REPO}/compare/{base}...{head}?per_page=100&page={page}")
        batch = body.get("commits", [])
        commits.extend(batch)
        if page == 1:
            log.info("compare %s...%s: %s commits total", base, head, body.get("total_commits"))
        if len(batch) < 100:
            break
        page += 1
    return commits


def release_commit_rows(tag: str, commits: list[dict]) -> list[dict]:
    """One row per commit in a release, typed merge / cherry_pick / direct."""
    rows = []
    for c in commits:
        msg = c["commit"]["message"]
        first = msg.splitlines()[0] if msg else ""
        picked = CHERRY_RE.findall(msg)
        merge = MERGE_PR_RE.match(first)
        if merge:
            kind, pr = "merge", int(merge.group(1))
        elif picked:
            kind, pr = "cherry_pick", None
        else:
            kind, pr = "direct", None
        rows.append({
            "release_tag": tag,
            "commit_sha": c["sha"],
            "committed_at": c["commit"]["committer"]["date"],
            "is_merge_commit": len(c.get("parents", [])) > 1,
            "commit_kind": kind,
            "pr_number": pr,
            "cherry_picked_from_sha": picked[-1] if picked else None,
            "author_type": author_type(c.get("author")),
        })
    return rows


def resolve_cherry_picks(gh: GitHub, shas: set[str]) -> dict[str, int | None]:
    """Map each cherry-picked source sha to the master PR it came from."""

    def one(sha: str) -> tuple[str, int | None]:
        """Resolve a single sha through commits/{sha}/pulls."""
        try:
            pulls = gh.get(f"/repos/{REPO}/commits/{sha}/pulls")
        except urllib.error.HTTPError as exc:
            log.error("commits/%s/pulls failed: %s", sha, exc)
            return sha, None
        merged = [p for p in pulls if p.get("merged_at") and p["base"]["ref"] == "master"]
        merged = merged or [p for p in pulls if p.get("merged_at")]
        return sha, (merged[0]["number"] if merged else None)

    out = {}
    with cf.ThreadPoolExecutor(max_workers=6) as pool:
        for i, (sha, pr) in enumerate(pool.map(one, sorted(shas)), 1):
            out[sha] = pr
            if i % 200 == 0:
                log.info("resolved %s/%s cherry-picks", i, len(shas))
    return out


def fetch_ci_runs(gh: GitHub, start: dt.datetime, end: dt.datetime) -> list[dict]:
    """Runner workflow runs, queried a week at a time to stay under the listing cap."""

    def week(day: dt.datetime) -> list[dict]:
        """Every runner run created in the seven days from `day`."""
        nxt = min(day + dt.timedelta(days=7), end)
        rng = f"{day:%Y-%m-%dT%H:%M:%SZ}..{nxt - dt.timedelta(seconds=1):%Y-%m-%dT%H:%M:%SZ}"
        out, page = [], 1
        while True:
            body = gh.get(f"/repos/{REPO}/actions/workflows/{RUNNER_WORKFLOW}/runs"
                          f"?created={rng}&per_page=100&page={page}")
            batch = body.get("workflow_runs", [])
            if page == 1 and body.get("total_count", 0) >= 1000:
                log.warning("week %s holds %s runs; listing may truncate", day.date(), body["total_count"])
            out.extend(batch)
            if len(batch) < 100:
                break
            page += 1
        log.info("ci runs week of %s: %s", day.date(), len(out))
        return out

    weeks, day = [], start
    while day < end:
        weeks.append(day)
        day += dt.timedelta(days=7)
    rows: dict[int, dict] = {}
    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        for batch in pool.map(week, weeks):
            for r in batch:
                push = r["event"] == "push"
                prs = r.get("pull_requests") or []
                rows[r["id"]] = {
                    "run_id": r["id"],
                    "event": r["event"],
                    # Only master and version branches are kept; anything else pushed to the repo,
                    # and every PR head branch, is a branch someone named for themselves.
                    "branch": (r["head_branch"] if push and KEPT_BRANCH_RE.match(r["head_branch"] or "")
                               else ("other" if push else None)),
                    "pr_number": prs[0]["number"] if prs else None,
                    "head_sha": r["head_sha"],
                    "status": r["status"],
                    "conclusion": r["conclusion"],
                    "run_attempt": r.get("run_attempt"),
                    "created_at": r["created_at"],
                    "run_started_at": r.get("run_started_at"),
                    "updated_at": r["updated_at"],
                }
    return sorted(rows.values(), key=lambda r: r["created_at"])


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    """Write rows to CSV with a fixed column order."""
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: ("" if r.get(k) is None else r[k]) for k in fields})
    log.info("wrote %s (%s rows)", path, len(rows))


def main() -> int:
    """Pull every source and write the snapshot CSVs."""
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--as-of", required=True, help="pin date, YYYY-MM-DD (window is the 12 months before it)")
    ap.add_argument("--out", type=Path, default=Path(__file__).resolve().parent,
                    help="where the CSVs go (default: beside this script)")
    ap.add_argument("--cache", type=Path, default=Path(tempfile.gettempdir()) / "godot-snapshot-cache",
                    help="response cache, so an interrupted pull resumes")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    end = dt.datetime.fromisoformat(args.as_of).replace(tzinfo=dt.timezone.utc)
    start = end.replace(year=end.year - 1)
    args.out.mkdir(parents=True, exist_ok=True)
    gh = GitHub(gh_token(), args.cache)

    prs = fetch_prs(gh, start, end)
    releases = [r for r in fetch_releases(gh) if start <= iso(r["published_at"]) < end]

    commit_rows = []
    for base, head in COMPARES:
        commit_rows += release_commit_rows(head, fetch_compare(gh, base, head))
    picks = {r["cherry_picked_from_sha"] for r in commit_rows if r["cherry_picked_from_sha"]}
    log.info("%s cherry-pick trailers to resolve", len(picks))
    resolved = resolve_cherry_picks(gh, picks)
    for r in commit_rows:
        if r["commit_kind"] == "cherry_pick":
            r["pr_number"] = resolved.get(r["cherry_picked_from_sha"])

    runs = fetch_ci_runs(gh, start, end)

    write_csv(args.out / "pull_requests.csv", prs,
              ["pr_number", "created_at", "merged_at", "base_branch", "author_type",
               "author_association", "milestone", "merge_commit_sha"])
    area_rows = [{"pr_number": p["pr_number"], "label_kind": a.split(":", 1)[0], "code_area": a.split(":", 1)[1]}
                 for p in prs for a in p["areas"]]
    write_csv(args.out / "pr_code_areas.csv", area_rows, ["pr_number", "label_kind", "code_area"])
    kind_rows = [{"pr_number": p["pr_number"], "change_kind": k} for p in prs for k in p["kinds"]]
    write_csv(args.out / "pr_change_kinds.csv", kind_rows, ["pr_number", "change_kind"])
    write_csv(args.out / "releases.csv", releases,
              ["release_tag", "release_line", "version", "release_type", "published_at"])
    write_csv(args.out / "release_commits.csv", commit_rows,
              ["release_tag", "commit_sha", "committed_at", "is_merge_commit", "commit_kind",
               "pr_number", "cherry_picked_from_sha", "author_type"])
    write_csv(args.out / "ci_runs.csv", runs,
              ["run_id", "event", "branch", "pr_number", "head_sha", "status", "conclusion",
               "run_attempt", "created_at", "run_started_at", "updated_at"])

    meta = {
        "repo": REPO,
        "as_of": args.as_of,
        "window_start": start.date().isoformat(),
        "window_end": end.date().isoformat(),
        "compares": [f"{b}...{h}" for b, h in COMPARES],
        "counts": {"pull_requests": len(prs), "releases": len(releases),
                   "release_commits": len(commit_rows), "cherry_picks_resolved":
                   sum(1 for v in resolved.values() if v), "cherry_picks_total": len(resolved),
                   "ci_runs": len(runs)},
    }
    (args.out / "snapshot.json").write_text(json.dumps(meta, indent=2) + "\n")
    log.info("done: %s", json.dumps(meta["counts"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
