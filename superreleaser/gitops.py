"""git + gh operations for the feedstock. dry_run prints commands instead of
running the mutating ones. Real by default per the decided model (Real PR, no
merge) — nothing here ever merges."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path

log = logging.getLogger("superreleaser.gitops")


def _run(cmd: list[str], *, cwd: Path | None = None, check: bool = True) -> str:
    log.info("$ %s%s", " ".join(cmd), f"  (cwd={cwd})" if cwd else "")
    p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if p.stdout:
        log.info(p.stdout.rstrip())
    if p.returncode != 0:
        log.error(p.stderr.rstrip())
        if check:
            raise RuntimeError(f"command failed: {' '.join(cmd)}\n{p.stderr}")
    return p.stdout.strip()


def diff(fs_dir: Path, path: str = "recipe/recipe.yaml") -> str:
    """`git diff` of one path in the feedstock, for display in a task log.
    Works whether or not the change is committed (diffs the working tree)."""
    return _run(["git", "diff", "--", path], cwd=fs_dir, check=False)


def branch_and_commit(
    fs_dir: Path, branch: str, message: str, *, dry_run: bool
) -> None:
    if dry_run:
        log.info("[dry-run] would: git -C %s checkout -b %s && commit -am '%s'",
                 fs_dir, branch, message)
        return
    _run(["git", "checkout", "-B", branch], cwd=fs_dir)
    _run(["git", "commit", "-am", message], cwd=fs_dir)


def push(fs_dir: Path, branch: str, *, dry_run: bool) -> None:
    if dry_run:
        log.info("[dry-run] would: git -C %s push -u origin %s", fs_dir, branch)
        return
    _run(["git", "push", "-u", "origin", branch], cwd=fs_dir)


def open_pr(
    fs_dir: Path, title: str, body: str, *, draft: bool, dry_run: bool
) -> str:
    flag = "--draft" if draft else ""
    if dry_run:
        log.info("[dry-run] would: gh pr create %s --title '%s' (body %d chars)",
                 flag, title, len(body))
        return f"https://github.com/conda-forge/<feedstock>/pull/DRYRUN{'-draft' if draft else ''}"
    cmd = ["gh", "pr", "create", "--title", title, "--body", body]
    if draft:
        cmd.append("--draft")
    return _run(cmd, cwd=fs_dir)


def comment(fs_dir: Path, pr_url: str, body: str, *, dry_run: bool) -> None:
    if dry_run:
        log.info("[dry-run] would comment on %s:\n%s", pr_url, body)
        return
    _run(["gh", "pr", "comment", pr_url, "--body", body], cwd=fs_dir)


def pr_checks_state(pr_url: str) -> str:
    """SUCCESS | FAILURE | PENDING — aggregate of the PR's status checks (on the
    current head commit; the rollup follows the branch head)."""
    out = _run(
        ["gh", "pr", "view", pr_url, "--json", "statusCheckRollup", "--jq",
         '[.statusCheckRollup[].conclusion] '
         '| if length == 0 then "PENDING" '
         'elif any(. == "FAILURE" or . == "CANCELLED" or . == "TIMED_OUT") then "FAILURE" '
         'elif all(. == "SUCCESS" or . == "NEUTRAL" or . == "SKIPPED") then "SUCCESS" '
         'else "PENDING" end'],
        check=False,
    )
    return out or "PENDING"


def pr_head_sha(pr_url: str) -> str:
    """The PR branch's current head commit SHA."""
    return _run(["gh", "pr", "view", pr_url, "--json", "headRefOid", "--jq",
                 ".headRefOid"], check=False)


def pr_last_commit_is_bot(pr_url: str) -> bool:
    """True if the PR's latest commit was authored by a conda-forge bot — i.e.
    the rerender has landed. The rerender commit is authored by
    `conda-forge-webservices[bot]`."""
    author = _run(
        ["gh", "pr", "view", pr_url, "--json", "commits", "--jq",
         '.commits[-1].authors[0].name // ""'],
        check=False,
    )
    return "conda-forge" in author.lower() or author.endswith("[bot]")


def merge_pr(fs_dir: Path, pr_url: str, *, dry_run: bool) -> None:
    """Merge the feedstock PR (squash). Real merges touch conda-forge → dry-run
    prints instead."""
    if dry_run:
        log.info("[dry-run] WOULD MERGE (squash): %s", pr_url)
        return
    _run(["gh", "pr", "merge", pr_url, "--squash"], cwd=fs_dir)


def branch_checks_state(fs_dir: Path, branch: str) -> str:
    """SUCCESS | FAILURE | PENDING for the latest commit on `branch` (e.g. the
    default branch after a merge). Uses the commit-status/check-runs rollup so
    it works off a branch name rather than a PR."""
    slug = _run(["gh", "repo", "view", "--json", "nameWithOwner", "--jq",
                 ".nameWithOwner"], cwd=fs_dir, check=False)
    out = _run(
        ["gh", "api", f"repos/{slug}/commits/{branch}/check-runs", "--jq",
         '[.check_runs[].conclusion] '
         '| if length == 0 then "PENDING" '
         'elif any(. == null) then "PENDING" '
         'elif any(. == "failure" or . == "cancelled" or . == "timed_out") then "FAILURE" '
         'elif all(. == "success" or . == "neutral" or . == "skipped") then "SUCCESS" '
         'else "PENDING" end'],
        cwd=fs_dir, check=False,
    )
    return out or "PENDING"
