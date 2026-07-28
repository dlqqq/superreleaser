"""Publish phase: open the PR, and the poke-callables for the CI / merge / CDN
waits, plus the merge itself.

The `_ok`/`_pass` functions are plain callables (not `@task`) because they back
`PythonSensor`s, which the DAG file constructs; the sensors' display names and
docs live on those operators. `open_pr` and `merge_pr` are `@task`s."""

from __future__ import annotations

from airflow.sdk import task
from airflow.exceptions import AirflowFailException

from superreleaser import condaforge, config, gitops

from ._common import log


@task(task_display_name="Open feedstock PR")
def open_pr(ctx: dict) -> dict:
    """Push a branch and open the feedstock PR, then comment `@conda-forge-admin,
    please rerender` to trigger the rerender. Opens as a **draft** (with a
    comment naming each unresolvable dependency) if any name was unresolved; a
    normal PR otherwise. Does not merge."""
    package, target = ctx["package"], ctx["target"]
    fs = config.feedstock_dir(package)
    branch = f"update-to-{target}"
    title = f"{package} v{target}"
    draft = bool(ctx["unresolved"])  # unresolved dep → draft for human

    body = f"Update `{package}` to `{target}` (sha256 `{ctx['sha256'][:12]}…`).\n\n"
    body += "Run requirements derived from the released package's PyPI metadata.\n"
    if ctx["unsatisfied"]:
        body += "\n⚠️ Version ranges with no matching conda-forge build yet:\n"
        body += "".join(f"- `{u}`\n" for u in ctx["unsatisfied"])

    gitops.branch_and_commit(fs, branch, title, dry_run=ctx["dry_run"])
    gitops.push(fs, branch, dry_run=ctx["dry_run"])
    pr_url = gitops.open_pr(fs, title, body, draft=draft, dry_run=ctx["dry_run"])

    # Rerender is required for conda-forge recipe changes.
    gitops.comment(fs, pr_url, "@conda-forge-admin, please rerender", dry_run=ctx["dry_run"])

    # For each unresolved dep, a comment naming the pypi-name so a human can add
    # the mapping. This is the "track state → comment on failure" path.
    for dep in ctx["unresolved"]:
        gitops.comment(
            fs, pr_url,
            f"⚠️ Could not find a corresponding conda-forge package for "
            f"PyPI dependency **`{dep}`**. This dependency was NOT added to "
            f"`requirements.run`. A maintainer needs to add the correct "
            f"conda-forge name manually before merging.",
            dry_run=ctx["dry_run"],
        )
    log.info("PR: %s (draft=%s)", pr_url, draft)
    return {**ctx, "pr_url": pr_url, "draft": draft}


@task(task_display_name="Merge PR")
def merge_pr(ctx: dict) -> dict:
    """Runs only after approval: squash-merge the feedstock PR. This is the
    destructive conda-forge action — gated behind the human. Skipped/printed in
    dry-run."""
    fs = config.feedstock_dir(ctx["package"])
    gitops.merge_pr(fs, ctx["pr_url"], dry_run=ctx["dry_run"])
    log.info("merged %s", ctx["pr_url"])
    return ctx


# --------------------------------------------------------------------------- #
# Sensor poke-callables (wired to PythonSensors in the DAG file)
# --------------------------------------------------------------------------- #
def checks_pass(ctx: dict) -> bool:
    """True only once the rerender has landed AND checks are green on that
    rerendered head.

    A `@conda-forge-admin, please rerender` on a recipe change makes the
    conda-forge bot push a new commit, which restarts CI. So we must NOT accept
    the brief green that can appear on our own commit before the rerender pushes
    — we first wait for the rerender commit to become the branch head (authored
    by `conda-forge-webservices[bot]`), then require green on it. (Edge: a no-op
    rerender comments instead of committing; a version bump always changes the
    recipe, so that path doesn't occur here.)"""
    if ctx["dry_run"]:
        log.info("[dry-run] skipping CI + rerender wait")
        return True
    pr = ctx["pr_url"]
    if not gitops.pr_last_commit_is_bot(pr):
        log.info("waiting for conda-forge rerender commit (head=%s)",
                 gitops.pr_head_sha(pr)[:8])
        return False
    state = gitops.pr_checks_state(pr)
    log.info("rerender landed (head=%s); CI state: %s",
             gitops.pr_head_sha(pr)[:8], state)
    if state == "FAILURE":
        raise AirflowFailException(f"feedstock CI failed: {pr}")
    return state == "SUCCESS"


def post_merge_ci_ok(ctx: dict) -> bool:
    """Wait for the default branch's post-merge build to settle, then raise if it
    failed. conda-forge builds/uploads the package from the default branch after
    merge; a red build there means the release didn't actually ship, so it must
    fail the run rather than pass silently."""
    if ctx["dry_run"]:
        log.info("[dry-run] skipping post-merge CI check")
        return True
    fs = config.feedstock_dir(ctx["package"])
    state = gitops.branch_checks_state(fs, ctx["default_branch"])
    log.info("post-merge CI on %s: %s", ctx["default_branch"], state)
    if state == "FAILURE":
        raise AirflowFailException(
            f"post-merge build failed on {ctx['default_branch']} for {ctx['pr_url']}"
        )
    return state == "SUCCESS"


def available_on_conda_forge(ctx: dict) -> bool:
    """Wait for the merged version to appear on the conda-forge channel. Only
    runs after a GREEN post-merge build — so we KNOW the package was uploaded and
    this is pure CDN/repodata propagation (~30 min historically), not a "will it
    ever ship" question. The sensor timeout therefore means "propagation is
    abnormally slow", an anomaly to surface, not an indefinite maybe."""
    if ctx["dry_run"]:
        log.info("[dry-run] skipping conda-forge availability wait")
        return True
    conda = ctx.get("conda_name") or ctx["package"]
    ok = condaforge.is_published(conda, ctx["target"])
    log.info("conda-forge availability %s==%s: %s", conda, ctx["target"],
             "available" if ok else "not yet (propagating)")
    return ok
