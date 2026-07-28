"""Prepare phase: get the feedstock, choose the version, update + verify the recipe."""

from __future__ import annotations

from airflow.sdk import task
from airflow.exceptions import AirflowFailException, AirflowSkipException

from superreleaser import condaforge, config, gitops, recipe as rcp

from ._common import conf, log


@task(task_display_name="Checkout feedstock")
def checkout(**context) -> dict:
    """Sync the `<package>-feedstock` submodule to its remote default branch so
    we branch off current state. Fails if the feedstock isn't checked out
    locally."""
    package = conf(context, "package") or context["params"]["package"]
    dry_run = bool(conf(context, "dry_run", context["params"]["dry_run"])
                   or config.DRY_RUN_DEFAULT)
    fs = config.feedstock_dir(package)
    if not fs.exists():
        raise AirflowFailException(f"feedstock not found: {fs}")
    gitops._run(["git", "fetch", "origin"], cwd=fs, check=False)
    head = gitops._run(
        ["git", "symbolic-ref", "refs/remotes/origin/HEAD"], cwd=fs, check=False
    ) or "refs/remotes/origin/main"
    default_branch = head.rsplit("/", 1)[-1]
    if not dry_run:
        gitops._run(["git", "checkout", default_branch], cwd=fs, check=False)
        gitops._run(["git", "reset", "--hard", f"origin/{default_branch}"], cwd=fs, check=False)
    log.info("feedstock %s on %s (dry_run=%s)", package, default_branch, dry_run)
    return {"package": package, "dry_run": dry_run, "default_branch": default_branch}


@task(task_display_name="Identify version")
def pick_version(ctx: dict, **context) -> dict:
    """Pick the **earliest stable** PyPI version missing from the feedstock (one
    bump per PR, conda-forge convention; prereleases skipped). Skips the run if
    the feedstock is already current. Override with the `version` trigger param."""
    package = ctx["package"]
    recipe_text = config.recipe_path(package).read_text()
    pypi_project = rcp.pypi_name(recipe_text)
    cur = rcp.current_version(recipe_text)
    forced = conf(context, "version") or context["params"]["version"]
    target = forced or rcp.earliest_missing_stable(pypi_project, cur)
    if not target:
        raise AirflowSkipException(
            f"{package}: feedstock at {cur} is already current on PyPI"
        )
    conda_name = rcp.conda_package_name(recipe_text)
    log.info("%s: feedstock=%s -> target=%s (pypi=%s, conda=%s)",
             package, cur, target, pypi_project, conda_name)
    return {**ctx, "pypi_project": pypi_project, "current": cur,
            "target": target, "conda_name": conda_name}


@task(task_display_name="Update recipe")
def update_recipe(ctx: dict) -> dict:
    """Set `context.version`, `source.sha256` (from the PyPI sdist), and rewrite
    `requirements.run` from the released package's own metadata. conda-forge
    names for **new** deps are probed against anaconda.org, never guessed;
    unresolvable ones are tracked (not added) for the draft-PR + comment path.
    Logs the full `git diff` of the recipe."""
    package, target, pypi_project = ctx["package"], ctx["target"], ctx["pypi_project"]
    recipe_file = config.recipe_path(package)
    fs = config.feedstock_dir(package)
    text = recipe_file.read_text()

    sha = condaforge.sdist_sha256(pypi_project, target)
    if not sha:
        raise AirflowFailException(f"no sdist on PyPI for {pypi_project} {target}")

    existing = rcp.current_run_requirements(text)
    mapping = rcp.map_dependencies(pypi_project, target, existing)
    new_text = rcp.apply_update(text, target, sha, mapping["run"])

    # Always write so we can show a real `git diff`; in dry-run we restore the
    # file afterward so nothing is left changed on disk.
    recipe_file.write_text(new_text)
    diff = gitops.diff(fs, "recipe/recipe.yaml")
    log.info("git diff recipe/recipe.yaml:\n%s", diff or "(no changes)")
    if ctx["dry_run"]:
        gitops._run(["git", "checkout", "--", "recipe/recipe.yaml"], cwd=fs, check=False)
        log.info("[dry-run] reverted recipe on disk (diff shown above only)")

    if mapping["unresolved"]:
        log.warning("UNRESOLVED conda-forge deps (will draft + comment): %s",
                    mapping["unresolved"])
    return {**ctx, "sha256": sha, "diff": diff, **mapping}


@task(task_display_name="Verify recipe dependencies")
def verify_cf(ctx: dict) -> dict:
    """For every `requirements.run` entry: confirm the conda-forge package
    **exists** AND the required **version range resolves** to at least one real
    build. Does not hard-fail — entries that don't resolve are recorded as
    `unsatisfied` and surfaced in the PR body and the approval gate, so a human
    sees the gap."""
    unsatisfied = rcp.verify_run(ctx["run"])
    log.info("verify: %d run deps, %d unresolved names, %d unsatisfied ranges",
             len(ctx["run"]), len(ctx["unresolved"]), len(unsatisfied))
    for u in unsatisfied:
        log.warning("no conda-forge build satisfies: %s", u)
    return {**ctx, "unsatisfied": unsatisfied}
