"""The `update_recipe` task group: build the release branch locally.

Runs after `prepare` produced the dependency diff. Everything here is local —
nothing is pushed — so the branch is fully built before `open_pr` publishes it,
and CI then runs exactly once on the final head.

  1. create_worktree — fresh git worktree under /tmp on a `release-<version>`
                       branch, off the clone's default branch
  2. write_recipe    — (Python) fetch the PyPI sdist sha256, apply the diff onto
                       the existing run block, bump version + sha256
  3. rerender        — `conda smithy rerender` (no commit; leaves changes staged)
  4. commit          — ONE commit "<pkg> v<version>" capturing recipe + rerender

The recipe write is a Python @task (YAML/sha/diff logic); the git/smithy I/O are
BashOperators running scripts/*.sh, so the exact commands show in the rendered
template.
"""

from __future__ import annotations

from pathlib import Path

from airflow.sdk import task, task_group
from airflow.providers.standard.operators.bash import BashOperator

from ._common import BASE, ENV
from superreleaser import condaforge, recipe as rcp, registry


@task(task_id="write_recipe")
def write_recipe(worktree: str, diff: dict, **context) -> str:
    """Write version + sha256 + the new run block into the worktree's recipe.
    Returns the worktree path (passthrough, so downstream bash can depend on it)."""
    package = context["params"]["package"]
    version = context["params"]["version"].lstrip("v")
    pypi_name = registry.get(package).pypi_name
    recipe_file = Path(worktree) / "recipe" / "recipe.yaml"
    text = recipe_file.read_text()

    sha = condaforge.sdist_sha256(pypi_name, version)
    if not sha:
        raise RuntimeError(f"no sdist on PyPI for {pypi_name} {version}")

    existing = rcp.current_run_requirements(text)
    new_run = rcp.apply_req_diff(existing, diff)
    recipe_file.write_text(rcp.apply_update(text, version, sha, new_run))
    return worktree


@task_group(group_id="update_recipe", group_display_name="Update feedstock")
def update_recipe(diff):
    """Build the release branch locally (no remote side effects).

    Returns a dict of handles:
      - `create_worktree` (task) — the entry task, to gate the group behind
        prepare and to anchor cleanup;
      - `commit` (task) — the last task, so the next group depends on it;
      - `worktree_path` (XComArg) — the /tmp worktree path.
    """
    # 1. Fresh worktree in /tmp on the release branch.
    create_worktree = BashOperator(
        task_id="create_worktree",
        bash_command="create_worktree.sh",
        env=ENV,
        **BASE,
        doc_md="Create a git worktree under /tmp on a `release-<version>` branch, "
        "off the feedstock clone's default branch.",
    )

    # 2. Write the recipe (Python: sha256 + apply diff + bump version).
    written = write_recipe(create_worktree.output, diff)
    wt = {"WORKTREE": written}

    # 3. Re-render locally (no commit — the commit step captures it).
    rerender = BashOperator(
        task_id="rerender",
        bash_command="rerender.sh",
        env={**ENV, **wt},
        **BASE,
        doc_md="Run `conda smithy rerender` locally, leaving changes uncommitted.",
    )

    # 4. One commit capturing the recipe bump + rerender output.
    commit = BashOperator(
        task_id="commit",
        bash_command="commit.sh",
        env={**ENV, **wt},
        **BASE,
        doc_md="Single commit `<pkg> v<version>` (recipe bump + rerender).",
    )

    create_worktree >> written >> rerender >> commit
    return {
        "create_worktree": create_worktree,
        "commit": commit,
        "worktree_path": create_worktree.output,
    }
