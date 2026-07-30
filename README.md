# Superreleaser — human-gated release orchestrator (Apache Airflow)

Releases a Jupyter extension package end-to-end — GitHub release → PyPI →
conda-forge — as human-gated Airflow 3 DAGs. You pick a package + version,
review at the gates **in the Airflow UI**, and it drives the rest.

> Airflow was chosen for its **first-class Human-in-the-Loop** support
> (`ApprovalOperator`, `awaiting_input` state) introduced in Airflow 3.1.

## Three DAGs

`e2e_release` and `cf_release` release **one** package: both take a `package`
(dropdown, from the registry) and an explicit `version`. `simple_jai_release`
releases a whole **wave** of Jupyter AI subpackages plus `jupyter-ai` itself.

**`e2e_release`** — the full release. Runs the repo's `Step 1: Prep Release` and
`Step 2: Publish Release` workflows with a human gate between them, waits for
PyPI, then hands off to `cf_release`:

| Task | Step |
|------|------|
| `prep_release` | `gh workflow run "Step 1: Prep Release"`, watch it, capture the draft-release URL. |
| `build_release_gate` + `approval` | Show the draft URL **and its changelog body**; Approve → publish, Reject → stop + delete the draft. |
| `publish_release` | `gh workflow run "Step 2: Publish Release"` with the draft URL (publishes to PyPI). |
| `await_pypi` | `@task.sensor` — poll PyPI until the version's metadata is available. |
| `conda_forge_release` | The `cf_release` group, entered once the version is live on PyPI. |
| `delete_rejected_draft` | `@task.run_if` — if a draft was made but never published, delete it (skipped on approval). |

**`cf_release`** — the conda-forge half (also runnable on its own if the version
is already on PyPI):

| Task group | Step |
|------|------|
| `Prepare` | Clone + fork the feedstock, compute the `requirements.run` diff from the released package's metadata, verify each dep exists on conda-forge. |
| `Update feedstock` | Worktree in `/tmp`, apply the diff + bump version/sha256, `conda smithy rerender`, single commit. |
| `Open feedstock PR` | Push the finished branch, open the PR titled `<pkg> v<version>` (with the conda-forge checklist). |
| `approval` → `wait_for_ci` | Human approves in the UI, then `gh pr checks --watch` blocks until CI is green. |
| `Publish on Conda Forge` | Squash-merge as `<pkg> v<version> (#N)`, then poll `conda search` until downloadable. |
| `Clean up` | Delete the worktree, close the PR if still open, delete the fork branch (all best-effort, always run). |

**`simple_jai_release`** — a whole Jupyter AI release in one run: every listed
subpackage in **parallel**, then `jupyter-ai` itself. Takes a `subpackages` array
and the `jupyter_ai_version` to cut:

```json
{
  "subpackages": [
    {"package": "jupyter-ai-router",  "version": "0.0.6"},
    {"package": "jupyter-ai-litellm", "version": "0.0.3", "range": ">=0.0.3,<0.1.0"}
  ],
  "jupyter_ai_version": "3.2.0"
}
```

| Phase | Step |
|-------|------|
| `plan_release` | Validate every entry, then check PyPI: a subpackage already at its version is dropped from the release list (but still range-bumped below), so a partially-completed wave can be re-run. |
| `release_subpackage` (mapped) | The **full `e2e_release` flow per subpackage, in parallel** — each with its own PyPI and conda-forge gates. Phase B waits for all of them. |
| `bump_ranges` | Rewrite jupyter-ai's `pyproject.toml` so each subpackage's range names the version just published, open a PR **from your fork** (labelled `maintenance`), gate, `wait_for_ci`, squash-merge. Skips itself when no range actually changes. |
| `release_docs` | `Step 0: Prep release documentation` → gate on the generated changelog → merge the docs PR (Step 1 builds from the merged branch). |
| `pypi_release` → `conda_forge_release` | jupyter-ai's own end-to-end release, the same two groups the single-package DAGs use. |

`range` controls what lands in `pyproject.toml`: `"auto"` (the default) raises the
**floor** to the released version and keeps the existing ceiling; an explicit spec
is used verbatim. An `auto` bump whose floor would collide with the existing
ceiling **fails the task** rather than writing a range that excludes the very
version being released — set an explicit `range` for that (breaking) case.

The `package` name, its GitHub repo, PyPI name, conda-forge name, and feedstock
repo are maintained in `superreleaser/registry.py` (none are derivable from each
other — e.g. `jupyterlab-chat` lives in `jupyterlab/jupyter-chat`).

### The dependency-mapping problem (the hard part)

conda-forge package names are **not** a deterministic transform of PyPI names —
verified against `api.anaconda.org`:

| PyPI name | conda-forge name |
|-----------|------------------|
| `jupyter-server` | `jupyter_server` (**underscore**) |
| `jupyterlab-chat` | `jupyterlab-chat` (hyphen) |
| `agent-client-protocol` | `agent-client-protocol` (hyphen) |

So names are **probed, never guessed** (`superreleaser/condaforge.py::resolve_conda_name`):

1. A dep **already in the recipe** keeps its (correct) conda name; only its
   version range is refreshed.
2. A **new** dep is looked up on conda-forge (as-is, hyphenated, underscored).
   Found → added. **Not found → tracked as `unresolved`**: the dep is *not*
   added, the PR is opened as a **draft**, and a comment is posted naming the
   PyPI dependency for a maintainer to map by hand.

This "continue but track state, then comment on the PR" behavior is the core
requirement — a missing mapping never silently drops a dependency or aborts the
release.

## Layout

```
superreleaser/
├── dags/
│   ├── e2e_release.py        # pypi_release() >> conda_forge_release()
│   ├── cf_release.py         # conda_forge_release() only
│   ├── simple_jai_release.py # mapped subpackage wave, then jupyter-ai
│   ├── .airflowignore        # marks cf_tasks/ as library code, not DAGs
│   └── cf_tasks/             # task groups (one module each) + their scripts
│       ├── pypi.py           # PyPI half: Step 1 → gate → Step 2 → await PyPI
│       ├── conda_forge.py    # conda-forge half: wraps the phase groups below
│       ├── prepare.py        # clone/fork, dep diff, verify on conda-forge
│       ├── update_recipe.py  # worktree, write recipe, rerender, commit
│       ├── open_pr.py        # push branch, open PR
│       ├── publish.py        # merge + await conda-forge availability
│       ├── cleanup.py        # delete worktree / close PR / delete fork branch
│       ├── bump_ranges.py    # jupyter-ai range-bump PR: edit → gate → merge
│       ├── release_docs.py   # Step 0 docs PR: run → gate → merge
│       ├── _common.py        # bootstrap, the `identity` task, registry macros
│       └── scripts/          # the bash step scripts (*.sh)
├── superreleaser/            # Airflow-independent logic (unit-testable)
│   ├── registry.py           # package → repo / pypi / conda-forge / feedstock
│   ├── config.py             # feedstock + source-clone paths
│   ├── condaforge.py         # anaconda.org + PyPI probes, name resolution
│   ├── pyproject.py          # jupyter-ai dependency-range editing
│   └── recipe.py             # recipe read/edit + dep mapping
├── tests/                    # unit tests (no network)
└── pixi.toml                 # conda-forge deps + tasks (start / test / release)
```

Every task group is parameterized by an **`ident`** (the `identity` task, which
resolves a package's registry names once) rather than by `params.package`. That's
what lets the same groups serve the single-package DAGs *and* run inside
`simple_jai_release`'s mapped group, where each expansion releases a different
package.

Bash I/O lives in `scripts/*.sh` (shown in each task's rendered template);
application logic (recipe edits, dep diff) is Python. Each `@task`'s docstring
becomes its description in the Airflow UI.

## Run it locally (single user, SQLite)

Managed entirely with [`pixi`](https://pixi.sh) — every dependency (Airflow,
conda-smithy, conda, jq, …) comes from conda-forge, so there's no separate
Python venv to manage and `conda-smithy` stays current. You also need a `gh` CLI
authenticated in your shell.

```bash
pixi install                                  # create the conda-forge env
pixi run start                                # Airflow all-in-one on SQLite; UI at http://localhost:8080
```

`pixi run start` disables auth (localhost only), so there's no login. It runs in
the foreground — leave it up. Then, from another shell:

```bash
pixi run release jupyter-ai-acp-client 0.2.1     # full e2e release
pixi run cf-release jupyter-ai-acp-client 0.2.1   # conda-forge only (already on PyPI)

# A whole Jupyter AI release: the wave in parallel, then jupyter-ai 3.2.0.
pixi run jai-release 3.2.0 '[{"package":"jupyter-ai-router","version":"0.0.6"}]'
```

or trigger any of them from the UI with a conf — `{ "package":
"jupyter-ai-acp-client", "version": "0.2.1" }` for the single-package DAGs, or the
`subpackages` + `jupyter_ai_version` object shown above for `simple_jai_release`.

Other tasks: `pixi run test` (unit tests), `pixi run reset` (wipe the local
Airflow home).

`version` is explicit. For `e2e_release` it's the version to cut; for a
standalone `cf_release` the package must already be on PyPI at that version (the
step tables above describe each DAG's tasks).

At each `approval` task the run enters `awaiting_input`; open it in the UI and
**Approve** or **Reject**.

## Guardrails

- **Nothing publishes or merges without your approval.** Each DAG pauses at a
  HITL gate (publish-to-PyPI in `e2e_release`, merge-to-conda-forge in
  `cf_release`); Reject stops the run and ships nothing. On reject/failure the
  cleanup steps still run (delete the abandoned draft / worktree / PR / branch).
- `simple_jai_release` is the same guarantee, N times over: every subpackage keeps
  its own two gates, and jupyter-ai adds gates for the range bump, the release
  notes, and its own draft. A rejected subpackage fails Phase A, which blocks the
  jupyter-ai release entirely — so a wave never ships half-pinned.
- `gh` auth comes from your shell. `AIRFLOW_HOME` (SQLite DB, logs, generated
  password) stays out of version control.
