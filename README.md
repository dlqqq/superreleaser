# Superreleaser — conda-forge release DAG (Apache Airflow)

Automates the **conda-forge feedstock release** for a single Jupyter AI
subpackage as a linear, human-gated Airflow 3 DAG. Trigger it, it opens the
feedstock PR for you, waits for CI, and pauses for your approval **in the
Airflow UI** — approve or reject there, no talking to an agent.

> Airflow was chosen for its **first-class Human-in-the-Loop** support
> (`ApprovalOperator`, `awaiting_input` state) introduced in Airflow 3.1.

## What it does (`cf_release` DAG)

Triggered with a conf like `{"package": "jupyter-ai-acp-client"}`:

| Task | Step |
|------|------|
| `checkout` | Sync the `<pkg>-feedstock` submodule to its remote default branch. |
| `pick_version` | Pick the **earliest STABLE** PyPI version missing from the feedstock (one bump per PR, conda-forge convention; prereleases skipped). Skips if caught up. |
| `update_recipe` | Set `context.version`, `source.sha256` (from the PyPI sdist), and rewrite `requirements.run` from the **released package's own metadata** (version ranges sorted floor-first). Logs the full `git diff` of the recipe. |
| `verify_cf` | For every run dep: the conda-forge package **exists** *and* the required **version range resolves** to a build. Doesn't hard-fail — unmet ranges are surfaced in the PR body and the approval gate. |
| `open_pr` | Push a branch and open the feedstock PR, then comment `@conda-forge-admin, please rerender` to trigger the rerender. **DRAFT** (with an explanatory comment) if any dep was unresolved; a normal PR otherwise. |
| `wait_for_ci` | `PythonSensor` (reschedule mode). Waits for the **rerender commit to land** (conda-forge pushes it, restarting CI) *then* for checks to be green on that rerendered head — so the brief green before the rerender doesn't count. |
| `build_gate_body` + `approval` | `ApprovalOperator` — review the PR in the UI and **Approve/Reject**. Reject fails the run and merges nothing. |
| `merge_pr` | Runs only on approval: squash-merge the PR. The one destructive conda-forge action, gated behind the human. |
| `verify_merge` | `PythonSensor` — after merge, poll CI on the default branch's new head and **fail the run** if that build is red. A green build here means conda-forge **uploaded** the package. |
| `await_conda_forge` | `PythonSensor` — poll anaconda.org until the version appears on the conda-forge channel. Reached only after a green post-merge build, so this is bounded CDN propagation (~30 min), not "will it ever ship." |

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

### "Never coming" vs. "still propagating"

After merge, a package doesn't appear on conda-forge for download immediately
(~30 min historically). The tricky part is telling *"it will never show up"*
from *"it's uploaded and propagating."* Polling anaconda.org alone can't — both
look like absence.

The discriminator is the **post-merge build**, not the poll. conda-forge builds
and uploads the package from the default-branch CI that runs *after* merge, so
`verify_merge` gates everything: a **green** build means the artifact was
uploaded (so any absence is pure CDN/repodata lag → `await_conda_forge` waits,
bounded); a **red/missing** build means nothing shipped (→ fail loudly, don't
wait forever). `await_conda_forge` is only reached in the first case, which is
why its timeout means "abnormally slow propagation," never "maybe it's coming."

## Layout

```
superreleaser/
├── dags/
│   ├── cf_release.py         # single-package release DAG — wiring only (~100 lines)
│   ├── cf_release_batch.py   # batch DAG: release packages in dependency order
│   ├── .airflowignore        # marks cf_tasks/ as library code, not DAGs
│   └── cf_tasks/             # the task bodies, split by phase
│       ├── prepare.py        # checkout, pick_version, update_recipe, verify_cf
│       ├── publish.py        # open_pr, merge_pr, and the CI/CDN sensor callables
│       ├── gate.py           # build the approval-gate message
│       └── _common.py        # sys.path bootstrap + dag_run.conf helper
├── superreleaser/            # Airflow-independent logic (unit-testable)
│   ├── config.py             # paths + DRY_RUN default
│   ├── condaforge.py         # anaconda.org + PyPI probes, name resolution
│   ├── recipe.py             # recipe read/edit + version selection + dep mapping
│   └── gitops.py             # git/gh ops (dry-run aware)
├── tests/                    # unit tests (no network)
└── pixi.toml                 # conda-forge deps + tasks (start / test / release)
```

Each `@task`'s **docstring** becomes its description in the Airflow UI, so the
task modules read as normal Python with no inline `doc_md` clutter.

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
pixi run release jupyter-ai-acp-client 0.2.1  # package + explicit version
```

or trigger `cf_release` from the UI with a conf like
`{ "package": "jupyter-ai-acp-client", "version": "0.2.1" }`.

Other tasks: `pixi run test` (unit tests), `pixi run reset` (wipe the local
Airflow home).

The version is **explicit** (the package must already be published to PyPI at
that version). The DAG then, in order:

1. **Prepare** — clone + fork the feedstock, compute the run-requirement diff,
   verify each dep exists on conda-forge.
2. **Update feedstock** — worktree in `/tmp`, apply the diff + bump version/sha256,
   `conda smithy rerender`, and make a single commit (all local).
3. **Open feedstock PR** — push the finished branch, open the PR (titled exactly
   `<pkg> v<version>`).
4. **Await green CI** and **Await human approval** — run in parallel; CI must go
   green *and* you must approve in the UI.
5. **Publish on Conda Forge** — squash-merge as `<pkg> v<version> (#N)`, then poll
   (`conda search`, every 60s) until the version is downloadable.
6. **cleanup** — remove the `/tmp` worktree (always runs).

At the `approval` task the run enters `awaiting_input`; open it in the UI and
click **Approve** (→ merge) or **Reject** (→ fail the run, merge nothing).

## Guardrails

- **Nothing merges without your approval.** The DAG opens the PR and waits at the
  HITL gate; Reject fails the run and merges nothing. That gate is the safety —
  keep an eye on the run and reject if the diff looks wrong.
- `gh` auth comes from your shell. `AIRFLOW_HOME` (SQLite DB, logs, generated
  password) stays out of version control.
