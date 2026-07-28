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
│   ├── cf_release.py         # single-package release DAG (orchestration only)
│   └── cf_release_batch.py   # batch DAG: release packages in dependency order
├── superreleaser/            # Airflow-independent logic (unit-testable)
│   ├── config.py             # paths + DRY_RUN default
│   ├── condaforge.py         # anaconda.org + PyPI probes, name resolution
│   ├── recipe.py             # recipe read/edit + version selection + dep mapping
│   └── gitops.py             # git/gh ops (dry-run aware)
├── tests/                    # unit tests (no network)
├── justfile                  # setup / start / release recipes
└── pyproject.toml            # pinned deps (uv)
```

## Run it locally (single user, SQLite)

Requires [`uv`](https://docs.astral.sh/uv/) and [`just`](https://just.systems/),
plus a `gh` CLI authenticated in your shell. Airflow **3.1+** is required for the
HITL gate (pinned to 3.3.0).

```bash
just setup       # install deps into a local .venv (uv)
just start       # launch Airflow all-in-one on SQLite; UI at http://localhost:8080
```

`just start` disables auth (localhost only), so there's no login. It runs in the
foreground — leave it up. Then, from another shell:

```bash
just release jupyter-ai-acp-client                 # dry-run, earliest-missing version
just release jupyter-ai-acp-client 0.2.0           # dry-run, forced version
just release jupyter-ai-acp-client 0.2.0 false     # REAL release (opens PR, merges on approval)
```

or trigger `cf_release` from the UI with a conf like
`{ "package": "jupyter-ai-acp-client", "dry_run": true }`.

- `dry_run: true` (default) → edits the local recipe but **prints** the
  branch/push/PR/comment/merge commands instead of running them. Safe to walk the
  whole DAG.
- `dry_run: false` → **opens a real feedstock PR** (+ rerender comment) and, on
  approval, **merges it**.
- a specific `version` → force a target instead of earliest-missing.

At the `approval` task the run enters `awaiting_input`; open it in the UI and
click **Approve** or **Reject**.

## Releasing several packages in order (`cf_release_batch` DAG)

To release a chain of packages — e.g. `jupyter-ai-acp-client 0.2.1` then
`jupyter-ai 3.1.1`, which depends on it — trigger `cf_release_batch` with an
ordered plan of **waves**:

```json
{
  "dry_run": true,
  "waves": [
    [ { "package": "jupyter-ai-acp-client", "version": "0.2.1" } ],
    [ { "package": "jupyter-ai", "version": "3.1.1" } ]
  ]
}
```

```bash
just release-batch                       # uses sample_batch_plan.json
just release-batch path/to/plan.json     # or your own plan
```

The batch runs the full single-package `cf_release` pipeline once per package
(each a child DAG run with its own PR and its own approval gate). Packages in the
same wave release **concurrently**; waves run **in sequence**; any failure or
rejection **stops the batch** so dependents never start.

**Why in order, not all-parallel:** `jupyter-ai`'s recipe pins
`jupyter-ai-acp-client >=0.2.1`, and `cf_release`'s verify step checks that range
actually resolves on conda-forge — which is only true once acp-client's release
has fully shipped. So an upstream must be **live** before a dependent's release
begins; that's exactly what the topological wave order guarantees. Independent
packages (no dependency between them) go in the same wave and parallelize.

For now the plan must be **topologically sorted by hand** — the batch trusts the
given order and doesn't compute the dependency graph itself.

## Guardrails

- **Merge is gated behind the human.** The DAG opens/annotates the PR, waits for
  the rerender + green CI, and only merges after you approve in the UI. Reject
  merges nothing. `dry_run` prints the merge instead of doing it.
- `gh` auth comes from your shell. `AIRFLOW_HOME` (SQLite DB, logs, generated
  password) stays out of version control.
