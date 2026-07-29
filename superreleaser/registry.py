"""Package registry: the lookup table mapping a package name to its GitHub repo,
PyPI project name, conda-forge package name, and feedstock repo.

These names are NOT derivable from each other — e.g. `jupyterlab-chat` lives in
the repo `jupyterlab/jupyter-chat`, and `jupyter_server_documents` uses
underscores on both PyPI and conda-forge. So we maintain them explicitly.

Every package here has "Step 1: Prep Release" / "Step 2: Publish Release"
workflow_dispatch workflows in its repo (verified), so it can run the full
end-to-end release DAG. Expand this table as more packages come online.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Package:
    repo: str          # GitHub source repo, "owner/name" (runs Step 1/Step 2)
    pypi_name: str     # PyPI project name (for the availability poll)
    cf_pkg_name: str   # conda-forge package name (may differ, e.g. underscores)
    feedstock_repo: str  # conda-forge feedstock repo, "owner/name"


# Keyed by the human-facing package name (what you pass at trigger time).
PACKAGES: dict[str, Package] = {
    "jupyter-ai": Package(
        repo="jupyterlab/jupyter-ai",
        pypi_name="jupyter-ai",
        cf_pkg_name="jupyter-ai",
        feedstock_repo="conda-forge/jupyter-ai-feedstock",
    ),
    "jupyter-ai-acp-client": Package(
        repo="jupyter-ai-contrib/jupyter-ai-acp-client",
        pypi_name="jupyter-ai-acp-client",
        cf_pkg_name="jupyter-ai-acp-client",
        feedstock_repo="conda-forge/jupyter-ai-acp-client-feedstock",
    ),
    "jupyter-ai-chat-commands": Package(
        repo="jupyter-ai-contrib/jupyter-ai-chat-commands",
        pypi_name="jupyter-ai-chat-commands",
        cf_pkg_name="jupyter-ai-chat-commands",
        feedstock_repo="conda-forge/jupyter-ai-chat-commands-feedstock",
    ),
    "jupyter-ai-litellm": Package(
        repo="jupyter-ai-contrib/jupyter-ai-litellm",
        pypi_name="jupyter-ai-litellm",
        cf_pkg_name="jupyter-ai-litellm",
        feedstock_repo="conda-forge/jupyter-ai-litellm-feedstock",
    ),
    "jupyter-ai-magic-commands": Package(
        repo="jupyter-ai-contrib/jupyter-ai-magic-commands",
        pypi_name="jupyter-ai-magic-commands",
        cf_pkg_name="jupyter-ai-magic-commands",
        feedstock_repo="conda-forge/jupyter-ai-magic-commands-feedstock",
    ),
    "jupyter-ai-persona-manager": Package(
        repo="jupyter-ai-contrib/jupyter-ai-persona-manager",
        pypi_name="jupyter-ai-persona-manager",
        cf_pkg_name="jupyter-ai-persona-manager",
        feedstock_repo="conda-forge/jupyter-ai-persona-manager-feedstock",
    ),
    "jupyter-ai-router": Package(
        repo="jupyter-ai-contrib/jupyter-ai-router",
        pypi_name="jupyter-ai-router",
        cf_pkg_name="jupyter-ai-router",
        feedstock_repo="conda-forge/jupyter-ai-router-feedstock",
    ),
    "jupyter-ai-tools": Package(
        repo="jupyter-ai-contrib/jupyter-ai-tools",
        pypi_name="jupyter-ai-tools",
        cf_pkg_name="jupyter-ai-tools",
        feedstock_repo="conda-forge/jupyter-ai-tools-feedstock",
    ),
    # Repo name differs from the package name.
    "jupyterlab-chat": Package(
        repo="jupyterlab/jupyter-chat",
        pypi_name="jupyterlab-chat",
        cf_pkg_name="jupyterlab-chat",
        feedstock_repo="conda-forge/jupyterlab-chat-feedstock",
    ),
    "jupyterlab-commands-toolkit": Package(
        repo="jupyter-ai-contrib/jupyterlab-commands-toolkit",
        pypi_name="jupyterlab-commands-toolkit",
        cf_pkg_name="jupyterlab-commands-toolkit",
        feedstock_repo="conda-forge/jupyterlab-commands-toolkit-feedstock",
    ),
    "jupyter-server-mcp": Package(
        repo="jupyter-ai-contrib/jupyter-server-mcp",
        pypi_name="jupyter-server-mcp",
        cf_pkg_name="jupyter-server-mcp",
        feedstock_repo="conda-forge/jupyter-server-mcp-feedstock",
    ),
    # Underscores on PyPI, conda-forge, and the feedstock repo name.
    "jupyter-server-documents": Package(
        repo="jupyter-ai-contrib/jupyter-server-documents",
        pypi_name="jupyter_server_documents",
        cf_pkg_name="jupyter_server_documents",
        feedstock_repo="conda-forge/jupyter_server_documents-feedstock",
    ),
}

# Sorted package names — used as the enum for the DAG `package` param (dropdown).
PACKAGE_NAMES = sorted(PACKAGES)


def get(package: str) -> Package:
    """Look up a package, with a clear error listing valid names."""
    try:
        return PACKAGES[package]
    except KeyError:
        raise KeyError(
            f"unknown package {package!r}; known: {', '.join(PACKAGE_NAMES)}"
        )
