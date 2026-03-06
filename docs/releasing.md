# Releasing point-collocation to PyPI

This document describes how to publish a new version of `point-collocation` to
[PyPI](https://pypi.org/project/point-collocation/) using the automated
GitHub Actions workflow.

## Overview

Publishing is fully automated via `.github/workflows/publish.yml`.  
The workflow uses **PyPI Trusted Publishing** (OIDC) so no API tokens need to
be stored in GitHub Secrets — authentication happens through a short-lived
OIDC token issued by GitHub Actions.

The workflow is triggered whenever a **GitHub Release is published**.

---

## Step 1 — Bump the version

Edit `pyproject.toml` and update the `version` field under `[project]`:

```toml
[project]
name = "point-collocation"
version = "0.2.0"   # ← new version
```

Commit the change directly to `main` (or open a PR and merge it first):

```bash
git add pyproject.toml
git commit -m "chore: bump version to 0.2.0"
git push origin main
```

---

## Step 2 — Create and push a git tag

Tags must match the version in `pyproject.toml` and follow the `vX.Y.Z` format:

```bash
git tag v0.2.0
git push origin v0.2.0
```

---

## Step 3 — Create a GitHub Release

1. Open the repository on GitHub.
2. Click **Releases → Draft a new release**.
3. Select the tag you just pushed (`v0.2.0`).
4. Fill in the **Release title** and **Release notes**.
5. Click **Publish release**.

Publishing the release triggers the workflow automatically.

---

## What the workflow does

1. **Build** — Runs `python -m build` to produce an sdist (`.tar.gz`) and a
   wheel (`.whl`) inside the `dist/` directory.
2. **Smoke check** — Installs the built wheel and runs
   `import point_collocation` to confirm the package is importable.
3. **Publish** — Uploads both artifacts to PyPI via
   [`pypa/gh-action-pypi-publish`](https://github.com/pypa/gh-action-pypi-publish)
   using Trusted Publishing (no stored token required).

---

## One-time PyPI setup (Trusted Publishing)

Before the workflow can publish for the first time you need to register
GitHub as a trusted publisher on PyPI:

1. Log in to <https://pypi.org> and navigate to the `point-collocation`
   project page (or create the project by uploading a first release manually).
2. Go to **Manage → Settings → Trusted Publishers**.
3. Click **Add a new publisher** and fill in:

   | Field | Value |
   |---|---|
   | Owner / organisation | `fish-pace` |
   | Repository name | `point-collocation` |
   | Workflow file name | `publish.yml` |
   | Environment name | *(leave blank)* |

4. Save.

From that point on, every GitHub Release will trigger a fully automated,
token-free publish.

---

## Troubleshooting

### `403 Forbidden` — trusted publisher not configured

The PyPI trusted publisher has not been set up yet, or the workflow filename /
repo name does not match what was registered.  
**Fix:** complete the one-time setup described above.

### `400 File already exists` — version already on PyPI

PyPI does not allow re-uploading a version.  
**Fix:** bump the version in `pyproject.toml`, commit, re-tag, and create a
new GitHub Release.

### Metadata validation errors

PyPI rejects packages with missing or invalid metadata.  
**Fix:** check `pyproject.toml` for required fields (`name`, `version`,
`requires-python`, `readme`, `license`) and run `python -m build` locally to
verify before tagging.

### Smoke check fails

The wheel is not importable — likely a packaging misconfiguration.  
**Fix:** confirm `[tool.hatch.build.targets.wheel].packages` in
`pyproject.toml` points to `["src/point_collocation"]` and run
`pip install dist/*.whl && python -c "import point_collocation"` locally.
