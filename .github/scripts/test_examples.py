#!/usr/bin/env python3
"""Run example notebooks and scripts and produce a markdown test report.

Discovers:
  - examples/docs_*.ipynb  (documentation notebooks)
  - examples/*.py           (public scripts — names not starting with ``_``)

Each file is executed independently.  A failure never stops the overall run;
all files are attempted and the results are collected into a single report.

Usage::

    python .github/scripts/test_examples.py

Outputs ``notebook_test_report.md`` in the current working directory and
exits with code 0 regardless of per-file failures so that the workflow step
always succeeds and the report artifact is always uploaded.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

EXAMPLES_DIR = Path("examples")
REPORT_FILE = Path("notebook_test_report.md")
NOTEBOOK_TIMEOUT = 600  # seconds per notebook
SCRIPT_TIMEOUT = 300  # seconds per script
MAX_DETAIL_CHARS = 10_000  # truncate very long error output in the report


def run_notebook(nb_path: Path) -> tuple[bool, str]:
    """Execute *nb_path* with ``jupyter nbconvert --execute``.

    Returns ``(passed, detail)`` where *detail* is non-empty only on failure.
    The notebook is executed in-place (the output cells are written back).
    """
    try:
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--inplace",
                f"--ExecutePreprocessor.timeout={NOTEBOOK_TIMEOUT}",
                str(nb_path),
            ],
            capture_output=True,
            text=True,
            timeout=NOTEBOOK_TIMEOUT + 60,
        )
    except subprocess.TimeoutExpired:
        return False, f"Timed out after {NOTEBOOK_TIMEOUT + 60} seconds."

    if proc.returncode == 0:
        return True, ""

    detail = "\n".join(
        part for part in (proc.stderr, proc.stdout) if part.strip()
    ).strip()
    return False, detail


def run_script(py_path: Path) -> tuple[bool, str]:
    """Execute *py_path* with the current Python interpreter.

    Returns ``(passed, detail)`` where *detail* is non-empty only on failure.
    """
    try:
        proc = subprocess.run(
            [sys.executable, str(py_path)],
            capture_output=True,
            text=True,
            timeout=SCRIPT_TIMEOUT,
        )
    except subprocess.TimeoutExpired:
        return False, f"Timed out after {SCRIPT_TIMEOUT} seconds."

    if proc.returncode == 0:
        return True, ""

    detail = "\n".join(
        part for part in (proc.stderr, proc.stdout) if part.strip()
    ).strip()
    return False, detail


def _truncate(text: str, max_chars: int = MAX_DETAIL_CHARS) -> str:
    if len(text) <= max_chars:
        return text
    # Keep the first 2 000 chars (context/setup info) and the last 8 000
    # chars (the actual error), with a separator in between.
    head = text[:2_000]
    tail = text[-(max_chars - 2_000):]
    return f"{head}\n\n[... middle of output truncated ...]\n\n{tail}"


def main() -> None:
    # ------------------------------------------------------------------
    # Discover files
    # ------------------------------------------------------------------
    notebooks = sorted(EXAMPLES_DIR.glob("docs_*.ipynb"))
    scripts = sorted(
        p for p in EXAMPLES_DIR.glob("*.py") if not p.name.startswith("_")
    )

    print("=" * 60)
    print("Example test runner")
    print(f"  Notebooks : {len(notebooks)}")
    print(f"  Scripts   : {len(scripts)}")
    print("=" * 60, flush=True)

    results: list[dict] = []

    # ------------------------------------------------------------------
    # Run notebooks
    # ------------------------------------------------------------------
    for nb in notebooks:
        print(f"\n[notebook] {nb} …", flush=True)
        passed, detail = run_notebook(nb)
        results.append(
            {"path": nb, "kind": "notebook", "passed": passed, "detail": detail}
        )
        print("  → PASS" if passed else "  → FAIL")

    # ------------------------------------------------------------------
    # Run scripts
    # ------------------------------------------------------------------
    for py in scripts:
        print(f"\n[script] {py} …", flush=True)
        passed, detail = run_script(py)
        results.append(
            {"path": py, "kind": "script", "passed": passed, "detail": detail}
        )
        print("  → PASS" if passed else "  → FAIL")

    # ------------------------------------------------------------------
    # Generate markdown report
    # ------------------------------------------------------------------
    n_pass = sum(r["passed"] for r in results)
    n_fail = len(results) - n_pass
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    with REPORT_FILE.open("w") as fh:
        fh.write("# Example Test Report\n\n")
        fh.write(f"**Date:** {now}  \n")
        fh.write(
            f"**Summary:** {n_pass} passed, {n_fail} failed"
            f" out of {len(results)} total\n\n"
        )

        fh.write("## Results\n\n")
        fh.write("| File | Kind | Status |\n")
        fh.write("|------|------|--------|\n")
        for r in results:
            status = "✅ PASS" if r["passed"] else "❌ FAIL"
            fh.write(f"| `{r['path']}` | {r['kind']} | {status} |\n")

        failures = [r for r in results if not r["passed"]]
        if failures:
            fh.write("\n## Failure Details\n\n")
            for r in failures:
                fh.write(f"### `{r['path']}`\n\n")
                fh.write("```\n")
                fh.write(_truncate(r["detail"]))
                fh.write("\n```\n\n")

    print(f"\nReport written to {REPORT_FILE}")
    print(f"Summary: {n_pass} passed, {n_fail} failed")


if __name__ == "__main__":
    main()
