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

import json
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

EXAMPLES_DIR = Path("examples")
REPORT_FILE = Path("notebook_test_report.md")
NOTEBOOK_TIMEOUT = 600  # seconds per notebook
SCRIPT_TIMEOUT = 300  # seconds per script
MAX_DETAIL_CHARS = 10_000  # truncate very long error output in the report

# Pattern for bare ``pip install`` lines in notebook cells.
# These are install-hint cells meant for interactive use and should be skipped
# during automated execution.  We match lines that start with optional
# whitespace followed by ``pip install``.  Lines prefixed with ``!`` or ``%``
# (Jupyter magic/shell commands) are automatically excluded because the pattern
# requires ``pip`` to appear immediately after the optional leading whitespace.
# Lines prefixed with ``#`` are excluded because the ``#`` character is not in
# ``[ \t]*``.  Occurrences inside multi-line strings are theoretically possible
# but vanishingly rare in documentation notebooks and are acceptable to comment
# out in the temporary execution copy.
_BARE_PIP_RE = re.compile(r"^(?P<indent>[ \t]*)pip[ \t]+install\b", re.MULTILINE)


def _notebook_with_pip_install_commented(nb_path: Path) -> Path | None:
    """Return a temp notebook path with bare ``pip install`` lines commented out.

    Returns *None* when no such lines are found (caller should use the original
    path unchanged).  The caller is responsible for deleting the temp file.
    """
    data = json.loads(nb_path.read_text(encoding="utf-8"))
    changed = False

    for cell in data.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = cell["source"]
        text = "".join(src) if isinstance(src, list) else src
        new_text = _BARE_PIP_RE.sub(
            lambda m: m.group("indent") + "# pip install", text
        )
        if new_text != text:
            changed = True
            cell["source"] = (
                new_text.splitlines(keepends=True) if isinstance(src, list) else new_text
            )

    if not changed:
        return None

    fd, tmp_path = tempfile.mkstemp(suffix=".ipynb", prefix=f"_pctest_{nb_path.stem}_")
    with open(fd, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return Path(tmp_path)


def run_notebook(nb_path: Path) -> tuple[bool, str]:
    """Execute *nb_path* with ``jupyter nbconvert --execute``.

    Returns ``(passed, detail)`` where *detail* is non-empty only on failure.
    The notebook is executed in-place (the output cells are written back).

    Before execution, bare ``pip install`` lines in code cells are commented
    out in a temp copy so that install-hint cells (e.g. ``pip install
    point-collocation``) do not abort the run with a SyntaxError.
    """
    tmp_path = _notebook_with_pip_install_commented(nb_path)
    exec_path = tmp_path if tmp_path is not None else nb_path
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
                str(exec_path),
            ],
            capture_output=True,
            text=True,
            timeout=NOTEBOOK_TIMEOUT + 60,
        )
    except subprocess.TimeoutExpired:
        return False, f"Timed out after {NOTEBOOK_TIMEOUT + 60} seconds."
    finally:
        if tmp_path is not None:
            tmp_path.unlink(missing_ok=True)

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
