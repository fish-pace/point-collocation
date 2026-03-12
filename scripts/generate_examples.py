#!/usr/bin/env python3
"""Auto-generate docs pages from examples/docs_*.ipynb and update mkdocs.yml nav.

Run from the repository root:
    python scripts/generate_examples.py

Any file matching examples/docs_*.ipynb is:
  1. Converted to Markdown in docs/ (e.g. docs_l3_examples.ipynb -> docs/l3_examples.md).
  2. Added to the "Examples" section of mkdocs.yml using the notebook's
     first H1 heading as the nav label.

The quickstart notebook (docs_quickstart.ipynb -> quickstart.md) is placed
first in the Examples section; the remaining notebooks follow in alphabetical
order (their numeric prefix, e.g. docs_1_*, docs_2_*, controls the sort).

Adding a new docs_*.ipynb requires no other changes — just re-run this script
(or push to main, which triggers the workflow to do it automatically).
"""

import json
import re
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES_DIR = ROOT / "examples"
DOCS_DIR = ROOT / "docs"
MKDOCS_YML = ROOT / "mkdocs.yml"

# Fixed nav entries (in order) that are not auto-generated.
_NAV_BEFORE_EXAMPLES = [
    {"Home": "index.md"},
]
_NAV_AFTER_EXAMPLES = [
    {"Installation": "installation.md"},
    {"API Reference": "api.md"},
    {"Contributing": "contributing.md"},
    {"Releasing": "releasing.md"},
]

# md filenames already placed in the fixed nav — convert them but don't add
# them to the "Examples" section (they have their own top-level slot).
_FIXED_NAV_FILES = {v for entry in _NAV_BEFORE_EXAMPLES + _NAV_AFTER_EXAMPLES for v in entry.values()}

# The quickstart page is always placed first in the Examples section.
_QUICKSTART_MD = "quickstart.md"


def _notebook_title(nb_path: Path) -> str:
    """Return the first H1 heading found in any markdown cell, or a humanised stem."""
    with open(nb_path, encoding="utf-8") as fh:
        nb = json.load(fh)
    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "markdown":
            source = "".join(cell.get("source", []))
            match = re.search(r"^#\s+(.+)", source, re.MULTILINE)
            if match:
                return match.group(1).strip()
    # Fallback: humanise the stem
    stem = nb_path.stem.removeprefix("docs_")
    return stem.replace("_", " ").title()


def _output_stem(nb_path: Path) -> str:
    """Return the markdown stem for a notebook (strips the docs_ prefix)."""
    return nb_path.stem.removeprefix("docs_")


def convert_notebooks() -> list[dict]:
    """Convert every docs_*.ipynb to markdown; return sorted nav entries.

    The quickstart notebook (quickstart.md) is placed first; all other
    notebooks follow in their natural alphabetical order (which matches the
    numeric prefix used in their filenames).
    """
    notebooks = sorted(EXAMPLES_DIR.glob("docs_*.ipynb"))
    if not notebooks:
        print("No docs_*.ipynb files found – nothing to do.")
        return []

    quickstart_entry: dict | None = None
    other_entries: list[dict] = []

    for nb in notebooks:
        stem = _output_stem(nb)
        title = _notebook_title(nb)
        md_file = f"{stem}.md"
        print(f"  {nb.name}  →  docs/{md_file}  (nav label: '{title}')")
        result = subprocess.run(
            [
                "jupyter", "nbconvert",
                "--to", "markdown",
                str(nb),
                "--output", stem,
                "--output-dir", str(DOCS_DIR),
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"ERROR: failed to convert {nb.name}\n{result.stderr}", flush=True)
            raise subprocess.CalledProcessError(result.returncode, result.args)
        # Only list in Examples if this file isn't already in the fixed nav.
        if md_file not in _FIXED_NAV_FILES:
            entry = {title: md_file}
            if md_file == _QUICKSTART_MD:
                quickstart_entry = entry
            else:
                other_entries.append(entry)

    # Quickstart is always first in the Examples section.
    nav_entries = ([quickstart_entry] if quickstart_entry else []) + other_entries
    return nav_entries


def update_mkdocs_nav(examples_nav: list[dict]) -> None:
    """Rewrite the nav section of mkdocs.yml with the generated example pages.

    Note: PyYAML is used here (ruamel.yaml is not a project dependency).
    The dump will normalise whitespace but preserves all keys and values.
    """
    with open(MKDOCS_YML, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)

    data["nav"] = (
        _NAV_BEFORE_EXAMPLES
        + [{"Examples": examples_nav}]
        + _NAV_AFTER_EXAMPLES
    )

    with open(MKDOCS_YML, "w", encoding="utf-8") as fh:
        yaml.dump(data, fh, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"  mkdocs.yml nav updated with {len(examples_nav)} example page(s).")


def main() -> None:
    print("Generating docs from notebooks …")
    nav_entries = convert_notebooks()
    if nav_entries:
        print("Updating mkdocs.yml …")
        update_mkdocs_nav(nav_entries)
    print("Done.")


if __name__ == "__main__":
    main()
