"""Example 3 — diagnostics and error handling.

Demonstrates the ``return_diagnostics=True`` mode, which returns a
:class:`~point_collocation.diagnostics.report.MatchupReport` alongside
the result DataFrame.  The report records timing, variables
found/missing, per-point warnings, and file-open errors.

Run::

    python examples/diagnostics.py

What it shows
-------------
* Using ``data_source='earthaccess'`` with ``return_diagnostics=True``.
* Requesting a variable that exists and one that does not.
* Reading the MatchupReport: total / succeeded / skipped counts, elapsed
  time, and per-granule details.
* Requires earthdata authentication (``earthaccess.login()``).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import earthaccess
import pandas as pd

import point_collocation as pc

earthaccess.login()

# ---------------------------------------------------------------------------
# 1. Points table.
# ---------------------------------------------------------------------------
df_points = pd.DataFrame(
    {
        "lat": [30.0, -20.0, 45.0],
        "lon": [-89.0, 80.0, 30.0],
        "time": pd.to_datetime(["2025-04-09", "2025-04-09", "2025-04-09"]),
    }
)

print("Input points:")
print(df_points.to_string(index=False))
print()

# ---------------------------------------------------------------------------
# 2. Run matchup with diagnostics enabled.
#    Request 'Rrs' (present in PACE OCI RRS) and 'nonexistent_var' (absent)
#    to show the variables_found / variables_missing tracking.
# ---------------------------------------------------------------------------
result, report = pc.matchup(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
        "granule_name": "*.DAY.*.4km.*",
    },
    variables=["Rrs", "nonexistent_var"],
    return_diagnostics=True,
)

# ---------------------------------------------------------------------------
# 3. Print result DataFrame.
# ---------------------------------------------------------------------------
print("Matchup result:")
print(result.to_string(index=False))
print()

# ---------------------------------------------------------------------------
# 4. Print full diagnostics report.
# ---------------------------------------------------------------------------
print(f"Summary: {report.summary()}")
print()
print(f"  Total granules attempted : {report.total}")
print(f"  Succeeded                : {report.succeeded}")
print(f"  Skipped (errors)         : {report.skipped}")
print(f"  Wall-clock time          : {report.elapsed_seconds:.2f}s")
print()

for i, g in enumerate(report.granules, start=1):
    print(f"  Granule {i}: {g.granule_id}")
    print(f"    Succeeded         : {g.succeeded}")
    print(f"    Elapsed           : {g.elapsed_seconds:.3f}s")
    print(f"    Variables found   : {g.variables_found}")
    print(f"    Variables missing : {g.variables_missing}")
    if g.warnings:
        for w in g.warnings:
            print(f"    Warning           : {w}")
    if g.error:
        print(f"    Error             : {g.error}")
    print()
