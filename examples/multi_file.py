"""Example 2 — multi-date matchup via earthaccess.

Demonstrates that the matchup engine automatically routes each
observation point to the granule whose temporal coverage contains
that point's date when using ``data_source='earthaccess'``.

Run::

    python examples/multi_file.py

What it shows
-------------
* Points spread across multiple dates in a single ``pc.matchup()`` call.
* The engine queries earthaccess once per unique date, so only the
  granules needed for the requested points are opened.
* Using ``return_diagnostics=True`` to inspect timing and per-granule
  results.
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
# 1. Build a points table that spans multiple dates.
# ---------------------------------------------------------------------------
df_points = pd.DataFrame(
    {
        "lat": [20.0, 20.0, 20.0, -30.0, -30.0],
        "lon": [10.0, 10.0, 10.0, 120.0, 120.0],
        "time": pd.to_datetime(
            [
                "2025-04-09",
                "2025-04-10",
                "2025-04-11",
                "2025-04-09",
                "2025-04-11",
            ]
        ),
        "label": ["day1", "day2", "day3", "day1-B", "day3-B"],
    }
)

print("Input points:")
print(df_points.to_string(index=False))
print()

# ---------------------------------------------------------------------------
# 2. Run matchup with diagnostics enabled.
#    source_kwargs are forwarded directly to earthaccess.search_data().
# ---------------------------------------------------------------------------
result, report = pc.matchup(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
        "granule_name": "*.DAY.*.4km.*",
    },
    variables=["Rrs"],
    return_diagnostics=True,
)

# ---------------------------------------------------------------------------
# 3. Inspect results and diagnostics.
# ---------------------------------------------------------------------------
print("Matchup result (first few Rrs columns):")
rrs_cols = [c for c in result.columns if c.startswith("Rrs_")][:5]
print(result[["lat", "lon", "time", "label"] + rrs_cols].to_string(index=False))
print()
print("Diagnostics summary:")
print(f"  {report.summary()}")
print()
print("Per-granule breakdown:")
for g in report.granules:
    status = "OK" if g.succeeded else f"ERROR: {g.error}"
    print(f"  [{status}] {g.granule_id}")
    print(f"           found={g.variables_found}  missing={g.variables_missing}")
