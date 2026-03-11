"""Example 3 — variable inspection and error handling.

Demonstrates how to use ``plan.show_variables()`` to inspect available
variables before running a matchup, and how to handle the case where a
requested variable is missing.

Run::

    python examples/diagnostics.py

What it shows
-------------
* Using ``pc.plan()`` with ``data_source='earthaccess'``.
* Calling ``plan.show_variables()`` to preview dimensions, variables, and
  geolocation detection results before committing to a full extraction.
* Running ``pc.matchup(plan, variables=[...])`` for L3/gridded data.
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
# 2. Build plan and inspect available variables.
# ---------------------------------------------------------------------------
plan = pc.plan(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
        "granule_name": "*.DAY.*.4km.*",
    },
)

print("Available variables:")
plan.show_variables()
print()

# ---------------------------------------------------------------------------
# 3. Run matchup.
# ---------------------------------------------------------------------------
result = pc.matchup(
    plan,
    variables=["Rrs"],
)

# ---------------------------------------------------------------------------
# 4. Print result DataFrame.
# ---------------------------------------------------------------------------
print("Matchup result:")
rrs_cols = [c for c in result.columns if c.startswith("Rrs_")][:5]
print(result[["lat", "lon", "time"] + rrs_cols].to_string(index=False))
print()
