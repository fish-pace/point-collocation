"""Example 1 — basic daily matchup.

Demonstrates the simplest possible use-case: a handful of observation
points spread over a single day, matched against a daily L3 granule
via ``data_source='earthaccess'``.

Run::

    python examples/basic_daily.py

What it shows
-------------
* Building a minimal ``df_points`` DataFrame manually.
* Using ``pc.plan()`` to search for granules.
* Calling ``pc.matchup(plan, geometry="grid", ...)`` for L3/gridded data.
* Inspecting the returned DataFrame.
* Requires earthdata authentication (``earthaccess.login()``).
"""

from __future__ import annotations

import sys
from pathlib import Path

# Allow running directly from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import earthaccess
import pandas as pd

import point_collocation as pc

earthaccess.login()

# ---------------------------------------------------------------------------
# 1. Build a small points table (lat / lon / time).
# ---------------------------------------------------------------------------
df_points = pd.DataFrame(
    {
        "lat": [0.0, 30.0, 45.5, -15.2, 60.0],
        "lon": [-150.0, -90.0, 20.0, 45.0, 10.0],
        "time": pd.to_datetime(
            ["2023-06-01", "2023-06-01", "2023-06-01", "2023-06-01", "2023-06-01"]
        ),
        "station_id": ["S1", "S2", "S3", "S4", "S5"],
    }
)

print("Input points:")
print(df_points.to_string(index=False))
print()

# ---------------------------------------------------------------------------
# 2. Build a plan.
#    data_source='earthaccess' searches NASA Earthdata automatically;
#    source_kwargs are passed directly to earthaccess.search_data().
# ---------------------------------------------------------------------------
plan = pc.plan(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "AQUA_MODIS_L3m_DAY_SST_sst_4km",
        "granule_name": "*.DAY.SST.sst.4km.*",
    },
)

# Inspect available variables before running the full matchup.
plan.show_variables(geometry="grid")

# ---------------------------------------------------------------------------
# 3. Run matchup.
#    geometry="grid" — L3/gridded data with 1-D lat/lon coordinates.
# ---------------------------------------------------------------------------
result = pc.matchup(
    plan,
    geometry="grid",
    variables=["sst", "chlor_a"],
)

# ---------------------------------------------------------------------------
# 4. Inspect results.
# ---------------------------------------------------------------------------
print("Matchup result:")
print(result.to_string(index=False))
print()
print(f"Matched {result['sst'].notna().sum()} / {len(result)} points for 'sst'")
print(f"Matched {result['chlor_a'].notna().sum()} / {len(result)} points for 'chlor_a'")
