"""Example 1 — basic daily matchup.

Demonstrates the simplest possible use-case: a handful of observation
points spread over a single day, matched against a daily L3 granule
via ``data_source='earthaccess'``.

Run::

    python examples/basic_daily.py

What it shows
-------------
* Building a minimal ``df_points`` DataFrame manually.
* Calling ``eam.matchup()`` with ``data_source='earthaccess'`` and
  ``source_kwargs`` specifying the collection to search.
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

import earthaccess_matchup as eam

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
# 2. Run matchup.
#    data_source='earthaccess' searches NASA Earthdata automatically;
#    source_kwargs are passed directly to earthaccess.search_data().
# ---------------------------------------------------------------------------
result = eam.matchup(
    df_points,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "AQUA_MODIS_L3m_DAY_SST_sst_4km",
        "granule_name": "*.DAY.SST.sst.4km.*",
    },
    variables=["sst", "chlor_a"],
)

# ---------------------------------------------------------------------------
# 3. Inspect results.
# ---------------------------------------------------------------------------
print("Matchup result:")
print(result.to_string(index=False))
print()
print(f"Matched {result['sst'].notna().sum()} / {len(result)} points for 'sst'")
print(f"Matched {result['chlor_a'].notna().sum()} / {len(result)} points for 'chlor_a'")
