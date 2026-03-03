"""Example — PACE AVW, a variable with (lat, lon)

Demonstrates ``data_source='earthaccess'``, which automatically searches
NASA Earthdata for granules covering the requested points and opens them
via ``earthaccess.open()``.  Authentication is handled by
``earthaccess.login()``.

Run::

    python examples/pace_avw.py

What it shows
-------------
* Using ``data_source='earthaccess'`` with ``source_kwargs`` to search the
  PACE OCI AVW L3m collection.
* Matching points from a CSV file and from an inline DataFrame.
* ``avw`` is a scalar variable (lat × lon), so the result contains a
  single ``avw`` column.
* Requires earthdata authentication (``earthaccess.login()``).
"""

from pathlib import Path
import earthaccess
import point_collocation as pc
import pandas as pd

HERE = Path(__file__).resolve().parent
POINTS_CSV = HERE / "fixtures" / "points.csv"

earthaccess.login()
df_points = pd.read_csv(POINTS_CSV)  # lat, lon, date columns

result = pc.matchup(
    df_points[0:1],
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_AVW",
        "granule_name": "*.DAY.*.4km.*",
    },
    variables=["avw"],
)

print(result)

# data point
time = "2025-04-09"
lat = 30.0
lon = -89.0

df = pd.DataFrame(
    {
        "lat": [lat],
        "lon": [lon],
        "time": [time],
    }
)
df["time"] = pd.to_datetime(df["time"])

result = pc.matchup(
    df,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_AVW",
        "granule_name": "*.DAY.*.4km.*",
    },
    variables=["avw"],
)

print(result)
