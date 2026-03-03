"""Example — PACE Rrs, a variable with (lat, lon, wavelength)

Demonstrates ``data_source='earthaccess'``, which automatically searches
NASA Earthdata for granules covering the requested points and opens them
via ``earthaccess.open()``.  Authentication is handled by
``earthaccess.login()``.

Run::

    python examples/pace_rrs.py

What it shows
-------------
* Using ``data_source='earthaccess'`` with ``source_kwargs`` to search the
  PACE OCI Rrs L3m collection.
* Matching points from a CSV file and from an inline DataFrame.
* ``Rrs`` is a multi-dimensional variable (lat × lon × wavelength), so
  the result contains one column per wavelength band (e.g. ``Rrs_412``,
  ``Rrs_443``, …).
* Requires earthdata authentication (``earthaccess.login()``).
"""

from pathlib import Path
import earthaccess
import earthaccess_matchup as eam
import pandas as pd

HERE = Path(__file__).resolve().parent
POINTS_CSV = HERE / "fixtures" / "points.csv"

earthaccess.login()
df_points = pd.read_csv(POINTS_CSV)  # lat, lon, date columns

result = eam.matchup(
    df_points[0:1],
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
        "granule_name": "*.DAY.*.4km.*",
    },
    variables=["Rrs"],
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

result = eam.matchup(
    df,
    data_source="earthaccess",
    source_kwargs={
        "short_name": "PACE_OCI_L3M_RRS",
        "granule_name": "*.DAY.*.4km.*",
    },
    variables=["Rrs"],
)

print(result)
