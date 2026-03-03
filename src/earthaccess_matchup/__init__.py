"""earthaccess_matchup — point-based matchups against cloud-hosted granules.

Public API
----------
:func:`matchup`
    Extract dataset variables at a table of lat/lon/time points.

Quick start
-----------
::

    import earthaccess
    import earthaccess_matchup as eam
    import pandas as pd

    earthaccess.login()

    df_points = pd.DataFrame({
        "lat": [34.5, 35.1],
        "lon": [-120.3, -119.8],
        "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
    })

    out = eam.matchup(
        df_points,
        data_source="earthaccess",
        source_kwargs={
            "short_name": "PACE_OCI_L3M_RRS",
            "granule_name": "*.DAY.*.4km.*",
        },
        variables=["Rrs"],
    )

Optional xarray accessor
-------------------------
Register the ``Dataset.eam`` accessor for interactive use::

    import earthaccess_matchup.extensions.accessor  # noqa: F401

    ds = xr.open_dataset(...)
    out = ds.eam.extract_points(df_points, variables=["sst"])
"""

from earthaccess_matchup.core.engine import matchup

__all__ = ["matchup"]
