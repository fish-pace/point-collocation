"""point_collocation — point-based matchups against cloud-hosted granules.

Public API
----------
:func:`plan`
    Build a matchup plan by searching for granules that cover the given points.
:func:`matchup`
    Execute a :class:`Plan` to extract dataset variables at each point.

Quick start
-----------
::

    import earthaccess
    import point_collocation as pc
    import pandas as pd

    earthaccess.login()

    df_points = pd.DataFrame({
        "lat": [34.5, 35.1],
        "lon": [-120.3, -119.8],
        "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
    })

    plan = pc.plan(
        df_points,
        data_source="earthaccess",
        source_kwargs={
            "short_name": "PACE_OCI_L3M_RRS",
            "granule_name": "*.DAY.*.4km.*",
        },
    )

    # Inspect what variables are available before running the full matchup
    plan.show_variables(geometry="grid")

    # Open a single granule interactively
    ds = plan.open_dataset(plan[0])

    out = pc.matchup(plan, geometry="grid", variables=["Rrs"])

Optional xarray accessor
-------------------------
Register the ``Dataset.pc`` accessor for interactive use::

    import point_collocation.extensions.accessor  # noqa: F401

    ds = xr.open_dataset(...)
    out = ds.pc.extract_points(df_points, variables=["sst"])
"""

from point_collocation.core.engine import matchup
from point_collocation.core.plan import Plan, plan

__all__ = ["matchup", "plan", "Plan"]
