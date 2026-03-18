"""Pre-defined open_method profiles for common NASA earth observation products.

These profiles are plain dicts that can be passed directly to the
``open_method`` argument of :func:`~point_collocation.matchup`,
:meth:`~point_collocation.Plan.open_dataset`,
:meth:`~point_collocation.Plan.open_mfdataset`.

Examples
--------
::

    import point_collocation as pc
    from point_collocation.profiles import pace_l3, pace_l2

    out = pc.matchup(plan, open_method=pace_l3, variables=["Rrs_412"])
    out = pc.matchup(plan, open_method=pace_l2, variables=["Rrs_412"],
                     spatial_method="xoak-kdtree")

Each profile may omit most keys and rely on the shared open_kwargs defaults
(``chunks={}``, ``engine="h5netcdf"``, ``decode_timedelta=False``).
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# PACE Level-3 (gridded) profile
# ---------------------------------------------------------------------------

#: Open-method profile for PACE OCI Level-3 mapped products.
#:
#: Uses ``xr.open_dataset`` (fast path).  Coordinate detection is automatic.
pace_l3: dict = {
    "xarray_open": "dataset",
}

# ---------------------------------------------------------------------------
# PACE Level-2 (swath) profile
# ---------------------------------------------------------------------------

#: Open-method profile for PACE OCI Level-2 swath products.
#:
#: Opens as a DataTree and merges all groups into a single flat Dataset.
#: For spatial matching, pass ``spatial_method="xoak-kdtree"`` to
#: :func:`~point_collocation.matchup`.
pace_l2: dict = {
    "xarray_open": "datatree",
    "merge": "all",
}

__all__ = ["pace_l3", "pace_l2"]
