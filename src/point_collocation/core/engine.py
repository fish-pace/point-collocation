"""Core matchup engine — no earthaccess dependency here.

Responsibilities
----------------
* Accept a :class:`~point_collocation.core.plan.Plan` object built with
  :func:`~point_collocation.plan`.
* Open each granule individually with ``xarray.open_dataset`` (never
  ``open_mfdataset``) to minimise cloud I/O and avoid memory leaks.
* Extract the requested variables at each point's location/time using
  nearest-neighbour selection.
* Collect results into a ``pandas.DataFrame`` with one row per
  (point, granule) pair.

The engine does **not** know about earthaccess, STAC, or any other
cloud-data provider.  All provider-specific logic lives in
``point_collocation.adapters``.

Future extension points
-----------------------
* ``pre_extract`` hook — spatial averaging, neighbourhood selection
* ``post_extract`` hook — QA filtering, unit conversion
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    from point_collocation.core.plan import Plan

# Candidate coordinate names tried in order when locating lat/lon dims.
_LAT_NAMES = ("lat", "latitude", "Latitude", "LAT")
_LON_NAMES = ("lon", "longitude", "Longitude", "LON")


def matchup(
    plan: "Plan",
    *,
    variables: list[str] | None = None,
    open_dataset_kwargs: dict | None = None,
) -> pd.DataFrame:
    """Extract variables from cloud-hosted granules at the given points.

    Parameters
    ----------
    plan:
        A :class:`~point_collocation.core.plan.Plan` object previously
        built with :func:`~point_collocation.plan`.  Data source and
        search parameters are taken from the plan.  One output row is
        produced per (point, granule) pair; points with zero matching
        granules produce a single NaN row.
    variables:
        Variable names to extract from each granule.  When provided,
        overrides any variables stored on the plan.  When omitted,
        falls back to ``plan.variables``.  If the resolved list is
        empty, the output will have no variable columns.
        Raises :exc:`ValueError` if a requested variable is not found
        in the opened dataset.
    open_dataset_kwargs:
        Optional dictionary of keyword arguments forwarded to
        ``xarray.open_dataset`` for every granule opened during the run.
        Defaults to ``{"chunks": {}}`` (lazy/dask loading) when not
        provided.  ``engine`` defaults to ``"h5netcdf"`` when no
        ``engine`` key is present in the dict.

    Returns
    -------
    pandas.DataFrame
        One row per (point, granule) pair, including a ``granule_id``
        column and one column per variable.  Points with zero matching
        granules contribute a single NaN row.

    Raises
    ------
    ValueError
        If a requested variable is not present in an opened dataset.
    """
    effective_vars: list[str] = variables if variables is not None else plan.variables
    effective_kwargs = {"chunks": {}} if open_dataset_kwargs is None else dict(open_dataset_kwargs)
    return _execute_plan(plan, variables=effective_vars, **effective_kwargs)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _execute_plan(
    plan: "Plan",
    *,
    variables: list[str],
    **open_dataset_kwargs: object,
) -> pd.DataFrame:
    """Execute a :class:`~point_collocation.core.plan.Plan`.

    Opens each granule once and extracts variable values for all points
    mapped to it.  Returns one row per (point, granule) pair; points
    with zero granule matches get a single NaN row.
    """
    try:
        import earthaccess  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "The 'earthaccess' package is required to execute a Plan. "
            "Install it with: pip install earthaccess"
        ) from exc

    opened_files: list[object] = earthaccess.open(plan.results, pqdm_kwargs={"disable": True})

    kwargs = dict(open_dataset_kwargs)
    if "engine" not in kwargs:
        kwargs["engine"] = "h5netcdf"

    # Build granule_index → [point_indices] for all matched granules
    granule_to_points: dict[int, list[object]] = {}
    zero_match_pt_indices: list[object] = []

    for pt_idx, g_indices in plan.point_granule_map.items():
        if not g_indices:
            zero_match_pt_indices.append(pt_idx)
        else:
            for g_idx in g_indices:
                granule_to_points.setdefault(g_idx, []).append(pt_idx)

    output_rows: list[dict] = []

    # Zero-match points → single NaN row each
    for pt_idx in zero_match_pt_indices:
        row: dict = plan.points.loc[pt_idx].to_dict()
        row["granule_id"] = float("nan")
        for var in variables:
            row[var] = float("nan")
        output_rows.append(row)

    # Process granules, opening each file once
    for g_idx, pt_indices in sorted(granule_to_points.items()):
        gm = plan.granules[g_idx]
        file_obj = opened_files[gm.result_index]

        try:
            with xr.open_dataset(file_obj, **kwargs) as ds:  # type: ignore[arg-type]
                lat_name = _find_coord(ds, _LAT_NAMES)
                lon_name = _find_coord(ds, _LON_NAMES)

                # Validate that all requested variables exist in the dataset.
                missing_vars = [v for v in variables if v not in ds]
                if missing_vars:
                    raise ValueError(
                        f"Variable(s) {missing_vars!r} not found in granule "
                        f"'{gm.granule_id}'. Available variables: "
                        f"{list(ds.data_vars)}"
                    )

                for pt_idx in pt_indices:
                    row = plan.points.loc[pt_idx].to_dict()
                    row["granule_id"] = gm.granule_id

                    for var in variables:
                        if lat_name is None or lon_name is None:
                            row[var] = float("nan")
                            continue
                        try:
                            selected = ds[var].sel(
                                {lat_name: row["lat"], lon_name: row["lon"]},
                                method="nearest",
                            )
                            if selected.ndim == 0:
                                row[var] = float(selected)
                            else:
                                # Multi-dimensional: expand into coord-keyed entries
                                row[var] = float("nan")  # placeholder removed later
                                for coord_val, val in selected.to_series().items():
                                    row[f"{var}_{int(coord_val)}"] = float(val)
                        except Exception:
                            row[var] = float("nan")

                    output_rows.append(row)

        except ValueError:
            raise
        except Exception:
            # Granule failed to open → emit NaN rows for its points
            for pt_idx in pt_indices:
                row = plan.points.loc[pt_idx].to_dict()
                row["granule_id"] = gm.granule_id
                for var in variables:
                    row[var] = float("nan")
                output_rows.append(row)

    if not output_rows:
        empty = plan.points.iloc[:0].copy()
        empty["granule_id"] = pd.Series(dtype=object)
        for var in variables:
            empty[var] = pd.Series(dtype=float)
        return empty

    df = pd.DataFrame(output_rows)

    # Drop bare placeholder columns for any variable that was expanded into
    # per-coordinate columns (e.g. Rrs → Rrs_412, Rrs_443, …).
    for var in variables:
        expanded = [c for c in df.columns if c.startswith(f"{var}_")]
        if expanded and var in df.columns:
            df = df.drop(columns=[var])

    return df


def _find_coord(ds: xr.Dataset, candidates: tuple[str, ...]) -> str | None:
    """Return the first name in *candidates* present in *ds* coords or dims."""
    for name in candidates:
        if name in ds.coords or name in ds.dims:
            return name
    return None
