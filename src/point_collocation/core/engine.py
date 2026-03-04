"""Core matchup engine — no earthaccess dependency here.

Responsibilities
----------------
* Accept a validated points ``DataFrame`` and an iterable of sources.
  Sources may be file-like objects (e.g., from ``earthaccess.open()``)
  or objects satisfying
  :class:`~point_collocation.core.types.SourceProtocol`.
* Open each source individually with ``xarray.open_dataset`` (never
  ``open_mfdataset``) to minimise cloud I/O and avoid memory leaks.
* Extract the requested variables at each point's location/time using
  nearest-neighbour selection.
* Collect results into a ``pandas.DataFrame`` with the original columns
  plus one new column per extracted variable.
* Populate a :class:`~point_collocation.diagnostics.report.MatchupReport`
  throughout the run.

The engine does **not** know about earthaccess, STAC, or any other
cloud-data provider.  All provider-specific logic lives in
``point_collocation.adapters``.

Future extension points
-----------------------
* ``pre_extract`` hook — spatial averaging, neighbourhood selection
* ``post_extract`` hook — QA filtering, unit conversion
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import TYPE_CHECKING, Literal, Union

import pandas as pd
import xarray as xr

from point_collocation.core._granule import get_source_id, parse_temporal_range
from point_collocation.core.types import PointsFrame
from point_collocation.diagnostics.report import GranuleSummary, MatchupReport

if TYPE_CHECKING:
    from point_collocation.core.plan import Plan

# Candidate coordinate names tried in order when locating lat/lon dims.
_LAT_NAMES = ("lat", "latitude", "Latitude", "LAT")
_LON_NAMES = ("lon", "longitude", "Longitude", "LON")


def matchup(
    points: Union[PointsFrame, "Plan"],
    *,
    variables: list[str] | None = None,
    data_source: str = "earthaccess",
    source_kwargs: dict | None = None,
    nc_type: Literal["grouped", "flat"] = "flat",
    return_diagnostics: bool = False,
    **open_dataset_kwargs: object,
) -> pd.DataFrame | tuple[pd.DataFrame, MatchupReport]:
    """Extract variables from cloud-hosted granules at the given points.

    Parameters
    ----------
    points:
        Either a ``DataFrame`` with at minimum the columns ``lat``,
        ``lon``, and ``time`` (or ``date`` as an alias for ``time``), **or**
        a :class:`~point_collocation.core.plan.Plan` object previously
        built with :func:`~point_collocation.plan`.

        When a :class:`~point_collocation.core.plan.Plan` is supplied the
        *data_source*, *source_kwargs*, and *variables* parameters are
        ignored and taken from the plan instead.  One output row is
        produced per (point, granule) pair; points with zero matching
        granules produce a single NaN row.
    variables:
        Names of the dataset variables to extract at each point.
        Required when *points* is a ``DataFrame``; ignored (taken from
        the plan) when *points* is a :class:`~point_collocation.core.plan.Plan`.
    data_source:
        Data source to use for locating files.  Defaults to
        ``"earthaccess"``, which searches NASA Earthdata via
        ``earthaccess.search_data()``.  Additional data sources may be
        added in the future.
    source_kwargs:
        Keyword arguments passed directly to the search function for the
        chosen *data_source*.  For ``data_source="earthaccess"`` these
        are forwarded to ``earthaccess.search_data()``; at minimum
        ``short_name`` must be provided.  Example::

            source_kwargs={
                "short_name": "PACE_OCI_L3M_RRS",
                "granule_name": "*.DAY.*.4km.*",
            }

    nc_type:
        ``"grouped"`` for NetCDF files that use groups (e.g., PACE),
        ``"flat"`` for conventional flat NetCDF/Zarr files.
        Currently only ``"flat"`` is supported.
    return_diagnostics:
        When ``True``, return ``(DataFrame, MatchupReport)`` instead of
        just the ``DataFrame``.  Not supported when *points* is a
        :class:`~point_collocation.core.plan.Plan`.
    **open_dataset_kwargs:
        Extra keyword arguments forwarded to ``xarray.open_dataset`` for
        every source opened during the run.  Defaults to
        ``engine="h5netcdf"`` when no ``engine`` key is provided.

    Returns
    -------
    pandas.DataFrame
        When *points* is a ``DataFrame``: original ``points`` columns
        plus one new column per variable in ``variables``.  Rows that
        could not be matched are preserved with ``NaN`` in the new columns.
        Row order matches the input.

        When *points* is a :class:`~point_collocation.core.plan.Plan`:
        one row per (point, granule) pair, including ``granule_id`` and
        variable columns.  Points with 0 matching granules contribute a
        single NaN row.
    MatchupReport
        Only returned when ``return_diagnostics=True`` (DataFrame path only).

    Raises
    ------
    ValueError
        If ``points`` is missing required columns (``lat``, ``lon``,
        ``time``/``date``), or if *data_source* is not recognised, or if
        ``variables`` is ``None`` when ``points`` is a DataFrame.
    """
    # ----------------------------------------------------------------
    # Plan-based execution path
    # ----------------------------------------------------------------
    from point_collocation.core.plan import Plan as _Plan  # avoid circular import at module level

    if isinstance(points, _Plan):
        effective_vars = variables if variables is not None else points.variables
        return _execute_plan(points, variables=effective_vars, **open_dataset_kwargs)

    # ----------------------------------------------------------------
    # DataFrame-based execution path (existing behaviour)
    # ----------------------------------------------------------------
    if variables is None:
        raise ValueError(
            "'variables' is required when 'points' is a DataFrame. "
            "Pass a list of variable names, e.g. variables=['sst']."
        )

    points = _normalise_time_column(points)
    _validate_points(points)

    if data_source == "earthaccess":
        sources: Iterable[object] = _resolve_earthaccess_sources(
            points, source_kwargs=source_kwargs
        )
    else:
        raise ValueError(
            f"Unknown data_source {data_source!r}. "
            "Currently only 'earthaccess' is supported."
        )

    report = MatchupReport()

    result = points.copy()
    result["time"] = pd.to_datetime(result["time"])
    for var in variables:
        result[var] = float("nan")

    vars_expanded: set[str] = set()

    for source in sources:
        t0 = time.monotonic()
        source_id = get_source_id(source)

        # ----------------------------------------------------------------
        # Determine which points this source covers temporally.
        # ----------------------------------------------------------------
        try:
            t_start, t_end = parse_temporal_range(source_id)
            date_col = result["time"].dt.normalize()
            mask = (date_col >= t_start) & (date_col <= t_end)
        except ValueError:
            # Cannot parse date from filename → try all points.
            mask = pd.Series(True, index=result.index)

        pts_subset = result.loc[mask]
        if pts_subset.empty:
            continue

        # ----------------------------------------------------------------
        # Open the file, extract values, then close immediately so that
        # memory doesn't accumulate across many granules.
        # ----------------------------------------------------------------
        try:
            if hasattr(source, "open_dataset"):
                ds = source.open_dataset(**open_dataset_kwargs)
                try:
                    result, vars_found, vars_missing, warns, newly_expanded = _extract_into(
                        result, ds, pts_subset, variables
                    )
                finally:
                    if hasattr(ds, "close"):
                        ds.close()
            else:
                kwargs = dict(open_dataset_kwargs)
                if "engine" not in kwargs:
                    kwargs["engine"] = "h5netcdf"
                with xr.open_dataset(source, **kwargs) as ds:
                    result, vars_found, vars_missing, warns, newly_expanded = _extract_into(
                        result, ds, pts_subset, variables
                    )
            vars_expanded.update(newly_expanded)
            gs = GranuleSummary(
                granule_id=source_id,
                elapsed_seconds=time.monotonic() - t0,
                variables_found=vars_found,
                variables_missing=vars_missing,
                warnings=warns,
            )
        except Exception as exc:
            gs = GranuleSummary(
                granule_id=source_id,
                elapsed_seconds=time.monotonic() - t0,
                error=str(exc),
            )

        report._add_granule(gs)

    # Drop the pre-initialized NaN placeholder column for any variable that was
    # expanded into per-coordinate columns (e.g. Rrs → Rrs_412, Rrs_443, …).
    for var in vars_expanded:
        if var in result.columns:
            result = result.drop(columns=[var])

    if return_diagnostics:
        return result, report
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_REQUIRED_COLUMNS = {"lat", "lon", "time"}


def _normalise_time_column(points: PointsFrame) -> PointsFrame:
    """Return *points* with a ``time`` column, renaming ``date`` if needed.

    If ``time`` is already present, *points* is returned unchanged.
    If ``time`` is absent but ``date`` is present, a copy is returned
    with ``date`` renamed to ``time``.
    """
    if "time" in points.columns:
        return points
    if "date" in points.columns:
        return points.rename(columns={"date": "time"})
    return points


def _validate_points(points: PointsFrame) -> None:
    """Raise ``ValueError`` if *points* is missing required columns."""
    missing = _REQUIRED_COLUMNS - set(points.columns)
    if missing:
        raise ValueError(
            f"points DataFrame is missing required columns: {sorted(missing)}"
        )


def _resolve_earthaccess_sources(
    points: PointsFrame,
    *,
    source_kwargs: dict | None,
) -> list[object]:
    """Search earthaccess for granules covering each unique date in *points*.

    Iterates over unique dates, calls ``earthaccess.search_data()`` for
    each date (``temporal=(date, date)``), then opens all found granules
    with ``earthaccess.open()``.

    Parameters
    ----------
    points:
        Points DataFrame with a ``time`` column.
    source_kwargs:
        Keyword arguments passed directly to ``earthaccess.search_data()``.
        Must contain at least ``"short_name"``.

    Returns
    -------
    list
        Flat list of file-like objects returned by ``earthaccess.open()``.

    Raises
    ------
    ImportError
        If the ``earthaccess`` package is not installed.
    ValueError
        If ``source_kwargs`` does not contain ``"short_name"``.
    """
    try:
        import earthaccess  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "The 'earthaccess' package is required when data_source='earthaccess'. "
            "Install it with: pip install earthaccess"
        ) from exc

    base_kwargs: dict = dict(source_kwargs or {})
    if "short_name" not in base_kwargs:
        raise ValueError(
            "'source_kwargs' must contain 'short_name' when data_source='earthaccess'."
        )

    unique_dates = sorted(
        pd.to_datetime(points["time"]).dt.normalize().unique()
    )

    all_sources: list[object] = []
    for date in unique_dates:
        date_str = date.strftime("%Y-%m-%d")
        search_kwargs = {**base_kwargs, "temporal": (date_str, date_str)}
        results = earthaccess.search_data(**search_kwargs)
        if results:
            opened = earthaccess.open(results, pqdm_kwargs={"disable": True})
            all_sources.extend(opened)
    return all_sources


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

                for pt_idx in pt_indices:
                    row = plan.points.loc[pt_idx].to_dict()
                    row["granule_id"] = gm.granule_id

                    for var in variables:
                        if var not in ds or lat_name is None or lon_name is None:
                            row[var] = float("nan")
                            continue
                        try:
                            selected = ds[var].sel(
                                {lat_name: row["lat"], lon_name: row["lon"]},
                                method="nearest",
                            )
                            if selected.ndim == 0:
                                row[var] = selected.item()
                            else:
                                # Multi-dimensional: expand into coord-keyed entries
                                row[var] = float("nan")  # placeholder removed later
                                for coord_val, val in selected.to_series().items():
                                    row[f"{var}_{int(coord_val)}"] = float(val)
                        except Exception:
                            row[var] = float("nan")

                    output_rows.append(row)

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


def _extract_into(
    result: pd.DataFrame,
    ds: xr.Dataset,
    pts_subset: pd.DataFrame,
    variables: list[str],
) -> tuple[pd.DataFrame, list[str], list[str], list[str], list[str]]:
    """Extract *variables* from *ds* at each row of *pts_subset*.

    Values are written into *result* using the index from *pts_subset*.

    Returns
    -------
    result:
        Updated DataFrame with extracted values.
    vars_found:
        Variables that were present in the dataset.
    vars_missing:
        Variables requested but absent from the dataset.
    warnings:
        Non-fatal per-point extraction failures.
    vars_expanded:
        Variables whose selection produced extra dimensions and were
        therefore expanded into per-coordinate columns (e.g. ``Rrs``
        → ``Rrs_412``, ``Rrs_443``, …).
    """
    lat_name = _find_coord(ds, _LAT_NAMES)
    lon_name = _find_coord(ds, _LON_NAMES)

    vars_found: list[str] = []
    vars_missing: list[str] = []
    warnings: list[str] = []
    vars_expanded: list[str] = []

    # Collect expanded column data here to avoid inserting columns one-by-one,
    # which causes a pandas PerformanceWarning about DataFrame fragmentation.
    # key: col_name, value: dict of {index -> scalar value}
    all_expanded: dict[str, dict] = {}

    for var in variables:
        if var not in ds:
            vars_missing.append(var)
            continue
        vars_found.append(var)

        if lat_name is None or lon_name is None:
            # Cannot locate spatial coordinates — leave as NaN.
            warnings.append(
                f"No lat/lon coordinates found in dataset for variable {var!r}"
            )
            continue

        da = ds[var]
        _var_expanded = False
        for idx, row in pts_subset.iterrows():
            try:
                selected = da.sel(
                    {lat_name: row["lat"], lon_name: row["lon"]},
                    method="nearest",
                )
                if selected.ndim == 0:
                    result.loc[idx, var] = selected.item()
                else:
                    # Multi-dimensional result (e.g. wavelength axis): expand
                    # into individual columns named {var}_{int(coord_value)}.
                    _var_expanded = True
                    for coord_val, val in selected.to_series().items():
                        col_name = f"{var}_{int(coord_val)}"
                        all_expanded.setdefault(col_name, {})[idx] = val
            except Exception as exc:
                warnings.append(
                    f"Could not extract {var!r} at index {idx} "
                    f"(lat={row['lat']}, lon={row['lon']}): {exc}"
                )
        if _var_expanded:
            vars_expanded.append(var)

    # Add all expanded columns at once using pd.concat to avoid fragmentation.
    # For subsequent source files, columns may already exist — fill their NaN
    # values rather than creating duplicate columns.
    if all_expanded:
        new_cols = pd.DataFrame(all_expanded, index=result.index)
        truly_new = [c for c in new_cols.columns if c not in result.columns]
        already_exist = [c for c in new_cols.columns if c in result.columns]
        if truly_new:
            result = pd.concat([result, new_cols[truly_new]], axis=1)
        for col in already_exist:
            result[col] = result[col].fillna(new_cols[col])

    return result, vars_found, vars_missing, warnings, vars_expanded
