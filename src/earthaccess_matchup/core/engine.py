"""Core matchup engine — no earthaccess dependency here.

Responsibilities
----------------
* Accept a validated points ``DataFrame`` and an iterable of sources.
  Sources may be file-like objects (e.g., from ``earthaccess.open()``)
  or objects satisfying
  :class:`~earthaccess_matchup.core.types.SourceProtocol`.
* Open each source individually with ``xarray.open_dataset`` (never
  ``open_mfdataset``) to minimise cloud I/O and avoid memory leaks.
* Extract the requested variables at each point's location/time using
  nearest-neighbour selection.
* Collect results into a ``pandas.DataFrame`` with the original columns
  plus one new column per extracted variable.
* Populate a :class:`~earthaccess_matchup.diagnostics.report.MatchupReport`
  throughout the run.

The engine does **not** know about earthaccess, STAC, or any other
cloud-data provider.  All provider-specific logic lives in
``earthaccess_matchup.adapters``.

Future extension points
-----------------------
* ``pre_extract`` hook — spatial averaging, neighbourhood selection
* ``post_extract`` hook — QA filtering, unit conversion
"""

from __future__ import annotations

import time
from collections.abc import Iterable
from typing import Literal

import pandas as pd
import xarray as xr

from earthaccess_matchup.core._granule import get_source_id, parse_temporal_range
from earthaccess_matchup.core.types import PointsFrame
from earthaccess_matchup.diagnostics.report import GranuleSummary, MatchupReport

# Candidate coordinate names tried in order when locating lat/lon dims.
_LAT_NAMES = ("lat", "latitude", "Latitude", "LAT")
_LON_NAMES = ("lon", "longitude", "Longitude", "LON")


def matchup(
    points: PointsFrame,
    sources: Iterable[object] | None = None,
    *,
    variables: list[str],
    data_source: str | None = None,
    short_name: str | None = None,
    granule_name: str | None = None,
    nc_type: Literal["grouped", "flat"] = "flat",
    return_diagnostics: bool = False,
    **open_dataset_kwargs: object,
) -> pd.DataFrame | tuple[pd.DataFrame, MatchupReport]:
    """Extract variables from cloud-hosted granules at the given points.

    Parameters
    ----------
    points:
        ``DataFrame`` with at minimum the columns ``lat``, ``lon``, and
        ``time`` (or ``date`` as an alias for ``time``).  Additional
        columns are preserved in the output.  Results are returned in the
        same row order as *points*.
    sources:
        An iterable of file-like objects (e.g., from
        ``earthaccess.open()``) or objects satisfying
        :class:`~earthaccess_matchup.core.types.SourceProtocol`.
        Only sources whose temporal coverage overlaps the requested
        points are opened, minimising unnecessary I/O.  May be ``None``
        when *data_source* is provided.
    variables:
        Names of the dataset variables to extract at each point.
    data_source:
        When set to ``"earthaccess"``, files are located automatically
        via ``earthaccess.search_data()`` using *short_name* and
        *granule_name*; *sources* must be ``None`` in this case.
    short_name:
        NASA CMR short name for the collection, e.g.
        ``"PACE_OCI_L3M_RRS"``.  Required when *data_source* is
        ``"earthaccess"``.
    granule_name:
        Glob-style pattern passed to ``earthaccess.search_data()`` to
        filter granules, e.g. ``"*.DAY.*.4km.*"``.  Required when
        *data_source* is ``"earthaccess"``.
    nc_type:
        ``"grouped"`` for NetCDF files that use groups (e.g., PACE),
        ``"flat"`` for conventional flat NetCDF/Zarr files.
        Currently only ``"flat"`` is supported.
    return_diagnostics:
        When ``True``, return ``(DataFrame, MatchupReport)`` instead of
        just the ``DataFrame``.
    **open_dataset_kwargs:
        Extra keyword arguments forwarded to ``xarray.open_dataset`` for
        every source opened during the run.  Defaults to
        ``engine="h5netcdf"`` when no ``engine`` key is provided.

    Returns
    -------
    pandas.DataFrame
        Original ``points`` columns plus one new column per variable in
        ``variables``.  Rows that could not be matched are preserved with
        ``NaN`` in the new columns.  Row order matches the input.
    MatchupReport
        Only returned when ``return_diagnostics=True``.

    Raises
    ------
    ValueError
        If ``points`` is missing required columns (``lat``, ``lon``,
        ``time``/``date``), or if neither *sources* nor *data_source* is
        provided.
    """
    points = _normalise_time_column(points)
    _validate_points(points)

    if data_source is not None:
        if sources is not None:
            raise ValueError(
                "Provide either 'sources' or 'data_source', not both."
            )
        if data_source == "earthaccess":
            sources = _resolve_earthaccess_sources(
                points, short_name=short_name, granule_name=granule_name
            )
        else:
            raise ValueError(
                f"Unknown data_source {data_source!r}. "
                "Currently only 'earthaccess' is supported."
            )
    elif sources is None:
        raise ValueError(
            "Either 'sources' or 'data_source' must be provided."
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
    short_name: str | None,
    granule_name: str | None,
) -> list[object]:
    """Search earthaccess for granules covering each unique date in *points*.

    Iterates over unique dates, calls ``earthaccess.search_data()`` for
    each date (``temporal=(date, date)``), then opens all found granules
    with ``earthaccess.open()``.

    Parameters
    ----------
    points:
        Points DataFrame with a ``time`` column.
    short_name:
        NASA CMR short name (e.g. ``"PACE_OCI_L3M_RRS"``).
    granule_name:
        Glob-style granule name filter (e.g. ``"*.DAY.*.4km.*"``).

    Returns
    -------
    list
        Flat list of file-like objects returned by ``earthaccess.open()``.

    Raises
    ------
    ImportError
        If the ``earthaccess`` package is not installed.
    ValueError
        If *short_name* is not provided.
    """
    try:
        import earthaccess  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "The 'earthaccess' package is required when data_source='earthaccess'. "
            "Install it with: pip install earthaccess"
        ) from exc

    if short_name is None:
        raise ValueError(
            "'short_name' must be provided when data_source='earthaccess'."
        )

    unique_dates = sorted(
        pd.to_datetime(points["time"]).dt.normalize().unique()
    )

    all_sources: list[object] = []
    for date in unique_dates:
        date_str = date.strftime("%Y-%m-%d")
        search_kwargs: dict[str, str | tuple[str, str]] = {
            "short_name": short_name,
            "temporal": (date_str, date_str),
        }
        if granule_name is not None:
            search_kwargs["granule_name"] = granule_name
        results = earthaccess.search_data(**search_kwargs)
        if results:
            opened = earthaccess.open(results)
            all_sources.extend(opened)
    return all_sources


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
