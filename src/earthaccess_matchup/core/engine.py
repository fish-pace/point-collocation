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
    sources: Iterable[object],
    *,
    variables: list[str],
    nc_type: Literal["grouped", "flat"] = "flat",
    return_diagnostics: bool = False,
    **open_dataset_kwargs: object,
) -> pd.DataFrame | tuple[pd.DataFrame, MatchupReport]:
    """Extract variables from cloud-hosted granules at the given points.

    Parameters
    ----------
    points:
        ``DataFrame`` with at minimum the columns ``lat``, ``lon``, and
        ``time``.  Additional columns are preserved in the output.
    sources:
        An iterable of file-like objects (e.g., from
        ``earthaccess.open()``) or objects satisfying
        :class:`~earthaccess_matchup.core.types.SourceProtocol`.
        Only sources whose temporal coverage overlaps the requested
        points are opened, minimising unnecessary I/O.
    variables:
        Names of the dataset variables to extract at each point.
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
        ``NaN`` in the new columns.
    MatchupReport
        Only returned when ``return_diagnostics=True``.

    Raises
    ------
    ValueError
        If ``points`` is missing required columns (``lat``, ``lon``,
        ``time``).
    """
    _validate_points(points)
    report = MatchupReport()

    result = points.copy()
    result["time"] = pd.to_datetime(result["time"])
    for var in variables:
        result[var] = float("nan")

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
                    vars_found, vars_missing, warns = _extract_into(
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
                    vars_found, vars_missing, warns = _extract_into(
                        result, ds, pts_subset, variables
                    )
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

    if return_diagnostics:
        return result, report
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_REQUIRED_COLUMNS = {"lat", "lon", "time"}


def _validate_points(points: PointsFrame) -> None:
    """Raise ``ValueError`` if *points* is missing required columns."""
    missing = _REQUIRED_COLUMNS - set(points.columns)
    if missing:
        raise ValueError(
            f"points DataFrame is missing required columns: {sorted(missing)}"
        )


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
) -> tuple[list[str], list[str], list[str]]:
    """Extract *variables* from *ds* at each row of *pts_subset*.

    Values are written directly into *result* (in-place) using the
    index from *pts_subset*.

    Returns
    -------
    vars_found:
        Variables that were present in the dataset.
    vars_missing:
        Variables requested but absent from the dataset.
    warnings:
        Non-fatal per-point extraction failures.
    """
    lat_name = _find_coord(ds, _LAT_NAMES)
    lon_name = _find_coord(ds, _LON_NAMES)

    vars_found: list[str] = []
    vars_missing: list[str] = []
    warnings: list[str] = []

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
        for idx, row in pts_subset.iterrows():
            try:
                val = da.sel(
                    {lat_name: row["lat"], lon_name: row["lon"]},
                    method="nearest",
                ).item()
                result.loc[idx, var] = val
            except Exception as exc:
                warnings.append(
                    f"Could not extract {var!r} at index {idx} "
                    f"(lat={row['lat']}, lon={row['lon']}): {exc}"
                )

    return vars_found, vars_missing, warnings
