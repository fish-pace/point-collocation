"""Core matchup engine — no earthaccess dependency here.

Responsibilities
----------------
* Accept a validated points ``DataFrame`` and an iterable of sources
  (anything satisfying :class:`~earthaccess_matchup.core.types.SourceProtocol`).
* Open each source individually with ``xarray.open_dataset`` (never
  ``open_mfdataset``) to minimise cloud I/O and avoid memory leaks.
* Extract the requested variables at each point's location/time.
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

from collections.abc import Iterable
from typing import Literal

import pandas as pd

from earthaccess_matchup.core.types import PointsFrame, SourceProtocol
from earthaccess_matchup.diagnostics.report import MatchupReport


def matchup(
    points: PointsFrame,
    sources: Iterable[SourceProtocol],
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
        An iterable of objects satisfying
        :class:`~earthaccess_matchup.core.types.SourceProtocol`.
        Typically produced by an adapter such as
        :class:`~earthaccess_matchup.adapters.earthaccess.EarthAccessAdapter`.
    variables:
        Names of the dataset variables to extract at each point.
    nc_type:
        ``"grouped"`` for NetCDF files that use groups (e.g., PACE),
        ``"flat"`` for conventional flat NetCDF/Zarr files.
    return_diagnostics:
        When ``True``, return ``(DataFrame, MatchupReport)`` instead of
        just the ``DataFrame``.
    **open_dataset_kwargs:
        Extra keyword arguments forwarded to ``xarray.open_dataset`` for
        every source opened during the run.

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

    Notes
    -----
    Matchup logic is not yet implemented.  This stub defines the public
    contract so that adapters, diagnostics, and tests can be developed
    in parallel.
    """
    _validate_points(points)
    report = MatchupReport()

    # TODO: implement extraction loop (one xarray.open_dataset per source)
    result = points.copy()
    for var in variables:
        result[var] = float("nan")

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
