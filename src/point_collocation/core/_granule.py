"""Helpers for working with individual granules (source files).

Responsibilities
----------------
* Extract a human-readable identifier from an arbitrary source object.
* Parse the temporal coverage (start/end date) from a NASA-style L3
  granule filename.

Supported filename conventions
------------------------------
``YYYYDOY``             — single day (DOY = day-of-year, 001–366)
``YYYYDOY_YYYYDOY``     — multi-day range (e.g., 8-day composites, monthly)
``YYYYMMDD``            — single day in calendar format
``YYYYMMDD_YYYYMMDD``   — multi-day range in calendar format

The period keyword embedded in the filename (``.DAY.``, ``.8D.``,
``.MO.``) is used to infer the end date when only a start date is
present.

Examples of supported filenames
--------------------------------
* ``PACE_OCI_2024070.L3m.DAY.RRS.Rrs_412.4km.nc``
* ``PACE_OCI_2024049_2024056.L3m.8D.CHL.chlor_a.9km.nc``
* ``AQUA_MODIS.20230601.L3m.DAY.SST.sst.4km.nc``
* ``AQUA_MODIS.20230601_20230630.L3m.MO.CHL.chlor_a.9km.nc``
"""

from __future__ import annotations

import calendar
import os
import pathlib
import re
from datetime import datetime, timedelta

import pandas as pd


def get_source_id(source: object) -> str:
    """Return a human-readable identifier (basename) for *source*.

    Tries, in order:

    1. ``pathlib.Path`` → ``path.name``
    2. Plain ``str`` → ``os.path.basename(source)``
    3. Object with a ``.path`` or ``.name`` string attribute
    4. ``str(source)`` as last resort
    """
    if isinstance(source, pathlib.Path):
        return source.name
    if isinstance(source, str):
        return os.path.basename(source)
    for attr in ("path", "name"):
        val = getattr(source, attr, None)
        if isinstance(val, str) and val:
            return os.path.basename(val)
    return str(source)


def parse_temporal_range(filename: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Return ``(start, end)`` timestamps for the granule named *filename*.

    Only the basename of *filename* is examined.

    Parameters
    ----------
    filename:
        File path or basename.

    Returns
    -------
    tuple[pandas.Timestamp, pandas.Timestamp]
        Inclusive start and end dates (time component is midnight UTC).

    Raises
    ------
    ValueError
        If no recognisable date pattern is found in *filename*.
    """
    basename = os.path.basename(filename)

    # ------------------------------------------------------------------
    # DOY-format pair:  YYYYDOY_YYYYDOY
    # ------------------------------------------------------------------
    m = re.search(r"(?<!\d)(\d{7})_(\d{7})(?!\d)", basename)
    if m:
        try:
            start = datetime.strptime(m.group(1), "%Y%j")
            end = datetime.strptime(m.group(2), "%Y%j")
            return pd.Timestamp(start), pd.Timestamp(end)
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # Calendar-format pair:  YYYYMMDD_YYYYMMDD
    # ------------------------------------------------------------------
    m = re.search(r"(?<!\d)(20\d{6})_(20\d{6})(?!\d)", basename)
    if m:
        try:
            start = datetime.strptime(m.group(1), "%Y%m%d")
            end = datetime.strptime(m.group(2), "%Y%m%d")
            return pd.Timestamp(start), pd.Timestamp(end)
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # Single DOY date:  YYYYDOY
    # ------------------------------------------------------------------
    m = re.search(r"(?<!\d)(\d{7})(?!\d)", basename)
    if m:
        try:
            start = datetime.strptime(m.group(1), "%Y%j")
            end = _infer_end_date(start, basename)
            return pd.Timestamp(start), pd.Timestamp(end)
        except ValueError:
            pass

    # ------------------------------------------------------------------
    # Single calendar date:  YYYYMMDD (must start with "20…")
    # ------------------------------------------------------------------
    m = re.search(r"(?<!\d)(20\d{6})(?!\d)", basename)
    if m:
        try:
            start = datetime.strptime(m.group(1), "%Y%m%d")
            end = _infer_end_date(start, basename)
            return pd.Timestamp(start), pd.Timestamp(end)
        except ValueError:
            pass

    raise ValueError(
        f"Cannot parse temporal range from filename: {basename!r}"
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _infer_end_date(start: datetime, filename: str) -> datetime:
    """Infer the end date from *start* and the period token in *filename*."""
    upper = filename.upper()
    if ".8D." in upper or ".8DAY." in upper:
        return start + timedelta(days=7)
    if ".MO." in upper or ".MON." in upper or ".MONTH." in upper:
        last_day = calendar.monthrange(start.year, start.month)[1]
        return start.replace(day=last_day)
    # Default: treat as a single day (daily composite or unknown period)
    return start
