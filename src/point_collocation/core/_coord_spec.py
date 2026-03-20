"""Coordinate specification (``coord_spec``) for point-collocation.

``coord_spec`` is the single source of truth for how the package maps
coordinate/column names between:

* the **points** :class:`pandas.DataFrame` (column names), and
* the **source** dataset (xarray Dataset coordinates or promotable variables).

The structure is::

    coord_spec = {
        "coordinate_system": "geographic",   # only "geographic" is supported
        "y":    {"source": "auto", "points": "auto"},  # latitude-like axis
        "x":    {"source": "auto", "points": "auto"},  # longitude-like axis
        "time": {"source": "auto", "points": "auto"},  # time axis
        # Optional additional axes, e.g.:
        "depth":      {"source": "z",          "points": "depth"},
        "wavelength": {"source": "wavelength", "points": "wave"},
    }

All keys are optional when building a user-provided spec; missing keys
receive sensible defaults via :func:`_normalize_coord_spec`.

``source`` is the coordinate/variable name in the source xarray Dataset.
``points`` is the column name in the points DataFrame.

Auto-detection for points
-------------------------
When ``"points": "auto"`` is set for a standard axis, the package tries
candidate column names in the following order:

* ``y`` (latitude):  ``lat``, ``latitude``, ``Latitude``, ``LATITUDE``
* ``x`` (longitude): ``lon``, ``longitude``, ``Longitude``, ``LONGITUDE``
* ``time``:          ``time``, ``date``, ``TIME``, ``DATE``, ``Time``, ``Date``

If multiple candidates are found or none are found, a :exc:`ValueError`
is raised with a clear message and instructions.

For other optional axes (e.g. ``depth``, ``wavelength``), ``"auto"``
uses the axis name itself as the column name, and silently skips the
axis if the column is absent (optional axes).

Auto-detection for source
-------------------------
When ``"source": "auto"`` is set, the package uses standard name detection
for ``y``/``x`` (via :func:`~point_collocation.core._open_method._find_geoloc_pair`)
and CF-convention / name-based detection for ``time``.  For optional axes, the
axis name itself is used as the variable name in the dataset.
"""

from __future__ import annotations

import copy
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

# ---------------------------------------------------------------------------
# Candidate column names for auto-detection
# ---------------------------------------------------------------------------

#: Candidate column names for latitude (y) in the points DataFrame.
_POINTS_Y_CANDIDATES: list[str] = ["lat", "latitude", "Latitude", "LATITUDE"]

#: Candidate column names for longitude (x) in the points DataFrame.
_POINTS_X_CANDIDATES: list[str] = ["lon", "longitude", "Longitude", "LONGITUDE"]

#: Candidate column names for time in the points DataFrame.
_POINTS_TIME_CANDIDATES: list[str] = ["time", "date", "TIME", "DATE", "Time", "Date"]

# ---------------------------------------------------------------------------
# Default coord_spec
# ---------------------------------------------------------------------------

#: Default coord_spec applied when the caller passes ``coord_spec=None``.
DEFAULT_COORD_SPEC: dict = {
    "coordinate_system": "geographic",
    "y":    {"source": "auto", "points": "auto"},
    "x":    {"source": "auto", "points": "auto"},
    "time": {"source": "auto", "points": "auto"},
}

# Reserved top-level keys that are not additional axes.
_RESERVED_COORD_SPEC_KEYS: frozenset[str] = frozenset(
    {"coordinate_system", "y", "x", "time"}
)

_VALID_COORDINATE_SYSTEMS: frozenset[str] = frozenset({"geographic"})

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def _validate_coord_spec(coord_spec: dict) -> None:
    """Raise :exc:`ValueError` if *coord_spec* has an invalid structure.

    Only structural keys are validated; value content (e.g. the column
    names themselves) is validated lazily at resolution time.

    Parameters
    ----------
    coord_spec:
        User-provided coord_spec dict.

    Raises
    ------
    TypeError
        If *coord_spec* is not a dict, or if any nested value has the
        wrong type.
    ValueError
        If ``coordinate_system`` is not a recognised value.
    """
    if not isinstance(coord_spec, dict):
        raise TypeError(
            f"coord_spec must be a dict, got {type(coord_spec).__name__!r}."
        )

    coord_sys = coord_spec.get("coordinate_system", "geographic")
    if coord_sys not in _VALID_COORDINATE_SYSTEMS:
        raise ValueError(
            f"coord_spec['coordinate_system']={coord_sys!r} is not valid. "
            f"Currently only {sorted(_VALID_COORDINATE_SYSTEMS)} is supported."
        )

    for axis in ("y", "x", "time"):
        if axis in coord_spec:
            if not isinstance(coord_spec[axis], dict):
                raise TypeError(
                    f"coord_spec[{axis!r}] must be a dict with "
                    f"'source' and 'points' keys."
                )

    # Validate optional additional axes (any key not in _RESERVED_COORD_SPEC_KEYS).
    for axis_name, axis_spec in coord_spec.items():
        if axis_name in _RESERVED_COORD_SPEC_KEYS:
            continue
        if not isinstance(axis_spec, dict):
            raise TypeError(
                f"coord_spec[{axis_name!r}] must be a dict with "
                f"'source' and/or 'points' keys."
            )


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def _normalize_coord_spec(coord_spec: dict | None) -> dict:
    """Return a fully-filled *coord_spec* by merging user values with defaults.

    Parameters
    ----------
    coord_spec:
        User-provided coord_spec dict, or ``None`` to get the pure defaults.

    Returns
    -------
    dict
        Normalized coord_spec with all required keys populated.

    Raises
    ------
    TypeError, ValueError
        If *coord_spec* fails structural validation.
    """
    if coord_spec is None:
        return copy.deepcopy(DEFAULT_COORD_SPEC)

    _validate_coord_spec(coord_spec)

    result = copy.deepcopy(coord_spec)

    # Fill top-level defaults.
    result.setdefault("coordinate_system", "geographic")

    for axis in ("y", "x", "time"):
        result.setdefault(axis, {})
        result[axis].setdefault("source", "auto")
        result[axis].setdefault("points", "auto")

    # Fill defaults for optional additional axes.
    for axis_name, axis_spec in result.items():
        if axis_name in _RESERVED_COORD_SPEC_KEYS:
            continue
        axis_spec.setdefault("source", "auto")
        axis_spec.setdefault("points", "auto")

    return result


# ---------------------------------------------------------------------------
# Points column detection helpers
# ---------------------------------------------------------------------------


def _detect_points_col_from_candidates(
    points: "pd.DataFrame",
    candidates: list[str],
    axis_description: str,
) -> str:
    """Detect exactly one column in *points* from an ordered *candidates* list.

    Parameters
    ----------
    points:
        Points DataFrame to inspect.
    candidates:
        Ordered list of candidate column names to search for.
    axis_description:
        Human-readable axis label for error messages (e.g. ``"latitude (y)"``).

    Returns
    -------
    str
        The single detected column name.

    Raises
    ------
    ValueError
        If zero candidates or more than one candidate is found.
    """
    found = [c for c in candidates if c in points.columns]
    if len(found) == 0:
        raise ValueError(
            f"Cannot auto-detect {axis_description} column in the points DataFrame. "
            f"None of the expected column names were found: {candidates}. "
            "Rename the column or set coord_spec explicitly, e.g.:\n"
            f"  coord_spec={{\"additional\": {{{axis_description!r}: {{\"points\": \"<your_col>\"}}}}}}"
        )
    if len(found) > 1:
        raise ValueError(
            f"Ambiguous {axis_description} column in the points DataFrame. "
            f"Multiple candidate names found: {found}. "
            "Rename or drop the extra column, or set coord_spec explicitly, e.g.:\n"
            f"  coord_spec={{\"additional\": {{{axis_description!r}: {{\"points\": \"<your_col>\"}}}}}}"
        )
    return found[0]


def _resolve_points_col(
    points: "pd.DataFrame",
    spec_val: str,
    candidates: list[str],
    axis_description: str,
    *,
    coord_sys: str = "geographic",
) -> str:
    """Resolve the actual column name in *points* for a single axis.

    Parameters
    ----------
    points:
        Points DataFrame.
    spec_val:
        Value from ``coord_spec[...]["points"]``.  Either ``"auto"`` or an
        explicit column name.
    candidates:
        Candidate column names tried when *spec_val* is ``"auto"``.
    axis_description:
        Human-readable axis label for error messages.
    coord_sys:
        Coordinate system in effect; only ``"geographic"`` supports ``"auto"``.

    Returns
    -------
    str
        The resolved column name.

    Raises
    ------
    ValueError
        If *spec_val* is ``"auto"`` and auto-detection fails, or if an
        explicitly-provided column name is not present in *points*.
    """
    if spec_val == "auto":
        if coord_sys != "geographic":
            raise ValueError(
                f"coord_spec[...][{axis_description!r}]['points']='auto' is not supported "
                f"for coordinate_system={coord_sys!r}. "
                "Specify the column name explicitly."
            )
        return _detect_points_col_from_candidates(points, candidates, axis_description)
    # Explicit column name.
    if spec_val not in points.columns:
        raise ValueError(
            f"coord_spec column {spec_val!r} for {axis_description} not found in "
            f"points DataFrame columns: {sorted(points.columns)}."
        )
    return spec_val


def resolve_points_columns(
    points: "pd.DataFrame",
    coord_spec: dict | None = None,
) -> dict[str, str]:
    """Resolve points DataFrame column names from *coord_spec*.

    Returns a mapping from axis name to the actual column name in *points*::

        {
            "y": "lat",       # or "latitude", etc.
            "x": "lon",
            "time": "time",
            "depth": "depth", # only if configured and present
            ...
        }

    Parameters
    ----------
    points:
        Points DataFrame.
    coord_spec:
        User coord_spec (raw or pre-normalized).  ``None`` uses the defaults.

    Returns
    -------
    dict[str, str]
        Maps each active axis to its column name in *points*.  Optional
        additional axes are included only when the configured column is
        present in *points*.

    Raises
    ------
    ValueError
        If auto-detection of y, x, or time fails (ambiguous or missing).
    """
    spec = _normalize_coord_spec(coord_spec)
    coord_sys = spec.get("coordinate_system", "geographic")

    result: dict[str, str] = {}

    # --- y (latitude-like) ---
    result["y"] = _resolve_points_col(
        points,
        spec.get("y", {}).get("points", "auto"),
        _POINTS_Y_CANDIDATES,
        "latitude (y)",
        coord_sys=coord_sys,
    )

    # --- x (longitude-like) ---
    result["x"] = _resolve_points_col(
        points,
        spec.get("x", {}).get("points", "auto"),
        _POINTS_X_CANDIDATES,
        "longitude (x)",
        coord_sys=coord_sys,
    )

    # --- time ---
    result["time"] = _resolve_points_col(
        points,
        spec.get("time", {}).get("points", "auto"),
        _POINTS_TIME_CANDIDATES,
        "time",
        coord_sys="geographic",  # time detection is always name-based
    )

    # --- optional additional axes ---
    for axis_name, axis_spec in spec.items():
        if axis_name in _RESERVED_COORD_SPEC_KEYS:
            continue
        pts_val = axis_spec.get("points", "auto")
        # Optional axis: use axis_name if "auto", explicit col name otherwise.
        col = axis_name if pts_val == "auto" else pts_val
        if col in points.columns:
            result[axis_name] = col
        # else: column absent — silently skip optional axis.

    return result


# ---------------------------------------------------------------------------
# Source coordinate resolution
# ---------------------------------------------------------------------------


def resolve_source_coord(
    ds: "object",  # xr.Dataset
    axis_name: str,
    axis_spec: dict,
) -> "str | None":
    """Resolve the source coordinate name for *axis_name* in *ds*.

    Parameters
    ----------
    ds:
        Source ``xarray.Dataset``.
    axis_name:
        Axis name (e.g. ``"time"``, ``"depth"``, ``"wavelength"``).
    axis_spec:
        ``coord_spec[axis_name]`` dict (e.g. ``coord_spec["time"]``,
        ``coord_spec["depth"]``).

    Returns
    -------
    str or None
        The coordinate name in *ds*, or ``None`` if not found (for optional
        axes).

    Raises
    ------
    ValueError
        If an explicitly-specified source coordinate is not present in *ds*.
    """
    import xarray as xr

    assert isinstance(ds, xr.Dataset)
    src = axis_spec.get("source", "auto")

    if src == "auto":
        # For time: try cf_xarray then name-based fallback.
        if axis_name == "time":
            from point_collocation.core.engine import _find_time_dim

            return _find_time_dim(ds)
        # For others: look for a coordinate with the same name as the axis.
        if axis_name in ds.coords:
            return axis_name
        if axis_name in ds.data_vars:
            return axis_name
        return None

    # Explicit source coordinate name.
    if src not in ds.coords and src not in ds.data_vars and src not in ds.dims:
        raise ValueError(
            f"Source coordinate {src!r} for axis {axis_name!r} not found in dataset. "
            f"Available coordinates: {list(ds.coords)}. "
            f"Available variables: {list(ds.data_vars)}. "
            "Check coord_spec or use open_method set_coords to promote variables."
        )
    return src
