"""Planning module — preview point-to-granule routing before running matchup.

Public entry point
------------------
:func:`plan`
    Build a :class:`Plan` from a points DataFrame and earthaccess search
    parameters.  The plan records which granules cover each point so the
    user can inspect the routing before committing to a full extraction.

Typical workflow
----------------
::

    import point_collocation as pc

    plan = pc.plan(
        df_points,
        data_source="earthaccess",
        source_kwargs={"short_name": "PACE_OCI_L3M_RRS"},
        time_buffer="0h",
    )
    plan.summary()
    plan.show_variables()

    result = pc.matchup(plan, variables=["Rrs"])   # executes the plan; one row per point×granule
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pandas as pd

from point_collocation.core.types import PointsFrame

if TYPE_CHECKING:
    import xarray as xr

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

_REQUIRED_COLUMNS = {"lat", "lon", "time"}


@dataclass
class GranuleMeta:
    """Lightweight metadata record for a single earthaccess granule."""

    granule_id: str
    """Data URL of the granule (the ``GET DATA`` URL from UMM RelatedUrls)."""

    begin: pd.Timestamp
    """Start of the granule's temporal coverage (UTC)."""

    end: pd.Timestamp
    """End of the granule's temporal coverage (UTC)."""

    bbox: tuple[float, float, float, float] | None
    """Spatial bounding box ``(west, south, east, north)``, or ``None``."""

    result_index: int
    """Position of this granule's result object in :attr:`Plan.results`."""


@dataclass
class Plan:
    """A planned matchup: stores the point→granule mapping and search results.

    Attributes
    ----------
    points:
        Normalised points DataFrame (always has a ``time`` column).
    results:
        Original earthaccess result objects in search order.  Passed
        directly to ``earthaccess.open()`` when executing the plan.
    granules:
        :class:`GranuleMeta` for every unique granule returned by the
        search (parallel with *results*).
    point_granule_map:
        Maps each row index of *points* to a (possibly empty) list of
        indices into *granules*.
    variables:
        Default variables to extract during :func:`~point_collocation.matchup`.
        Can be overridden by passing ``variables`` directly to
        :func:`~point_collocation.matchup`.
    source_kwargs:
        earthaccess search kwargs used to build this plan.
    time_buffer:
        Temporal buffer that was applied when matching points to granules.
    """

    points: pd.DataFrame
    results: list[Any]
    granules: list[GranuleMeta]
    point_granule_map: dict[Any, list[int]]
    variables: list[str] = field(default_factory=list)
    source_kwargs: dict[str, Any] = field(default_factory=dict)
    time_buffer: pd.Timedelta = field(default_factory=lambda: pd.Timedelta(0))

    # ------------------------------------------------------------------
    # Indexing — plan[0] or plan[0:2] returns result objects
    # ------------------------------------------------------------------

    def __getitem__(self, idx: int | slice) -> Any:
        """Return earthaccess result object(s) at *idx*.

        Supports integer and slice indexing so that ``plan[0]`` and
        ``plan[0:2]`` can be passed to :meth:`open_dataset` and
        :meth:`open_mfdataset` respectively.
        """
        return self.results[idx]

    # ------------------------------------------------------------------
    # Dataset opening helpers
    # ------------------------------------------------------------------

    def open_dataset(
        self,
        result: Any,
        open_dataset_kwargs: dict[str, Any] | None = None,
    ) -> "xr.Dataset":
        """Open a single granule result as an :class:`xarray.Dataset`.

        Parameters
        ----------
        result:
            A single earthaccess result object, typically obtained via
            ``plan[n]``.
        open_dataset_kwargs:
            Keyword arguments forwarded to ``xarray.open_dataset``.
            Defaults to ``{"chunks": {}}`` (lazy/dask loading).
            ``engine`` defaults to ``"h5netcdf"`` when not specified.

        Returns
        -------
        xarray.Dataset
        """
        try:
            import earthaccess  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The 'earthaccess' package is required. "
                "Install it with: pip install earthaccess"
            ) from exc

        import xarray as xr

        kwargs = {"chunks": {}} if open_dataset_kwargs is None else dict(open_dataset_kwargs)
        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        file_objs = earthaccess.open([result], pqdm_kwargs={"disable": True})
        if len(file_objs) != 1:
            raise RuntimeError(
                f"Expected 1 file object from earthaccess.open, got {len(file_objs)}."
            )
        return xr.open_dataset(file_objs[0], **kwargs)  # type: ignore[arg-type]

    def open_mfdataset(
        self,
        results: list[Any],
        open_dataset_kwargs: dict[str, Any] | None = None,
    ) -> "xr.Dataset":
        """Open multiple granule results as a single :class:`xarray.Dataset`.

        Parameters
        ----------
        results:
            A list of earthaccess result objects, typically obtained via
            ``plan[start:stop]``.
        open_dataset_kwargs:
            Keyword arguments forwarded to ``xarray.open_mfdataset``.
            Defaults to ``{"chunks": {}}`` (lazy/dask loading).
            ``engine`` defaults to ``"h5netcdf"`` when not specified.

        Returns
        -------
        xarray.Dataset
        """
        try:
            import earthaccess  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The 'earthaccess' package is required. "
                "Install it with: pip install earthaccess"
            ) from exc

        import xarray as xr

        kwargs = {"chunks": {}} if open_dataset_kwargs is None else dict(open_dataset_kwargs)
        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        file_objs = earthaccess.open(list(results), pqdm_kwargs={"disable": True})
        return xr.open_mfdataset(file_objs, **kwargs)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # Variable inspection
    # ------------------------------------------------------------------

    def show_variables(
        self,
        open_dataset_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Open the first granule and print its dimensions and variables.

        Uses :meth:`open_dataset` to load the first result in the plan,
        then prints the dataset dimensions and data variable names.  This
        lets users discover available variable names before running a full
        :func:`~point_collocation.matchup`.

        Parameters
        ----------
        open_dataset_kwargs:
            Keyword arguments forwarded to ``xarray.open_dataset`` when
            opening the first granule.  Passed unchanged to
            :meth:`open_dataset`.

        Raises
        ------
        ValueError
            If the plan contains no granules.
        """
        if not self.results:
            raise ValueError("No granules in plan — cannot show variables.")

        with self.open_dataset(self.results[0], open_dataset_kwargs=open_dataset_kwargs) as ds:
            print(f"Dimensions : {dict(ds.sizes)}")
            print(f"Variables  : {list(ds.data_vars)}")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------

    def summary(self, n: int | None = None) -> None:
        """Print a human-readable summary of the plan.

        Parameters
        ----------
        n:
            Number of points to show in the per-point section.
            Defaults to ``min(5, len(self.points))``.
            ``0`` or negative values suppress the per-point section.
        """
        if n is None:
            n = min(5, len(self.points))
        elif n < 0:
            n = 0

        zero_match = sum(
            1 for g_list in self.point_granule_map.values() if len(g_list) == 0
        )
        multi_match = sum(
            1 for g_list in self.point_granule_map.values() if len(g_list) > 1
        )

        lines: list[str] = [
            f"Plan: {len(self.points)} points → {len(self.granules)} unique granule(s)",
            f"  Points with 0 matches : {zero_match}",
            f"  Points with >1 matches: {multi_match}",
            f"  Variables  : {self.variables}",
            f"  Time buffer: {self.time_buffer}",
        ]

        n_show = min(n, len(self.points))
        if n_show > 0:
            lines.append("")
            lines.append(f"First {n_show} point(s):")
            for pt_idx, row in self.points.head(n_show).iterrows():
                g_indices = self.point_granule_map.get(pt_idx, [])
                lines.append(
                    f"  [{pt_idx}] lat={row['lat']:.4f}, lon={row['lon']:.4f}, "
                    f"time={row['time']}: {len(g_indices)} match(es)"
                )
                for g_idx in g_indices:
                    lines.append(f"    → {self.granules[g_idx].granule_id}")

        print("\n".join(lines))


# ---------------------------------------------------------------------------
# Public factory function
# ---------------------------------------------------------------------------


def plan(
    points: PointsFrame,
    *,
    data_source: str = "earthaccess",
    source_kwargs: dict[str, Any] | None = None,
    time_buffer: str | pd.Timedelta | datetime.timedelta | int = "0h",
) -> Plan:
    """Build a :class:`Plan` previewing which granules cover each point.

    Parameters
    ----------
    points:
        DataFrame with at minimum ``lat``, ``lon``, and ``time`` (or
        ``date`` as an alias).  If the column is named ``date`` and
        contains date-only values, the time-of-day is set to noon
        (12:00 UTC) for matching purposes.
    data_source:
        Data source to search.  Currently only ``"earthaccess"`` is
        supported.
    source_kwargs:
        Keyword arguments forwarded to ``earthaccess.search_data()``.
        Must contain at least ``"short_name"``.
    time_buffer:
        Extra temporal margin when matching a point to a granule.  A
        point at time *t* matches a granule whose coverage is
        ``[begin, end]`` if ``begin - buffer ≤ t ≤ end + buffer``.
        Accepts a :class:`pandas.Timedelta`, :class:`datetime.timedelta`,
        or a pandas-parseable string (``"12H"``, ``"30min"``, …).
        Default is ``"0h"`` (exact overlap required).

    Returns
    -------
    Plan
        The planning object; inspect with :meth:`Plan.summary` and
        execute with :func:`~point_collocation.matchup`.

    Raises
    ------
    ValueError
        If *points* is missing required columns, *data_source* is not
        recognised, or ``source_kwargs`` does not contain ``"short_name"``.
    ImportError
        If the ``earthaccess`` package is not installed.
    """
    if data_source != "earthaccess":
        raise ValueError(
            f"Unknown data_source {data_source!r}. "
            "Currently only 'earthaccess' is supported."
        )

    points = _plan_normalise_time(points)
    _plan_validate_points(points)

    buffer = _parse_time_buffer(time_buffer)
    results, granule_metas = _search_earthaccess(points, source_kwargs=source_kwargs)
    point_granule_map = _match_points_to_granules(points, granule_metas, buffer)

    return Plan(
        points=points,
        results=results,
        granules=granule_metas,
        point_granule_map=point_granule_map,
        source_kwargs=dict(source_kwargs or {}),
        time_buffer=buffer,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _plan_normalise_time(points: PointsFrame) -> PointsFrame:
    """Return *points* with a ``time`` column.

    * If ``time`` is already present, a copy is returned with the column
      converted to :class:`pandas.Timestamp`.
    * If only ``date`` is present, it is renamed to ``time`` and the
      time-of-day is set to **noon (12:00 UTC)** to represent date-only
      inputs in temporal matching.
    * If neither column exists, the DataFrame is returned as-is (the
      subsequent validation step raises the appropriate error).
    """
    if "time" in points.columns:
        out = points.copy()
        out["time"] = pd.to_datetime(out["time"])
        return out

    if "date" in points.columns:
        out = points.copy().rename(columns={"date": "time"})
        out["time"] = pd.to_datetime(out["time"]).dt.normalize() + pd.Timedelta(hours=12)
        return out

    # Neither column present — return unchanged so validation can raise.
    return points


def _plan_validate_points(points: PointsFrame) -> None:
    """Raise ``ValueError`` if *points* is missing required columns."""
    missing = _REQUIRED_COLUMNS - set(points.columns)
    if missing:
        raise ValueError(
            f"points DataFrame is missing required columns: {sorted(missing)}"
        )


def _parse_time_buffer(
    time_buffer: str | pd.Timedelta | datetime.timedelta | int,
) -> pd.Timedelta:
    """Coerce *time_buffer* to a :class:`pandas.Timedelta`."""
    if isinstance(time_buffer, pd.Timedelta):
        return time_buffer
    if isinstance(time_buffer, datetime.timedelta):
        return pd.Timedelta(time_buffer)
    if isinstance(time_buffer, int) and time_buffer == 0:
        return pd.Timedelta(0)
    return pd.to_timedelta(time_buffer)


# ---------------------------------------------------------------------------
# earthaccess search & metadata extraction
# ---------------------------------------------------------------------------


def _search_earthaccess(
    points: pd.DataFrame,
    *,
    source_kwargs: dict[str, Any] | None,
) -> tuple[list[Any], list[GranuleMeta]]:
    """Search earthaccess over the full date range of *points*.

    Returns
    -------
    results:
        Earthaccess result objects in original search order.
    granule_metas:
        :class:`GranuleMeta` for each result (same order as *results*).

    Raises
    ------
    ImportError
        If ``earthaccess`` is not installed.
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

    base_kwargs: dict[str, Any] = dict(source_kwargs or {})
    if "short_name" not in base_kwargs:
        raise ValueError(
            "'source_kwargs' must contain 'short_name' when data_source='earthaccess'."
        )

    times = pd.to_datetime(points["time"])
    min_date = str(times.min().date())
    max_date = str(times.max().date())
    search_kwargs = {**base_kwargs, "temporal": (min_date, max_date)}

    results: list[Any] = list(earthaccess.search_data(**search_kwargs))

    granule_metas: list[GranuleMeta] = []
    for i, result in enumerate(results):
        granule_metas.append(_extract_granule_meta(result, result_index=i))

    return results, granule_metas


def _extract_granule_meta(result: Any, *, result_index: int) -> GranuleMeta:
    """Build a :class:`GranuleMeta` from a single earthaccess result object."""
    umm = _get_umm(result)

    rdt = umm["TemporalExtent"]["RangeDateTime"]
    begin = pd.Timestamp(rdt["BeginningDateTime"])
    end = pd.Timestamp(rdt["EndingDateTime"])

    granule_id = _get_data_url(umm)
    bbox = _get_bbox(umm)

    return GranuleMeta(
        granule_id=granule_id,
        begin=begin,
        end=end,
        bbox=bbox,
        result_index=result_index,
    )


def _get_umm(result: Any) -> dict[str, Any]:
    """Extract the UMM dict from an earthaccess result object.

    Supports both live DataGranule objects (``result["umm"]``) and the
    fixture/serialised format (``result["render_dict"]["umm"]``).
    """
    # Standard earthaccess DataGranule: dict-like with "umm" key.
    try:
        umm = result["umm"]
        if isinstance(umm, dict):
            return umm
    except (KeyError, TypeError, IndexError):
        pass

    # Fixture/serialised format: nested under "render_dict".
    try:
        umm = result["render_dict"]["umm"]
        if isinstance(umm, dict):
            return umm
    except (KeyError, TypeError, IndexError):
        pass

    raise ValueError(
        f"Cannot extract UMM metadata from result of type {type(result).__name__!r}."
    )


def _get_data_url(umm: dict[str, Any]) -> str:
    """Return the ``GET DATA`` URL from UMM ``RelatedUrls``.

    Prefers non-S3 URLs (i.e., HTTPS) when both are available.
    """
    related_urls: list[dict[str, Any]] = umm.get("RelatedUrls", [])
    # Prefer HTTPS GET DATA
    for url_info in related_urls:
        url = url_info.get("URL", "")
        if url_info.get("Type") == "GET DATA" and not url.startswith("s3://"):
            return url
    # Fall back to any GET DATA URL (S3 included)
    for url_info in related_urls:
        if url_info.get("Type") == "GET DATA":
            return url_info["URL"]
    raise ValueError(
        "No 'GET DATA' URL found in granule RelatedUrls. "
        f"Available types: {[u.get('Type') for u in related_urls]}"
    )


def _get_bbox(
    umm: dict[str, Any],
) -> tuple[float, float, float, float]:
    """Return the bounding box ``(west, south, east, north)`` for the granule.

    Supports both ``BoundingRectangles`` and ``GPolygons`` geometry.

    Raises
    ------
    ValueError
        If no spatial extent is found (indicates a malformed result object).
    """
    spatial: dict[str, Any] = umm.get("SpatialExtent", {})
    geom: dict[str, Any] = (
        spatial.get("HorizontalSpatialDomain", {}).get("Geometry", {})
    )

    bboxes: list[dict[str, Any]] = geom.get("BoundingRectangles", [])
    if bboxes:
        b = bboxes[0]
        return (
            float(b["WestBoundingCoordinate"]),
            float(b["SouthBoundingCoordinate"]),
            float(b["EastBoundingCoordinate"]),
            float(b["NorthBoundingCoordinate"]),
        )

    polygons: list[dict[str, Any]] = geom.get("GPolygons", [])
    if polygons:
        pts = polygons[0]["Boundary"]["Points"]
        lons = [float(p["Longitude"]) for p in pts]
        lats = [float(p["Latitude"]) for p in pts]
        return (min(lons), min(lats), max(lons), max(lats))

    raise ValueError(
        "No spatial extent (BoundingRectangles or GPolygons) found in "
        f"granule SpatialExtent: {spatial!r}"
    )


def _match_points_to_granules(
    points: pd.DataFrame,
    granule_metas: list[GranuleMeta],
    buffer: pd.Timedelta,
) -> dict[Any, list[int]]:
    """Return a mapping from each point's index to the list of matching granule indices.

    Matching criteria (all must be satisfied):

    * **Temporal**: ``granule.begin - buffer ≤ t_point ≤ granule.end + buffer``
    * **Spatial**: point ``(lat, lon)`` falls within the granule's bounding box.

    All timestamps are compared as timezone-naive UTC values so that
    tz-aware granule timestamps (ending in ``Z``) and tz-naive point
    timestamps can be mixed without error.
    """
    def _to_utc_naive(ts: pd.Timestamp) -> pd.Timestamp:
        """Strip tz info from a Timestamp, treating it as UTC."""
        if ts.tzinfo is not None:
            return ts.tz_convert("UTC").tz_localize(None)
        return ts

    result: dict[Any, list[int]] = {}

    for pt_idx, row in points.iterrows():
        t = _to_utc_naive(pd.Timestamp(row["time"]))
        lat = float(row["lat"])
        lon = float(row["lon"])

        matching: list[int] = []
        for g_idx, gm in enumerate(granule_metas):
            begin = _to_utc_naive(gm.begin)
            end = _to_utc_naive(gm.end)
            # Temporal check
            if not (begin - buffer <= t <= end + buffer):
                continue
            # Spatial check
            if gm.bbox is not None:
                west, south, east, north = gm.bbox
                if not (south <= lat <= north and west <= lon <= east):
                    continue
            matching.append(g_idx)

        result[pt_idx] = matching

    return result
