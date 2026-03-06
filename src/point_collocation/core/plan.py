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

import bisect
import datetime
import fnmatch
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

    polygon: list[tuple[float, float]] | None = None
    """GPolygon boundary as ``(lon, lat)`` pairs, or ``None`` for rectangular geometries.

    When present, point containment is tested using the actual polygon boundary
    (ray-casting algorithm) rather than the coarser bounding-box approximation.
    """


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
    # Indexing — plan[0] returns a result object; plan[0:10] returns a
    # subset Plan restricted to the sliced points.
    # ------------------------------------------------------------------

    def __getitem__(self, idx: int | slice) -> "Plan | Any":
        """Return a subset :class:`Plan` or a single earthaccess result.

        Parameters
        ----------
        idx:
            * **Integer** — returns the earthaccess result object at that
              position (``self.results[idx]``), so that ``plan[0]`` can
              still be passed to :meth:`open_dataset`.
            * **Slice** — returns a new :class:`Plan` whose ``points``
              are the rows selected by the slice (``points.iloc[idx]``),
              with ``point_granule_map``, ``granules``, and ``results``
              filtered and re-indexed accordingly.  This allows users to
              test a subset of a large plan::

                  res = pc.matchup(plan[0:10], geometry="grid", variables=["avw"])
        """
        if isinstance(idx, int):
            return self.results[idx]

        # --- Slice: subset by points ---
        subset_points = self.points.iloc[idx]
        subset_pt_indices = list(subset_points.index)

        # Collect granule indices (into self.granules) needed by the subset.
        needed_g_idx: list[int] = []
        seen_g: set[int] = set()
        for pt_idx in subset_pt_indices:
            for g_idx in self.point_granule_map.get(pt_idx, []):
                if g_idx not in seen_g:
                    needed_g_idx.append(g_idx)
                    seen_g.add(g_idx)
        needed_g_idx.sort()

        # Build re-index map: old granule index → new granule index.
        g_remap: dict[int, int] = {old: new for new, old in enumerate(needed_g_idx)}

        # New granules with corrected result_index (sequential from 0).
        new_granules = [
            GranuleMeta(
                granule_id=self.granules[old_g].granule_id,
                begin=self.granules[old_g].begin,
                end=self.granules[old_g].end,
                bbox=self.granules[old_g].bbox,
                result_index=new_g,
            )
            for new_g, old_g in enumerate(needed_g_idx)
        ]

        # New results list — only the results referenced by kept granules.
        new_results = [self.results[self.granules[old_g].result_index] for old_g in needed_g_idx]

        # New point_granule_map using re-indexed granule indices.
        new_pgm: dict[Any, list[int]] = {
            pt_idx: [g_remap[g] for g in self.point_granule_map.get(pt_idx, [])]
            for pt_idx in subset_pt_indices
        }

        return Plan(
            points=subset_points,
            results=new_results,
            granules=new_granules,
            point_granule_map=new_pgm,
            variables=list(self.variables),
            source_kwargs=dict(self.source_kwargs),
            time_buffer=self.time_buffer,
        )

    # ------------------------------------------------------------------
    # Dataset opening helpers
    # ------------------------------------------------------------------

    def open_dataset(
        self,
        result: Any,
        geometry: str | None = None,
        open_method: str | None = None,
        open_dataset_kwargs: dict[str, Any] | None = None,
    ) -> "xr.Dataset":
        """Open a single granule result as an :class:`xarray.Dataset`.

        Parameters
        ----------
        result:
            A single earthaccess result object, typically obtained via
            ``plan[n]``.
        geometry:
            Data geometry type.  ``"grid"`` (L3/gridded) or ``"swath"``
            (L2/swath).  When provided, determines the default
            ``open_method`` if *open_method* is not given explicitly.
        open_method:
            How to open the granule.  ``"dataset"`` uses a plain
            ``xarray.open_dataset`` call (the default when *geometry* is
            ``None`` or ``"grid"``).  ``"datatree-merge"`` opens as a
            DataTree and merges all groups into a flat dataset (the
            default when *geometry* is ``"swath"``).
        open_dataset_kwargs:
            Keyword arguments forwarded to ``xarray.open_dataset`` or
            ``xarray.open_datatree``.  ``chunks`` defaults to ``{}``
            (lazy/dask loading) unless explicitly overridden.  ``engine``
            defaults to ``"h5netcdf"`` when not specified.

        Returns
        -------
        xarray.Dataset
        """
        from point_collocation.core.engine import (
            _VALID_GEOMETRIES,
            _VALID_OPEN_METHODS,
            _merge_datatree,
            _open_datatree,
        )

        if geometry is not None and geometry not in _VALID_GEOMETRIES:
            raise ValueError(
                f"geometry={geometry!r} is not valid. "
                f"Must be one of {sorted(_VALID_GEOMETRIES)}."
            )

        # Resolve open_method default from geometry.
        if open_method is None:
            open_method = "datatree-merge" if geometry == "swath" else "dataset"

        if open_method not in _VALID_OPEN_METHODS:
            raise ValueError(
                f"open_method={open_method!r} is not valid. "
                f"Must be one of {sorted(_VALID_OPEN_METHODS)}."
            )

        try:
            import earthaccess  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The 'earthaccess' package is required. "
                "Install it with: pip install earthaccess"
            ) from exc

        import xarray as xr

        kwargs = {"chunks": {}, **(open_dataset_kwargs or {})}
        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        file_objs = earthaccess.open([result], pqdm_kwargs={"disable": True})
        if len(file_objs) != 1:
            raise RuntimeError(
                f"Expected 1 file object from earthaccess.open, got {len(file_objs)}."
            )

        if open_method == "datatree-merge":
            dt = _open_datatree(file_objs[0], kwargs)
            try:
                return _merge_datatree(dt)
            finally:
                if hasattr(dt, "close"):
                    dt.close()

        return xr.open_dataset(file_objs[0], **kwargs)  # type: ignore[arg-type]

    def open_mfdataset(
        self,
        results: "list[Any] | Plan",
        geometry: str | None = None,
        open_method: str | None = None,
        open_dataset_kwargs: dict[str, Any] | None = None,
    ) -> "xr.Dataset":
        """Open multiple granule results as a single :class:`xarray.Dataset`.

        Parameters
        ----------
        results:
            A list of earthaccess result objects, or a :class:`Plan`
            (e.g. ``plan[0:2]``).  When a :class:`Plan` is passed its
            ``results`` attribute is used.
        geometry:
            Data geometry type.  ``"grid"`` (L3/gridded) or ``"swath"``
            (L2/swath).  When provided, determines the default
            ``open_method`` if *open_method* is not given explicitly.
        open_method:
            How to open each granule.  ``"dataset"`` uses
            ``xarray.open_mfdataset`` across all file objects (the default
            when *geometry* is ``None`` or ``"grid"``).
            ``"datatree-merge"`` opens each granule as a DataTree, merges
            its groups into a flat dataset, then concatenates all granules
            along a new ``granule`` dimension (the default when *geometry*
            is ``"swath"``).
        open_dataset_kwargs:
            Keyword arguments forwarded to ``xarray.open_mfdataset`` or
            ``xarray.open_datatree``.  ``chunks`` defaults to ``{}``
            (lazy/dask loading) unless explicitly overridden.  ``engine``
            defaults to ``"h5netcdf"`` when not specified.

        Returns
        -------
        xarray.Dataset
        """
        from point_collocation.core.engine import (
            _VALID_GEOMETRIES,
            _VALID_OPEN_METHODS,
            _merge_datatree,
            _open_datatree,
        )

        if geometry is not None and geometry not in _VALID_GEOMETRIES:
            raise ValueError(
                f"geometry={geometry!r} is not valid. "
                f"Must be one of {sorted(_VALID_GEOMETRIES)}."
            )

        # Resolve open_method default from geometry.
        if open_method is None:
            open_method = "datatree-merge" if geometry == "swath" else "dataset"

        if open_method not in _VALID_OPEN_METHODS:
            raise ValueError(
                f"open_method={open_method!r} is not valid. "
                f"Must be one of {sorted(_VALID_OPEN_METHODS)}."
            )

        try:
            import earthaccess  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The 'earthaccess' package is required. "
                "Install it with: pip install earthaccess"
            ) from exc

        import xarray as xr

        kwargs = {"chunks": {}, **(open_dataset_kwargs or {})}
        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        result_list = results.results if isinstance(results, Plan) else list(results)
        file_objs = earthaccess.open(result_list, pqdm_kwargs={"disable": True})

        if open_method == "datatree-merge":
            # Open each granule as a DataTree, merge its groups, then
            # concatenate all granule datasets along a new "granule" dim.
            merged_datasets: list[xr.Dataset] = []
            for file_obj in file_objs:
                dt = _open_datatree(file_obj, kwargs)
                try:
                    merged_datasets.append(_merge_datatree(dt))
                finally:
                    if hasattr(dt, "close"):
                        dt.close()
            if not merged_datasets:
                return xr.Dataset()
            return xr.concat(merged_datasets, dim="granule")

        return xr.open_mfdataset(file_objs, **kwargs)  # type: ignore[arg-type]

    # ------------------------------------------------------------------
    # Variable inspection
    # ------------------------------------------------------------------

    def show_variables(
        self,
        geometry: str,
        open_method: str | None = None,
        open_dataset_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Open the first granule and print its dimensions and variables.

        Uses :meth:`open_dataset` (or a DataTree for
        ``open_method="datatree-merge"``) to load the first result in the
        plan, then prints the dataset dimensions, data variable names, and
        geolocation detection results.  This lets users discover available
        variable names before running a full :func:`~point_collocation.matchup`.

        Parameters
        ----------
        geometry:
            Data geometry type.  Must be ``"grid"`` (L3/gridded, 1-D
            lat/lon coordinates) or ``"swath"`` (L2/swath, 2-D lat/lon
            arrays).  This is a required argument — no default is provided.
        open_method:
            How to open the granule.  ``"dataset"`` uses a plain
            ``xarray.open_dataset`` call.  ``"datatree-merge"`` opens as a
            DataTree, merges into a flat dataset, then prints the merged
            summary followed by group details at the end.  Defaults to
            ``"dataset"`` when ``geometry="grid"`` and ``"datatree-merge"``
            when ``geometry="swath"``.
        open_dataset_kwargs:
            Keyword arguments forwarded to ``xarray.open_dataset`` when
            opening the first granule.  Passed unchanged to
            :meth:`open_dataset`.

        Raises
        ------
        ValueError
            If the plan contains no granules.
        """
        from point_collocation.core.engine import (
            _GEOLOC_PAIRS,
            _VALID_GEOMETRIES,
            _VALID_OPEN_METHODS,
            _merge_datatree,
            _open_datatree,
        )

        if geometry not in _VALID_GEOMETRIES:
            raise ValueError(
                f"geometry={geometry!r} is not valid. "
                f"Must be one of {sorted(_VALID_GEOMETRIES)}."
            )

        if open_method is None:
            open_method = "dataset" if geometry == "grid" else "datatree-merge"

        if open_method not in _VALID_OPEN_METHODS:
            raise ValueError(
                f"open_method={open_method!r} is not valid. "
                f"Must be one of {sorted(_VALID_OPEN_METHODS)}."
            )

        if not self.results:
            raise ValueError("No granules in plan — cannot show variables.")

        import xarray as xr

        kwargs: dict[str, Any] = {"chunks": {}, **(open_dataset_kwargs or {})}
        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"

        try:
            import earthaccess  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The 'earthaccess' package is required. "
                "Install it with: pip install earthaccess"
            ) from exc

        file_objs = earthaccess.open([self.results[0]], pqdm_kwargs={"disable": True})
        if len(file_objs) != 1:
            raise RuntimeError(
                f"Expected 1 file object from earthaccess.open, got {len(file_objs)}."
            )
        file_obj = file_objs[0]

        print(f"geometry     : {geometry!r}")
        print(f"open_method  : {open_method!r}")

        if open_method == "datatree-merge":
            # Open as DataTree and merge for the summary view.
            dt = _open_datatree(file_obj, kwargs)
            ds_flat = _merge_datatree(dt)

            # Print merged summary first.
            print(f"Dimensions : {dict(ds_flat.sizes)}")
            print(f"Variables  : {list(ds_flat.data_vars)}")
        else:
            ds_flat = xr.open_dataset(file_obj, **kwargs)  # type: ignore[arg-type]
            print(f"Dimensions : {dict(ds_flat.sizes)}")
            print(f"Variables  : {list(ds_flat.data_vars)}")

        # Geolocation detection results.
        found_pairs: list[tuple[str, str]] = []
        for lon_name, lat_name in _GEOLOC_PAIRS:
            has_lon = lon_name in ds_flat.coords or lon_name in ds_flat.data_vars
            has_lat = lat_name in ds_flat.coords or lat_name in ds_flat.data_vars
            if has_lon and has_lat:
                found_pairs.append((lon_name, lat_name))

        if len(found_pairs) == 0:
            alt_open_method = "datatree-merge" if open_method == "dataset" else "dataset"
            alt = f"plan.show_variables(geometry={geometry!r}, open_method={alt_open_method!r})"
            print(
                f"\nGeolocation: NONE detected with open_method={open_method!r}. "
                f"Try {alt}."
            )
        elif len(found_pairs) == 1:
            lon_n, lat_n = found_pairs[0]
            lon_var = ds_flat.coords[lon_n] if lon_n in ds_flat.coords else ds_flat[lon_n]
            lat_var = ds_flat.coords[lat_n] if lat_n in ds_flat.coords else ds_flat[lat_n]
            print(
                f"\nGeolocation: ({lon_n!r}, {lat_n!r}) — "
                f"lon dims={tuple(lon_var.dims)}, lat dims={tuple(lat_var.dims)}"
            )
        else:
            print(f"\nGeolocation: ambiguous — detected pairs: {found_pairs}")

        # For datatree-merge, print group details at the end.
        if open_method == "datatree-merge":
            print("\nDataTree groups (detail):")
            try:
                # xarray DataTree API (>= 2024.x).
                for node in dt.subtree:  # type: ignore[union-attr]
                    path = node.path if hasattr(node, "path") else str(node.name)
                    ds_node = node.ds
                    if ds_node is not None:
                        dims_str = dict(ds_node.sizes)
                        vars_list = list(ds_node.data_vars)
                        print(f"  {path or '/'}")
                        print(f"    Dimensions : {dims_str}")
                        print(f"    Variables  : {vars_list}")
            except AttributeError:
                # datatree package API.
                for path, node in dt.items():  # type: ignore[union-attr]
                    ds_node = node.ds
                    if ds_node is not None:
                        dims_str = dict(ds_node.sizes)
                        vars_list = list(ds_node.data_vars)
                        print(f"  {path or '/'}")
                        print(f"    Dimensions : {dims_str}")
                        print(f"    Variables  : {vars_list}")

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

        matched_granule_count = len(
            {g_idx for g_list in self.point_granule_map.values() for g_idx in g_list}
        )

        lines: list[str] = [
            f"Plan: {len(self.points)} points → {matched_granule_count} unique granule(s)",
            f"  Points with 0 matches : {zero_match}",
            f"  Points with >1 matches: {multi_match}",
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
    """Search earthaccess over the full date range and spatial extent of *points*.

    If ``"granule_name"`` is present in *source_kwargs*, it is extracted
    and used to filter results after the search via :func:`fnmatch.fnmatch`
    on each result's ``data_links()``.  This is faster than passing
    ``granule_name`` directly to ``earthaccess.search_data()``.

    A ``bounding_box`` ``(lon_min, lat_min, lon_max, lat_max)`` is
    automatically derived from *points* and added to the search unless the
    caller already supplies ``"bounding_box"`` in *source_kwargs*.  This
    ensures that for L2 products—whose granules are non-rectangular and
    non-global—only granules that intersect the point cloud are returned.

    Returns
    -------
    results:
        Earthaccess result objects in original search order, filtered by
        ``granule_name`` pattern when provided.
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

    # Extract granule_name for post-search filtering (faster than passing to search_data).
    granule_name_pattern: str | None = base_kwargs.pop("granule_name", None)

    times = pd.to_datetime(points["time"])
    min_date = str(times.min().date())
    max_date = str(times.max().date())
    search_kwargs = {**base_kwargs, "temporal": (min_date, max_date)}

    # Derive bounding_box from points if not already provided in source_kwargs.
    # bounding_box = (lon_min, lat_min, lon_max, lat_max)
    if "bounding_box" not in search_kwargs:
        lons = points["lon"].astype(float)
        lats = points["lat"].astype(float)
        search_kwargs["bounding_box"] = (
            float(lons.min()),
            float(lats.min()),
            float(lons.max()),
            float(lats.max()),
        )

    results: list[Any] = list(earthaccess.search_data(**search_kwargs))

    if granule_name_pattern is not None:
        results = [
            res
            for res in results
            if any(
                fnmatch.fnmatch(link, granule_name_pattern)
                for link in res.data_links()
            )
        ]

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
    polygon = _get_polygon_points(umm)

    return GranuleMeta(
        granule_id=granule_id,
        begin=begin,
        end=end,
        bbox=bbox,
        result_index=result_index,
        polygon=polygon,
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


def _get_polygon_points(
    umm: dict[str, Any],
) -> list[tuple[float, float]] | None:
    """Return GPolygon boundary as ``(lon, lat)`` pairs, or ``None``.

    Returns ``None`` when the granule uses ``BoundingRectangles`` geometry
    (e.g. L3 global products), in which case the bounding-box check in
    :func:`_match_points_to_granules` is used instead.

    Parameters
    ----------
    umm:
        UMM metadata dict for a single granule.
    """
    spatial: dict[str, Any] = umm.get("SpatialExtent", {})
    geom: dict[str, Any] = (
        spatial.get("HorizontalSpatialDomain", {}).get("Geometry", {})
    )
    polygons: list[dict[str, Any]] = geom.get("GPolygons", [])
    if not polygons:
        return None
    try:
        pts = polygons[0]["Boundary"]["Points"]
        return [(float(p["Longitude"]), float(p["Latitude"])) for p in pts]
    except (KeyError, TypeError) as exc:
        raise ValueError(
            "Malformed GPolygon in granule SpatialExtent: expected "
            "'GPolygons[0].Boundary.Points' with 'Longitude'/'Latitude' keys. "
            f"Got: {polygons[0]!r}"
        ) from exc


def _point_in_polygon(
    lon: float,
    lat: float,
    polygon: list[tuple[float, float]],
) -> bool:
    """Return ``True`` if ``(lon, lat)`` lies inside *polygon*.

    Uses the ray-casting (even-odd rule) algorithm in lon/lat space.
    The polygon need not be explicitly closed (first == last point), but
    closing it is harmless.

    Parameters
    ----------
    lon, lat:
        The point to test.
    polygon:
        Sequence of ``(lon, lat)`` pairs describing the polygon boundary.

    Notes
    -----
    This implementation uses planar geometry in lon/lat space.  Results may
    be incorrect for polygons that cross the antimeridian (±180°) — for
    example, a PACE OCI swath that starts in the western Pacific, crosses
    180°, and ends in the eastern Pacific.  For typical regional use cases
    (e.g. Gulf of Mexico, coastal US) the granule polygons do not cross the
    antimeridian and this algorithm is accurate.  If antimeridian-crossing
    granules are a concern, consider providing an explicit ``bounding_box``
    in ``source_kwargs`` to restrict the earthaccess search to the region of
    interest, which will exclude such far-field granules before they reach
    the polygon test.
    """
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if (yi > lat) != (yj > lat):
            intersect_x = (xj - xi) * (lat - yi) / (yj - yi) + xi
            if lon < intersect_x:
                inside = not inside
        j = i
    return inside


def _match_points_to_granules(
    points: pd.DataFrame,
    granule_metas: list[GranuleMeta],
    buffer: pd.Timedelta,
) -> dict[Any, list[int]]:
    """Return a mapping from each point's index to the list of matching granule indices.

    Matching criteria (all must be satisfied):

    * **Temporal**: ``granule.begin - buffer ≤ t_point ≤ granule.end + buffer``
    * **Spatial**:

      - For L2 GPolygon granules (``gm.polygon`` is set): the point must lie
        *inside* the polygon boundary (ray-casting algorithm).
      - For L3 BoundingRectangle granules (``gm.bbox`` set, ``gm.polygon``
        is ``None``): the point must fall within the bounding box.

    Using the actual GPolygon for L2 data avoids false positives that arise
    from the coarser bounding-box approximation.

    All timestamps are compared as timezone-naive UTC values so that
    tz-aware granule timestamps (ending in ``Z``) and tz-naive point
    timestamps can be mixed without error.

    Performance
    -----------
    Granules are sorted by ``begin`` time once, then per-point candidate
    selection uses :func:`bisect.bisect_right` to find the temporal upper
    bound in O(log M) rather than scanning all M granules.  When granule
    end-times are also monotonically non-decreasing in begin-sorted order
    (true for non-overlapping products such as daily L3 tiles), a second
    binary search on ``end`` gives the temporal lower bound too, reducing
    per-point work to O(log M + k) where *k* is the number of granules that
    actually overlap the point's timestamp.  For typical daily-granule
    products *k* ≈ 1, giving near-linear O(N log M) scaling overall.
    """
    def _to_utc_naive(ts: pd.Timestamp) -> pd.Timestamp:
        """Strip tz info from a Timestamp, treating it as UTC."""
        if ts.tzinfo is not None:
            return ts.tz_convert("UTC").tz_localize(None)
        return ts

    result: dict[Any, list[int]] = {}

    if not granule_metas:
        return {pt_idx: [] for pt_idx in points.index}

    # ------------------------------------------------------------------
    # Pre-process granules once (avoid repeated timestamp conversions
    # inside the per-point loop).
    # ------------------------------------------------------------------
    g_begins = [_to_utc_naive(gm.begin) for gm in granule_metas]
    g_ends = [_to_utc_naive(gm.end) for gm in granule_metas]

    # Sort by begin time so we can binary-search for the temporal upper bound.
    sort_order = sorted(range(len(granule_metas)), key=lambda i: g_begins[i])
    sorted_begins = [g_begins[i] for i in sort_order]
    sorted_ends = [g_ends[i] for i in sort_order]

    # If end-times are also non-decreasing in begin-sorted order (true for
    # non-overlapping granules such as daily L3 products), we can binary-
    # search for the lower temporal bound as well.  ``all()`` short-circuits
    # on the first out-of-order pair so this is O(1) for overlapping sets
    # and O(M) in the worst case — a one-time preprocessing cost.
    ends_sorted = all(
        sorted_ends[i] <= sorted_ends[i + 1] for i in range(len(sorted_ends) - 1)
    )

    # ------------------------------------------------------------------
    # Pre-convert the point time column to UTC-naive once.
    # ------------------------------------------------------------------
    pt_times: pd.Series = pd.to_datetime(points["time"])
    if hasattr(pt_times.dtype, "tz") and pt_times.dtype.tz is not None:
        pt_times = pt_times.dt.tz_convert("UTC").dt.tz_localize(None)

    # ------------------------------------------------------------------
    # Per-point matching — itertuples is ~10× faster than iterrows.
    # ------------------------------------------------------------------
    lats = points["lat"].to_numpy(dtype=float)
    lons = points["lon"].to_numpy(dtype=float)
    pt_index = list(points.index)

    for row_pos, pt_idx in enumerate(pt_index):
        t = _to_utc_naive(pd.Timestamp(pt_times.iloc[row_pos]))
        lat = lats[row_pos]
        lon = lons[row_pos]

        t_lo = t - buffer
        t_hi = t + buffer

        # Upper bound: first granule whose begin > t + buffer.
        hi = bisect.bisect_right(sorted_begins, t_hi)

        # Lower bound: first granule whose end >= t - buffer.
        lo = bisect.bisect_left(sorted_ends, t_lo, hi=hi) if ends_sorted else 0

        matching: list[int] = []
        for i in range(lo, hi):
            # For the unsorted-ends case, still apply the end check explicitly.
            if sorted_ends[i] < t_lo:
                continue
            g_idx = sort_order[i]
            gm = granule_metas[g_idx]
            # Spatial check — polygon (L2 GPolygon) takes priority over bbox
            if gm.polygon is not None:
                if not _point_in_polygon(lon, lat, gm.polygon):
                    continue
            elif gm.bbox is not None:
                west, south, east, north = gm.bbox
                if not (south <= lat <= north and west <= lon <= east):
                    continue
            matching.append(g_idx)

        result[pt_idx] = matching

    return result
