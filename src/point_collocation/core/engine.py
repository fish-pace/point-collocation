"""Core matchup engine — no earthaccess dependency here.

Responsibilities
----------------
* Accept a :class:`~point_collocation.core.plan.Plan` object built with
  :func:`~point_collocation.plan`.
* Open each granule individually (never ``open_mfdataset``) to minimise
  cloud I/O and avoid memory leaks.
* Extract the requested variables at each point's location/time using
  nearest-neighbor selection (gridded) or kdtree (non-gridded, e.g. swath).
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

import gc
import os
import pathlib
import time
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import xarray as xr

from point_collocation.core._open_method import (
    _apply_coords,
    _build_effective_open_kwargs,
    _cf_geoloc_names,
    _ensure_coords,
    _find_geoloc_pair,
    _merge_datatree_with_spec,
    _normalize_open_method,
    _open_as_flat_dataset,
    _open_datatree_fn,
    _resolve_auto_spec,
)

if TYPE_CHECKING:
    from point_collocation.core.plan import Plan

# Re-export geolocation pairs for callers that import them from this module.
from point_collocation.core._open_method import _GEOLOC_PAIRS  # noqa: F401

_VALID_SPATIAL_METHODS = {"nearest", "xoak-kdtree", "kdtree", "auto", "xoak-haversine"}

# Time dimension names used as a fallback when cf_xarray is not installed or
# when the dataset lacks CF-convention axis/units attributes.  Tried in order.
_TIME_DIM_NAMES = ["time", "Time", "TIME"]


def matchup(
    plan: "Plan",
    *,
    open_method: str | dict | None = None,
    variables: list[str] | None = None,
    spatial_method: str | None = None,
    open_dataset_kwargs: dict | None = None,
    silent: bool = True,
    batch_size: int | None = None,
    save_dir: str | os.PathLike | None = None,
    granule_range: tuple[int, int] | None = None,
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
    open_method:
        How granules are opened.  Accepts a string preset or a dict spec.

        **String presets:**

        * ``"dataset"`` — open with ``xarray.open_dataset`` (fast path for
          typical flat NetCDF files).
        * ``"datatree"`` — open as a raw DataTree without merging groups.
        * ``"datatree-merge"`` — open as DataTree and merge all groups into
          a flat Dataset (for grouped/HDF5-ish files).
        * ``"auto"`` *(default)* — try the fast ``"dataset"`` path first; if
          lat/lon coordinates cannot be detected, fall back to
          ``"datatree-merge"`` automatically.

        **Dict spec** (advanced):

        .. code-block:: python

            open_method = {
                "xarray_open":           "dataset" | "datatree",
                "open_kwargs":           {},
                "merge":                 None | "all" | "root" | ["/path/a"],
                "merge_kwargs":          {},
                "coords":                "auto" | ["Lat", "Lon"] | {"lat": "...", "lon": "..."},
                "set_coords":            True,
                "dim_renames":           None | {"node": {"old": "new"}},
                "auto_align_phony_dims": None | "safe",
            }

        All keys are optional; missing keys receive sensible defaults.
        Unknown keys raise :exc:`ValueError`.

        Pre-defined profiles for common products are importable from
        :mod:`point_collocation.profiles` (e.g. ``pace_l3``, ``pace_l2``).
    variables:
        Variable names to extract from each granule.  When provided,
        overrides any variables stored on the plan.  When omitted,
        falls back to ``plan.variables``.  If the resolved list is
        empty, the output will have no variable columns.
        Raises :exc:`ValueError` if a requested variable is not found
        in the opened dataset.
    spatial_method:
        Method used for spatial matching.

        * ``"auto"`` *(default)* — automatically selects the best method
          based on the dimensionality of the geolocation coordinates:

          - **1-D coordinates** (regular/gridded data): uses ``"nearest"``
            (``ds.sel(..., method="nearest")``).  If ``"nearest"`` fails for
            any reason, falls back to ``"kdtree"`` automatically.
          - **2-D coordinates** (irregular/swath data): uses ``"kdtree"``.

          ``xoak-kdtree`` and ``xoak-haversine`` are never selected
          automatically; set them explicitly if needed.

        * ``"nearest"`` — ``ds.sel(..., method="nearest")`` directly.
          Requires 1-D coordinate arrays; raises :exc:`ValueError` with a
          suggestion to use ``"auto"`` or ``"kdtree"`` for 2-D coordinates.
        * ``"kdtree"`` — xarray's built-in
          :class:`xarray.indexes.NDPointIndex` with the default
          ``ScipyKDTreeAdapter``.  Works with both 1-D and 2-D coordinate
          arrays (requires ``scipy``).
        * ``"xoak-kdtree"`` — the ``xoak`` package's ``SklearnKDTreeAdapter``.
          Works with both 1-D and 2-D arrays (requires ``xoak`` and
          ``scikit-learn``).
        * ``"xoak-haversine"`` — the ``xoak`` package's
          ``SklearnGeoBallTreeAdapter``, which uses the haversine metric for
          accurate great-circle distance calculations.  Recommended for data
          near the poles where the Euclidean k-d tree used by
          ``"xoak-kdtree"`` can return incorrect nearest neighbours due to
          coordinate distortion.  Works with both 1-D and 2-D arrays
          (requires ``xoak`` and ``scikit-learn``).  Lat/lon values are
          passed in degrees; the adapter converts them to radians internally.
    open_dataset_kwargs:
        Optional dictionary of keyword arguments forwarded to the xarray
        open function for every granule opened during the run.  These
        override any ``"open_kwargs"`` in *open_method* but are themselves
        overridden by their respective defaults only for missing keys
        (``chunks`` → ``{}``, ``engine`` → ``"h5netcdf"``,
        ``decode_timedelta`` → ``False``).
    silent:
        When ``True`` (default), all progress output is suppressed.
        Set to ``False`` to print a progress message to stdout after
        every *batch_size* granules.
    batch_size:
        Number of granules to process between progress reports (and
        between intermediate saves when *save_dir* is set).  Defaults
        to ``None``, which sets the batch size to one more than the
        total number of matched granules so that all granules are
        processed in a single batch.
    save_dir:
        Directory in which intermediate results are saved as Parquet
        files after each batch of *batch_size* granules.  The directory
        is created automatically if it does not exist.  Each batch is
        saved as ``plan_<first>_<last>.parquet`` where *first* and
        *last* are the granule indices from the plan.  When ``None``
        (default), no intermediate files are written.
    granule_range:
        Optional ``(start, end)`` tuple (both **1-based and inclusive**)
        that restricts processing to a contiguous slice of the matched
        granules, ordered by granule index.  For example,
        ``granule_range=(261, 620)`` resumes from granule 261 after a
        crash that completed granules 1–260.  Progress messages continue
        to report absolute granule numbers (e.g.
        "granules 261-270 of 620 processed") so the output is directly
        comparable with messages from the original run.  When ``None``
        (default), all matched granules are processed.

    Returns
    -------
    pandas.DataFrame
        One row per (point, granule) pair.  In addition to the original
        point columns and one column per requested variable, the output
        always includes:

        ``pc_id``
            Point identifier.  If the input dataframe contains a ``pc_id``
            column those values are preserved as-is; otherwise the row
            index from the input dataframe is used.  Duplicate ``pc_id``
            values in the input are not allowed and raise a
            :class:`ValueError` during planning.
        ``granule_id``
            Identifier of the granule that provided this row's values.
        ``granule_lat``
            Latitude of the matched location inside the granule (i.e.
            the nearest-neighbour grid or swath position).
        ``granule_lon``
            Longitude of the matched location inside the granule.
        ``granule_time``
            Midpoint of the granule's temporal coverage, derived from
            the granule metadata (``begin + (end - begin) / 2``).  For
            earthaccess granules, temporal information is stored in the
            search result metadata rather than in the dataset itself.
            For zero-match rows, this column is ``pandas.NaT``.

        Any extra columns present in the input dataframe are retained in
        the output.  Points with zero matching granules contribute a
        single NaN row.  The output is sorted to match the ``pc_id``
        order from the input dataframe.

    Raises
    ------
    ValueError
        If *open_method* is a string that is not a valid preset, or a dict
        with unknown keys or an invalid ``"xarray_open"`` value.
    ValueError
        If a requested variable is not present in an opened dataset.
    ValueError
        If geolocation variables cannot be detected unambiguously.
    ValueError
        If ``granule_range`` is not a 2-tuple of positive integers with
        ``start <= end``, or if either bound exceeds the number of matched
        granules in the plan.
    ImportError
        If ``spatial_method="xoak-kdtree"`` and the ``xoak`` package is not
        installed.
    ImportError
        If ``spatial_method="xoak-haversine"`` and the ``xoak`` package is not
        installed.
    ImportError
        If ``spatial_method="kdtree"`` and ``scipy`` is not installed.
    """
    if granule_range is not None:
        if (
            len(granule_range) != 2
            or not isinstance(granule_range[0], int)
            or not isinstance(granule_range[1], int)
            or granule_range[0] < 1
            or granule_range[1] < granule_range[0]
        ):
            raise ValueError(
                f"granule_range={granule_range!r} is not valid. "
                "Must be a (start, end) tuple of positive integers with start <= end, "
                "both 1-based and inclusive (e.g. granule_range=(261, 620))."
            )

    if spatial_method is None:
        spatial_method = "auto"

    if spatial_method not in _VALID_SPATIAL_METHODS:
        raise ValueError(
            f"spatial_method={spatial_method!r} is not valid. "
            f"Must be one of {sorted(_VALID_SPATIAL_METHODS)}."
        )

    # Validate xoak is importable before we start processing granules.
    if spatial_method == "xoak-kdtree":
        try:
            from xoak.tree_adapters import SklearnKDTreeAdapter  # type: ignore[import-untyped]  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "The 'xoak' package (and scikit-learn) are required for spatial_method='xoak-kdtree'. "
                "Install them with: pip install xoak scikit-learn"
            ) from exc

    # Validate xoak is importable before we start processing granules.
    if spatial_method == "xoak-haversine":
        try:
            from xoak.tree_adapters import SklearnGeoBallTreeAdapter  # type: ignore[import-untyped]  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "The 'xoak' package (and scikit-learn) are required for spatial_method='xoak-haversine'. "
                "Install them with: pip install xoak scikit-learn"
            ) from exc

    # Validate scipy is importable before we start processing granules.
    if spatial_method == "kdtree":
        try:
            from scipy.spatial import KDTree  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "The 'scipy' package is required for spatial_method='kdtree'. "
                "Install it with: pip install scipy"
            ) from exc

    # Normalize open_method to a full dict spec (raises ValueError on invalid input).
    effective_open_method = "auto" if open_method is None else open_method
    spec = _normalize_open_method(effective_open_method, open_dataset_kwargs)

    effective_vars: list[str] = variables if variables is not None else plan.variables
    return _execute_plan(
        plan,
        spec=spec,
        spatial_method=spatial_method,
        variables=effective_vars,
        silent=silent,
        batch_size=batch_size,
        save_dir=save_dir,
        granule_range=granule_range,
    )



# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

# Time dimension names used as a fallback when cf_xarray is not installed or
# when the dataset lacks CF-convention axis/units attributes.  Tried in order.
_TIME_DIM_NAMES = ["time", "Time", "TIME"]


def _find_time_dim(ds: xr.Dataset) -> str | None:
    """Return the name of the time dimension in *ds*, or ``None`` if absent.

    Detection strategy
    ------------------
    1. **cf_xarray** (primary, if installed): inspects CF-convention attributes
       such as ``axis='T'``, ``standard_name``, and ``units`` to identify the
       time axis.
    2. **Name-based fallback**: if ``cf_xarray`` is not installed or the dataset
       lacks CF attributes, searches :data:`_TIME_DIM_NAMES` in ``ds.dims`` and
       ``ds.coords``.

    Only dimensions are returned (not scalar coordinates) because only a
    dimensional time axis requires special handling during extraction.
    """
    # --- primary: cf_xarray ---
    try:
        import cf_xarray  # noqa: F401  (registers the .cf accessor)

        time_coords = ds.cf.axes.get("T", [])
        for name in time_coords:
            if name in ds.dims:
                return name
        # If cf_xarray found a time coordinate that is not a dimension (e.g. a
        # scalar), still check whether a dimension with a standard time name
        # exists so we do not silently miss it.
    except ImportError:
        pass
    except (AttributeError, KeyError):
        # cf_xarray is installed but the dataset lacks the attributes needed for
        # CF-axis detection (e.g. no standard_name / units on any variable).
        pass

    # --- fallback: name-based search ---
    for name in _TIME_DIM_NAMES:
        if name in ds.dims:
            return name

    return None


def _select_time(
    da: xr.DataArray,
    time_dim: str,
    point_time: object,
) -> xr.DataArray:
    """Select the appropriate time step from *da* along *time_dim*.

    Parameters
    ----------
    da:
        DataArray produced after spatial selection (lat/lon already resolved).
    time_dim:
        Name of the time dimension to handle.
    point_time:
        Timestamp of the observation point (``row["time"]``).  Used to find
        the nearest time step when *da* has multiple time steps.

    Returns
    -------
    xr.DataArray
        * *da* unchanged if *time_dim* is not one of ``da.dims``.
        * *da* with the time dimension squeezed out when there is exactly one
          time step.
        * *da* with the nearest time step selected when there are multiple time
          steps and *point_time* is a valid timestamp; falls back to the first
          time step if *point_time* is unusable.
    """
    if time_dim not in da.dims:
        return da

    n_times = da.sizes[time_dim]

    if n_times == 1:
        return da.squeeze(time_dim)

    # Multiple time steps: select nearest to the point timestamp.
    try:
        ts = pd.Timestamp(point_time)
        if pd.isna(ts):
            raise ValueError("NaT")
        return da.sel({time_dim: ts}, method="nearest")
    except (TypeError, ValueError, KeyError):
        # Fallback: first time step.
        # - TypeError / ValueError: point_time cannot be converted to a Timestamp
        #   or is NaT.
        # - KeyError: the time coordinate is absent or the sel fails on this ds.
        return da.isel({time_dim: 0})


def _check_spatial_compat(
    ds: xr.Dataset,
    lon_name: str,
    lat_name: str,
    spatial_method: str,
) -> None:
    """Raise if lat/lon dimensionality is incompatible with *spatial_method*.

    Only validates for ``spatial_method="nearest"``, which requires 1-D
    coordinate arrays.  ``spatial_method="xoak-kdtree"``,
    ``spatial_method="xoak-haversine"``, ``spatial_method="kdtree"``, and
    ``spatial_method="auto"`` work with both 1-D and 2-D arrays and are not
    validated here.

    Uses only metadata (``dims``) — does **not** load array data.
    """
    if spatial_method != "nearest":
        return

    lon_var = ds.coords[lon_name] if lon_name in ds.coords else ds[lon_name]
    lat_var = ds.coords[lat_name] if lat_name in ds.coords else ds[lat_name]

    lon_ndim = len(lon_var.dims)
    lat_ndim = len(lat_var.dims)

    if lon_ndim != 1 or lat_ndim != 1:
        raise ValueError(
            f"spatial_method='nearest' requires 1-D geolocation arrays, but found "
            f"{lon_name!r} with dims={tuple(lon_var.dims)} and "
            f"{lat_name!r} with dims={tuple(lat_var.dims)}. "
            "Use spatial_method='auto' or spatial_method='kdtree' for 2-D "
            "(swath/irregular) coordinates."
        )


def _safe_close(file_obj: object) -> None:
    """Call ``file_obj.close()`` if it exists, suppressing any errors.

    Used to close earthaccess file objects (S3/HTTPS streams) promptly after
    each granule is processed.  Errors are suppressed because the object may
    have already been closed internally by a higher-level layer (e.g. h5py),
    and a failure to close must never abort the overall matchup run.
    """
    close_fn = getattr(file_obj, "close", None)
    if callable(close_fn):
        try:
            close_fn()
        except Exception:
            pass


def _execute_plan(
    plan: "Plan",
    *,
    spec: dict,
    spatial_method: str,
    variables: list[str],
    silent: bool = True,
    batch_size: int | None = None,
    save_dir: str | os.PathLike | None = None,
    granule_range: tuple[int, int] | None = None,
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

    # NOTE: We intentionally do NOT call earthaccess.open(plan.results) all at
    # once here.  Opening every file upfront holds all S3/HTTPS file handles in
    # memory simultaneously, causing RAM to grow linearly with the number of
    # granules.  Instead we open only the files needed for each batch below so
    # that handles from previous batches can be released by the OS, keeping
    # peak memory proportional to batch_size rather than the total number of
    # granules.

    # Prepare save directory if requested.
    save_path: pathlib.Path | None = None
    if save_dir is not None:
        try:
            import pyarrow  # type: ignore[import-untyped]  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "The 'pyarrow' package is required to save progress as Parquet files. "
                "Install it with: pip install pyarrow"
            ) from exc
        save_path = pathlib.Path(save_dir)
        save_path.mkdir(parents=True, exist_ok=True)

    # Determine whether the user supplied their own pc_id column.  If so, use
    # those values as-is; otherwise assign the DataFrame row index as pc_id.
    has_user_pc_id: bool = "pc_id" in plan.points.columns

    # Build a mapping from pc_id value → its position in the input DataFrame so
    # the output can be sorted to match the user's original point order.
    if has_user_pc_id:
        pc_id_order: dict = {val: pos for pos, val in enumerate(plan.points["pc_id"])}
    else:
        pc_id_order = {idx: pos for pos, idx in enumerate(plan.points.index)}

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
        if not has_user_pc_id:
            row["pc_id"] = pt_idx
        row["granule_id"] = float("nan")
        row["granule_lat"] = float("nan")
        row["granule_lon"] = float("nan")
        row["granule_time"] = pd.NaT
        for var in variables:
            row[var] = float("nan")
        output_rows.append(row)

    # Track whether we have already validated spatial compat on the first granule.
    spatial_checked = False

    # For "auto" spatial_method, the effective method ("nearest" or "kdtree")
    # is determined on the first opened granule based on lat/lon dimensionality.
    # For explicit methods this always equals spatial_method.
    effective_spatial: str = spatial_method
    # When auto resolves to "nearest" on 1-D data, allow one fallback to
    # "kdtree" per granule if nearest extraction fails.
    auto_1d_fallback: bool = (spatial_method == "auto")

    # For "auto" open_method, probe only the first granule to determine whether
    # to use the "dataset" or "datatree" path, then reuse that resolved spec for
    # all subsequent granules (avoids redundant probing and ensures consistency).
    auto_spec_resolved = spec.get("xarray_open") != "auto"

    sorted_granule_items = sorted(granule_to_points.items())
    # Total matched granules in the full plan — used in progress messages so
    # that the "of N" counter is always relative to the whole plan, not just
    # the requested range.
    total_granules_all = len(sorted_granule_items)

    # Apply granule_range: restrict to a 1-based inclusive slice.
    granule_offset = 0  # absolute index of the first item in the slice (0-based)
    if granule_range is not None:
        range_start, range_end = granule_range
        if range_start > total_granules_all:
            raise ValueError(
                f"granule_range start ({range_start}) exceeds the number of matched "
                f"granules in the plan ({total_granules_all})."
            )
        if range_end > total_granules_all:
            raise ValueError(
                f"granule_range end ({range_end}) exceeds the number of matched "
                f"granules in the plan ({total_granules_all})."
            )
        sorted_granule_items = sorted_granule_items[range_start - 1 : range_end]
        granule_offset = range_start - 1

    total_granules = len(sorted_granule_items)
    granules_processed = 0
    start_time = time.monotonic()

    # Resolve effective batch size: None means process everything in one batch.
    effective_batch_size = batch_size if batch_size is not None else total_granules + 1

    # Process granules in batches.  We open only the files needed for each
    # batch so that file handles from previous batches can be released by the
    # OS, keeping peak memory proportional to batch_size rather than the total
    # number of granules.
    for batch_offset in range(0, total_granules, effective_batch_size):
        batch_items = sorted_granule_items[batch_offset : batch_offset + effective_batch_size]

        # Collect the earthaccess result objects for this batch (preserving
        # order) so that opened_batch[i] corresponds to batch_items[i].
        batch_results = [
            plan.results[plan.granules[g_idx].result_index]
            for g_idx, _ in batch_items
        ]
        opened_batch: list[object] = list(earthaccess.open(batch_results, pqdm_kwargs={"disable": True}))

        batch_matched_points = 0
        batch_rows: list[dict] = []

        for batch_pos, (g_idx, pt_indices) in enumerate(batch_items):
            gm = plan.granules[g_idx]
            # Pop the file object out of the batch list immediately so the
            # reference is dropped as soon as this granule is processed.
            # This ensures that at most one granule's S3/HTTPS buffers are
            # alive at a time, regardless of how large batch_size is.
            file_obj = opened_batch[batch_pos]
            opened_batch[batch_pos] = None

            try:
                # For "auto" mode: probe the first file to determine whether to
                # use the dataset or datatree path, then lock in the resolved
                # spec for all remaining granules.
                if not auto_spec_resolved:
                    spec = _resolve_auto_spec(file_obj, spec)  # type: ignore[arg-type]
                    auto_spec_resolved = True

                try:
                    with _open_as_flat_dataset(file_obj, spec) as (ds, lon_name, lat_name):  # type: ignore[arg-type]
                        # Resolve the effective spatial method once, on the first
                        # opened granule.  For "auto", we inspect the lat/lon
                        # dimensionality here; for explicit methods we just
                        # validate compatibility.
                        if not spatial_checked:
                            if spatial_method == "auto":
                                lat_var_check = (
                                    ds.coords[lat_name]
                                    if lat_name in ds.coords
                                    else ds[lat_name]
                                )
                                if lat_var_check.ndim == 1:
                                    effective_spatial = "nearest"
                                    # auto_1d_fallback already True; keep it so
                                    # that a nearest failure falls back to kdtree.
                                else:
                                    effective_spatial = "kdtree"
                                    auto_1d_fallback = False
                                if not silent:
                                    print(
                                        f"spatial_method='auto': using '{effective_spatial}' "
                                        f"(lat/lon dims: {lat_var_check.ndim}-D)"
                                    )
                            else:
                                effective_spatial = spatial_method
                                auto_1d_fallback = False
                                _check_spatial_compat(ds, lon_name, lat_name, effective_spatial)
                            spatial_checked = True

                        # Validate that all requested variables exist in the dataset.
                        missing_vars = [v for v in variables if v not in ds]
                        if missing_vars:
                            avail = list(ds.data_vars)
                            raise ValueError(
                                f"Variable(s) {missing_vars!r} not found in granule "
                                f"'{gm.granule_id}'. "
                                f"open_method={spec['xarray_open']!r}, "
                                f"spatial_method={spatial_method!r}. "
                                f"Available variables: {avail}. "
                                "Use plan.open_dataset(0) to inspect the dataset."
                            )

                        # For xoak-kdtree/xoak-haversine/kdtree with 1-D (gridded) lat/lon, pre-slice the dataset
                        # to the spatial extent of the query points before building
                        # the k-d tree.  A global granule with only a few scattered
                        # points would otherwise cause the index to cover the entire global
                        # grid, which is very slow.  Skip this step for "nearest" (1-D)
                        # since it does not build an index.
                        if effective_spatial in ("xoak-kdtree", "xoak-haversine", "kdtree"):
                            lat_var = ds.coords[lat_name] if lat_name in ds.coords else ds[lat_name]
                            if lat_var.ndim == 1:
                                pt_lats = [float(plan.points.loc[idx]["lat"]) for idx in pt_indices]
                                pt_lons = [float(plan.points.loc[idx]["lon"]) for idx in pt_indices]
                                ds = _slice_grid_to_points(ds, pt_lats, pt_lons, lat_name, lon_name)

                        # Compute granule_time from the granule metadata.  Earthaccess
                        # granules store their temporal coverage in GranuleMeta.begin/end;
                        # the dataset itself may not have a time coordinate at all.
                        granule_time = gm.begin + (gm.end - gm.begin) / 2

                        # Detect time dimension once per granule so that
                        # extraction functions can handle (time, lat, lon) variables.
                        time_dim = _find_time_dim(ds)

                        if effective_spatial in ("xoak-kdtree", "xoak-haversine", "kdtree"):
                            # Build the k-d tree index once for all points in this
                            # granule instead of rebuilding it per point.  This
                            # dramatically reduces memory pressure and speeds up
                            # processing when a granule has many query points.
                            rows_for_granule = []
                            for pt_idx in pt_indices:
                                row = plan.points.loc[pt_idx].to_dict()
                                if not has_user_pc_id:
                                    row["pc_id"] = pt_idx
                                row["granule_id"] = gm.granule_id
                                row["granule_time"] = granule_time
                                rows_for_granule.append(row)
                            if effective_spatial == "xoak-kdtree":
                                _extract_xoak_batch(ds, rows_for_granule, variables, lon_name, lat_name, time_dim)
                            elif effective_spatial == "xoak-haversine":
                                _extract_xoak_batch(ds, rows_for_granule, variables, lon_name, lat_name, time_dim, use_haversine=True)
                            else:
                                _extract_ndpoint_batch(ds, rows_for_granule, variables, lon_name, lat_name, time_dim)
                            output_rows.extend(rows_for_granule)
                            batch_rows.extend(rows_for_granule)
                        elif auto_1d_fallback:
                            # auto resolved to "nearest" on 1-D coords.  Try
                            # nearest for each point; if it fails, fall back to
                            # ndpoint for the whole granule (and all future ones).
                            def _make_row(pt_idx: object) -> dict:
                                r = plan.points.loc[pt_idx].to_dict()
                                if not has_user_pc_id:
                                    r["pc_id"] = pt_idx
                                r["granule_id"] = gm.granule_id
                                r["granule_time"] = granule_time
                                return r

                            rows_for_granule = [_make_row(idx) for idx in pt_indices]
                            try:
                                for row in rows_for_granule:
                                    _extract_nearest(ds, row, variables, lon_name, lat_name, time_dim)
                            except Exception as _nearest_exc:
                                # nearest failed; rebuild clean rows and retry with kdtree.
                                rows_for_granule = [_make_row(idx) for idx in pt_indices]
                                # Apply slicing for kdtree on 1-D coords.
                                pt_lats = [float(plan.points.loc[idx]["lat"]) for idx in pt_indices]
                                pt_lons = [float(plan.points.loc[idx]["lon"]) for idx in pt_indices]
                                ds_nd = _slice_grid_to_points(ds, pt_lats, pt_lons, lat_name, lon_name)
                                try:
                                    _extract_ndpoint_batch(
                                        ds_nd, rows_for_granule, variables, lon_name, lat_name, time_dim
                                    )
                                    # kdtree succeeded — switch all future granules to kdtree.
                                    effective_spatial = "kdtree"
                                    auto_1d_fallback = False
                                except Exception as nd_exc:
                                    raise ValueError(
                                        "spatial_method='auto' tried both 'nearest' and 'kdtree' "
                                        "for a granule with 1-D lat/lon coordinates, but both "
                                        "failed.  Check that the dataset has valid geolocation "
                                        f"coordinates.  'nearest' error: {_nearest_exc!r}; "
                                        f"'kdtree' error: {nd_exc!r}"
                                    ) from nd_exc
                            output_rows.extend(rows_for_granule)
                            batch_rows.extend(rows_for_granule)
                        else:
                            for pt_idx in pt_indices:
                                row = plan.points.loc[pt_idx].to_dict()
                                if not has_user_pc_id:
                                    row["pc_id"] = pt_idx
                                row["granule_id"] = gm.granule_id
                                row["granule_time"] = granule_time
                                _extract_nearest(ds, row, variables, lon_name, lat_name, time_dim)
                                output_rows.append(row)
                                batch_rows.append(row)

                        batch_matched_points += len(pt_indices)

                except ValueError:
                    raise
                except Exception:
                    # Granule failed to open → emit NaN rows for its points.
                    # Still use the metadata midpoint time since the dataset couldn't be opened.
                    failed_granule_time = gm.begin + (gm.end - gm.begin) / 2
                    for pt_idx in pt_indices:
                        row = plan.points.loc[pt_idx].to_dict()
                        if not has_user_pc_id:
                            row["pc_id"] = pt_idx
                        row["granule_id"] = gm.granule_id
                        row["granule_lat"] = float("nan")
                        row["granule_lon"] = float("nan")
                        row["granule_time"] = failed_granule_time
                        for var in variables:
                            row[var] = float("nan")
                        output_rows.append(row)
                        batch_rows.append(row)
            finally:
                # Explicitly close the earthaccess file object (S3/HTTPS
                # stream) to release its internal buffers immediately.
                # xarray/h5netcdf closes the HDF5 layer but does NOT close
                # the underlying file-like object, so without this call the
                # object — and its buffers — stays alive until the entire
                # batch is finished, causing peak memory to scale with
                # batch_size rather than staying constant at ~1 granule.
                _safe_close(file_obj)
                # xarray datasets opened with dask (chunks={}) and DataTree
                # nodes both hold internal reference cycles that Python's
                # reference counting cannot free.  Without a GC call here,
                # these objects accumulate in memory for an entire batch
                # before the single gc.collect() at the end of the batch
                # runs — causing retained memory to scale with batch_size
                # rather than staying constant at ~1 granule.  Calling
                # gc.collect() once per granule keeps peak memory bounded
                # regardless of batch_size and open_method.
                gc.collect()


            granules_processed += 1

        # End of batch: report progress and save intermediate results.
        # Use absolute (1-based) granule numbers so the output matches the
        # user's original run when granule_range is used for crash recovery.
        batch_start = batch_offset + 1 + granule_offset
        batch_end = granules_processed + granule_offset
        if not silent:
            elapsed = int(time.monotonic() - start_time)
            hh, remainder = divmod(elapsed, 3600)
            mm, ss = divmod(remainder, 60)
            print(
                f"granules {batch_start}-{batch_end} of {total_granules_all} processed, "
                f"{batch_matched_points} points matched, "
                f"{hh:02d}:{mm:02d}:{ss:02d}"
            )
        if save_path is not None and batch_rows:
            batch_df = pd.DataFrame(batch_rows)
            parquet_name = f"plan_{batch_start}_{batch_end}.parquet"
            batch_df.to_parquet(save_path / parquet_name, index=False)

        # The opened_batch list has already been nulled out entry-by-entry
        # inside the loop above (each slot set to None and the file object
        # explicitly closed via _safe_close).  Delete the list itself here,
        # then run the cyclic GC to promptly free any DataTree objects that
        # contain parent→child reference cycles.
        del opened_batch
        gc.collect()

    if not output_rows:
        empty = plan.points.iloc[:0].copy()
        if not has_user_pc_id:
            empty["pc_id"] = pd.Series(dtype=object)
        empty["granule_id"] = pd.Series(dtype=object)
        empty["granule_lat"] = pd.Series(dtype=float)
        empty["granule_lon"] = pd.Series(dtype=float)
        empty["granule_time"] = pd.Series(dtype="datetime64[ns]")
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

    # Sort by the pc_id order from the input DataFrame so that output rows
    # follow the same point ordering the user provided.  A stable sort
    # preserves the relative order of rows with the same pc_id (e.g. multiple
    # granules for one point).
    df["_pc_sort"] = df["pc_id"].map(pc_id_order)
    df = df.sort_values("_pc_sort", kind="stable").drop(columns=["_pc_sort"]).reset_index(drop=True)

    return df


def _slice_grid_to_points(
    ds: xr.Dataset,
    lats: list[float],
    lons: list[float],
    lat_name: str,
    lon_name: str,
    buffer_deg: float = 1.0,
) -> xr.Dataset:
    """Slice a regular-grid dataset to the smallest region covering *lats*/*lons*.

    When ``spatial_method='xoak-kdtree'`` or ``'kdtree'`` and lat/lon are
    1-D (regular grid), building a k-d tree over an entire global granule is
    very slow if only a few points need to be matched.  This function slices
    the dataset to a padded bounding box around the query points so the index
    covers the minimum required region.

    Only applies to datasets with 1-D coordinate arrays (regular grids). Returns
    *ds* unchanged for 2-D coordinates or if the resulting slice would be empty.

    Parameters
    ----------
    ds:
        The dataset to slice.
    lats, lons:
        Latitudes and longitudes of the query points.
    lat_name, lon_name:
        Coordinate names detected by :func:`_find_geoloc_pair`.
    buffer_deg:
        Extra degrees to pad the bounding box on each side (default 1°).
        Ensures at least one grid cell surrounds each query point.

    Returns
    -------
    xr.Dataset
        A lazy slice of *ds* covering the padded bounding box, or *ds* unchanged
        if the coordinates are not 1-D or the slice would be empty.
    """
    lat_coord = ds.coords.get(lat_name) if lat_name in ds.coords else ds.get(lat_name)
    lon_coord = ds.coords.get(lon_name) if lon_name in ds.coords else ds.get(lon_name)

    if lat_coord is None or lon_coord is None:
        return ds
    if lat_coord.ndim != 1 or lon_coord.ndim != 1:
        return ds

    lat_min_data = float(lat_coord.min())
    lat_max_data = float(lat_coord.max())
    lon_min_data = float(lon_coord.min())
    lon_max_data = float(lon_coord.max())

    min_lat = max(min(lats) - buffer_deg, lat_min_data)
    max_lat = min(max(lats) + buffer_deg, lat_max_data)
    min_lon = max(min(lons) - buffer_deg, lon_min_data)
    max_lon = min(max(lons) + buffer_deg, lon_max_data)

    # xarray slice() is order-aware: if the coordinate is stored in descending
    # order (e.g. 90→-90), the larger bound must come first.
    lat_vals = lat_coord.values
    if len(lat_vals) > 1 and lat_vals[0] > lat_vals[-1]:
        lat_slice = slice(max_lat, min_lat)
    else:
        lat_slice = slice(min_lat, max_lat)

    lon_vals = lon_coord.values
    if len(lon_vals) > 1 and lon_vals[0] > lon_vals[-1]:
        lon_slice = slice(max_lon, min_lon)
    else:
        lon_slice = slice(min_lon, max_lon)

    sliced = ds.sel({lat_name: lat_slice, lon_name: lon_slice})

    # Guard against an empty slice (e.g., all query points fall outside the
    # coordinate range, or the grid is coarser than the buffer).
    lat_dim = lat_coord.dims[0]
    lon_dim = lon_coord.dims[0]
    if sliced.sizes.get(lat_dim, 0) == 0 or sliced.sizes.get(lon_dim, 0) == 0:
        return ds

    return sliced


def _slice_2d_to_points(
    ds: xr.Dataset,
    lats: list[float],
    lons: list[float],
    lat_name: str,
    lon_name: str,
    buffer_deg: float = 1.0,
) -> xr.Dataset:
    """Slice a 2-D irregular-grid (swath) dataset to the bbox of *lats*/*lons*.

    Unlike :func:`_slice_grid_to_points` (which uses ``ds.sel`` on 1-D indexed
    coordinates), this function handles 2-D swath/irregular data by computing a
    per-pixel boolean mask and retaining rows and columns that contain at least
    one pixel within the padded bounding box.  No stacking is required, so
    there is no xarray version dependency.

    This is a conservative filter: a row (or column) is kept if *any* of its
    pixels falls within the bbox, which may include a small number of pixels
    just outside the exact boundary.  That is acceptable and mirrors the
    strategy used by :func:`_slice_grid_to_points` for regular grids.

    Only applies to datasets with 2-D coordinate arrays (irregular/swath grids).
    Returns *ds* unchanged for 1-D coordinates or if no pixels lie in the bbox.

    Parameters
    ----------
    ds:
        The dataset to slice.
    lats, lons:
        Latitudes and longitudes of the query points.
    lat_name, lon_name:
        Coordinate names detected by :func:`_find_geoloc_pair`.
    buffer_deg:
        Extra degrees to pad the bounding box on each side (default 1°).

    Returns
    -------
    xr.Dataset
        A subset of *ds* covering the padded bounding box, or *ds* unchanged
        if the coordinates are not 2-D or no pixels lie in the bbox.
    """
    lat_coord = ds.coords.get(lat_name) if lat_name in ds.coords else ds.get(lat_name)
    lon_coord = ds.coords.get(lon_name) if lon_name in ds.coords else ds.get(lon_name)

    if lat_coord is None or lon_coord is None:
        return ds
    if lat_coord.ndim != 2:
        return ds  # Only handles 2-D swath data; 1-D grids use _slice_grid_to_points.

    lat_vals = np.asarray(lat_coord)
    lon_vals = np.asarray(lon_coord)

    min_lat = min(lats) - buffer_deg
    max_lat = max(lats) + buffer_deg
    min_lon = min(lons) - buffer_deg
    max_lon = max(lons) + buffer_deg

    # Build a per-pixel mask; ignore NaN/Inf pixels for the bbox check
    # (those will be handled separately by _drop_nan_geoloc).
    finite = np.isfinite(lat_vals) & np.isfinite(lon_vals)
    in_bbox = (
        finite
        & (lat_vals >= min_lat) & (lat_vals <= max_lat)
        & (lon_vals >= min_lon) & (lon_vals <= max_lon)
    )

    # Reduce to row and column masks by taking the OR across the other axis.
    row_mask = in_bbox.any(axis=1)
    col_mask = in_bbox.any(axis=0)

    if not row_mask.any() or not col_mask.any():
        # No pixel in bbox; return unchanged so the caller can emit NaN results.
        return ds

    dim0, dim1 = lat_coord.dims
    sliced = ds.isel({dim0: row_mask, dim1: col_mask})

    # Guard against a degenerate result.
    if sliced.sizes.get(dim0, 0) == 0 or sliced.sizes.get(dim1, 0) == 0:
        return ds

    return sliced


def _drop_nan_geoloc(
    ds: xr.Dataset,
    lat_name: str,
    lon_name: str,
) -> xr.Dataset:
    """Return *ds* with pixels that have NaN/Inf lat or lon removed.

    Some swath products (e.g. DSCOVR EPIC HE5) store a large fill value
    (≈ −1.27e30) for pixels outside the valid Earth disk.  When xarray
    reads those pixels it converts the fill value to NaN.  Passing NaN
    coordinates to scipy's or xoak's KD-tree raises a ``ValueError``.
    This helper stacks all spatial dimensions, removes the bad pixels,
    and returns a dataset whose 1-D layout is safe for ``set_xindex()``.
    Stacking requires xarray ≥ 2026.2 (``NDPointIndex`` support for
    multiple coordinate variables sharing one dimension).

    If all coordinates are finite the dataset is returned unchanged.

    Call :func:`_slice_2d_to_points` (for 2-D swath data) or
    :func:`_slice_grid_to_points` (for 1-D regular grids) *before* calling
    this function to restrict the dataset to the bounding box of the query
    points so that the k-d tree is not built over the entire swath.
    """
    lat_arr = ds.coords[lat_name] if lat_name in ds.coords else ds[lat_name]
    lon_arr = ds.coords[lon_name] if lon_name in ds.coords else ds[lon_name]

    lat_vals = np.asarray(lat_arr)
    lon_vals = np.asarray(lon_arr)

    if np.all(np.isfinite(lat_vals)) and np.all(np.isfinite(lon_vals)):
        return ds  # Fast path — nothing to do.

    # NaN/Inf values detected — stacking is required.  NDPointIndex in
    # xarray < 2026.2 cannot handle 2 coordinate variables on 1 stacked
    # dimension and will raise a confusing ValueError.  Check the version
    # now and exit with a clear message rather than letting xarray crash.
    import importlib.metadata
    from packaging.version import Version

    _xarray_ver_str = importlib.metadata.version("xarray")
    if Version(_xarray_ver_str) < Version("2026.2"):
        raise RuntimeError(
            "point-collocation: NaN/Inf values were found in the latitude or "
            "longitude of this dataset (e.g. fill values outside the sensor "
            "swath).  Removing those bad pixels requires stacking the spatial "
            "dimensions, which in turn requires xarray ≥ 2026.2.  Please "
            f"upgrade xarray (currently {_xarray_ver_str}):\n"
            "    pip install 'xarray>=2026.2'"
        )

    spatial_dims = lat_arr.dims
    stacked = ds.stack({"__pc__": spatial_dims}).reset_index("__pc__")

    lat_s = stacked.coords[lat_name] if lat_name in stacked.coords else stacked[lat_name]
    lon_s = stacked.coords[lon_name] if lon_name in stacked.coords else stacked[lon_name]
    valid = np.isfinite(lat_s.values) & np.isfinite(lon_s.values)

    if not np.any(valid):
        # All pixels are bad; return the stacked-but-unfiltered dataset so that
        # the caller can propagate NaN results rather than crashing here.
        return stacked

    return stacked.isel({"__pc__": valid})


def _extract_nearest(
    ds: xr.Dataset,
    row: dict,
    variables: list[str],
    lon_name: str,
    lat_name: str,
    time_dim: str | None = None,
) -> None:
    """Extract values using ``ds.sel(..., method='nearest')`` (1-D coords).

    Modifies *row* in-place, including ``granule_lat`` and ``granule_lon``
    columns for the matched grid location.  ``granule_time`` is set by the
    caller from granule metadata before this function is called.

    Parameters
    ----------
    time_dim:
        Name of the time dimension in *ds*, as detected by
        :func:`_find_time_dim`.  When not ``None``, each variable is
        squeezed or nearest-selected along this dimension after spatial
        selection so that the result is always free of the time axis.
    """
    # Extract the actual matched coordinates (nearest-neighbour grid position).
    try:
        matched_lat = ds.coords[lat_name].sel({lat_name: row["lat"]}, method="nearest")
        matched_lon = ds.coords[lon_name].sel({lon_name: row["lon"]}, method="nearest")
        row["granule_lat"] = float(matched_lat)
        row["granule_lon"] = float(matched_lon)
    except Exception:
        row["granule_lat"] = float("nan")
        row["granule_lon"] = float("nan")

    for var in variables:
        try:
            selected = ds[var].sel(
                {lat_name: row["lat"], lon_name: row["lon"]},
                method="nearest",
            )
            if time_dim is not None:
                selected = _select_time(selected, time_dim, row.get("time"))
            if selected.ndim == 0:
                row[var] = float(selected)
            else:
                # Multi-dimensional: expand into coord-keyed entries
                row[var] = float("nan")  # placeholder removed later
                for coord_val, val in selected.to_series().items():
                    row[f"{var}_{int(coord_val)}"] = float(val)
        except Exception:
            row[var] = float("nan")


def _extract_xoak(
    ds: xr.Dataset,
    row: dict,
    variables: list[str],
    lon_name: str,
    lat_name: str,
    time_dim: str | None = None,
) -> None:
    """Extract values using xoak nearest-neighbour (1-D or 2-D lat/lon arrays).

    Uses the ``xarray.indexes.NDPointIndex`` API with xoak's
    ``SklearnKDTreeAdapter``.  The lat/lon coordinate arrays are computed
    from dask (if chunked) before building the k-d tree index.

    For 1-D (gridded) lat/lon coordinates, the arrays are broadcast to a
    shared 2-D meshgrid so that ``NDPointIndex`` can build a joint spatial
    index over both dimensions.

    Modifies *row* in-place.

    Parameters
    ----------
    time_dim:
        Name of the time dimension in *ds*, as detected by
        :func:`_find_time_dim`.  When not ``None``, each variable is
        squeezed or nearest-selected along this dimension after spatial
        selection so that the result is always free of the time axis.
    """
    try:
        from xoak.tree_adapters import SklearnKDTreeAdapter  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "The 'xoak' package is required for spatial_method='xoak-kdtree'. "
            "Install it with: pip install xoak scikit-learn"
        ) from exc

    # Compute coordinate arrays if they are lazy (dask) — building a k-d
    # tree requires all values to be in memory.
    # Use a shallow copy so we only copy metadata, not data arrays.
    ds_work = ds.copy(deep=False)
    if lat_name in ds_work.coords and hasattr(ds_work.coords[lat_name].data, "compute"):
        ds_work[lat_name] = ds_work.coords[lat_name].compute()
    if lon_name in ds_work.coords and hasattr(ds_work.coords[lon_name].data, "compute"):
        ds_work[lon_name] = ds_work.coords[lon_name].compute()

    # NDPointIndex requires lat and lon to share the same dimensions.  For
    # regular grid data (1-D lat/lon with separate dimensions), broadcast both
    # coordinates to a common 2-D meshgrid so that the joint index can be built.
    lat_arr = ds_work.coords[lat_name] if lat_name in ds_work.coords else ds_work[lat_name]
    lon_arr = ds_work.coords[lon_name] if lon_name in ds_work.coords else ds_work[lon_name]
    if lat_arr.ndim == 1 and lon_arr.ndim == 1:
        lat_2d, lon_2d = np.meshgrid(lat_arr.values, lon_arr.values, indexing="ij")
        lat_dims = lat_arr.dims + lon_arr.dims  # e.g. ('lat', 'lon')
        ds_work[lat_name] = xr.DataArray(lat_2d, dims=lat_dims)
        ds_work[lon_name] = xr.DataArray(lon_2d, dims=lat_dims)

    # Drop pixels where lat/lon are NaN or Inf (e.g. fill values outside swath).
    ds_work = _drop_nan_geoloc(ds_work, lat_name, lon_name)

    # Build the NDPointIndex using the sklearn k-d tree adapter.
    indexed_ds = ds_work.set_xindex(
        [lat_name, lon_name],
        xr.indexes.NDPointIndex,
        tree_adapter_cls=SklearnKDTreeAdapter,
    )

    # Build the target selection (one query point).
    target = xr.Dataset(
        {
            lat_name: xr.DataArray([row["lat"]]),
            lon_name: xr.DataArray([row["lon"]]),
        }
    )

    try:
        selected = indexed_ds.sel(
            {lat_name: target[lat_name], lon_name: target[lon_name]},
            method="nearest",
        )
        for var in variables:
            try:
                # sel(..., method='nearest') returns a 1-element array per query
                # point; squeeze out that query dimension so that 2-D spatial
                # variables become scalar (0-D) and 3-D variables (e.g. Rrs with
                # a wavelength dimension) become 1-D.
                squeezed = selected[var].squeeze()
                if time_dim is not None:
                    squeezed = _select_time(squeezed, time_dim, row.get("time"))
                if squeezed.ndim == 0:
                    row[var] = float(squeezed)
                else:
                    # Additional dimensions remain (e.g. wavelength) — expand
                    # into coord-keyed columns (Rrs_346, Rrs_348, …).
                    row[var] = float("nan")  # placeholder; removed later
                    for coord_val, val in squeezed.to_series().items():
                        row[f"{var}_{int(coord_val)}"] = float(val)
            except Exception:
                row[var] = float("nan")
    except Exception:
        for var in variables:
            row[var] = float("nan")


def _extract_xoak_batch(
    ds: xr.Dataset,
    rows: list[dict],
    variables: list[str],
    lon_name: str,
    lat_name: str,
    time_dim: str | None = None,
    *,
    use_haversine: bool = False,
) -> None:
    """Extract values for all *rows* using a single xoak k-d tree index.

    Builds the k-d tree index **once** for the entire dataset, then queries
    all points simultaneously.  This avoids the O(N) index-rebuild cost of
    calling :func:`_extract_xoak` once per point and substantially reduces
    peak memory when a granule has many query points.

    Uses the ``xarray.indexes.NDPointIndex`` API with xoak's
    ``SklearnKDTreeAdapter`` by default, or ``SklearnGeoBallTreeAdapter``
    when *use_haversine* is ``True`` (i.e. ``spatial_method="xoak-haversine"``).

    Modifies each dict in *rows* in-place.

    Parameters
    ----------
    time_dim:
        Name of the time dimension in *ds*, as detected by
        :func:`_find_time_dim`.  When not ``None``, each variable is
        squeezed or nearest-selected along this dimension after spatial
        selection so that the result is always free of the time axis.
    use_haversine:
        When ``True``, use ``SklearnGeoBallTreeAdapter`` (haversine metric)
        instead of ``SklearnKDTreeAdapter`` (Euclidean metric).
    """
    if use_haversine:
        try:
            from xoak.tree_adapters import SklearnGeoBallTreeAdapter as _TreeAdapter  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The 'xoak' package is required for spatial_method='xoak-haversine'. "
                "Install it with: pip install xoak scikit-learn"
            ) from exc
    else:
        try:
            from xoak.tree_adapters import SklearnKDTreeAdapter as _TreeAdapter  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "The 'xoak' package is required for spatial_method='xoak-kdtree'. "
                "Install it with: pip install xoak scikit-learn"
            ) from exc

    if not rows:
        return

    # Compute coordinate arrays if they are lazy (dask) — building a k-d
    # tree requires all values to be in memory.
    # Use a shallow copy so we only copy metadata, not data arrays.
    ds_work = ds.copy(deep=False)
    if lat_name in ds_work.coords and hasattr(ds_work.coords[lat_name].data, "compute"):
        ds_work[lat_name] = ds_work.coords[lat_name].compute()
    if lon_name in ds_work.coords and hasattr(ds_work.coords[lon_name].data, "compute"):
        ds_work[lon_name] = ds_work.coords[lon_name].compute()

    # For 2-D irregular-grid (swath) coordinates, pre-filter to the bounding
    # box of the query points before building the index.  This must happen
    # before the meshgrid expansion below so that the check fires only for
    # genuinely 2-D coords (not the synthetic meshgrid we create for 1-D data,
    # which has already been sliced by _slice_grid_to_points at the call site).
    _pt_lats = [float(r["lat"]) for r in rows]
    _pt_lons = [float(r["lon"]) for r in rows]
    ds_work = _slice_2d_to_points(ds_work, _pt_lats, _pt_lons, lat_name, lon_name)

    # NDPointIndex requires lat and lon to share the same dimensions.  For
    # regular grid data (1-D lat/lon with separate dimensions), broadcast both
    # coordinates to a common 2-D meshgrid so that the joint index can be built.
    lat_arr = ds_work.coords[lat_name] if lat_name in ds_work.coords else ds_work[lat_name]
    lon_arr = ds_work.coords[lon_name] if lon_name in ds_work.coords else ds_work[lon_name]
    if lat_arr.ndim == 1 and lon_arr.ndim == 1:
        lat_2d, lon_2d = np.meshgrid(lat_arr.values, lon_arr.values, indexing="ij")
        lat_dims = lat_arr.dims + lon_arr.dims  # e.g. ('lat', 'lon')
        ds_work[lat_name] = xr.DataArray(lat_2d, dims=lat_dims)
        ds_work[lon_name] = xr.DataArray(lon_2d, dims=lat_dims)

    # Drop pixels where lat/lon are NaN or Inf (e.g. fill values outside swath).
    ds_work = _drop_nan_geoloc(ds_work, lat_name, lon_name)

    # Build the NDPointIndex once for all query points.
    indexed_ds = ds_work.set_xindex(
        [lat_name, lon_name],
        xr.indexes.NDPointIndex,
        tree_adapter_cls=_TreeAdapter,
    )

    # Build the target dataset with all query points at once.
    lats = [row["lat"] for row in rows]
    lons = [row["lon"] for row in rows]
    target = xr.Dataset(
        {
            lat_name: xr.DataArray(lats),
            lon_name: xr.DataArray(lons),
        }
    )
    # The auto-generated dimension name for the query coordinate array
    # (e.g. 'dim_0') is used to index individual results per point.
    query_dim = target[lat_name].dims[0]

    try:
        selected = indexed_ds.sel(
            {lat_name: target[lat_name], lon_name: target[lon_name]},
            method="nearest",
        )
        # Extract matched granule coordinates for each query point.
        try:
            matched_lats = selected.coords[lat_name].values
            matched_lons = selected.coords[lon_name].values
            for i, row in enumerate(rows):
                row["granule_lat"] = float(matched_lats[i])
                row["granule_lon"] = float(matched_lons[i])
        except Exception:
            for row in rows:
                row["granule_lat"] = float("nan")
                row["granule_lon"] = float("nan")

        for var in variables:
            try:
                var_data = selected[var]
                for i, row in enumerate(rows):
                    # Extract the i-th query point.  After sel() the query
                    # dimension is prepended; squeeze removes any remaining
                    # size-1 spatial dims so extra dims (e.g. wavelength) are
                    # kept intact.
                    point_data = var_data.isel({query_dim: i}).squeeze()
                    if time_dim is not None:
                        point_data = _select_time(point_data, time_dim, row.get("time"))
                    if point_data.ndim == 0:
                        row[var] = float(point_data)
                    else:
                        # Additional dimensions (e.g. wavelength) — expand
                        # into coord-keyed columns (Rrs_346, Rrs_348, …).
                        row[var] = float("nan")  # placeholder; removed later
                        for coord_val, val in point_data.to_series().items():
                            row[f"{var}_{int(coord_val)}"] = float(val)
            except Exception:
                for r in rows:
                    r[var] = float("nan")
    except Exception:
        for row in rows:
            row["granule_lat"] = float("nan")
            row["granule_lon"] = float("nan")
        for var in variables:
            for r in rows:
                r[var] = float("nan")


def _extract_ndpoint_batch(
    ds: xr.Dataset,
    rows: list[dict],
    variables: list[str],
    lon_name: str,
    lat_name: str,
    time_dim: str | None = None,
) -> None:
    """Extract values for all *rows* using xarray's built-in NDPointIndex.

    Builds the k-d tree index **once** for the entire dataset using xarray's
    built-in ``ScipyKDTreeAdapter`` (no ``xoak`` dependency required — only
    ``scipy``), then queries all points simultaneously.

    This function mirrors :func:`_extract_xoak_batch` in logic and memory
    containment strategy, but uses :class:`xarray.indexes.NDPointIndex`
    without a custom tree adapter so that ``xoak`` is not required.

    Modifies each dict in *rows* in-place.

    Parameters
    ----------
    time_dim:
        Name of the time dimension in *ds*, as detected by
        :func:`_find_time_dim`.  When not ``None``, each variable is
        squeezed or nearest-selected along this dimension after spatial
        selection so that the result is always free of the time axis.
    """
    if not rows:
        return

    # Compute coordinate arrays if they are lazy (dask) — building a k-d
    # tree requires all values to be in memory.
    # Use a shallow copy so we only copy metadata, not data arrays.
    ds_work = ds.copy(deep=False)
    if lat_name in ds_work.coords and hasattr(ds_work.coords[lat_name].data, "compute"):
        ds_work[lat_name] = ds_work.coords[lat_name].compute()
    if lon_name in ds_work.coords and hasattr(ds_work.coords[lon_name].data, "compute"):
        ds_work[lon_name] = ds_work.coords[lon_name].compute()

    # For 2-D irregular-grid (swath) coordinates, pre-filter to the bounding
    # box of the query points before building the index.  This must happen
    # before the meshgrid expansion below so that the check fires only for
    # genuinely 2-D coords (not the synthetic meshgrid we create for 1-D data,
    # which has already been sliced by _slice_grid_to_points at the call site).
    _pt_lats = [float(r["lat"]) for r in rows]
    _pt_lons = [float(r["lon"]) for r in rows]
    ds_work = _slice_2d_to_points(ds_work, _pt_lats, _pt_lons, lat_name, lon_name)

    # NDPointIndex requires lat and lon to share the same dimensions.  For
    # regular grid data (1-D lat/lon with separate dimensions), broadcast both
    # coordinates to a common 2-D meshgrid so that the joint index can be built.
    # Without this step, set_xindex() would raise because the two 1-D arrays
    # each have their own dimension (e.g. 'lat' vs 'lon') and NDPointIndex
    # needs all indexed coordinates to be defined over the same set of dims.
    lat_arr = ds_work.coords[lat_name] if lat_name in ds_work.coords else ds_work[lat_name]
    lon_arr = ds_work.coords[lon_name] if lon_name in ds_work.coords else ds_work[lon_name]
    if lat_arr.ndim == 1 and lon_arr.ndim == 1:
        lat_2d, lon_2d = np.meshgrid(lat_arr.values, lon_arr.values, indexing="ij")
        lat_dims = lat_arr.dims + lon_arr.dims  # e.g. ('lat', 'lon')
        ds_work[lat_name] = xr.DataArray(lat_2d, dims=lat_dims)
        ds_work[lon_name] = xr.DataArray(lon_2d, dims=lat_dims)

    # Drop pixels where lat/lon are NaN or Inf (e.g. fill values outside swath).
    ds_work = _drop_nan_geoloc(ds_work, lat_name, lon_name)

    # Build the NDPointIndex once for all query points using the built-in
    # scipy adapter (ScipyKDTreeAdapter).  No tree_adapter_cls argument is
    # passed so xarray's default applies.
    indexed_ds = ds_work.set_xindex(
        [lat_name, lon_name],
        xr.indexes.NDPointIndex,
    )

    # Build the target dataset with all query points at once.
    lats = [row["lat"] for row in rows]
    lons = [row["lon"] for row in rows]
    target = xr.Dataset(
        {
            lat_name: xr.DataArray(lats),
            lon_name: xr.DataArray(lons),
        }
    )
    # The auto-generated dimension name for the query coordinate array
    # (e.g. 'dim_0') is used to index individual results per point.
    query_dim = target[lat_name].dims[0]

    try:
        selected = indexed_ds.sel(
            {lat_name: target[lat_name], lon_name: target[lon_name]},
            method="nearest",
        )
        # Extract matched granule coordinates for each query point.
        try:
            matched_lats = selected.coords[lat_name].values
            matched_lons = selected.coords[lon_name].values
            for i, row in enumerate(rows):
                row["granule_lat"] = float(matched_lats[i])
                row["granule_lon"] = float(matched_lons[i])
        except Exception:
            for row in rows:
                row["granule_lat"] = float("nan")
                row["granule_lon"] = float("nan")

        for var in variables:
            try:
                var_data = selected[var]
                for i, row in enumerate(rows):
                    # Extract the i-th query point.  After sel() the query
                    # dimension is prepended; squeeze removes any remaining
                    # size-1 spatial dims so extra dims (e.g. wavelength) are
                    # kept intact.
                    point_data = var_data.isel({query_dim: i}).squeeze()
                    if time_dim is not None:
                        point_data = _select_time(point_data, time_dim, row.get("time"))
                    if point_data.ndim == 0:
                        row[var] = float(point_data)
                    else:
                        # Additional dimensions (e.g. wavelength) — expand
                        # into coord-keyed columns (Rrs_346, Rrs_348, …).
                        row[var] = float("nan")  # placeholder; removed later
                        for coord_val, val in point_data.to_series().items():
                            row[f"{var}_{int(coord_val)}"] = float(val)
            except Exception:
                for r in rows:
                    r[var] = float("nan")
    except Exception:
        for row in rows:
            row["granule_lat"] = float("nan")
            row["granule_lon"] = float("nan")
        for var in variables:
            for r in rows:
                r[var] = float("nan")
