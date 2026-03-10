"""Core matchup engine — no earthaccess dependency here.

Responsibilities
----------------
* Accept a :class:`~point_collocation.core.plan.Plan` object built with
  :func:`~point_collocation.plan`.
* Open each granule individually with ``xarray.open_dataset`` (never
  ``open_mfdataset``) to minimise cloud I/O and avoid memory leaks.
* Extract the requested variables at each point's location/time using
  nearest-neighbour selection (gridded) or xoak k-d tree (swath).
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
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator

import numpy as np
import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    from point_collocation.core.plan import Plan

# Geolocation name pairs (case-sensitive, tried in order).
# Each element is (lon_name, lat_name).
_GEOLOC_PAIRS = [
    ("lon", "lat"),
    ("longitude", "latitude"),
    ("Longitude", "Latitude"),
    ("LONGITUDE", "LATITUDE"),
]

_VALID_GEOMETRIES = {"grid", "swath"}
_VALID_OPEN_METHODS = {"dataset", "datatree-merge"}
_VALID_SPATIAL_METHODS = {"nearest", "xoak"}


def matchup(
    plan: "Plan",
    *,
    geometry: str,
    variables: list[str] | None = None,
    open_method: str | None = None,
    spatial_method: str | None = None,
    open_dataset_kwargs: dict | None = None,
    silent: bool = False,
    batch_size: int = 10,
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
    geometry:
        Data geometry type.  Must be ``"grid"`` (L3/gridded, 1-D lat/lon
        coordinates) or ``"swath"`` (L2/swath, 2-D lat/lon arrays).
        This is a required argument — no default is provided.
    variables:
        Variable names to extract from each granule.  When provided,
        overrides any variables stored on the plan.  When omitted,
        falls back to ``plan.variables``.  If the resolved list is
        empty, the output will have no variable columns.
        Raises :exc:`ValueError` if a requested variable is not found
        in the opened dataset.
    open_method:
        How granules are opened.  ``"dataset"`` opens each granule with
        ``xarray.open_dataset``; ``"datatree-merge"`` opens with
        DataTree and merges groups into a flat dataset.  Defaults to
        ``"dataset"`` when ``geometry="grid"`` and ``"datatree-merge"``
        when ``geometry="swath"``.
    spatial_method:
        Method used for spatial matching.  ``"nearest"`` uses
        ``ds.sel(..., method="nearest")`` and requires 1-D coordinates
        (gridded data).  ``"xoak"`` uses the ``xoak`` package for
        nearest-neighbour matching on 2-D (irregular/swath) grids.
        Defaults to ``"nearest"`` when ``geometry="grid"`` and
        ``"xoak"`` when ``geometry="swath"``.
    open_dataset_kwargs:
        Optional dictionary of keyword arguments forwarded to
        ``xarray.open_dataset`` for every granule opened during the run.
        ``chunks`` defaults to ``{}`` (lazy/dask loading) unless
        explicitly overridden.  ``engine`` defaults to ``"h5netcdf"``
        when no ``engine`` key is present in the dict.
    silent:
        When ``False`` (default), a progress message is printed to
        stdout after every *batch_size* granules.  Set to ``True`` to
        suppress all progress output.
    batch_size:
        Number of granules to process between progress reports (and
        between intermediate saves when *save_dir* is set).  Defaults
        to ``10``.
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
        One row per (point, granule) pair, including a ``granule_id``
        column and one column per variable.  Points with zero matching
        granules contribute a single NaN row.

    Raises
    ------
    ValueError
        If ``geometry`` is not ``"grid"`` or ``"swath"``.
    ValueError
        If a requested variable is not present in an opened dataset.
    ValueError
        If geolocation variables cannot be detected unambiguously.
    ValueError
        If the geolocation array dimensionality does not match *geometry*.
    ValueError
        If ``granule_range`` is not a 2-tuple of positive integers with
        ``start <= end``, or if either bound exceeds the number of matched
        granules in the plan.
    ImportError
        If ``spatial_method="xoak"`` and the ``xoak`` package is not
        installed.
    """
    if geometry not in _VALID_GEOMETRIES:
        raise ValueError(
            f"geometry={geometry!r} is not valid. "
            f"Must be one of {sorted(_VALID_GEOMETRIES)}."
        )

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

    # Apply geometry-based defaults.
    if open_method is None:
        open_method = "dataset" if geometry == "grid" else "datatree-merge"
    if spatial_method is None:
        spatial_method = "nearest" if geometry == "grid" else "xoak"

    if open_method not in _VALID_OPEN_METHODS:
        raise ValueError(
            f"open_method={open_method!r} is not valid. "
            f"Must be one of {sorted(_VALID_OPEN_METHODS)}."
        )
    if spatial_method not in _VALID_SPATIAL_METHODS:
        raise ValueError(
            f"spatial_method={spatial_method!r} is not valid. "
            f"Must be one of {sorted(_VALID_SPATIAL_METHODS)}."
        )

    # Validate xoak is importable before we start processing granules.
    if spatial_method == "xoak":
        try:
            from xoak.tree_adapters import SklearnKDTreeAdapter  # type: ignore[import-untyped]  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "The 'xoak' package (and scikit-learn) are required for spatial_method='xoak'. "
                "Install them with: pip install xoak scikit-learn"
            ) from exc

    effective_vars: list[str] = variables if variables is not None else plan.variables
    effective_kwargs = {"chunks": {}, **(open_dataset_kwargs or {})}
    return _execute_plan(
        plan,
        geometry=geometry,
        open_method=open_method,
        spatial_method=spatial_method,
        variables=effective_vars,
        silent=silent,
        batch_size=batch_size,
        save_dir=save_dir,
        granule_range=granule_range,
        **effective_kwargs,
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _find_geoloc_pair(ds: xr.Dataset) -> tuple[str, str]:
    """Find exactly one ``(lon_name, lat_name)`` pair in *ds*.

    Searches both ``ds.coords`` and ``ds.data_vars`` for each pair in
    :data:`_GEOLOC_PAIRS`.

    Returns
    -------
    tuple[str, str]
        ``(lon_name, lat_name)`` of the single detected pair.

    Raises
    ------
    ValueError
        If zero pairs are found ("no geolocation variables found") or
        more than one pair is found ("ambiguous geolocation variables").
    """
    found: list[tuple[str, str]] = []
    for lon_name, lat_name in _GEOLOC_PAIRS:
        has_lon = lon_name in ds.coords or lon_name in ds.data_vars
        has_lat = lat_name in ds.coords or lat_name in ds.data_vars
        if has_lon and has_lat:
            found.append((lon_name, lat_name))

    if len(found) == 0:
        raise ValueError(
            "no geolocation variables found. "
            "Expected one of the following (lon, lat) name pairs in ds.coords "
            f"or ds.data_vars: {_GEOLOC_PAIRS}"
        )
    if len(found) > 1:
        raise ValueError(
            f"ambiguous geolocation variables; detected pairs: {found}. "
            "The dataset contains more than one recognised (lon, lat) pair. "
            "Rename or drop the extra coordinates before running matchup."
        )
    return found[0]


def _ensure_coords(ds: xr.Dataset, lon_name: str, lat_name: str) -> xr.Dataset:
    """Promote *lon_name* and *lat_name* to coordinates if they are data variables."""
    to_promote = [
        name
        for name in (lon_name, lat_name)
        if name in ds.data_vars and name not in ds.coords
    ]
    if to_promote:
        ds = ds.set_coords(to_promote)
    return ds


def _check_geometry(
    ds: xr.Dataset,
    lon_name: str,
    lat_name: str,
    geometry: str,
) -> None:
    """Raise if the dimensionality of lat/lon does not match *geometry*.

    Uses only metadata (``dims``) — does **not** load array data.
    """
    # Access the variable through coords or data_vars (prefer coords after promote step).
    lon_var = ds.coords[lon_name] if lon_name in ds.coords else ds[lon_name]
    lat_var = ds.coords[lat_name] if lat_name in ds.coords else ds[lat_name]

    lon_ndim = len(lon_var.dims)
    lat_ndim = len(lat_var.dims)

    if geometry == "grid" and (lon_ndim != 1 or lat_ndim != 1):
        raise ValueError(
            f"geometry='grid' requires 1-D geolocation arrays, but found "
            f"{lon_name!r} with dims={tuple(lon_var.dims)} and "
            f"{lat_name!r} with dims={tuple(lat_var.dims)}. "
            "Try geometry='swath'."
        )
    if geometry == "swath" and (lon_ndim != 2 or lat_ndim != 2):
        raise ValueError(
            f"geometry='swath' requires 2-D geolocation arrays, but found "
            f"{lon_name!r} with dims={tuple(lon_var.dims)} and "
            f"{lat_name!r} with dims={tuple(lat_var.dims)}. "
            "Try geometry='grid'."
        )


@contextmanager
def _open_as_flat_dataset(
    file_obj: object,
    open_method: str,
    kwargs: dict,
) -> Generator["xr.Dataset", None, None]:
    """Context manager that opens *file_obj* and yields a flat :class:`xarray.Dataset`.

    For ``open_method="dataset"``, wraps ``xr.open_dataset``.
    For ``open_method="datatree-merge"``, opens as a DataTree (using
    ``xarray.open_datatree`` if available, or the ``datatree`` package),
    merges all leaves into a single Dataset, and explicitly closes the
    DataTree on exit so that all underlying file handles are released
    promptly — without relying on Python's cyclic garbage collector.
    """
    if open_method == "dataset":
        with xr.open_dataset(file_obj, **kwargs) as ds:  # type: ignore[arg-type]
            yield ds
        return

    # datatree-merge: open as DataTree, merge groups, close the tree on exit.
    dt = _open_datatree(file_obj, kwargs)
    try:
        ds = _merge_datatree(dt)
        yield ds
    finally:
        # Explicitly close the DataTree to release all underlying file handles.
        # Without this the DataTree (which typically contains parent→child cycles)
        # is not freed until Python's cyclic GC runs, causing the dataset's
        # memory (~200 MB per swath granule) to accumulate across granules.
        dt.close()


def _open_datatree(file_obj: object, kwargs: dict) -> object:
    """Open *file_obj* as a DataTree using whichever API is available."""
    # Suppress xarray FutureWarning about timedelta decoding by opting into
    # the future behaviour (do not decode timedelta-like variables by default).
    dt_kwargs = dict(kwargs)
    dt_kwargs.setdefault("decode_timedelta", False)

    # Try xarray built-in DataTree (xarray >= 2024.x).
    try:
        open_dt = xr.open_datatree  # type: ignore[attr-defined]
        return open_dt(file_obj, **dt_kwargs)  # type: ignore[arg-type]
    except AttributeError:
        pass

    # Fall back to the standalone datatree package.
    try:
        import datatree  # type: ignore[import-untyped]

        return datatree.open_datatree(file_obj, **dt_kwargs)  # type: ignore[arg-type]
    except ImportError as exc:
        raise ImportError(
            "open_method='datatree-merge' requires either xarray >= 2024.x (with "
            "built-in DataTree support) or the 'datatree' package. "
            "Install it with: pip install datatree"
        ) from exc


def _merge_datatree(dt: object) -> xr.Dataset:
    """Merge all leaf datasets in *dt* into a single flat Dataset."""
    # Both xarray.DataTree and datatree.DataTree expose .subtree or .items()
    # for iteration over nodes.  We collect all datasets and merge them.
    datasets: list[xr.Dataset] = []

    try:
        # xarray DataTree API (>= 2024.x).
        for node in dt.subtree:  # type: ignore[union-attr]
            if node.ds is not None and len(node.ds.data_vars) > 0:
                datasets.append(node.ds)
    except AttributeError:
        # datatree package API.
        for _path, node in dt.items():  # type: ignore[union-attr]
            ds = node.ds
            if ds is not None and len(ds.data_vars) > 0:
                datasets.append(ds)

    if not datasets:
        return xr.Dataset()

    merged = xr.merge(datasets, compat="override", join="outer")
    return merged


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
    geometry: str,
    open_method: str,
    spatial_method: str,
    variables: list[str],
    silent: bool = False,
    batch_size: int = 10,
    save_dir: str | os.PathLike | None = None,
    granule_range: tuple[int, int] | None = None,
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

    # NOTE: We intentionally do NOT call earthaccess.open(plan.results) all at
    # once here.  Opening every file upfront holds all S3/HTTPS file handles in
    # memory simultaneously, causing RAM to grow linearly with the number of
    # granules.  Instead we open only the files needed for each batch below so
    # that handles from previous batches can be released by the OS, keeping
    # peak memory proportional to batch_size rather than the total number of
    # granules.

    kwargs = dict(open_dataset_kwargs)
    if "engine" not in kwargs:
        kwargs["engine"] = "h5netcdf"

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

    # Track whether we have already validated geometry on the first granule.
    geometry_checked = False

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

    # Process granules in batches.  We open only the files needed for each
    # batch so that file handles from previous batches can be released by the
    # OS, keeping peak memory proportional to batch_size rather than the total
    # number of granules.
    for batch_offset in range(0, total_granules, batch_size):
        batch_items = sorted_granule_items[batch_offset : batch_offset + batch_size]

        # Collect the earthaccess result objects for this batch (preserving
        # order) so that opened_batch[i] corresponds to batch_items[i].
        batch_results = [
            plan.results[plan.granules[g_idx].result_index]
            for g_idx, _ in batch_items
        ]
        opened_batch: list[object] = earthaccess.open(
            batch_results, pqdm_kwargs={"disable": True}
        )

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
                try:
                    with _open_as_flat_dataset(file_obj, open_method, kwargs) as ds:  # type: ignore[arg-type]
                        lon_name, lat_name = _find_geoloc_pair(ds)
                        ds = _ensure_coords(ds, lon_name, lat_name)

                        # Validate geometry against actual array dims — once only.
                        if not geometry_checked:
                            _check_geometry(ds, lon_name, lat_name, geometry)
                            geometry_checked = True

                        # Validate that all requested variables exist in the dataset.
                        missing_vars = [v for v in variables if v not in ds]
                        if missing_vars:
                            avail = list(ds.data_vars)
                            raise ValueError(
                                f"Variable(s) {missing_vars!r} not found in granule "
                                f"'{gm.granule_id}'. "
                                f"geometry={geometry!r}, open_method={open_method!r}, "
                                f"spatial_method={spatial_method!r}. "
                                f"Available variables: {avail}. "
                                "Use plan.show_variables() to inspect the dataset."
                            )

                        # For grid+xoak, pre-slice the dataset to the spatial extent
                        # of the query points before building the k-d tree.  A global
                        # granule with only a few scattered points would otherwise cause
                        # xoak to index the entire global grid, which is very slow.
                        if spatial_method == "xoak" and geometry == "grid":
                            pt_lats = [float(plan.points.loc[idx]["lat"]) for idx in pt_indices]
                            pt_lons = [float(plan.points.loc[idx]["lon"]) for idx in pt_indices]
                            ds = _slice_grid_to_points(ds, pt_lats, pt_lons, lat_name, lon_name)

                        if spatial_method == "xoak":
                            # Build the k-d tree index once for all points in this
                            # granule instead of rebuilding it per point.  This
                            # dramatically reduces memory pressure and speeds up
                            # processing when a granule has many query points.
                            rows_for_granule = []
                            for pt_idx in pt_indices:
                                row = plan.points.loc[pt_idx].to_dict()
                                row["granule_id"] = gm.granule_id
                                rows_for_granule.append(row)
                            _extract_xoak_batch(ds, rows_for_granule, variables, lon_name, lat_name)
                            output_rows.extend(rows_for_granule)
                            batch_rows.extend(rows_for_granule)
                        else:
                            for pt_idx in pt_indices:
                                row = plan.points.loc[pt_idx].to_dict()
                                row["granule_id"] = gm.granule_id
                                _extract_nearest(ds, row, variables, lon_name, lat_name)
                                output_rows.append(row)
                                batch_rows.append(row)

                        batch_matched_points += len(pt_indices)

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
                # For datatree-merge (swath), DataTree nodes hold
                # parent↔child reference cycles that Python's reference
                # counting cannot collect.  Without a GC call here, all
                # DataTree objects for an entire batch accumulate in memory
                # before the single gc.collect() at the end of the batch
                # runs — causing retained memory to scale with batch_size
                # rather than staying constant at ~1 granule.  Calling
                # gc.collect() once per granule keeps peak memory bounded
                # regardless of batch_size.
                if open_method == "datatree-merge":
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


def _slice_grid_to_points(
    ds: xr.Dataset,
    lats: list[float],
    lons: list[float],
    lat_name: str,
    lon_name: str,
    buffer_deg: float = 1.0,
) -> xr.Dataset:
    """Slice a regular-grid dataset to the smallest region covering *lats*/*lons*.

    When ``geometry='grid'`` and ``spatial_method='xoak'``, building a k-d tree
    over an entire global granule is very slow if only a few points need to be
    matched.  This function slices the dataset to a padded bounding box around
    the query points so xoak indexes the minimum required region.

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


def _extract_nearest(
    ds: xr.Dataset,
    row: dict,
    variables: list[str],
    lon_name: str,
    lat_name: str,
) -> None:
    """Extract values using ``ds.sel(..., method='nearest')`` (1-D coords).

    Modifies *row* in-place.
    """
    for var in variables:
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


def _extract_xoak(
    ds: xr.Dataset,
    row: dict,
    variables: list[str],
    lon_name: str,
    lat_name: str,
) -> None:
    """Extract values using xoak nearest-neighbour (1-D or 2-D lat/lon arrays).

    Uses the ``xarray.indexes.NDPointIndex`` API with xoak's
    ``SklearnKDTreeAdapter``.  The lat/lon coordinate arrays are computed
    from dask (if chunked) before building the k-d tree index.

    For 1-D (gridded) lat/lon coordinates, the arrays are broadcast to a
    shared 2-D meshgrid so that ``NDPointIndex`` can build a joint spatial
    index over both dimensions.

    Modifies *row* in-place.
    """
    try:
        from xoak.tree_adapters import SklearnKDTreeAdapter  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "The 'xoak' package is required for spatial_method='xoak'. "
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
) -> None:
    """Extract values for all *rows* using a single xoak k-d tree index.

    Builds the k-d tree index **once** for the entire dataset, then queries
    all points simultaneously.  This avoids the O(N) index-rebuild cost of
    calling :func:`_extract_xoak` once per point and substantially reduces
    peak memory when a granule has many query points.

    Uses the ``xarray.indexes.NDPointIndex`` API with xoak's
    ``SklearnKDTreeAdapter``.

    Modifies each dict in *rows* in-place.
    """
    try:
        from xoak.tree_adapters import SklearnKDTreeAdapter  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "The 'xoak' package is required for spatial_method='xoak'. "
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

    # Build the NDPointIndex once for all query points.
    indexed_ds = ds_work.set_xindex(
        [lat_name, lon_name],
        xr.indexes.NDPointIndex,
        tree_adapter_cls=SklearnKDTreeAdapter,
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
        for var in variables:
            try:
                var_data = selected[var]
                for i, row in enumerate(rows):
                    # Extract the i-th query point.  After sel() the query
                    # dimension is prepended; squeeze removes any remaining
                    # size-1 spatial dims so extra dims (e.g. wavelength) are
                    # kept intact.
                    point_data = var_data.isel({query_dim: i}).squeeze()
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
        for var in variables:
            for r in rows:
                r[var] = float("nan")
