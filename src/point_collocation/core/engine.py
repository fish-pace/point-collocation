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

from typing import TYPE_CHECKING

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
    ImportError
        If ``spatial_method="xoak"`` and the ``xoak`` package is not
        installed.
    """
    if geometry not in _VALID_GEOMETRIES:
        raise ValueError(
            f"geometry={geometry!r} is not valid. "
            f"Must be one of {sorted(_VALID_GEOMETRIES)}."
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


def _open_as_flat_dataset(
    file_obj: object,
    open_method: str,
    kwargs: dict,
) -> "xr.Dataset":
    """Open *file_obj* and return a flat :class:`xarray.Dataset`.

    For ``open_method="dataset"``, wraps ``xr.open_dataset``.
    For ``open_method="datatree-merge"``, opens as a DataTree (using
    ``xarray.open_datatree`` if available, or the ``datatree`` package)
    and merges all leaves into a single Dataset.
    """
    if open_method == "dataset":
        return xr.open_dataset(file_obj, **kwargs)  # type: ignore[arg-type]

    # datatree-merge: open as DataTree and merge groups.
    dt = _open_datatree(file_obj, kwargs)
    return _merge_datatree(dt)


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


def _execute_plan(
    plan: "Plan",
    *,
    geometry: str,
    open_method: str,
    spatial_method: str,
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

    # Track whether we have already validated geometry on the first granule.
    geometry_checked = False

    # Process granules, opening each file once
    for g_idx, pt_indices in sorted(granule_to_points.items()):
        gm = plan.granules[g_idx]
        file_obj = opened_files[gm.result_index]

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

                for pt_idx in pt_indices:
                    row = plan.points.loc[pt_idx].to_dict()
                    row["granule_id"] = gm.granule_id

                    if spatial_method == "nearest":
                        _extract_nearest(ds, row, variables, lon_name, lat_name)
                    else:
                        _extract_xoak(ds, row, variables, lon_name, lat_name)

                    output_rows.append(row)

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
    """Extract values using xoak nearest-neighbour (2-D lat/lon arrays).

    Uses the ``xarray.indexes.NDPointIndex`` API with xoak's
    ``SklearnKDTreeAdapter``.  The lat/lon coordinate arrays are computed
    from dask (if chunked) before building the k-d tree index.

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
                # point; .flat[0] safely extracts the scalar regardless of dims.
                val = selected[var].values.flat[0]
                row[var] = float(val)
            except Exception:
                row[var] = float("nan")
    except Exception:
        for var in variables:
            row[var] = float("nan")
