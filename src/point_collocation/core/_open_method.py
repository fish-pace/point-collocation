"""Open-method specification and pipeline for point-collocation granule opening.

This module handles the "Open granule → matchup-ready xarray.Dataset" pipeline.
It is responsible for:

1. Normalizing the ``open_method`` argument (string preset → dict spec).
2. Building effective ``open_kwargs`` (applying defaults for ``chunks``,
   ``engine``, and ``decode_timedelta``).
3. Opening a file as a flat ``xarray.Dataset`` (via dataset or datatree path).
4. Normalizing coordinates (detecting lat/lon and promoting to xarray coords).

The ``open_method`` argument accepted by :func:`~point_collocation.matchup`
and related functions may be:

* A **string preset**: ``"dataset"``, ``"datatree-merge"``, or ``"auto"``.
* A **dict spec** conforming to the schema below.

Dict schema
-----------
::

    open_method = {
        "xarray_open":           "dataset" | "datatree",
        "open_kwargs":           {},
        "merge":                 "all" | "root" | ["/path/a", "/path/b"],
        "merge_kwargs":          {},
        "coords":                "auto" | ["Latitude", "Longitude"] | {"lat": "...", "lon": "..."},
        "set_coords":            True,
        "dim_renames":           None | {"node_path": {"old_dim": "new_dim"}},
        "auto_align_phony_dims": None | "safe",
    }

All keys are optional; missing keys receive sensible defaults.
Unknown keys raise a clear :exc:`ValueError` to prevent silent typos.
"""

from __future__ import annotations

import contextlib
import os
import re
from contextlib import contextmanager
from typing import TYPE_CHECKING, Generator

import xarray as xr

if TYPE_CHECKING:
    pass

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_VALID_SPEC_KEYS: frozenset[str] = frozenset(
    {
        "xarray_open",
        "open_kwargs",
        "merge",
        "merge_kwargs",
        "coords",
        "set_coords",
        "dim_renames",
        "auto_align_phony_dims",
    }
)

_VALID_XARRAY_OPEN: frozenset[str] = frozenset({"dataset", "datatree"})
_VALID_PRESETS: frozenset[str] = frozenset({"dataset", "datatree", "datatree-merge", "auto"})

# Default open kwargs applied to both xr.open_dataset and xr.open_datatree
# unless explicitly overridden by the user.
_DEFAULT_OPEN_KWARGS: dict = {
    "chunks": {},
    "engine": "h5netcdf",
    "decode_timedelta": False,
}


# ---------------------------------------------------------------------------
# Progress-suppression helper
# ---------------------------------------------------------------------------


@contextmanager
def _suppress_dask_progress() -> Generator[None, None, None]:
    """Suppress dask progress bar output during file-open operations.

    When opening HE5/HDF5 files with ``chunks={}``, dask (or pqdm used
    internally by earthaccess) may emit verbose progress bar output
    (e.g. ``QUEUEING TASKS``, ``PROCESSING TASKS``, ``COLLECTING RESULTS``).
    This context manager suppresses that output without affecting the data.

    In a Jupyter environment it uses :func:`IPython.utils.io.capture_output`;
    otherwise it redirects both ``stdout`` and ``stderr`` to ``/dev/null`` for
    the duration of the open call.
    """
    try:
        from IPython.utils import io as _ipy_io  # type: ignore[import]

        with _ipy_io.capture_output():
            yield
        return
    except ImportError:
        pass

    with open(os.devnull, "w") as _devnull:
        with contextlib.redirect_stdout(_devnull), contextlib.redirect_stderr(_devnull):
            yield


# ---------------------------------------------------------------------------
# Open kwargs helpers
# ---------------------------------------------------------------------------


def _build_effective_open_kwargs(user_kwargs: dict) -> dict:
    """Build effective open kwargs by applying defaults to *user_kwargs*.

    Defaults applied (if not already present in *user_kwargs*):

    * ``chunks``: ``{}`` (lazy/dask loading)
    * ``engine``: ``"h5netcdf"``
    * ``decode_timedelta``: ``False``

    Parameters
    ----------
    user_kwargs:
        User-provided kwargs (e.g. from ``spec["open_kwargs"]``).

    Returns
    -------
    dict
        Effective kwargs dict with all defaults applied.
    """
    result = dict(user_kwargs)
    for key, value in _DEFAULT_OPEN_KWARGS.items():
        result.setdefault(key, value)
    return result


# ---------------------------------------------------------------------------
# Spec normalization
# ---------------------------------------------------------------------------


def _normalize_open_method(
    open_method: str | dict,
    open_dataset_kwargs: dict | None = None,
) -> dict:
    """Normalize a string preset or dict spec into a fully-specified dict.

    Parameters
    ----------
    open_method:
        Either a string preset (``"dataset"``, ``"datatree-merge"``,
        ``"auto"``) or a dict spec conforming to the open-method schema.
    open_dataset_kwargs:
        Optional top-level override for open kwargs.  These take precedence
        over any ``"open_kwargs"`` already in *open_method* (when it is a
        dict), and are applied before the shared defaults.

    Returns
    -------
    dict
        Normalized full dict spec with all required keys present.

    Raises
    ------
    TypeError
        If *open_method* is neither a str nor a dict.
    ValueError
        If a string preset is not recognised, or if a dict spec contains
        unknown keys.
    """
    if isinstance(open_method, str):
        spec = _expand_preset(open_method)
    elif isinstance(open_method, dict):
        spec = _validate_and_fill_spec(open_method)
    else:
        raise TypeError(
            f"open_method must be a string preset or dict spec, "
            f"got {type(open_method).__name__!r}."
        )

    # Merge top-level open_dataset_kwargs into spec's open_kwargs.
    # open_dataset_kwargs takes precedence over the spec's open_kwargs.
    if open_dataset_kwargs:
        merged = {**spec.get("open_kwargs", {}), **open_dataset_kwargs}
        spec = {**spec, "open_kwargs": merged}

    return spec


def _expand_preset(preset: str) -> dict:
    """Expand a string preset to a normalized dict spec.

    Parameters
    ----------
    preset:
        One of ``"dataset"``, ``"datatree"``, ``"datatree-merge"``, or
        ``"auto"``.

    Returns
    -------
    dict
        Full dict spec.

    Raises
    ------
    ValueError
        If *preset* is not a valid string preset.
    """
    if preset == "dataset":
        return {
            "xarray_open": "dataset",
            "open_kwargs": {},
            "merge": None,
            "coords": "auto",
            "set_coords": True,
            "dim_renames": None,
            "auto_align_phony_dims": None,
        }
    if preset == "datatree":
        return {
            "xarray_open": "datatree",
            "open_kwargs": {},
            "merge": None,
            "coords": "auto",
            "set_coords": True,
            "dim_renames": None,
            "auto_align_phony_dims": None,
        }
    if preset == "datatree-merge":
        return {
            "xarray_open": "datatree",
            "open_kwargs": {},
            "merge": "all",
            "merge_kwargs": {},
            "coords": "auto",
            "set_coords": True,
            "dim_renames": None,
            "auto_align_phony_dims": None,
        }
    if preset == "auto":
        return {
            "xarray_open": "auto",
            "open_kwargs": {},
            "coords": "auto",
            "set_coords": True,
            "dim_renames": None,
            "auto_align_phony_dims": None,
        }
    raise ValueError(
        f"open_method={preset!r} is not a valid string preset. "
        f"Must be one of {sorted(_VALID_PRESETS)} or a dict spec."
    )


def _validate_and_fill_spec(spec: dict) -> dict:
    """Validate and fill missing keys in a dict spec with sensible defaults.

    Parameters
    ----------
    spec:
        User-provided dict spec.

    Returns
    -------
    dict
        Validated and filled spec.

    Raises
    ------
    ValueError
        If *spec* contains unknown keys, or if ``"xarray_open"`` is not a
        valid value.
    """
    unknown = set(spec.keys()) - _VALID_SPEC_KEYS
    if unknown:
        raise ValueError(
            f"open_method dict contains unknown keys: {sorted(unknown)}. "
            f"Valid keys are: {sorted(_VALID_SPEC_KEYS)}."
        )

    result = dict(spec)
    result.setdefault("xarray_open", "dataset")
    result.setdefault("open_kwargs", {})
    result.setdefault("coords", "auto")
    result.setdefault("set_coords", True)
    result.setdefault("dim_renames", None)
    result.setdefault("auto_align_phony_dims", None)

    xarray_open = result["xarray_open"]
    if xarray_open not in _VALID_XARRAY_OPEN:
        raise ValueError(
            f"open_method['xarray_open']={xarray_open!r} is not valid. "
            f"Must be one of {sorted(_VALID_XARRAY_OPEN)}."
        )

    if xarray_open == "datatree":
        result.setdefault("merge", None)
        if result.get("merge") is not None:
            result.setdefault("merge_kwargs", {})
    elif xarray_open == "dataset":
        result.setdefault("merge", None)
        if result.get("merge") is not None:
            result.setdefault("merge_kwargs", {})

    return result


# ---------------------------------------------------------------------------
# Geolocation detection
# ---------------------------------------------------------------------------

# Geolocation name pairs used as a fallback when cf_xarray is not installed or
# when the dataset lacks CF-convention attributes.
# Each element is (lon_name, lat_name), tried in order (case-sensitive).
_GEOLOC_PAIRS = [
    ("lon", "lat"),
    ("longitude", "latitude"),
    ("Longitude", "Latitude"),
    ("LONGITUDE", "LATITUDE"),
]


def _cf_geoloc_names(ds: xr.Dataset, key: str) -> list[str]:
    """Return variable names that match a CF *key* in *ds*.

    Searches both ``ds.coords`` and ``ds.data_vars`` via the ``cf_xarray``
    accessor.  Returns an empty list when ``cf_xarray`` is not installed or
    when no variables match the key.
    """
    try:
        import cf_xarray  # noqa: F401  (registers the .cf accessor)
    except ImportError:
        return []

    try:
        matched = ds.cf[[key]]
    except KeyError:
        return []

    return list(matched.coords) + list(matched.data_vars)


def _find_geoloc_pair(ds: xr.Dataset) -> tuple[str, str]:
    """Find exactly one ``(lon_name, lat_name)`` pair in *ds*.

    Detection strategy
    ------------------
    1. **cf_xarray** (primary, if installed): inspects CF-convention
       attributes such as ``standard_name``, ``units``, and ``long_name``
       in both ``ds.coords`` and ``ds.data_vars``.
    2. **Name-based fallback**: searches ``ds.coords`` and ``ds.data_vars``
       for each ``(lon_name, lat_name)`` pair in :data:`_GEOLOC_PAIRS`.

    Returns
    -------
    tuple[str, str]
        ``(lon_name, lat_name)`` of the single detected pair.

    Raises
    ------
    ValueError
        If zero pairs are found or more than one pair is found.
    """
    lon_names = _cf_geoloc_names(ds, "longitude")
    lat_names = _cf_geoloc_names(ds, "latitude")

    if lon_names or lat_names:
        if not lon_names or not lat_names:
            raise ValueError(
                "no geolocation variables found. "
                f"cf_xarray detected longitude={lon_names}, latitude={lat_names}; "
                "expected exactly one variable for each."
            )
        if len(lon_names) > 1 or len(lat_names) > 1:
            raise ValueError(
                f"ambiguous geolocation variables; "
                f"cf_xarray detected longitude={lon_names}, latitude={lat_names}. "
                "Rename or drop the extra coordinates before running matchup."
            )
        return lon_names[0], lat_names[0]

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
            f"or ds.data_vars: {_GEOLOC_PAIRS}. "
            "Specify coords explicitly via open_method={'coords': {'lat': '...', 'lon': '...'}}."
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


# ---------------------------------------------------------------------------
# Coordinate normalization
# ---------------------------------------------------------------------------


def _apply_coords(ds: xr.Dataset, spec: dict) -> tuple[xr.Dataset, str, str]:
    """Apply coordinate normalization from *spec* to *ds*.

    Parameters
    ----------
    ds:
        Dataset to normalize.
    spec:
        Normalized open_method dict spec.

    Returns
    -------
    tuple[xr.Dataset, str, str]
        ``(ds, lon_name, lat_name)`` where *ds* has lat/lon promoted to
        coordinates (when ``set_coords=True``).

    Raises
    ------
    ValueError
        If lat/lon cannot be identified or specified variables are absent.
    """
    coords = spec.get("coords", "auto")
    set_coords_flag = spec.get("set_coords", True)

    if coords == "auto":
        lon_name, lat_name = _find_geoloc_pair(ds)
        if set_coords_flag:
            ds = _ensure_coords(ds, lon_name, lat_name)
        return ds, lon_name, lat_name

    if isinstance(coords, list):
        missing = [n for n in coords if n not in ds and n not in ds.coords]
        if missing:
            raise ValueError(
                f"coords={coords!r}: variable(s) {missing!r} not found in dataset. "
                f"Available: {list(ds.data_vars) + list(ds.coords)}."
            )
        if set_coords_flag:
            to_promote = [n for n in coords if n in ds.data_vars and n not in ds.coords]
            if to_promote:
                ds = ds.set_coords(to_promote)
        # Auto-detect lat/lon from the (now-promoted) coords.
        lon_name, lat_name = _find_geoloc_pair(ds)
        return ds, lon_name, lat_name

    if isinstance(coords, dict):
        lat_name = coords.get("lat")
        lon_name = coords.get("lon")
        if lat_name is None or lon_name is None:
            raise ValueError(
                f"coords dict must have both 'lat' and 'lon' keys, got: {coords!r}."
            )
        for name, key in [(lat_name, "lat"), (lon_name, "lon")]:
            if name not in ds and name not in ds.coords:
                raise ValueError(
                    f"coords[{key!r}]={name!r} not found in dataset. "
                    f"Available: {list(ds.data_vars) + list(ds.coords)}."
                )
        if set_coords_flag:
            ds = _ensure_coords(ds, lon_name, lat_name)
        return ds, lon_name, lat_name

    raise ValueError(
        f"coords={coords!r} is not valid. "
        "Must be 'auto', a list of variable names, or a dict with 'lat'/'lon' keys."
    )


def _geoloc_description(ds: "xr.Dataset", lon_name: str, lat_name: str, spec: dict) -> str:
    """Return a human-readable geolocation line for printing in open_dataset.

    The description notes *how* the pair was found:

    * ``"auto detected with cf_xarray"`` — CF-convention attributes used.
    * ``"auto detected by name"`` — name-based fallback used.
    * ``"specified"`` — caller provided an explicit ``coords`` dict.
    """
    coords = spec.get("coords", "auto")

    lon_var = ds.coords[lon_name] if lon_name in ds.coords else ds[lon_name]
    lat_var = ds.coords[lat_name] if lat_name in ds.coords else ds[lat_name]
    dims_str = f"lon dims={tuple(lon_var.dims)}, lat dims={tuple(lat_var.dims)}"
    pair_str = f"({lon_name!r}, {lat_name!r})"

    if isinstance(coords, dict):
        return f"Geolocation specified: {pair_str} — {dims_str}"

    # "auto" or list — check whether cf_xarray drove the detection.
    cf_lons = _cf_geoloc_names(ds, "longitude")
    method = "auto detected with cf_xarray" if lon_name in cf_lons else "auto detected by name"
    return f"Geolocation {method}: {pair_str} — {dims_str}"


# ---------------------------------------------------------------------------
# Dataset-based group merge helpers
# ---------------------------------------------------------------------------


def _get_groups_from_h5py(file_obj: object) -> list[str]:
    """Return all group paths (including root ``'/'``) in an HDF5 file.

    Uses h5py to traverse the file hierarchy without loading any data.

    Parameters
    ----------
    file_obj:
        A file path (str/Path) or a seekable file-like object pointing to an
        HDF5/NetCDF4 file.

    Returns
    -------
    list[str]
        List of group paths such as ``['/', '/monthly', '/monthly/extra']``.

    Raises
    ------
    ImportError
        If h5py is not installed.
    """
    try:
        import h5py  # type: ignore[import-untyped]
    except ImportError as exc:
        raise ImportError(
            "merge='all' with xarray_open='dataset' requires h5py. "
            "Install it with: pip install h5py"
        ) from exc

    groups: list[str] = ["/"]

    def _collect(name: str, obj: object) -> None:
        if isinstance(obj, h5py.Group):
            groups.append("/" + name)

    with h5py.File(file_obj, "r") as h:  # type: ignore[arg-type]
        h.visititems(_collect)

    return groups


def _merge_opened_datasets(datasets: list[xr.Dataset], spec: dict) -> xr.Dataset:
    """Merge a list of already-open datasets using *spec*'s ``merge_kwargs``.

    Parameters
    ----------
    datasets:
        Open :class:`xarray.Dataset` objects to merge.
    spec:
        Normalized open_method dict spec.

    Returns
    -------
    xr.Dataset
        Merged dataset (empty if *datasets* is empty).
    """
    merge_kwargs: dict = spec.get("merge_kwargs", {})
    if not datasets:
        return xr.Dataset()
    if len(datasets) == 1:
        return datasets[0]
    effective_merge_kwargs = {"compat": "override", "join": "outer", **merge_kwargs}
    return xr.merge(datasets, **effective_merge_kwargs)


def _open_and_merge_dataset_groups(
    file_obj: object,
    spec: dict,
    effective_kwargs: dict,
) -> xr.Dataset:
    """Open HDF5/NetCDF4 groups as :class:`xarray.Dataset` objects and merge.

    Opens each group specified by ``spec['merge']`` using
    ``xr.open_dataset(..., group=path)``, merges the results, and returns the
    merged dataset.  Source datasets are **not** closed so that dask lazy arrays
    can still access their underlying file handles after this function returns.
    The source datasets remain alive as long as the merged dataset or its dask
    arrays hold references to their underlying stores; they will be
    garbage-collected (and their file handles released) when no longer
    referenced.  The caller is responsible for closing the returned merged
    dataset when finished (or allowing it to be garbage-collected).

    Parameters
    ----------
    file_obj:
        File path or seekable file-like object.
    spec:
        Normalized open_method dict spec.  ``spec['merge']`` must be
        ``'all'``, ``'root'``, or a list of group paths.
    effective_kwargs:
        Effective open kwargs (with defaults applied) to pass to
        ``xr.open_dataset``.

    Returns
    -------
    xr.Dataset
        Merged flat dataset.

    Raises
    ------
    ImportError
        If ``merge='all'`` is requested but h5py is not installed.
    """
    merge = spec.get("merge")

    if merge == "root":
        group_paths: list[str] = ["/"]
    elif merge == "all":
        group_paths = _get_groups_from_h5py(file_obj)
    elif isinstance(merge, list):
        group_paths = list(merge)
    else:
        # No merge requested — open the root dataset directly.
        with _suppress_dask_progress():
            return xr.open_dataset(file_obj, **effective_kwargs)  # type: ignore[arg-type]

    opened: list[xr.Dataset] = []
    for path in group_paths:
        kwargs = {**effective_kwargs, "group": path}
        try:
            with _suppress_dask_progress():
                ds = xr.open_dataset(file_obj, **kwargs)  # type: ignore[arg-type]
            if ds.data_vars:
                opened.append(ds)
            else:
                ds.close()
        except Exception:
            pass  # Skip unreadable groups silently (mirrors datatree merge behaviour)

    # Do NOT close source datasets here.  When dask lazy loading is active
    # (any truthy ``chunks`` specification such as ``{}``, ``"auto"``, or
    # ``{"dim": 100}``), closing a source dataset closes its underlying file
    # handle, making any dask arrays backed by that file uncomputable.  The
    # source datasets remain open and will be garbage-collected once no live
    # dask arrays reference their stores.
    return _merge_opened_datasets(opened, spec)


# ---------------------------------------------------------------------------
# h5py-based file structure inspection (for show_variables)
# ---------------------------------------------------------------------------


def _h5py_file_info(
    file_obj: object,
) -> list[tuple[str, dict[str, dict]]] | None:
    """Return file structure metadata using h5py (without loading data).

    Parameters
    ----------
    file_obj:
        A file path (str/Path) or a seekable file-like object.

    Returns
    -------
    list of (group_path, vars_dict) or None
        Each entry is ``(group_path, vars_dict)`` where *vars_dict* maps
        variable names to ``{'dims': tuple[str, ...], 'shape': tuple[int, ...]}``.
        Returns ``None`` if h5py is not installed or cannot open the file.
    """
    try:
        import h5py  # type: ignore[import-untyped]
    except ImportError:
        return None

    try:
        h = h5py.File(file_obj, "r")  # type: ignore[arg-type]
    except Exception:
        return None

    def _dim_names(item: object, h5file: object) -> tuple[str, ...]:
        """Return dimension names for an h5py Dataset item."""
        dim_list = item.attrs.get("DIMENSION_LIST")  # type: ignore[union-attr]
        if dim_list is None:
            return tuple(f"dim_{i}" for i in range(item.ndim))  # type: ignore[union-attr]
        dims: list[str] = []
        for refs in dim_list:
            if len(refs) > 0:
                try:
                    dim_ds = h5file[refs[0]]  # type: ignore[index]
                    dims.append(dim_ds.name.split("/")[-1])
                except Exception:
                    dims.append("?")
            else:
                dims.append("?")
        return tuple(dims)

    def _group_vars(group: object, h5file: object) -> dict[str, dict]:
        vars_info: dict[str, dict] = {}
        for name, obj in group.items():  # type: ignore[union-attr]
            if isinstance(obj, h5py.Dataset):
                vars_info[name] = {
                    "dims": _dim_names(obj, h5file),
                    "shape": obj.shape,  # type: ignore[union-attr]
                }
        return vars_info

    result: list[tuple[str, dict[str, dict]]] = []
    try:
        with h:
            root_vars = _group_vars(h, h)
            result.append(("/", root_vars))

            sub_groups: list[tuple[str, object]] = []

            def _visit(name: str, obj: object) -> None:
                if isinstance(obj, h5py.Group):
                    sub_groups.append(("/" + name, obj))

            h.visititems(_visit)
            for group_path, group in sub_groups:
                gvars = _group_vars(group, h)
                result.append((group_path, gvars))
    except Exception:
        return None

    return result


# ---------------------------------------------------------------------------
# DataTree helpers
# ---------------------------------------------------------------------------


def _open_datatree_fn(file_obj: object, kwargs: dict) -> object:
    """Open *file_obj* as a DataTree using whichever API is available."""
    with _suppress_dask_progress():
        try:
            open_dt = xr.open_datatree  # type: ignore[attr-defined]
            return open_dt(file_obj, **kwargs)  # type: ignore[arg-type]
        except AttributeError:
            pass

        try:
            import datatree  # type: ignore[import-untyped]

            return datatree.open_datatree(file_obj, **kwargs)  # type: ignore[arg-type]
        except ImportError as exc:
            raise ImportError(
                "open_method='datatree-merge' requires either xarray >= 2024.x (with "
                "built-in DataTree support) or the 'datatree' package. "
                "Install it with: pip install datatree"
            ) from exc


def _merge_datatree_with_spec(dt: object, spec: dict) -> xr.Dataset:
    """Merge DataTree nodes into a flat Dataset according to *spec*.

    Parameters
    ----------
    dt:
        An open DataTree object (``xarray.DataTree`` or ``datatree.DataTree``).
    spec:
        Normalized open_method dict spec.

    Returns
    -------
    xr.Dataset
        Merged flat dataset.
    """
    merge = spec.get("merge", "all")
    merge_kwargs: dict = spec.get("merge_kwargs", {})
    dim_renames = spec.get("dim_renames", None)
    auto_align_phony_dims = spec.get("auto_align_phony_dims", None)

    datasets: list[xr.Dataset] = []

    if merge == "root":
        root_ds = getattr(dt, "ds", None)
        if root_ds is not None and len(root_ds.data_vars) > 0:
            datasets.append(root_ds)

    elif merge == "all":
        try:
            # xarray DataTree API (>= 2024.x)
            for node in dt.subtree:  # type: ignore[union-attr]
                ds_node = node.ds
                if ds_node is not None and len(ds_node.data_vars) > 0:
                    datasets.append(ds_node)
        except AttributeError:
            # datatree package API
            for _path, node in dt.items():  # type: ignore[union-attr]
                ds_node = node.ds
                if ds_node is not None and len(ds_node.data_vars) > 0:
                    datasets.append(ds_node)

    elif isinstance(merge, list):
        for path in merge:
            try:
                node = dt[path]  # type: ignore[index]
                ds_node = node.ds
                if ds_node is not None:
                    datasets.append(ds_node)
            except (KeyError, TypeError):
                pass  # skip paths that don't exist; document: silently ignored

    else:
        raise ValueError(
            f"spec['merge']={merge!r} is not valid. "
            "Must be 'all', 'root', or a list of node paths."
        )

    # Apply dim_renames per node (before merge).
    # dim_renames maps {"node_path": {"old_dim": "new_dim", ...}}.
    # Since we don't track paths for merge="all", apply renames to each
    # dataset for all matching dim names from any path spec.
    if dim_renames and isinstance(dim_renames, dict):
        for i, ds_node in enumerate(datasets):
            rename_map: dict[str, str] = {}
            for _path, renames in dim_renames.items():
                for old_dim, new_dim in renames.items():
                    if old_dim in ds_node.dims:
                        rename_map[old_dim] = new_dim
            if rename_map:
                datasets[i] = ds_node.rename_dims(rename_map)

    if auto_align_phony_dims == "safe" and len(datasets) > 1:
        datasets = _safe_align_phony_dims(datasets)

    if not datasets:
        return xr.Dataset()

    if len(datasets) == 1:
        return datasets[0]

    effective_merge_kwargs = {"compat": "override", "join": "outer", **merge_kwargs}
    return xr.merge(datasets, **effective_merge_kwargs)


_PHONY_DIM_PATTERN = re.compile(r"^phony_dim_\d+$")


def _safe_align_phony_dims(datasets: list[xr.Dataset]) -> list[xr.Dataset]:
    """Conservative phony-dim alignment to enable merging datasets.

    Only renames when:

    * Datasets have dims matching ``phony_dim_N`` patterns.
    * The mapping is unambiguous (sizes match, at most ``len(canonical)`` dims).

    Canonical target dim names are ``("y", "x")``.

    Parameters
    ----------
    datasets:
        List of datasets to align.

    Returns
    -------
    list[xr.Dataset]
        Datasets with phony dims renamed (or unchanged if ambiguous).
    """
    canonical = ("y", "x")

    result = list(datasets)
    for i, ds in enumerate(datasets):
        phony_dims = [dim for dim in ds.dims if _PHONY_DIM_PATTERN.match(dim)]
        if not phony_dims:
            continue
        phony_sorted = sorted(phony_dims)
        if len(phony_sorted) > len(canonical):
            return datasets  # ambiguous: too many phony dims
        rename_map = {}
        for phony, canon in zip(phony_sorted, canonical):
            if canon not in ds.dims:
                rename_map[phony] = canon
        if rename_map:
            result[i] = ds.rename_dims(rename_map)

    return result


# ---------------------------------------------------------------------------
# Auto-mode resolution helper
# ---------------------------------------------------------------------------


def _resolve_auto_spec(file_obj: object, spec: dict) -> dict:
    """Probe *file_obj* to resolve an ``"auto"`` spec to ``"dataset"`` or ``"datatree"``.

    Attempts the fast ``xr.open_dataset`` path first; if lat/lon can be
    identified, returns a copy of *spec* with ``"xarray_open"`` set to
    ``"dataset"`` and ``"merge"`` set to ``None``.

    On failure, falls back to opening as a DataTree (with ``"merge": None``
    so the raw DataTree is returned to the caller).  The caller is responsible
    for specifying which groups to merge via an explicit dict spec, e.g.
    ``open_method={'xarray_open': 'datatree', 'merge': ['group1', 'group2'],
    'coords': {'lat': '...', 'lon': '...'}}``.

    *file_obj* is seeked back to position 0 after each probe attempt so that
    the caller can re-open it for actual data extraction.

    Parameters
    ----------
    file_obj:
        File path or seekable file-like object.
    spec:
        Normalized spec with ``"xarray_open": "auto"``.

    Returns
    -------
    dict
        Resolved spec with ``"xarray_open"`` set to ``"dataset"`` or
        ``"datatree"``.

    Raises
    ------
    ValueError
        If neither the dataset nor the DataTree path succeeds.
    """
    effective_kwargs = _build_effective_open_kwargs(spec.get("open_kwargs", {}))

    def _seek_back() -> None:
        if hasattr(file_obj, "seek"):
            try:
                file_obj.seek(0)  # type: ignore[attr-defined]
            except Exception:
                pass

    # --- Try the fast dataset path ---
    dataset_error: BaseException | None = None
    ds_probe: xr.Dataset | None = None
    try:
        with _suppress_dask_progress():
            ds_probe = xr.open_dataset(file_obj, **effective_kwargs)  # type: ignore[arg-type]
        _apply_coords(ds_probe, spec)
        _seek_back()
        return {**spec, "xarray_open": "dataset"}
    except Exception as exc:
        dataset_error = exc
    finally:
        if ds_probe is not None:
            ds_probe.close()

    _seek_back()

    # --- Try the DataTree path ---
    # Return a raw DataTree (merge=None) — it is the caller's responsibility
    # to specify which groups to merge and where the coords are via an explicit
    # open_method dict.  We only need to verify that the file opens as a
    # DataTree with at least one non-empty node.
    datatree_spec: dict = {
        **spec,
        "xarray_open": "datatree",
        "merge": None,
    }
    datatree_error: BaseException | None = None
    try:
        dt = _open_datatree_fn(file_obj, effective_kwargs)
        try:
            has_data = any(
                len(node.ds.data_vars) > 0 or len(node.ds.coords) > 0
                for node in dt.subtree
            )
            if not has_data:
                raise ValueError("DataTree has no data in any group.")
        finally:
            if hasattr(dt, "close"):
                dt.close()
        _seek_back()
        return datatree_spec
    except Exception as exc:
        datatree_error = exc

    raise ValueError(
        "open_method='auto' failed to open the granule via both "
        "the flat-dataset and DataTree paths.\n"
        f"  Dataset attempt: {dataset_error!s}\n"
        f"  DataTree attempt: {datatree_error!s}\n"
        "Specify open_method='datatree-merge' or a dict spec to diagnose further."
    ) from None


# ---------------------------------------------------------------------------
# Main context manager
# ---------------------------------------------------------------------------


@contextmanager
def _open_as_flat_dataset(
    file_obj: object,
    spec: dict,
) -> Generator[tuple[xr.Dataset, str, str], None, None]:
    """Open *file_obj* and yield ``(ds, lon_name, lat_name)``.

    The dataset *ds* has lat/lon promoted to xarray coordinates (when
    ``spec["set_coords"]`` is ``True``).

    Parameters
    ----------
    file_obj:
        File path or file-like object to open.
    spec:
        Normalized open_method dict spec (from :func:`_normalize_open_method`).

    Yields
    ------
    tuple[xr.Dataset, str, str]
        ``(ds, lon_name, lat_name)`` where *ds* is a flat dataset with
        lat/lon promoted to coordinates.
    """
    xarray_open = spec.get("xarray_open", "dataset")
    effective_kwargs = _build_effective_open_kwargs(spec.get("open_kwargs", {}))

    if xarray_open == "dataset":
        merge = spec.get("merge")
        if merge is not None:
            # Dataset-based group merge: open each group and keep sources alive
            # while the caller is using the merged dataset.
            if merge == "root":
                group_paths: list[str] = ["/"]
            elif merge == "all":
                group_paths = _get_groups_from_h5py(file_obj)
            elif isinstance(merge, list):
                group_paths = list(merge)
            else:
                raise ValueError(
                    f"spec['merge']={merge!r} is not valid for xarray_open='dataset'. "
                    "Must be 'all', 'root', or a list of group paths."
                )
            opened: list[xr.Dataset] = []
            try:
                for path in group_paths:
                    kwargs = {**effective_kwargs, "group": path}
                    try:
                        with _suppress_dask_progress():
                            ds_grp = xr.open_dataset(file_obj, **kwargs)  # type: ignore[arg-type]
                        if ds_grp.data_vars:
                            opened.append(ds_grp)
                        else:
                            ds_grp.close()
                    except Exception:
                        pass  # Skip unreadable groups silently (mirrors datatree merge behaviour)
                ds_merged = _merge_opened_datasets(opened, spec)
                ds_merged, lon_name, lat_name = _apply_coords(ds_merged, spec)
                yield (ds_merged, lon_name, lat_name)
            finally:
                for ds_src in opened:
                    try:
                        ds_src.close()
                    except Exception:
                        pass
        else:
            ds_simple: xr.Dataset | None = None
            try:
                with _suppress_dask_progress():
                    ds_simple = xr.open_dataset(file_obj, **effective_kwargs)  # type: ignore[arg-type]
                ds_simple, lon_name, lat_name = _apply_coords(ds_simple, spec)
                yield (ds_simple, lon_name, lat_name)
            finally:
                if ds_simple is not None:
                    ds_simple.close()

    elif xarray_open == "datatree":
        dt = _open_datatree_fn(file_obj, effective_kwargs)
        try:
            ds = _merge_datatree_with_spec(dt, spec)
            ds, lon_name, lat_name = _apply_coords(ds, spec)
            yield (ds, lon_name, lat_name)
        finally:
            if hasattr(dt, "close"):
                dt.close()

    elif xarray_open == "auto":
        yield from _open_as_flat_dataset_auto(file_obj, spec, effective_kwargs)

    else:
        raise ValueError(
            f"open_method['xarray_open']={xarray_open!r} is not valid. "
            f"Must be one of {sorted(_VALID_XARRAY_OPEN)}."
        )


def _open_as_flat_dataset_auto(
    file_obj: object,
    spec: dict,
    effective_kwargs: dict,
) -> Generator[tuple[xr.Dataset, str, str], None, None]:
    """Implement the ``"auto"`` open mode.

    Algorithm:

    1. Try ``xr.open_dataset`` (fast path).
    2. Attempt ``coords="auto"`` lat/lon discovery.
    3. If both succeed, yield the dataset.
    4. Otherwise fall back to DataTree merge (using ``merge="all"`` unless
       the user supplied an explicit ``merge`` key in the spec).
    5. If the fallback also fails to identify lat/lon, raise a combined error.
    """
    dataset_exc: BaseException | None = None
    ds_fast: xr.Dataset | None = None
    lon_name_fast: str | None = None
    lat_name_fast: str | None = None

    # --- Fast path: try xr.open_dataset ---
    try:
        with _suppress_dask_progress():
            ds_fast = xr.open_dataset(file_obj, **effective_kwargs)  # type: ignore[arg-type]
        ds_fast, lon_name_fast, lat_name_fast = _apply_coords(ds_fast, spec)
    except Exception as exc:
        dataset_exc = exc
        if ds_fast is not None:
            try:
                ds_fast.close()
            except Exception:
                pass
            ds_fast = None

    if ds_fast is not None:
        # Fast path succeeded.
        try:
            yield (ds_fast, lon_name_fast, lat_name_fast)  # type: ignore[misc]
        finally:
            try:
                ds_fast.close()
            except Exception:
                pass
        return

    # --- Fall back to DataTree ---
    # Attempt to seek back to start of the file object (works for seekable
    # streams; silently ignored for non-seekable objects).
    if hasattr(file_obj, "seek"):
        try:
            file_obj.seek(0)  # type: ignore[attr-defined]
        except Exception:
            pass

    datatree_spec: dict = {
        **spec,
        "xarray_open": "datatree",
        "merge": spec.get("merge", "all"),
        "merge_kwargs": spec.get("merge_kwargs", {}),
    }

    dt = None
    try:
        try:
            dt = _open_datatree_fn(file_obj, effective_kwargs)
        except Exception as dt_open_exc:
            raise ValueError(
                "open_method='auto' failed to open granule as both a flat "
                "dataset and a DataTree.\n"
                f"  Dataset attempt: {dataset_exc!s}\n"
                f"  DataTree attempt: {dt_open_exc!s}\n"
                "Specify open_method='datatree-merge' or a dict spec to "
                "diagnose further."
            ) from None

        ds = _merge_datatree_with_spec(dt, datatree_spec)
        try:
            ds, lon_name, lat_name = _apply_coords(ds, datatree_spec)
        except ValueError as coord_exc:
            raise ValueError(
                "open_method='auto' could not identify lat/lon coordinates "
                "in the granule.\n"
                f"  Dataset path: {dataset_exc!s}\n"
                f"  DataTree path: {coord_exc!s}\n"
                "Fix: specify open_method with explicit coords, e.g.\n"
                "  open_method={'xarray_open': 'datatree', 'merge': 'all', "
                "'coords': {'lat': 'VariableName', 'lon': 'VariableName'}}"
            ) from None

        yield (ds, lon_name, lat_name)
    finally:
        if dt is not None and hasattr(dt, "close"):
            dt.close()
