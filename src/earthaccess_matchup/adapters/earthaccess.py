"""earthaccess adapter.

Wraps the file-like objects returned by ``earthaccess.open()`` so they
satisfy :class:`~earthaccess_matchup.core.types.SourceProtocol` and can
be consumed by the core engine without modification.

Usage
-----
::

    import earthaccess
    import earthaccess_matchup as eam
    from earthaccess_matchup.adapters.earthaccess import EarthAccessAdapter

    results = earthaccess.search_data(...)
    files = earthaccess.open(results)

    # The top-level eam.matchup() wraps this automatically when it
    # detects earthaccess file-like objects; the adapter can also be
    # used directly for advanced use-cases.
    sources = [EarthAccessAdapter(f) for f in files]
    out = eam.matchup(df_points, sources, variables=["sst"])

Responsibilities
----------------
* Accept a single ``earthaccess``-opened file-like object.
* Open it with ``xarray.open_dataset`` using ``engine="h5netcdf"`` by
  default.
* Return the ``xarray.Dataset`` to the caller; the caller is responsible
  for closing it.
"""

from __future__ import annotations

import xarray as xr

from earthaccess_matchup.adapters.base import SourceAdapter


class EarthAccessAdapter(SourceAdapter):
    """Adapter for ``earthaccess.open()`` file-like objects.

    Parameters
    ----------
    source:
        A single file-like object as returned by ``earthaccess.open()``.
    """

    def __init__(self, source: object) -> None:
        self._source = source

    def open_dataset(self, **kwargs: object) -> xr.Dataset:
        """Open the underlying source with ``xarray.open_dataset``.

        Parameters
        ----------
        **kwargs:
            Forwarded to ``xarray.open_dataset``.  Defaults to
            ``engine="h5netcdf"`` when no ``engine`` key is provided.

        Returns
        -------
        xarray.Dataset
        """
        if "engine" not in kwargs:
            kwargs["engine"] = "h5netcdf"
        return xr.open_dataset(self._source, **kwargs)  # type: ignore[arg-type]
