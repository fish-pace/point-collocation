"""earthaccess adapter.

Wraps the file-like objects returned by ``earthaccess.open()`` so they
satisfy :class:`~point_collocation.core.types.SourceProtocol` and can
be consumed by the core engine without modification.

Usage
-----
::

    import earthaccess
    import point_collocation as pc

    out = pc.matchup(
        df_points,
        data_source="earthaccess",
        source_kwargs={
            "short_name": "PACE_OCI_L3M_RRS",
            "granule_name": "*.DAY.*.4km.*",
        },
        variables=["Rrs"],
    )

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

from point_collocation.adapters.base import SourceAdapter


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
