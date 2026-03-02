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
* Detect whether the underlying file is grouped or flat NetCDF.
* Open it with ``xarray.open_dataset`` (engine auto-detected).
* Close the dataset immediately after extraction to avoid file-handle
  leaks (the engine controls this via a context-manager protocol).
"""

from __future__ import annotations

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

    def open_dataset(self, **kwargs: object) -> object:
        """Open the underlying source with ``xarray.open_dataset``.

        Parameters
        ----------
        **kwargs:
            Forwarded to ``xarray.open_dataset``.

        Returns
        -------
        xarray.Dataset
        """
        # TODO: implement — call xarray.open_dataset(self._source, **kwargs)
        raise NotImplementedError
