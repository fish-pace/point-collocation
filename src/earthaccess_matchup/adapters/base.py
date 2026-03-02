"""Base class for all source adapters.

A source adapter wraps a heterogeneous input — a file-like object,
a URL, a STAC asset — and exposes the uniform
:class:`~earthaccess_matchup.core.types.SourceProtocol` interface that
the core engine consumes.

Subclasses must implement :meth:`open_dataset`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod


class SourceAdapter(ABC):
    """Abstract base for source adapters.

    Subclass this to add support for a new data source.  The core
    engine only calls :meth:`open_dataset`; everything else is internal
    to the adapter.
    """

    @abstractmethod
    def open_dataset(self, **kwargs: object) -> object:
        """Return an ``xarray.Dataset`` for this source.

        Parameters
        ----------
        **kwargs:
            Forwarded verbatim to ``xarray.open_dataset``.
        """
        raise NotImplementedError  # pragma: no cover
