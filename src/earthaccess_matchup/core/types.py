"""Protocol and type definitions shared across the package.

Keeping types in a dedicated module lets every sub-package import them
without creating circular dependencies.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

import pandas as pd


@runtime_checkable
class SourceProtocol(Protocol):
    """Minimal interface that every source adapter must satisfy.

    The core engine only calls ``open_dataset`` on each source; adapters
    are responsible for translating their underlying object (file-like,
    URL, STAC asset, …) into an ``xarray.Dataset``.
    """

    def open_dataset(self, **kwargs: object) -> object:
        """Return an ``xarray.Dataset`` for this source.

        Parameters
        ----------
        **kwargs:
            Forwarded verbatim to ``xarray.open_dataset``.
        """
        ...  # pragma: no cover


# Convenience type alias used throughout the package.
PointsFrame = pd.DataFrame
"""A ``DataFrame`` that must contain at minimum the columns
``lat``, ``lon``, and ``time``."""
