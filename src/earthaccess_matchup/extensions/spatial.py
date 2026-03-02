"""Spatial averaging / neighbourhood extraction extension.

This extension provides pre-extraction hooks that replace point-exact
lookups with spatial aggregates (e.g., mean over a box, median of the
nearest *N* pixels).

Slot in the extension pipeline
--------------------------------
Pass a ``SpatialAverager`` instance as the ``pre_extract`` argument to
:func:`earthaccess_matchup.matchup` once the hook API is implemented.

Not yet implemented.
"""

from __future__ import annotations


class SpatialAverager:
    """Apply a spatial averaging window around each matchup point.

    Parameters
    ----------
    radius_km:
        Half-width of the extraction box in kilometres.
    method:
        Aggregation method — ``"mean"``, ``"median"``, or ``"nearest"``.
    """

    def __init__(self, radius_km: float = 5.0, method: str = "mean") -> None:
        self.radius_km = radius_km
        self.method = method

    def __call__(self, dataset: object, lat: float, lon: float) -> object:
        """Extract a spatially averaged value from *dataset*.

        Not yet implemented.
        """
        raise NotImplementedError
