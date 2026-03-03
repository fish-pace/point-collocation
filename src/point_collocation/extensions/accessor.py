"""Optional xarray accessor — ``xarray.Dataset.pc``.

Why include an accessor?
------------------------
An xarray accessor lets users work interactively with individual
granules without going through the full matchup pipeline.  This is
useful for exploration and debugging.

Registration
------------
The accessor is **not** registered automatically on import of the
top-level package.  Users must opt in::

    import point_collocation.extensions.accessor  # registers .pc

    ds = xr.open_dataset(...)
    matched = ds.pc.extract_points(df_points, variables=["sst"])

The accessor depends only on ``xarray`` and ``pandas``, both of which
are core dependencies, so no additional optional install is required.

Not yet implemented.
"""

from __future__ import annotations

import xarray as xr


@xr.register_dataset_accessor("pc")
class PointCollocationAccessor:
    """``xarray.Dataset.pc`` accessor for interactive point extraction.

    Attach to any open ``xarray.Dataset`` to extract matchup values
    without assembling a full source list.
    """

    def __init__(self, xarray_obj: xr.Dataset) -> None:
        self._ds = xarray_obj

    def extract_points(
        self,
        points: object,
        variables: list[str],
        *,
        nc_type: str = "flat",
    ) -> object:
        """Extract *variables* at each row of *points*.

        Parameters
        ----------
        points:
            ``DataFrame`` with ``lat``, ``lon``, and ``time`` columns.
        variables:
            Dataset variable names to extract.
        nc_type:
            ``"grouped"`` or ``"flat"`` (see :func:`point_collocation.matchup`).

        Returns
        -------
        pandas.DataFrame
            Same contract as :func:`point_collocation.matchup`.

        Not yet implemented.
        """
        raise NotImplementedError
