"""Tests for source adapters."""

from __future__ import annotations

import pytest

from point_collocation.adapters.base import SourceAdapter
from point_collocation.adapters.earthaccess import EarthAccessAdapter
from point_collocation.core.types import SourceProtocol

# ---------------------------------------------------------------------------
# SourceAdapter (abstract base)
# ---------------------------------------------------------------------------

class TestSourceAdapterABC:
    def test_cannot_instantiate_directly(self) -> None:
        with pytest.raises(TypeError):
            SourceAdapter()  # type: ignore[abstract]

    def test_subclass_without_open_dataset_cannot_instantiate(self) -> None:
        class Incomplete(SourceAdapter):
            pass

        with pytest.raises(TypeError):
            Incomplete()  # type: ignore[abstract]

    def test_subclass_with_open_dataset_can_instantiate(self) -> None:
        class Complete(SourceAdapter):
            def open_dataset(self, **kwargs: object) -> object:
                return {}

        adapter = Complete()
        assert isinstance(adapter, SourceAdapter)


# ---------------------------------------------------------------------------
# EarthAccessAdapter
# ---------------------------------------------------------------------------

class TestEarthAccessAdapter:
    def test_instantiates_with_any_source(self) -> None:
        adapter = EarthAccessAdapter(source=object())
        assert isinstance(adapter, EarthAccessAdapter)

    def test_satisfies_source_protocol(self) -> None:
        adapter = EarthAccessAdapter(source=object())
        assert isinstance(adapter, SourceProtocol)

    def test_is_subclass_of_source_adapter(self) -> None:
        assert issubclass(EarthAccessAdapter, SourceAdapter)

    def test_open_dataset_returns_dataset(self, daily_nc_file: str) -> None:
        """open_dataset should return a real xr.Dataset when given a valid path."""
        import xarray as xr

        adapter = EarthAccessAdapter(source=daily_nc_file)
        ds = adapter.open_dataset(engine="netcdf4")
        assert isinstance(ds, xr.Dataset)
        ds.close()
