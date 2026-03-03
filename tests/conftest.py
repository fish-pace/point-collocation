"""Shared pytest fixtures for point_collocation tests."""

from __future__ import annotations

import pathlib

import numpy as np
import pandas as pd
import pytest
import xarray as xr


def _make_l3_dataset(seed: int = 0) -> xr.Dataset:
    """Return a small synthetic L3 flat dataset (1-degree grid)."""
    rng = np.random.default_rng(seed)
    lats = np.arange(-90.0, 91.0, 1.0)
    lons = np.arange(-180.0, 181.0, 1.0)
    sst = rng.uniform(20.0, 30.0, (lats.size, lons.size)).astype(np.float32)
    chlor_a = rng.uniform(0.1, 1.0, (lats.size, lons.size)).astype(np.float32)
    return xr.Dataset(
        {
            "sst": (["lat", "lon"], sst),
            "chlor_a": (["lat", "lon"], chlor_a),
        },
        coords={"lat": lats, "lon": lons},
    )


@pytest.fixture()
def daily_nc_file(tmp_path: pathlib.Path) -> str:
    """Synthetic daily L3 flat NetCDF: AQUA_MODIS.20230601.L3m.DAY.SST…nc"""
    ds = _make_l3_dataset(seed=1)
    path = tmp_path / "AQUA_MODIS.20230601.L3m.DAY.SST.sst.4km.nc"
    ds.to_netcdf(path)
    return str(path)


@pytest.fixture()
def eight_day_nc_file(tmp_path: pathlib.Path) -> str:
    """Synthetic 8-day L3 flat NetCDF: …20230601_20230608.L3m.8D…nc"""
    ds = _make_l3_dataset(seed=2)
    path = tmp_path / "AQUA_MODIS.20230601_20230608.L3m.8D.SST.sst.4km.nc"
    ds.to_netcdf(path)
    return str(path)


@pytest.fixture()
def monthly_nc_file(tmp_path: pathlib.Path) -> str:
    """Synthetic monthly L3 flat NetCDF: …20230601_20230630.L3m.MO…nc"""
    ds = _make_l3_dataset(seed=3)
    path = tmp_path / "AQUA_MODIS.20230601_20230630.L3m.MO.SST.sst.4km.nc"
    ds.to_netcdf(path)
    return str(path)


@pytest.fixture()
def doy_daily_nc_file(tmp_path: pathlib.Path) -> str:
    """Synthetic daily L3 flat NetCDF with DOY filename convention."""
    ds = _make_l3_dataset(seed=4)
    # 2023-06-01 is day-of-year 152
    path = tmp_path / "PACE_OCI_2023152.L3m.DAY.SST.sst.4km.nc"
    ds.to_netcdf(path)
    return str(path)


@pytest.fixture()
def two_day_nc_files(tmp_path: pathlib.Path) -> list[str]:
    """Two consecutive daily files (2023-06-01 and 2023-06-02)."""
    paths = []
    for day, seed in ((1, 10), (2, 11)):
        ds = _make_l3_dataset(seed=seed)
        fname = f"AQUA_MODIS.2023060{day}.L3m.DAY.SST.sst.4km.nc"
        p = tmp_path / fname
        ds.to_netcdf(p)
        paths.append(str(p))
    return paths


@pytest.fixture()
def points_on_day1() -> pd.DataFrame:
    """Two points that fall on 2023-06-01."""
    return pd.DataFrame(
        {
            "lat": [34.0, -10.0],
            "lon": [-120.0, 50.0],
            "time": pd.to_datetime(["2023-06-01", "2023-06-01"]),
        }
    )


@pytest.fixture()
def points_two_days() -> pd.DataFrame:
    """Two points, one per day (2023-06-01 and 2023-06-02)."""
    return pd.DataFrame(
        {
            "lat": [34.0, -10.0],
            "lon": [-120.0, 50.0],
            "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
        }
    )
