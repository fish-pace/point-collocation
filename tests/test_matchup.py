"""Integration tests for the matchup engine using synthetic NetCDF data."""

from __future__ import annotations

import math
import pathlib

import pandas as pd
import pytest
import xarray as xr

from earthaccess_matchup.core._granule import get_source_id, parse_temporal_range
from earthaccess_matchup.core.engine import matchup

# ---------------------------------------------------------------------------
# Tests for get_source_id
# ---------------------------------------------------------------------------

class TestGetSourceId:
    def test_string_path(self) -> None:
        assert get_source_id("/data/AQUA_MODIS.20230601.nc") == "AQUA_MODIS.20230601.nc"

    def test_basename_string(self) -> None:
        assert get_source_id("AQUA_MODIS.20230601.nc") == "AQUA_MODIS.20230601.nc"

    def test_pathlib_path(self, tmp_path: pathlib.Path) -> None:
        p = pathlib.Path("/some/path/file.nc")
        assert get_source_id(p) == "file.nc"

    def test_object_with_path_attr(self) -> None:
        class FakeFSFile:
            path = "/s3/bucket/AQUA_MODIS.20230601.nc"

        assert get_source_id(FakeFSFile()) == "AQUA_MODIS.20230601.nc"

    def test_object_with_name_attr(self) -> None:
        class FakeFSFile:
            name = "AQUA_MODIS.20230601.nc"

        assert get_source_id(FakeFSFile()) == "AQUA_MODIS.20230601.nc"

    def test_fallback_to_str(self) -> None:
        result = get_source_id(42)
        assert result == "42"


# ---------------------------------------------------------------------------
# Tests for parse_temporal_range
# ---------------------------------------------------------------------------

class TestParseTemporalRange:
    def test_daily_calendar_format(self) -> None:
        start, end = parse_temporal_range("AQUA_MODIS.20230601.L3m.DAY.SST.sst.4km.nc")
        assert start == pd.Timestamp("2023-06-01")
        assert end == pd.Timestamp("2023-06-01")

    def test_daily_doy_format(self) -> None:
        # 2024-DOY-070 = 2024-03-10
        start, end = parse_temporal_range("PACE_OCI_2024070.L3m.DAY.RRS.Rrs_412.4km.nc")
        assert start == pd.Timestamp("2024-03-10")
        assert end == pd.Timestamp("2024-03-10")

    def test_eight_day_calendar_pair(self) -> None:
        start, end = parse_temporal_range(
            "AQUA_MODIS.20230601_20230608.L3m.8D.SST.sst.4km.nc"
        )
        assert start == pd.Timestamp("2023-06-01")
        assert end == pd.Timestamp("2023-06-08")

    def test_eight_day_doy_pair(self) -> None:
        # DOY 049 = 2024-02-18, DOY 056 = 2024-02-25
        start, end = parse_temporal_range(
            "PACE_OCI_2024049_2024056.L3m.8D.CHL.chlor_a.9km.nc"
        )
        assert start == pd.Timestamp("2024-02-18")
        assert end == pd.Timestamp("2024-02-25")

    def test_monthly_calendar_pair(self) -> None:
        start, end = parse_temporal_range(
            "AQUA_MODIS.20230601_20230630.L3m.MO.CHL.chlor_a.9km.nc"
        )
        assert start == pd.Timestamp("2023-06-01")
        assert end == pd.Timestamp("2023-06-30")

    def test_single_doy_with_8d_token(self) -> None:
        # DOY 049 of 2024 = 2024-02-18; +7 days = 2024-02-25
        start, end = parse_temporal_range("PRODUCT_2024049.L3m.8D.CHL.nc")
        assert start == pd.Timestamp("2024-02-18")
        assert end == pd.Timestamp("2024-02-25")

    def test_single_doy_with_mo_token(self) -> None:
        start, end = parse_temporal_range("PRODUCT_2024032.L3m.MO.CHL.nc")
        # DOY 032 = 2024-02-01; end of Feb 2024 = 29 (leap year)
        assert start == pd.Timestamp("2024-02-01")
        assert end == pd.Timestamp("2024-02-29")

    def test_raises_on_unrecognised_name(self) -> None:
        with pytest.raises(ValueError, match="Cannot parse"):
            parse_temporal_range("nodate_here.nc")

    def test_basename_extracted_from_full_path(self) -> None:
        start, end = parse_temporal_range(
            "/some/long/path/AQUA_MODIS.20230601.L3m.DAY.SST.sst.4km.nc"
        )
        assert start == pd.Timestamp("2023-06-01")
        assert end == pd.Timestamp("2023-06-01")


# ---------------------------------------------------------------------------
# Matchup engine integration tests
# ---------------------------------------------------------------------------

class TestMatchupWithRealFiles:
    def test_extracts_values_from_daily_file(
        self, daily_nc_file: str, points_on_day1: pd.DataFrame
    ) -> None:
        """Values must be extracted and not NaN when points overlap the file."""
        result = matchup(
            points_on_day1,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        assert "sst" in result.columns
        assert not result["sst"].isna().all(), "Expected at least one non-NaN value"

    def test_extracted_values_match_dataset(
        self, daily_nc_file: str, points_on_day1: pd.DataFrame
    ) -> None:
        """Extracted values must match a direct nearest-neighbour xarray lookup."""
        result = matchup(
            points_on_day1,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        with xr.open_dataset(daily_nc_file) as ds:
            for i, row in points_on_day1.iterrows():
                expected = ds["sst"].sel(
                    lat=row["lat"], lon=row["lon"], method="nearest"
                ).item()
                assert math.isclose(result.loc[i, "sst"], expected, rel_tol=1e-5)

    def test_two_variables_extracted(
        self, daily_nc_file: str, points_on_day1: pd.DataFrame
    ) -> None:
        result = matchup(
            points_on_day1,
            sources=[daily_nc_file],
            variables=["sst", "chlor_a"],
            engine="netcdf4",
        )
        assert "sst" in result.columns
        assert "chlor_a" in result.columns
        assert not result["sst"].isna().all()
        assert not result["chlor_a"].isna().all()

    def test_points_outside_temporal_coverage_remain_nan(
        self, daily_nc_file: str
    ) -> None:
        """Points on a different day get NaN because no source covers them."""
        points_other_day = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-07-15"]),  # different day
            }
        )
        result = matchup(
            points_other_day,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        assert math.isnan(result.loc[0, "sst"])

    def test_missing_variable_leaves_nan(
        self, daily_nc_file: str, points_on_day1: pd.DataFrame
    ) -> None:
        """Requesting a variable not in the dataset results in NaN."""
        result = matchup(
            points_on_day1,
            sources=[daily_nc_file],
            variables=["nonexistent_var"],
            engine="netcdf4",
        )
        assert result["nonexistent_var"].isna().all()

    def test_multiple_files_temporal_routing(
        self, two_day_nc_files: list[str], points_two_days: pd.DataFrame
    ) -> None:
        """Each point is matched to the correct daily file."""
        result = matchup(
            points_two_days,
            sources=two_day_nc_files,
            variables=["sst"],
            engine="netcdf4",
        )
        assert not result["sst"].isna().any(), "Both points should be matched"

        # The two files have different data (different seeds), so the extracted
        # values should differ for points with the same lat/lon on different days.
        assert result.loc[0, "sst"] != result.loc[1, "sst"]

    def test_eight_day_file_matches_points_in_range(
        self, eight_day_nc_file: str
    ) -> None:
        """Points within an 8-day composite window are matched."""
        points = pd.DataFrame(
            {
                "lat": [10.0, 10.0],
                "lon": [20.0, 20.0],
                "time": pd.to_datetime(["2023-06-03", "2023-06-07"]),
            }
        )
        result = matchup(
            points,
            sources=[eight_day_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        assert not result["sst"].isna().any()

    def test_return_diagnostics(
        self, daily_nc_file: str, points_on_day1: pd.DataFrame
    ) -> None:
        out = matchup(
            points_on_day1,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
            return_diagnostics=True,
        )
        assert isinstance(out, tuple)
        df, report = out
        assert isinstance(df, pd.DataFrame)
        assert report.total == 1
        assert report.succeeded == 1
        assert "sst" in report.granules[0].variables_found

    def test_diagnostics_record_missing_variable(
        self, daily_nc_file: str, points_on_day1: pd.DataFrame
    ) -> None:
        _, report = matchup(
            points_on_day1,
            sources=[daily_nc_file],
            variables=["nope"],
            engine="netcdf4",
            return_diagnostics=True,
        )
        assert "nope" in report.granules[0].variables_missing

    def test_bad_file_recorded_as_error(
        self, tmp_path: pathlib.Path, points_on_day1: pd.DataFrame
    ) -> None:
        """An unreadable file is skipped and recorded with an error."""
        bad = str(tmp_path / "AQUA_MODIS.20230601.L3m.DAY.SST.sst.4km.nc")
        # Write garbage bytes so xarray fails to open it
        with open(bad, "wb") as f:
            f.write(b"not a netcdf file")
        _, report = matchup(
            points_on_day1,
            sources=[bad],
            variables=["sst"],
            engine="netcdf4",
            return_diagnostics=True,
        )
        assert report.skipped == 1
        assert report.granules[0].error is not None

    def test_original_columns_preserved(
        self, daily_nc_file: str
    ) -> None:
        """Extra columns in points are preserved in the output."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
                "station_id": ["STA001"],
            }
        )
        result = matchup(
            points,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        assert "station_id" in result.columns
        assert result.loc[0, "station_id"] == "STA001"

    def test_doy_filename_convention(
        self, doy_daily_nc_file: str
    ) -> None:
        """DOY-style filenames are parsed and points are matched correctly."""
        # 2023-06-01 is DOY 152
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
            }
        )
        result = matchup(
            points,
            sources=[doy_daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        assert not math.isnan(result.loc[0, "sst"])


# ---------------------------------------------------------------------------
# EarthAccessAdapter integration
# ---------------------------------------------------------------------------

class TestEarthAccessAdapterIntegration:
    def test_adapter_open_dataset_returns_dataset(
        self, daily_nc_file: str
    ) -> None:
        from earthaccess_matchup.adapters.earthaccess import EarthAccessAdapter

        adapter = EarthAccessAdapter(source=daily_nc_file)
        ds = adapter.open_dataset(engine="netcdf4")
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds
        ds.close()

    def test_adapter_used_as_source_in_matchup(
        self, daily_nc_file: str, points_on_day1: pd.DataFrame
    ) -> None:
        from earthaccess_matchup.adapters.earthaccess import EarthAccessAdapter

        adapter = EarthAccessAdapter(source=daily_nc_file)
        # Manually give the adapter an identifiable path attribute
        adapter._source = daily_nc_file  # path string → get_source_id works
        result = matchup(
            points_on_day1,
            sources=[adapter],
            variables=["sst"],
            engine="netcdf4",
        )
        assert not result["sst"].isna().all()
