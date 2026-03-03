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
# Multi-dimensional variable extraction (e.g. PACE Rrs with wavelength axis)
# ---------------------------------------------------------------------------

class TestMultiDimVariableExtraction:
    """matchup() must expand variables with extra dims into per-coord columns."""

    @pytest.fixture()
    def pace_rrs_nc_file(self, tmp_path: pathlib.Path) -> str:
        """Synthetic PACE-style file with Rrs(lat, lon, wavelength)."""
        import numpy as np

        lats = np.arange(-90.0, 91.0, 1.0)
        lons = np.arange(-180.0, 181.0, 1.0)
        wavelengths = np.array([412, 443, 490], dtype=np.float32)
        rng = np.random.default_rng(42)
        rrs = rng.uniform(
            0.0, 0.05, (lats.size, lons.size, wavelengths.size)
        ).astype(np.float32)
        ds = xr.Dataset(
            {"Rrs": (["lat", "lon", "wavelength"], rrs)},
            coords={"lat": lats, "lon": lons, "wavelength": wavelengths},
        )
        path = tmp_path / "PACE_OCI_2023152.L3m.DAY.RRS.Rrs.4km.nc"
        ds.to_netcdf(path, engine="netcdf4")
        return str(path)

    def test_multidim_var_expands_to_per_wavelength_columns(
        self, pace_rrs_nc_file: str
    ) -> None:
        """Rrs(lat,lon,wavelength) must produce Rrs_412, Rrs_443, Rrs_490."""
        points = pd.DataFrame(
            {
                "lat": [34.0, -10.0],
                "lon": [-120.0, 50.0],
                "time": pd.to_datetime(["2023-06-01", "2023-06-01"]),
            }
        )
        result = matchup(
            points,
            sources=[pace_rrs_nc_file],
            variables=["Rrs"],
            engine="netcdf4",
        )
        assert "Rrs_412" in result.columns
        assert "Rrs_443" in result.columns
        assert "Rrs_490" in result.columns
        # The bare 'Rrs' placeholder column must be dropped.
        assert "Rrs" not in result.columns

    def test_multidim_var_values_match_dataset(
        self, pace_rrs_nc_file: str
    ) -> None:
        """Extracted Rrs values must match a direct xarray nearest-neighbour lookup."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
            }
        )
        result = matchup(
            points,
            sources=[pace_rrs_nc_file],
            variables=["Rrs"],
            engine="netcdf4",
        )
        with xr.open_dataset(pace_rrs_nc_file) as ds:
            selected = ds["Rrs"].sel(lat=34.0, lon=-120.0, method="nearest")
            for wl in [412, 443, 490]:
                expected = float(selected.sel(wavelength=wl))
                assert math.isclose(result.loc[0, f"Rrs_{wl}"], expected, rel_tol=1e-5)

    def test_multidim_and_scalar_vars_coexist(
        self, pace_rrs_nc_file: str, daily_nc_file: str, tmp_path: pathlib.Path
    ) -> None:
        """Requesting both a scalar and multi-dim variable at once must work."""
        # Build a combined dataset (scalar sst + multi-dim Rrs) in one file.
        with (
            xr.open_dataset(daily_nc_file) as sst_ds,
            xr.open_dataset(pace_rrs_nc_file) as rrs_ds,
        ):
            combined = xr.merge([sst_ds, rrs_ds])

        combined_path = str(
            tmp_path / "AQUA_MODIS.20230601.L3m.DAY.combined.nc"
        )
        combined.to_netcdf(combined_path, engine="netcdf4")

        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
            }
        )
        result = matchup(
            points,
            sources=[combined_path],
            variables=["sst", "Rrs"],
            engine="netcdf4",
        )

        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"])
        assert "Rrs_412" in result.columns
        assert "Rrs" not in result.columns


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


# ---------------------------------------------------------------------------
# date column normalisation
# ---------------------------------------------------------------------------

class TestDateColumnNormalisation:
    def test_date_column_accepted_as_time(
        self, daily_nc_file: str
    ) -> None:
        """A 'date' column is treated as a synonym for 'time'."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "date": pd.to_datetime(["2023-06-01"]),
            }
        )
        result = matchup(
            points,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"])

    def test_date_column_renamed_to_time_in_output(
        self, daily_nc_file: str
    ) -> None:
        """When 'date' is renamed to 'time', output contains 'time' column."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "date": pd.to_datetime(["2023-06-01"]),
            }
        )
        result = matchup(
            points,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        assert "time" in result.columns

    def test_time_column_takes_precedence_over_date(
        self, daily_nc_file: str
    ) -> None:
        """When both 'time' and 'date' are present, 'time' is used."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
                "date": pd.to_datetime(["2023-07-01"]),  # different, should be ignored
            }
        )
        result = matchup(
            points,
            sources=[daily_nc_file],
            variables=["sst"],
            engine="netcdf4",
        )
        # Point matches on 2023-06-01 (time), not 2023-07-01 (date)
        assert not math.isnan(result.loc[0, "sst"])


# ---------------------------------------------------------------------------
# Sources / data_source validation
# ---------------------------------------------------------------------------

class TestSourcesValidation:
    def test_raises_when_neither_sources_nor_data_source(self) -> None:
        """ValueError when neither sources nor data_source is provided."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
            }
        )
        with pytest.raises(ValueError, match="sources.*data_source|data_source.*sources"):
            matchup(points, variables=["sst"])

    def test_raises_when_both_sources_and_data_source(self) -> None:
        """ValueError when both sources and data_source are provided."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
            }
        )
        with pytest.raises(ValueError, match="not both"):
            matchup(
                points,
                sources=[],
                variables=["sst"],
                data_source="earthaccess",
            )

    def test_raises_on_unknown_data_source(self) -> None:
        """ValueError for unsupported data_source values."""
        points = pd.DataFrame(
            {
                "lat": [34.0],
                "lon": [-120.0],
                "time": pd.to_datetime(["2023-06-01"]),
            }
        )
        with pytest.raises(ValueError, match="Unknown data_source"):
            matchup(points, variables=["sst"], data_source="s3")


# ---------------------------------------------------------------------------
# earthaccess data_source integration (mocked)
# ---------------------------------------------------------------------------

_FIXTURES_CSV = (
    pathlib.Path(__file__).parent.parent / "examples" / "fixtures" / "points.csv"
)


class TestMatchupWithEarthaccessDataSource:
    """Tests for matchup() using data_source='earthaccess' (mocked)."""

    def _make_fake_ea_file(
        self, tmp_path: pathlib.Path, date_str: str, seed: int = 0
    ) -> object:
        """Return a fake earthaccess file-like object backed by a real NetCDF."""
        import numpy as np

        lats = np.arange(-90.0, 91.0, 1.0)
        lons = np.arange(-180.0, 181.0, 1.0)
        rng = np.random.default_rng(seed)
        rrs = rng.uniform(0.0, 0.05, (lats.size, lons.size)).astype(np.float32)
        ds = xr.Dataset(
            {"Rrs": (["lat", "lon"], rrs)},
            coords={"lat": lats, "lon": lons},
        )
        # Use PACE-style DOY filename so parse_temporal_range works
        import datetime
        date = datetime.date.fromisoformat(date_str)
        doy = date.timetuple().tm_yday
        fname = f"PACE_OCI_{date.year}{doy:03d}.L3m.DAY.RRS.Rrs.4km.nc"
        path = tmp_path / fname
        ds.to_netcdf(path, engine="netcdf4")

        class _FakeEAFile:
            """Mimics the path attribute of an earthaccess file object."""
            def __init__(self, p: str) -> None:
                self.path = p

        return _FakeEAFile(str(path))

    def _mock_earthaccess(self, search_return: object, open_return: object) -> object:
        """Return a MagicMock that acts as the earthaccess module."""
        from unittest.mock import MagicMock
        ea = MagicMock()
        ea.search_data.return_value = search_return
        ea.open.return_value = open_return
        return ea

    def test_matchup_with_mocked_earthaccess(
        self, tmp_path: pathlib.Path
    ) -> None:
        """matchup() with data_source='earthaccess' calls search_data per date."""
        import sys
        from unittest.mock import MagicMock

        date = "2024-06-13"
        fake_file = self._make_fake_ea_file(tmp_path, date, seed=1)
        fake_results = [MagicMock()]
        mock_ea = self._mock_earthaccess(fake_results, [fake_file])

        points = pd.DataFrame(
            {
                "lat": [27.3835, 27.119],
                "lon": [-82.7375, -82.7125],
                "time": pd.to_datetime([date, date]),
            }
        )

        sys.modules["earthaccess"] = mock_ea  # type: ignore[assignment]
        try:
            result = matchup(
                points,
                data_source="earthaccess",
                short_name="PACE_OCI_L3M_RRS",
                granule_name="*.DAY.*.4km.*",
                variables=["Rrs"],
                engine="netcdf4",
            )
        finally:
            sys.modules.pop("earthaccess", None)

        mock_ea.search_data.assert_called_once_with(
            short_name="PACE_OCI_L3M_RRS",
            temporal=(date, date),
            granule_name="*.DAY.*.4km.*",
        )
        assert "Rrs" in result.columns
        assert len(result) == len(points)

    def test_matchup_result_order_matches_input(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Results are returned in the same row order as the input points."""
        import sys
        from unittest.mock import MagicMock

        # Two different dates so we get different granules
        dates = ["2024-06-13", "2024-06-14"]
        fake_files = [
            self._make_fake_ea_file(tmp_path, d, seed=i)
            for i, d in enumerate(dates)
        ]

        points = pd.DataFrame(
            {
                "lat": [27.3835, 27.119, 26.9435],
                "lon": [-82.7375, -82.7125, -82.817],
                "time": pd.to_datetime([dates[0], dates[1], dates[1]]),
                "station_id": ["S1", "S2", "S3"],
            }
        )

        call_count = [0]

        def fake_search(**kwargs: object) -> list[object]:
            i = call_count[0]
            call_count[0] += 1
            return [MagicMock()]

        def fake_open(results: object) -> list[object]:
            idx = min(call_count[0] - 1, len(fake_files) - 1)
            return [fake_files[idx]]

        mock_ea = MagicMock()
        mock_ea.search_data.side_effect = fake_search
        mock_ea.open.side_effect = fake_open

        sys.modules["earthaccess"] = mock_ea  # type: ignore[assignment]
        try:
            result = matchup(
                points,
                data_source="earthaccess",
                short_name="PACE_OCI_L3M_RRS",
                granule_name="*.DAY.*.4km.*",
                variables=["Rrs"],
                engine="netcdf4",
            )
        finally:
            sys.modules.pop("earthaccess", None)

        # Row count and index order are preserved
        assert len(result) == 3
        assert list(result.index) == [0, 1, 2]
        assert list(result["station_id"]) == ["S1", "S2", "S3"]

    def test_matchup_with_points_csv_date_column(
        self, tmp_path: pathlib.Path
    ) -> None:
        """Loads fixtures/points.csv (which has a 'date' column) and runs matchup."""
        import sys
        from unittest.mock import MagicMock

        assert _FIXTURES_CSV.exists(), f"Fixture not found: {_FIXTURES_CSV}"

        # Load a small slice of the CSV (first 3 rows, all on 2024-06-13)
        df = pd.read_csv(_FIXTURES_CSV, nrows=3)
        assert "date" in df.columns, "Expected 'date' column in points.csv"

        date_str = df["date"].iloc[0]
        fake_file = self._make_fake_ea_file(tmp_path, date_str, seed=99)
        fake_results = [MagicMock()]
        mock_ea = self._mock_earthaccess(fake_results, [fake_file])

        sys.modules["earthaccess"] = mock_ea  # type: ignore[assignment]
        try:
            result = matchup(
                df,
                data_source="earthaccess",
                short_name="PACE_OCI_L3M_RRS",
                granule_name="*.DAY.*.4km.*",
                variables=["Rrs"],
                engine="netcdf4",
            )
        finally:
            sys.modules.pop("earthaccess", None)

        assert "Rrs" in result.columns
        # Result has same number of rows as input and preserves order
        assert len(result) == len(df)
        assert list(result.index) == list(df.index)

    def test_no_search_when_no_results(
        self, tmp_path: pathlib.Path
    ) -> None:
        """earthaccess.open() is not called when search returns no results."""
        import sys
        from unittest.mock import MagicMock

        mock_ea = MagicMock()
        mock_ea.search_data.return_value = []

        points = pd.DataFrame(
            {
                "lat": [27.3835],
                "lon": [-82.7375],
                "time": pd.to_datetime(["2024-06-13"]),
            }
        )

        sys.modules["earthaccess"] = mock_ea  # type: ignore[assignment]
        try:
            result = matchup(
                points,
                data_source="earthaccess",
                short_name="PACE_OCI_L3M_RRS",
                variables=["Rrs"],
            )
        finally:
            sys.modules.pop("earthaccess", None)

        mock_ea.search_data.assert_called_once()
        mock_ea.open.assert_not_called()
        assert math.isnan(result.loc[0, "Rrs"])
