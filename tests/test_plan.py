"""Tests for pc.plan() and pc.matchup(plan) plan-based execution."""

from __future__ import annotations

import datetime
import math
import pathlib
from typing import Any
from unittest.mock import MagicMock, call, patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

import point_collocation as pc
from point_collocation.core.plan import (
    GranuleMeta,
    Plan,
    _extract_granule_meta,
    _get_bbox,
    _get_data_url,
    _get_umm,
    _match_points_to_granules,
    _parse_time_buffer,
    _plan_normalise_time,
    plan,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_result(
    *,
    begin: str,
    end: str,
    bbox: tuple[float, float, float, float] | None = None,
    polygon: list[dict[str, float]] | None = None,
    data_url: str = "https://example.com/granule.nc",
) -> dict:
    """Build a minimal mock earthaccess result dict (dict-based format).

    Supports BoundingRectangles or GPolygons spatial extent.
    """
    if bbox is not None:
        west, south, east, north = bbox
        spatial: dict = {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "BoundingRectangles": [
                        {
                            "WestBoundingCoordinate": west,
                            "SouthBoundingCoordinate": south,
                            "EastBoundingCoordinate": east,
                            "NorthBoundingCoordinate": north,
                        }
                    ]
                }
            }
        }
    elif polygon is not None:
        spatial = {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "GPolygons": [
                        {"Boundary": {"Points": polygon}}
                    ]
                }
            }
        }
    else:
        pytest.fail("Either bbox or polygon must be provided")

    return {
        "umm": {
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": begin,
                    "EndingDateTime": end,
                }
            },
            "SpatialExtent": spatial,
            "RelatedUrls": [
                {"Type": "GET DATA", "URL": data_url},
            ],
        }
    }


def _make_global_result(begin: str, end: str, data_url: str = "https://example.com/g.nc") -> dict:
    """Convenience: result with global bounding box."""
    return _make_result(begin=begin, end=end, bbox=(-180, -90, 180, 90), data_url=data_url)


@pytest.fixture()
def global_granule_meta() -> GranuleMeta:
    return GranuleMeta(
        granule_id="https://example.com/g.nc",
        begin=pd.Timestamp("2023-06-01T00:00:00Z"),
        end=pd.Timestamp("2023-06-01T23:59:59Z"),
        bbox=(-180.0, -90.0, 180.0, 90.0),
        result_index=0,
    )


@pytest.fixture()
def simple_points() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "lat": [34.0, -10.0, 60.0],
            "lon": [-120.0, 50.0, 10.0],
            "time": pd.to_datetime(
                ["2023-06-01T12:00:00", "2023-06-01T12:00:00", "2023-07-01T12:00:00"]
            ),
        }
    )


# ---------------------------------------------------------------------------
# _parse_time_buffer
# ---------------------------------------------------------------------------

class TestParseTimeBuffer:
    def test_zero_int(self) -> None:
        assert _parse_time_buffer(0) == pd.Timedelta(0)

    def test_zero_string(self) -> None:
        assert _parse_time_buffer("0h") == pd.Timedelta(0)

    def test_hours_string(self) -> None:
        assert _parse_time_buffer("12H") == pd.Timedelta(hours=12)

    def test_minutes_string(self) -> None:
        assert _parse_time_buffer("30min") == pd.Timedelta(minutes=30)

    def test_timedelta(self) -> None:
        assert _parse_time_buffer(datetime.timedelta(hours=3)) == pd.Timedelta(hours=3)

    def test_pandas_timedelta(self) -> None:
        assert _parse_time_buffer(pd.Timedelta("6h")) == pd.Timedelta(hours=6)


# ---------------------------------------------------------------------------
# _plan_normalise_time
# ---------------------------------------------------------------------------

class TestPlanNormaliseTime:
    def test_time_column_preserved(self) -> None:
        pts = pd.DataFrame({"lat": [1.0], "lon": [2.0], "time": pd.to_datetime(["2023-06-01T10:00:00"])})
        out = _plan_normalise_time(pts)
        assert "time" in out.columns
        # Existing time-of-day preserved
        assert out.loc[0, "time"].hour == 10

    def test_date_column_renamed_to_time(self) -> None:
        pts = pd.DataFrame({"lat": [1.0], "lon": [2.0], "date": pd.to_datetime(["2023-06-01"])})
        out = _plan_normalise_time(pts)
        assert "time" in out.columns
        assert "date" not in out.columns

    def test_date_column_gets_noon(self) -> None:
        """When input is named 'date', time-of-day is set to noon."""
        pts = pd.DataFrame({"lat": [1.0], "lon": [2.0], "date": pd.to_datetime(["2023-06-01"])})
        out = _plan_normalise_time(pts)
        assert out.loc[0, "time"].hour == 12
        assert out.loc[0, "time"].minute == 0

    def test_time_column_not_set_to_noon(self) -> None:
        """When input is already named 'time', midnight is kept."""
        pts = pd.DataFrame({"lat": [1.0], "lon": [2.0], "time": pd.to_datetime(["2023-06-01"])})
        out = _plan_normalise_time(pts)
        assert out.loc[0, "time"].hour == 0  # midnight preserved

    def test_no_time_or_date_returns_unchanged(self) -> None:
        pts = pd.DataFrame({"lat": [1.0], "lon": [2.0], "x": [99]})
        out = _plan_normalise_time(pts)
        assert "time" not in out.columns  # validation will catch this later


# ---------------------------------------------------------------------------
# UMM metadata extraction helpers
# ---------------------------------------------------------------------------

class TestGetUmm:
    def test_standard_format(self) -> None:
        result = {"umm": {"TemporalExtent": {}}, "meta": {}}
        assert _get_umm(result) == {"TemporalExtent": {}}

    def test_fixture_format(self) -> None:
        result = {"render_dict": {"umm": {"TemporalExtent": {}}, "meta": {}}}
        assert _get_umm(result) == {"TemporalExtent": {}}

    def test_raises_on_unknown(self) -> None:
        with pytest.raises(ValueError, match="Cannot extract UMM"):
            _get_umm({"foo": "bar"})


class TestGetDataUrl:
    def test_prefers_https(self) -> None:
        umm = {
            "RelatedUrls": [
                {"Type": "GET DATA VIA DIRECT ACCESS", "URL": "s3://bucket/file.nc"},
                {"Type": "GET DATA", "URL": "https://example.com/file.nc"},
            ]
        }
        assert _get_data_url(umm) == "https://example.com/file.nc"

    def test_falls_back_to_s3(self) -> None:
        umm = {
            "RelatedUrls": [
                {"Type": "GET DATA VIA DIRECT ACCESS", "URL": "s3://bucket/file.nc"},
                {"Type": "GET DATA", "URL": "s3://bucket/file.nc"},
            ]
        }
        assert _get_data_url(umm).startswith("s3://")

    def test_raises_when_no_get_data(self) -> None:
        umm = {"RelatedUrls": [{"Type": "OTHER", "URL": "https://x.com/"}]}
        with pytest.raises(ValueError):
            _get_data_url(umm)


class TestGetBbox:
    def test_bounding_rectangles(self) -> None:
        umm = {
            "SpatialExtent": {
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": -180,
                                "SouthBoundingCoordinate": -90,
                                "EastBoundingCoordinate": 180,
                                "NorthBoundingCoordinate": 90,
                            }
                        ]
                    }
                }
            }
        }
        assert _get_bbox(umm) == (-180.0, -90.0, 180.0, 90.0)

    def test_gpolygons_computes_bbox(self) -> None:
        umm = {
            "SpatialExtent": {
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "GPolygons": [
                            {
                                "Boundary": {
                                    "Points": [
                                        {"Longitude": -50, "Latitude": 30},
                                        {"Longitude": -40, "Latitude": 50},
                                        {"Longitude": -30, "Latitude": 30},
                                        {"Longitude": -50, "Latitude": 30},
                                    ]
                                }
                            }
                        ]
                    }
                }
            }
        }
        west, south, east, north = _get_bbox(umm)
        assert west == -50.0
        assert south == 30.0
        assert east == -30.0
        assert north == 50.0

    def test_raises_when_no_geometry(self) -> None:
        umm: dict = {"SpatialExtent": {}}
        with pytest.raises(ValueError):
            _get_bbox(umm)


# ---------------------------------------------------------------------------
# _match_points_to_granules
# ---------------------------------------------------------------------------

class TestMatchPointsToGranules:
    def _make_granule(
        self,
        begin: str,
        end: str,
        bbox: tuple[float, float, float, float] = (-180.0, -90.0, 180.0, 90.0),
        result_index: int = 0,
        data_url: str = "https://example.com/g.nc",
    ) -> GranuleMeta:
        return GranuleMeta(
            granule_id=data_url,
            begin=pd.Timestamp(begin),
            end=pd.Timestamp(end),
            bbox=bbox,
            result_index=result_index,
        )

    def test_zero_match(self) -> None:
        """A point outside all granules' temporal coverage → empty list."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2020-01-01T12:00:00"])}
        )
        granules = [self._make_granule("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")]
        mapping = _match_points_to_granules(pts, granules, pd.Timedelta(0))
        assert mapping[0] == []

    def test_one_match(self) -> None:
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        granules = [self._make_granule("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")]
        mapping = _match_points_to_granules(pts, granules, pd.Timedelta(0))
        assert mapping[0] == [0]

    def test_multi_match(self) -> None:
        """A point that falls within two granules' coverage."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        granules = [
            self._make_granule(
                "2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z",
                result_index=0, data_url="https://x.com/a.nc"
            ),
            self._make_granule(
                "2023-06-01T06:00:00Z", "2023-06-01T18:00:00Z",
                result_index=1, data_url="https://x.com/b.nc"
            ),
        ]
        mapping = _match_points_to_granules(pts, granules, pd.Timedelta(0))
        assert set(mapping[0]) == {0, 1}

    def test_spatial_exclusion(self) -> None:
        """A point outside the granule bbox is excluded."""
        # Granule only covers the northern hemisphere
        pts = pd.DataFrame(
            {"lat": [-45.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        granules = [
            self._make_granule(
                "2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z",
                bbox=(-180.0, 0.0, 180.0, 90.0),  # northern only
            )
        ]
        mapping = _match_points_to_granules(pts, granules, pd.Timedelta(0))
        assert mapping[0] == []

    def test_time_buffer_expands_match(self) -> None:
        """A point just outside the temporal window matches with a buffer."""
        # Point is 2 hours after end of granule
        t_point = pd.Timestamp("2023-06-01T14:00:00")
        t_end = pd.Timestamp("2023-06-01T12:00:00Z")
        pts = pd.DataFrame({"lat": [0.0], "lon": [0.0], "time": [t_point]})
        granules = [
            self._make_granule("2023-06-01T00:00:00Z", "2023-06-01T12:00:00Z")
        ]
        # No buffer → no match
        assert _match_points_to_granules(pts, granules, pd.Timedelta(0))[0] == []
        # 2-hour buffer → matches
        assert _match_points_to_granules(pts, granules, pd.Timedelta(hours=2))[0] == [0]

    def test_time_buffer_edge_condition(self) -> None:
        """Point exactly at boundary with buffer=0 still matches."""
        t_end = pd.Timestamp("2023-06-01T12:00:00Z")
        pts = pd.DataFrame({"lat": [0.0], "lon": [0.0], "time": [t_end]})
        granules = [self._make_granule("2023-06-01T00:00:00Z", "2023-06-01T12:00:00Z")]
        assert _match_points_to_granules(pts, granules, pd.Timedelta(0))[0] == [0]


# ---------------------------------------------------------------------------
# pc.plan() — public API
# ---------------------------------------------------------------------------

class TestPlanPublicApi:
    def test_plan_importable_from_top_level(self) -> None:
        assert callable(pc.plan)

    def test_plan_in_all(self) -> None:
        assert "plan" in pc.__all__

    def test_plan_class_importable(self) -> None:
        assert pc.Plan is Plan

    def test_plan_returns_plan_instance(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = [
            _make_global_result("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")
        ]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        result = plan(pts, source_kwargs={"short_name": "TEST"}, variables=["sst"])
        assert isinstance(result, Plan)

    def test_plan_raises_on_unknown_data_source(self) -> None:
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        with pytest.raises(ValueError, match="Unknown data_source"):
            plan(pts, data_source="stac", source_kwargs={}, variables=["sst"])

    def test_plan_raises_when_neither_time_nor_date(self) -> None:
        pts = pd.DataFrame({"lat": [0.0], "lon": [0.0], "x": [1]})
        with pytest.raises(ValueError, match="time"):
            plan(pts, source_kwargs={"short_name": "TEST"}, variables=["sst"])

    def test_plan_raises_without_short_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_ea = MagicMock()
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        with pytest.raises(ValueError, match="short_name"):
            plan(pts, source_kwargs={}, variables=["sst"])


class TestPlanMapping:
    """Tests for the point→granule mapping built by pc.plan()."""

    def _run_plan(
        self,
        monkeypatch: pytest.MonkeyPatch,
        points: pd.DataFrame,
        fake_results: list[dict],
        variables: list[str] | None = None,
        time_buffer: str = "0h",
    ) -> Plan:
        """Helper: run pc.plan() with mocked earthaccess.search_data."""
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = fake_results
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        return plan(
            points,
            source_kwargs={"short_name": "TEST"},
            variables=variables or ["sst"],
            time_buffer=time_buffer,
        )

    def test_zero_match_point(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A point with no matching granule → empty list in mapping."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2020-01-01T12:00:00"])}
        )
        results = [_make_global_result("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")]
        p = self._run_plan(monkeypatch, pts, results)
        assert p.point_granule_map[0] == []

    def test_one_match_point(self, monkeypatch: pytest.MonkeyPatch) -> None:
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        results = [_make_global_result("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")]
        p = self._run_plan(monkeypatch, pts, results)
        assert p.point_granule_map[0] == [0]

    def test_multi_match_point(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A point that matches two granules."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        results = [
            _make_global_result(
                "2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z",
                data_url="https://example.com/a.nc"
            ),
            _make_global_result(
                "2023-06-01T06:00:00Z", "2023-06-01T18:00:00Z",
                data_url="https://example.com/b.nc"
            ),
        ]
        p = self._run_plan(monkeypatch, pts, results)
        assert set(p.point_granule_map[0]) == {0, 1}

    def test_mixed_match_scenario(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """3 points: 0-match, 1-match, 2-match."""
        pts = pd.DataFrame(
            {
                "lat": [0.0, 0.0, 0.0],
                "lon": [0.0, 0.0, 0.0],
                "time": pd.to_datetime(
                    [
                        "2020-01-01T12:00:00",  # no match
                        "2023-06-01T14:00:00",  # 1 match (only granule 0, after granule 1 end)
                        "2023-06-01T10:00:00",  # 2 matches (both granules cover it)
                    ]
                ),
            }
        )
        results = [
            _make_global_result(
                "2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z",
                data_url="https://example.com/a.nc"
            ),
            _make_global_result(
                "2023-06-01T08:00:00Z", "2023-06-01T11:59:59Z",  # ends before 14:00
                data_url="https://example.com/b.nc"
            ),
        ]
        p = self._run_plan(monkeypatch, pts, results)
        assert p.point_granule_map[0] == []       # no match
        assert p.point_granule_map[1] == [0]      # only granule 0 covers 14:00
        assert set(p.point_granule_map[2]) == {0, 1}  # both cover 10:00

    def test_time_buffer_applied(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """time_buffer extends temporal matching."""
        # Point is 30 minutes after granule end
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:30:00"])}
        )
        results = [_make_global_result("2023-06-01T00:00:00Z", "2023-06-01T12:00:00Z")]

        # No buffer → no match
        p_no_buf = self._run_plan(monkeypatch, pts, results, time_buffer="0h")
        assert p_no_buf.point_granule_map[0] == []

        # 1-hour buffer → matches
        p_buf = self._run_plan(monkeypatch, pts, results, time_buffer="1H")
        assert p_buf.point_granule_map[0] == [0]

    def test_date_column_gets_noon(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When points use a 'date' column, noon is used for temporal matching."""
        # Granule covers only the afternoon — noon must fall inside
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "date": pd.to_datetime(["2023-06-01"])}
        )
        # Granule covers 6am–6pm → noon is inside
        results = [
            _make_result(
                begin="2023-06-01T06:00:00Z",
                end="2023-06-01T18:00:00Z",
                bbox=(-180, -90, 180, 90),
            )
        ]
        p = self._run_plan(monkeypatch, pts, results)
        assert p.point_granule_map[0] == [0]

    def test_plan_stores_variables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = plan(pts, source_kwargs={"short_name": "TEST"}, variables=["Rrs", "sst"])
        assert p.variables == ["Rrs", "sst"]

    def test_plan_stores_results(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_results = [_make_global_result("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")]
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = fake_results
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = plan(pts, source_kwargs={"short_name": "TEST"}, variables=["sst"])
        assert p.results is fake_results or p.results == fake_results


# ---------------------------------------------------------------------------
# Plan.summary()
# ---------------------------------------------------------------------------

class TestPlanSummary:
    def _make_plan(self) -> Plan:
        pts = pd.DataFrame(
            {
                "lat": [0.0, 1.0, 2.0],
                "lon": [0.0, 1.0, 2.0],
                "time": pd.to_datetime(
                    ["2023-06-01T12:00:00", "2023-06-01T12:00:00", "2023-06-01T12:00:00"]
                ),
            }
        )
        granules = [
            GranuleMeta(
                granule_id="https://example.com/a.nc",
                begin=pd.Timestamp("2023-06-01T00:00:00Z"),
                end=pd.Timestamp("2023-06-01T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=0,
            ),
            GranuleMeta(
                granule_id="https://example.com/b.nc",
                begin=pd.Timestamp("2023-06-01T00:00:00Z"),
                end=pd.Timestamp("2023-06-01T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=1,
            ),
        ]
        return Plan(
            points=pts,
            results=[],
            granules=granules,
            point_granule_map={0: [], 1: [0], 2: [0, 1]},  # 0→none, 1→1, 2→2
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_summary_is_string(self) -> None:
        p = self._make_plan()
        assert isinstance(p.summary(), str)

    def test_summary_contains_zero_match_count(self) -> None:
        p = self._make_plan()
        s = p.summary()
        assert "0 matches" in s or "0 match" in s

    def test_summary_contains_multi_match_count(self) -> None:
        p = self._make_plan()
        s = p.summary()
        assert ">1" in s

    def test_summary_n_limits_points(self) -> None:
        p = self._make_plan()
        s_limited = p.summary(n=1)
        # Only shows 1 point's detail
        assert s_limited.count("→") <= 1  # at most 1 granule URL shown

    def test_summary_contains_granule_urls(self) -> None:
        p = self._make_plan()
        s = p.summary()
        assert "https://example.com/a.nc" in s or "https://example.com/b.nc" in s


# ---------------------------------------------------------------------------
# matchup(plan) — plan-based execution
# ---------------------------------------------------------------------------


def _make_l3_3d_dataset(
    lats: list[float],
    lons: list[float],
    dim3: list[float],
    dim3_name: str = "wavelength",
    var_name: str = "Rrs",
    seed: int = 0,
) -> xr.Dataset:
    """Synthetic L3 dataset with a 3D variable (lat, lon, dim3)."""
    rng = np.random.default_rng(seed)
    lat_arr = np.array(lats)
    lon_arr = np.array(lons)
    dim3_arr = np.array(dim3)
    data = rng.uniform(0.001, 0.01, (lat_arr.size, lon_arr.size, dim3_arr.size)).astype(np.float32)
    return xr.Dataset(
        {var_name: (["lat", "lon", dim3_name], data)},
        coords={"lat": lat_arr, "lon": lon_arr, dim3_name: dim3_arr},
    )


def _make_l3_dataset(lats: list[float], lons: list[float], seed: int = 0) -> xr.Dataset:
    """Synthetic L3 dataset."""
    rng = np.random.default_rng(seed)
    lat_arr = np.array(lats)
    lon_arr = np.array(lons)
    sst = rng.uniform(20.0, 30.0, (lat_arr.size, lon_arr.size)).astype(np.float32)
    return xr.Dataset(
        {"sst": (["lat", "lon"], sst)},
        coords={"lat": lat_arr, "lon": lon_arr},
    )


class TestMatchupWithPlan:
    """Tests for pc.matchup(plan) plan-based execution."""

    def _build_plan(
        self,
        tmp_path: pathlib.Path,
        points: pd.DataFrame,
        nc_files: list[str],
        granule_metas: list[GranuleMeta],
        variables: list[str] | None = None,
    ) -> tuple[Plan, list[object]]:
        """Build a Plan and fake opened file list for testing."""
        # results are just placeholders; opened_files maps result_index → nc path
        results = [object() for _ in nc_files]
        mapping = _match_points_to_granules(points, granule_metas, pd.Timedelta(0))
        p = Plan(
            points=points,
            results=results,
            granules=granule_metas,
            point_granule_map=mapping,
            variables=variables or ["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        return p, nc_files  # nc_files act as opened_files (strings work with xr.open_dataset)

    def test_matchup_with_plan_does_not_call_search_data(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup(plan) must NOT call earthaccess.search_data."""
        nc_path = str(tmp_path / "AQUA_MODIS.20230601.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)

        mock_ea = MagicMock()
        # earthaccess.open returns the nc_path as the file object (xr can open strings)
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/g.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        pc.matchup(p, open_dataset_kwargs={"engine": "netcdf4"})
        mock_ea.search_data.assert_not_called()

    def test_matchup_with_plan_calls_open(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup(plan) must call earthaccess.open with plan.results."""
        nc_path = str(tmp_path / "AQUA_MODIS.20230601.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)

        mock_ea = MagicMock()
        fake_results = [object()]
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/g.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = Plan(
            points=pts,
            results=fake_results,
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        pc.matchup(p, open_dataset_kwargs={"engine": "netcdf4"})
        mock_ea.open.assert_called_once_with(fake_results, pqdm_kwargs={"disable": True})

    def test_matchup_plan_zero_match_returns_nan_row(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Points with 0 granule matches must produce one NaN row."""
        mock_ea = MagicMock()
        mock_ea.open.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: []},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p, open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 1
        assert math.isnan(result.loc[0, "sst"])
        assert "granule_id" in result.columns

    def test_matchup_plan_single_match_one_row(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A single-match point → one output row with values."""
        nc_path = str(tmp_path / "AQUA_MODIS.20230601.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/g.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p, open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])
        assert result.loc[0, "granule_id"] == "https://example.com/g.nc"

    def test_matchup_plan_multi_match_returns_multiple_rows(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A point matching 2 granules → 2 output rows."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(nc_a)
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=2).to_netcdf(nc_b)

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm_a = GranuleMeta(
            granule_id="https://example.com/a.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        gm_b = GranuleMeta(
            granule_id="https://example.com/b.nc",
            begin=pd.Timestamp("2023-06-01T06:00:00Z"),
            end=pd.Timestamp("2023-06-01T18:00:00Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=1,
        )
        p = Plan(
            points=pts,
            results=[object(), object()],
            granules=[gm_a, gm_b],
            point_granule_map={0: [0, 1]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p, open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 2, "One row per (point, granule) pair"
        assert set(result["granule_id"]) == {
            "https://example.com/a.nc",
            "https://example.com/b.nc",
        }

    def test_matchup_plan_uses_plan_variables_when_none_passed(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup(plan) uses plan.variables when variables kwarg is not given."""
        mock_ea = MagicMock()
        mock_ea.open.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: []},
            variables=["chlor_a"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p)  # no variables kwarg
        assert "chlor_a" in result.columns

    def test_matchup_plan_output_includes_original_columns(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Output preserves all original point columns."""
        mock_ea = MagicMock()
        mock_ea.open.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0],
                "lon": [0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
                "station_id": ["STA001"],
            }
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: []},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p)
        assert "station_id" in result.columns
        assert result.loc[0, "station_id"] == "STA001"

    def test_matchup_3d_variable_expands_without_open_dataset_kwargs(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """3D variables (lat, lon, wavelength) are expanded into per-wavelength columns."""
        wavelengths = [346, 348, 351]
        nc_path = str(tmp_path / "PACE_OCI_2023152.L3m.DAY.RRS.Rrs.4km.nc")
        _make_l3_3d_dataset(
            [-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], wavelengths, seed=5
        ).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/rrs.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["Rrs"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p, open_dataset_kwargs={"engine": "netcdf4"})
        assert "Rrs" not in result.columns, "bare 'Rrs' column should be dropped after expansion"
        for wl in wavelengths:
            assert f"Rrs_{wl}" in result.columns, f"Rrs_{wl} column missing"
        assert len(result) == 1

    def test_matchup_3d_variable_expands_with_open_dataset_kwargs(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """3D variable expansion works when open_dataset_kwargs dict is passed."""
        wavelengths = [346, 348, 351]
        nc_path = str(tmp_path / "PACE_OCI_2023152.L3m.DAY.RRS.Rrs.4km.nc")
        _make_l3_3d_dataset(
            [-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], wavelengths, seed=6
        ).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/rrs.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["Rrs"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # This is the key test: passing open_dataset_kwargs with chunks={} (the original
        # bug report scenario) must not break 3D variable expansion.
        result = pc.matchup(p, open_dataset_kwargs={"chunks": {}, "engine": "netcdf4"})
        assert "Rrs" not in result.columns, "bare 'Rrs' column should be dropped after expansion"
        for wl in wavelengths:
            assert f"Rrs_{wl}" in result.columns, f"Rrs_{wl} column missing"
        assert len(result) == 1

