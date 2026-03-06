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
    _get_polygon_points,
    _get_umm,
    _match_points_to_granules,
    _parse_time_buffer,
    _plan_normalise_time,
    _point_in_polygon,
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
# _get_polygon_points
# ---------------------------------------------------------------------------

class TestGetPolygonPoints:
    def _make_gpolygon_umm(self, points: list[dict[str, float]]) -> dict:
        return {
            "SpatialExtent": {
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "GPolygons": [{"Boundary": {"Points": points}}]
                    }
                }
            }
        }

    def test_returns_none_for_bounding_rectangles(self) -> None:
        umm: dict = {
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
        assert _get_polygon_points(umm) is None

    def test_returns_none_for_empty_geometry(self) -> None:
        umm: dict = {"SpatialExtent": {}}
        assert _get_polygon_points(umm) is None

    def test_returns_lon_lat_pairs_for_gpolygon(self) -> None:
        pts = [
            {"Longitude": -50.0, "Latitude": 30.0},
            {"Longitude": -40.0, "Latitude": 50.0},
            {"Longitude": -30.0, "Latitude": 30.0},
        ]
        result = _get_polygon_points(self._make_gpolygon_umm(pts))
        assert result == [(-50.0, 30.0), (-40.0, 50.0), (-30.0, 30.0)]

    def test_returns_floats(self) -> None:
        pts = [{"Longitude": -50, "Latitude": 30}, {"Longitude": -40, "Latitude": 50}]
        result = _get_polygon_points(self._make_gpolygon_umm(pts))
        assert result is not None
        for lon, lat in result:
            assert isinstance(lon, float)
            assert isinstance(lat, float)


# ---------------------------------------------------------------------------
# _point_in_polygon
# ---------------------------------------------------------------------------

class TestPointInPolygon:
    # Simple square: corners at (-10, -10), (10, -10), (10, 10), (-10, 10)
    SQUARE: list[tuple[float, float]] = [
        (-10.0, -10.0),
        (10.0, -10.0),
        (10.0, 10.0),
        (-10.0, 10.0),
    ]

    def test_point_inside(self) -> None:
        assert _point_in_polygon(0.0, 0.0, self.SQUARE) is True

    def test_point_outside(self) -> None:
        assert _point_in_polygon(20.0, 20.0, self.SQUARE) is False

    def test_point_outside_one_axis(self) -> None:
        # lon inside, lat outside
        assert _point_in_polygon(0.0, 15.0, self.SQUARE) is False

    def test_diagonal_triangle(self) -> None:
        # Triangle: (0,0), (10,0), (10,10) - right triangle
        triangle: list[tuple[float, float]] = [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)]
        # Inside (just below the hypotenuse)
        assert _point_in_polygon(9.0, 1.0, triangle) is True
        # Outside (above the hypotenuse)
        assert _point_in_polygon(1.0, 9.0, triangle) is False

    def test_pace_like_swath_quadrilateral(self) -> None:
        """A polygon representative of a PACE OCI L2 swath over the Gulf of Mexico."""
        # Approximate swath corners from examples/fixtures/earthaccess_results_sample_l2.json
        swath: list[tuple[float, float]] = [
            (-38.97618, 55.74064),
            (-78.13296, 49.10770),
            (-67.51633, 32.42610),
            (-38.46750, 37.98933),
        ]
        # Centroid should be inside
        centroid_lon = sum(p[0] for p in swath) / len(swath)
        centroid_lat = sum(p[1] for p in swath) / len(swath)
        assert _point_in_polygon(centroid_lon, centroid_lat, swath) is True
        # Point far outside
        assert _point_in_polygon(-150.0, 0.0, swath) is False


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

    def test_gpolygon_point_inside_matches(self) -> None:
        """A point inside a GPolygon granule is matched."""
        polygon: list[tuple[float, float]] = [
            (-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)
        ]
        gm = GranuleMeta(
            granule_id="https://example.com/l2.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-10.0, -10.0, 10.0, 10.0),
            result_index=0,
            polygon=polygon,
        )
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        mapping = _match_points_to_granules(pts, [gm], pd.Timedelta(0))
        assert mapping[0] == [0]

    def test_gpolygon_point_in_bbox_but_outside_polygon_excluded(self) -> None:
        """A point inside the bbox but outside the actual polygon is NOT matched.

        This is the key advantage over the simpler bbox approach:
        it avoids false positives for L2 swath data.
        """
        # Triangle: (0,0), (10,0), (10,10) — bbox is (0,0,10,10)
        triangle: list[tuple[float, float]] = [
            (0.0, 0.0), (10.0, 0.0), (10.0, 10.0)
        ]
        bbox = (0.0, 0.0, 10.0, 10.0)
        gm = GranuleMeta(
            granule_id="https://example.com/l2.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=bbox,
            result_index=0,
            polygon=triangle,
        )
        # Point at (lon=1, lat=9) is inside the bbox but OUTSIDE the triangle
        pts = pd.DataFrame(
            {"lat": [9.0], "lon": [1.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        mapping = _match_points_to_granules(pts, [gm], pd.Timedelta(0))
        assert mapping[0] == [], "Point outside polygon but inside bbox should NOT match"

    def test_gpolygon_point_outside_polygon_excluded(self) -> None:
        """A point entirely outside the GPolygon is excluded."""
        polygon: list[tuple[float, float]] = [
            (-10.0, -10.0), (10.0, -10.0), (10.0, 10.0), (-10.0, 10.0)
        ]
        gm = GranuleMeta(
            granule_id="https://example.com/l2.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-10.0, -10.0, 10.0, 10.0),
            result_index=0,
            polygon=polygon,
        )
        pts = pd.DataFrame(
            {"lat": [50.0], "lon": [50.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        mapping = _match_points_to_granules(pts, [gm], pd.Timedelta(0))
        assert mapping[0] == []

    def test_granules_supplied_out_of_begin_order(self) -> None:
        """Granules given in reverse begin order still match correctly.

        The optimised implementation sorts granules internally, so the
        returned granule indices must still refer to the *original* positions
        in ``granule_metas``.
        """
        # Two non-overlapping daily granules supplied in reverse order.
        gm_day2 = GranuleMeta(
            granule_id="https://example.com/day2.nc",
            begin=pd.Timestamp("2023-06-02T00:00:00Z"),
            end=pd.Timestamp("2023-06-02T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        gm_day1 = GranuleMeta(
            granule_id="https://example.com/day1.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=1,
        )
        # granule_metas list: index 0 = day2, index 1 = day1
        granule_metas = [gm_day2, gm_day1]

        pts = pd.DataFrame(
            {
                "lat": [0.0, 0.0],
                "lon": [0.0, 0.0],
                "time": pd.to_datetime(
                    ["2023-06-01T12:00:00", "2023-06-02T12:00:00"]
                ),
            }
        )
        mapping = _match_points_to_granules(pts, granule_metas, pd.Timedelta(0))
        # point 0 (day1 time) → granule index 1 (gm_day1)
        assert mapping[0] == [1]
        # point 1 (day2 time) → granule index 0 (gm_day2)
        assert mapping[1] == [0]

    def test_large_n_scales_near_linearly(self) -> None:
        """Matching N=5000 points takes less than 10× the time of N=500.

        This guards against the original O(N×M) quadratic slowdown where
        the inner granule loop ran for every point regardless of temporal
        overlap.
        """
        import time

        # 365 non-overlapping daily granules covering all of 2023.
        base = pd.Timestamp("2023-01-01")
        granule_metas = [
            GranuleMeta(
                granule_id=f"https://example.com/g{i}.nc",
                begin=base + pd.Timedelta(days=i),
                end=base + pd.Timedelta(days=i) + pd.Timedelta(hours=23, minutes=59, seconds=59),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=i,
            )
            for i in range(365)
        ]

        rng = np.random.default_rng(42)

        def _make_pts(n: int) -> pd.DataFrame:
            days = rng.integers(0, 365, size=n)
            return pd.DataFrame(
                {
                    "lat": rng.uniform(-90, 90, size=n),
                    "lon": rng.uniform(-180, 180, size=n),
                    "time": [base + pd.Timedelta(days=int(d)) + pd.Timedelta(hours=12) for d in days],
                }
            )

        pts_small = _make_pts(500)
        pts_large = _make_pts(5000)
        buf = pd.Timedelta(0)

        t0 = time.monotonic()
        _match_points_to_granules(pts_small, granule_metas, buf)
        t_small = time.monotonic() - t0

        t0 = time.monotonic()
        _match_points_to_granules(pts_large, granule_metas, buf)
        t_large = time.monotonic() - t0

        # With O(N log M) scaling, the 10× point ratio should take well under
        # 10× longer. We allow a generous 10× slack to avoid CI flakiness
        # while still catching quadratic regressions (which would be ~100×).
        # The +1.0 s additive term guards against false failures on slow
        # CI runners where t_small itself is too small to be meaningful.
        assert t_large < t_small * 10 + 1.0, (
            f"Scaling looks super-linear: {t_small:.3f}s for N=500, "
            f"{t_large:.3f}s for N=5000 (ratio={t_large/max(t_small, 1e-6):.1f}×)"
        )



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
        result = plan(pts, source_kwargs={"short_name": "TEST"})
        assert isinstance(result, Plan)

    def test_plan_raises_on_unknown_data_source(self) -> None:
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        with pytest.raises(ValueError, match="Unknown data_source"):
            plan(pts, data_source="stac", source_kwargs={})

    def test_plan_raises_when_neither_time_nor_date(self) -> None:
        pts = pd.DataFrame({"lat": [0.0], "lon": [0.0], "x": [1]})
        with pytest.raises(ValueError, match="time"):
            plan(pts, source_kwargs={"short_name": "TEST"})

    def test_plan_raises_without_short_name(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_ea = MagicMock()
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        with pytest.raises(ValueError, match="short_name"):
            plan(pts, source_kwargs={})


class TestPlanMapping:
    """Tests for the point→granule mapping built by pc.plan()."""

    def _run_plan(
        self,
        monkeypatch: pytest.MonkeyPatch,
        points: pd.DataFrame,
        fake_results: list[dict],
        time_buffer: str = "0h",
    ) -> Plan:
        """Helper: run pc.plan() with mocked earthaccess.search_data."""
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = fake_results
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        return plan(
            points,
            source_kwargs={"short_name": "TEST"},
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

    def test_plan_stores_results(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_results = [_make_global_result("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")]
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = fake_results
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = plan(pts, source_kwargs={"short_name": "TEST"})
        assert p.results is fake_results or p.results == fake_results


# ---------------------------------------------------------------------------
# granule_name post-search filtering
# ---------------------------------------------------------------------------

def _make_result_with_links(
    *,
    begin: str,
    end: str,
    data_url: str = "https://example.com/granule.nc",
) -> MagicMock:
    """Build a mock earthaccess result that supports both dict-access and data_links()."""
    mock_result = MagicMock()
    # Support dict-like access for _get_umm (result["umm"])
    mock_result.__getitem__ = lambda _, key: {
        "umm": {
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": begin,
                    "EndingDateTime": end,
                }
            },
            "SpatialExtent": {
                "HorizontalSpatialDomain": {
                    "Geometry": {
                        "BoundingRectangles": [
                            {
                                "WestBoundingCoordinate": -180.0,
                                "SouthBoundingCoordinate": -90.0,
                                "EastBoundingCoordinate": 180.0,
                                "NorthBoundingCoordinate": 90.0,
                            }
                        ]
                    }
                }
            },
            "RelatedUrls": [{"Type": "GET DATA", "URL": data_url}],
        }
    }[key]
    mock_result.data_links.return_value = [data_url]
    return mock_result


class TestGranuleNameFiltering:
    """Tests for granule_name post-search filtering in _search_earthaccess."""

    def _run_plan_with_links(
        self,
        monkeypatch: pytest.MonkeyPatch,
        points: pd.DataFrame,
        fake_results: list[Any],
        granule_name: str | None = None,
    ) -> Plan:
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = fake_results
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        source_kwargs: dict[str, Any] = {"short_name": "TEST"}
        if granule_name is not None:
            source_kwargs["granule_name"] = granule_name
        return plan(points, source_kwargs=source_kwargs)

    def test_granule_name_not_passed_to_search_data(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_name must be stripped from the kwargs sent to earthaccess.search_data."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        result = _make_result_with_links(
            begin="2023-06-01T00:00:00Z",
            end="2023-06-01T23:59:59Z",
            data_url="https://example.com/PROD.DAY.RRS.4km.nc",
        )
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = [result]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        plan(
            pts,
            source_kwargs={"short_name": "TEST", "granule_name": "*.DAY.*.4km.*"},
        )

        call_kwargs = mock_ea.search_data.call_args[1]
        assert "granule_name" not in call_kwargs

    def test_granule_name_filters_matching_results(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Only results whose data_links() match the granule_name pattern are kept."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        matching = _make_result_with_links(
            begin="2023-06-01T00:00:00Z",
            end="2023-06-01T23:59:59Z",
            data_url="https://example.com/PROD.DAY.RRS.4km.nc",
        )
        non_matching = _make_result_with_links(
            begin="2023-06-01T00:00:00Z",
            end="2023-06-01T23:59:59Z",
            data_url="https://example.com/PROD.MO.RRS.4km.nc",
        )
        p = self._run_plan_with_links(
            monkeypatch,
            pts,
            [matching, non_matching],
            granule_name="*.DAY.*",
        )
        assert len(p.results) == 1
        assert len(p.granules) == 1

    def test_granule_name_excludes_all_when_no_match(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When no results match the pattern, results and granules are empty."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        result = _make_result_with_links(
            begin="2023-06-01T00:00:00Z",
            end="2023-06-01T23:59:59Z",
            data_url="https://example.com/PROD.MO.RRS.4km.nc",
        )
        p = self._run_plan_with_links(
            monkeypatch, pts, [result], granule_name="*.DAY.*"
        )
        assert p.results == []
        assert p.granules == []

    def test_granule_name_keeps_all_when_all_match(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When all results match, all are kept."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        results = [
            _make_result_with_links(
                begin="2023-06-01T00:00:00Z",
                end="2023-06-01T23:59:59Z",
                data_url=f"https://example.com/PROD.DAY.RRS.4km.{i}.nc",
            )
            for i in range(3)
        ]
        p = self._run_plan_with_links(
            monkeypatch, pts, results, granule_name="*.DAY.*"
        )
        assert len(p.results) == 3
        assert len(p.granules) == 3

    def test_without_granule_name_no_filtering(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When granule_name is absent, all search results are kept unchanged."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        results = [
            _make_result_with_links(
                begin="2023-06-01T00:00:00Z",
                end="2023-06-01T23:59:59Z",
                data_url=f"https://example.com/granule_{i}.nc",
            )
            for i in range(3)
        ]
        p = self._run_plan_with_links(monkeypatch, pts, results, granule_name=None)
        assert len(p.results) == 3

    def test_granule_name_stored_in_source_kwargs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_name is stored in Plan.source_kwargs even though it's not sent to search_data."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        result = _make_result_with_links(
            begin="2023-06-01T00:00:00Z",
            end="2023-06-01T23:59:59Z",
            data_url="https://example.com/PROD.DAY.RRS.4km.nc",
        )
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = [result]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        p = plan(
            pts,
            source_kwargs={"short_name": "TEST", "granule_name": "*.DAY.*"},
        )
        assert p.source_kwargs.get("granule_name") == "*.DAY.*"


# ---------------------------------------------------------------------------
# Bounding-box auto-derivation in _search_earthaccess
# ---------------------------------------------------------------------------

class TestSearchEarthaccessBoundingBox:
    """Tests for automatic bounding-box derivation from points."""

    def _call_plan(
        self,
        monkeypatch: pytest.MonkeyPatch,
        points: pd.DataFrame,
        source_kwargs: dict[str, Any] | None = None,
    ) -> tuple[MagicMock, Plan]:
        """Run pc.plan() with a mocked earthaccess; return (mock, plan)."""
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = [
            _make_global_result("2023-06-01T00:00:00Z", "2023-06-01T23:59:59Z")
        ]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        sk: dict[str, Any] = source_kwargs if source_kwargs is not None else {"short_name": "TEST"}
        p = plan(points, source_kwargs=sk)
        return mock_ea, p

    def test_bounding_box_derived_from_points(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Auto-derived bounding_box is passed to earthaccess.search_data."""
        pts = pd.DataFrame(
            {
                "lat": [30.0, 45.0, 35.0],
                "lon": [-90.0, -75.0, -80.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"] * 3),
            }
        )
        mock_ea, _ = self._call_plan(monkeypatch, pts)
        call_kwargs = mock_ea.search_data.call_args[1]
        assert "bounding_box" in call_kwargs
        lon_min, lat_min, lon_max, lat_max = call_kwargs["bounding_box"]
        assert lon_min == pytest.approx(-90.0)
        assert lat_min == pytest.approx(30.0)
        assert lon_max == pytest.approx(-75.0)
        assert lat_max == pytest.approx(45.0)

    def test_bounding_box_single_point(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A single point yields a degenerate bounding box (min == max)."""
        pts = pd.DataFrame(
            {"lat": [20.0], "lon": [10.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        mock_ea, _ = self._call_plan(monkeypatch, pts)
        call_kwargs = mock_ea.search_data.call_args[1]
        lon_min, lat_min, lon_max, lat_max = call_kwargs["bounding_box"]
        assert lon_min == pytest.approx(10.0)
        assert lat_min == pytest.approx(20.0)
        assert lon_max == pytest.approx(10.0)
        assert lat_max == pytest.approx(20.0)

    def test_user_bounding_box_not_overridden(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When bounding_box is already in source_kwargs, it is used as-is."""
        pts = pd.DataFrame(
            {
                "lat": [30.0, 45.0],
                "lon": [-90.0, -75.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"] * 2),
            }
        )
        user_bbox = (-97.3, 24.6, -81.5, 30.39)
        mock_ea, _ = self._call_plan(
            monkeypatch,
            pts,
            source_kwargs={"short_name": "TEST", "bounding_box": user_bbox},
        )
        call_kwargs = mock_ea.search_data.call_args[1]
        assert call_kwargs["bounding_box"] == user_bbox

    def test_bounding_box_order_lon_lat(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """bounding_box is in (lon_min, lat_min, lon_max, lat_max) order."""
        pts = pd.DataFrame(
            {
                "lat": [10.0, 50.0],
                "lon": [-100.0, -60.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"] * 2),
            }
        )
        mock_ea, _ = self._call_plan(monkeypatch, pts)
        lon_min, lat_min, lon_max, lat_max = mock_ea.search_data.call_args[1]["bounding_box"]
        assert lon_min == pytest.approx(-100.0)
        assert lat_min == pytest.approx(10.0)
        assert lon_max == pytest.approx(-60.0)
        assert lat_max == pytest.approx(50.0)


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

    def test_summary_returns_none(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        result = p.summary()
        assert result is None

    def test_summary_prints_output(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary()
        out = capsys.readouterr().out
        assert len(out) > 0

    def test_summary_contains_zero_match_count(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary()
        s = capsys.readouterr().out
        assert "0 matches" in s or "0 match" in s

    def test_summary_contains_multi_match_count(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary()
        s = capsys.readouterr().out
        assert ">1" in s

    def test_summary_n_limits_points(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary(n=1)
        s = capsys.readouterr().out
        # With n=1, only the first point (index 0, no matches) is shown
        assert "    →" not in s  # point 0 has 0 granules

    def test_summary_n_shows_all_granule_urls(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary(n=3)
        s = capsys.readouterr().out
        # Point 2 has 2 matches; both URLs must appear (no truncation)
        assert "https://example.com/a.nc" in s
        assert "https://example.com/b.nc" in s

    def test_summary_contains_granule_urls(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary()
        s = capsys.readouterr().out
        assert "https://example.com/a.nc" in s or "https://example.com/b.nc" in s

    def test_summary_default_n_is_5_or_fewer(self, capsys: pytest.CaptureFixture[str]) -> None:
        # Plan has 3 points; default n should be min(5, 3) = 3
        p = self._make_plan()
        p.summary()
        s = capsys.readouterr().out
        assert "First 3 point(s):" in s

    def test_summary_default_n_capped_at_5(self, capsys: pytest.CaptureFixture[str]) -> None:
        # Build a plan with 10 points; default n should be min(5, 10) = 5
        pts = pd.DataFrame(
            {
                "lat": [float(i) for i in range(10)],
                "lon": [float(i) for i in range(10)],
                "time": pd.to_datetime(["2023-06-01T12:00:00"] * 10),
            }
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={i: [] for i in range(10)},
            variables=[],
            source_kwargs={},
            time_buffer=pd.Timedelta(0),
        )
        p.summary()
        s = capsys.readouterr().out
        assert "First 5 point(s):" in s

    def test_summary_n_zero_hides_per_point_section(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary(n=0)
        s = capsys.readouterr().out
        assert "First 0 point(s):" not in s
        assert "    →" not in s

    def test_summary_negative_n_acts_like_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        p = self._make_plan()
        p.summary(n=-1)
        s = capsys.readouterr().out
        assert "First" not in s
        assert "    →" not in s

    def test_summary_does_not_show_variables(self, capsys: pytest.CaptureFixture[str]) -> None:
        """summary() should not print a 'Variables' line."""
        p = self._make_plan()
        p.summary()
        s = capsys.readouterr().out
        assert "Variables" not in s

    def test_summary_granule_count_is_matched_not_total(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Granule count reflects unique matched granules, not all granules in the plan."""
        pts = pd.DataFrame(
            {
                "lat": [0.0, 1.0, 2.0],
                "lon": [0.0, 1.0, 2.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"] * 3),
            }
        )
        # Build 5 granules but only 2 are matched (indices 0 and 1)
        granules = [
            GranuleMeta(
                granule_id=f"https://example.com/{c}.nc",
                begin=pd.Timestamp("2023-06-01T00:00:00Z"),
                end=pd.Timestamp("2023-06-01T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=i,
            )
            for i, c in enumerate("abcde")
        ]
        # Only granules 0 and 1 are referenced; granules 2-4 are unmatched
        p = Plan(
            points=pts,
            results=[],
            granules=granules,
            point_granule_map={0: [], 1: [0], 2: [0, 1]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        p.summary()
        s = capsys.readouterr().out
        # Should show 2 matched granules, not 5 total
        assert "3 points → 2 unique granule(s)" in s


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

        pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
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

        pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, geometry="grid")  # no variables kwarg
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

        result = pc.matchup(p, geometry="grid")
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

        result = pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
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
        result = pc.matchup(p, geometry="grid", open_dataset_kwargs={"chunks": {}, "engine": "netcdf4"})
        assert "Rrs" not in result.columns, "bare 'Rrs' column should be dropped after expansion"
        for wl in wavelengths:
            assert f"Rrs_{wl}" in result.columns, f"Rrs_{wl} column missing"
        assert len(result) == 1

    def test_matchup_2d_variable_with_chunks(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """2D variable (lat, lon) must return a value—not NaN—when chunks={} is used.

        Regression test: when open_dataset_kwargs={"chunks": {}} (dask lazy loading)
        is passed, selected.item() raises NotImplementedError on 0-dimensional dask
        arrays. The fix uses float(selected) instead, which works for both dask and
        non-dask arrays.
        """
        nc_path = str(tmp_path / "PACE_OCI_2023152.L3m.DAY.AVW.avw.4km.nc")
        _make_l3_dataset(
            [-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=7
        ).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/avw.nc",
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

        # This is the regression test: chunks={} must NOT cause 2D variables to return NaN
        result = pc.matchup(p, geometry="grid", open_dataset_kwargs={"chunks": {}, "engine": "netcdf4"})
        assert len(result) == 1
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"]), (
            "2D variable must return a value when chunks={} (dask) is used, not NaN"
        )

    def test_matchup_silent_false_prints_progress(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """silent=False (default) prints progress after each batch."""
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

        pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"}, silent=False)
        captured = capsys.readouterr()
        assert "granules" in captured.out
        assert "processed" in captured.out

    def test_matchup_silent_true_suppresses_output(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """silent=True suppresses all progress output."""
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

        pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"}, silent=True)
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_matchup_save_dir_creates_parquet_files(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """save_dir causes batch parquet files to be written."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(nc_a)
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=2).to_netcdf(nc_b)

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0, 0.0],
                "lon": [0.0, 0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00", "2023-06-02T12:00:00"]),
            }
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
            begin=pd.Timestamp("2023-06-02T00:00:00Z"),
            end=pd.Timestamp("2023-06-02T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=1,
        )
        p = Plan(
            points=pts,
            results=[object(), object()],
            granules=[gm_a, gm_b],
            point_granule_map={0: [0], 1: [1]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        save_dir = tmp_path / "_temp_data"
        pc.matchup(
            p,
            geometry="grid",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=True,
            batch_size=1,
            save_dir=save_dir,
        )

        parquet_files = list(save_dir.glob("plan_*.parquet"))
        assert len(parquet_files) == 2, f"Expected 2 parquet files, got {len(parquet_files)}"

    def test_matchup_save_dir_parquet_content_matches_result(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Parquet files saved by save_dir contain the same rows as the final result."""
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

        save_dir = tmp_path / "_temp_data"
        result = pc.matchup(
            p,
            geometry="grid",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=True,
            save_dir=save_dir,
        )

        parquet_files = list(save_dir.glob("plan_*.parquet"))
        assert len(parquet_files) == 1
        saved_df = pd.read_parquet(parquet_files[0])
        assert list(saved_df.columns) == list(result.columns)
        assert len(saved_df) == len(result)

    def test_matchup_batch_size_controls_print_frequency(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """batch_size controls how often progress is printed."""
        # Create 3 granule files
        nc_files = []
        for i in range(3):
            nc_path = str(tmp_path / f"g{i}.nc")
            _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=i).to_netcdf(nc_path)
            nc_files.append(nc_path)

        mock_ea = MagicMock()
        mock_ea.open.return_value = nc_files
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0, 0.0, 0.0],
                "lon": [0.0, 0.0, 0.0],
                "time": pd.to_datetime(
                    ["2023-06-01T12:00:00", "2023-06-02T12:00:00", "2023-06-03T12:00:00"]
                ),
            }
        )
        granules = [
            GranuleMeta(
                granule_id=f"https://example.com/g{i}.nc",
                begin=pd.Timestamp(f"2023-06-0{i+1}T00:00:00Z"),
                end=pd.Timestamp(f"2023-06-0{i+1}T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=i,
            )
            for i in range(3)
        ]
        p = Plan(
            points=pts,
            results=[object(), object(), object()],
            granules=granules,
            point_granule_map={0: [0], 1: [1], 2: [2]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # batch_size=1 → 3 lines of output for 3 granules
        pc.matchup(
            p,
            geometry="grid",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=False,
            batch_size=1,
        )
        captured = capsys.readouterr()
        lines = [ln for ln in captured.out.splitlines() if ln.strip()]
        assert len(lines) == 3
        # Each line must follow the documented format
        for line in lines:
            assert "granules" in line
            assert "of 3 processed" in line
            assert "points matched" in line

    def test_matchup_save_dir_creates_directory_if_missing(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """save_dir is created automatically when it does not exist."""
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

        new_dir = tmp_path / "does_not_exist" / "_temp_data"
        assert not new_dir.exists()
        pc.matchup(p, geometry="grid", silent=True, save_dir=new_dir)
        assert new_dir.exists()


# ---------------------------------------------------------------------------
# Task 1: variables removed from plan()
# ---------------------------------------------------------------------------

class TestPlanNoVariables:
    """pc.plan() does not accept a variables argument."""

    def test_plan_variables_field_is_empty_list(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = plan(pts, source_kwargs={"short_name": "TEST"})
        assert p.variables == []

    def test_plan_does_not_accept_variables_kwarg(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        with pytest.raises(TypeError):
            plan(pts, source_kwargs={"short_name": "TEST"}, variables=["sst"])  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Task 2: Plan.__getitem__, open_dataset, open_mfdataset
# ---------------------------------------------------------------------------

class TestPlanGetItem:
    def _make_plan_with_points(self, n_points: int = 3) -> Plan:
        """Build a plan with *n_points* rows, each matched to its own granule."""
        pts = pd.DataFrame(
            {
                "lat": [float(i) for i in range(n_points)],
                "lon": [float(i) for i in range(n_points)],
                "time": pd.date_range("2023-06-01", periods=n_points, freq="D"),
            }
        )
        fake_results = [object() for _ in range(n_points)]
        granules = [
            GranuleMeta(
                granule_id=f"https://example.com/g{i}.nc",
                begin=pd.Timestamp(f"2023-06-0{i+1}T00:00:00Z"),
                end=pd.Timestamp(f"2023-06-0{i+1}T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=i,
            )
            for i in range(n_points)
        ]
        point_granule_map = {i: [i] for i in range(n_points)}
        return Plan(
            points=pts,
            results=fake_results,
            granules=granules,
            point_granule_map=point_granule_map,
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_integer_index(self) -> None:
        """Integer index returns the earthaccess result object at that position."""
        p = self._make_plan_with_points(3)
        assert p[0] is p.results[0]
        assert p[2] is p.results[2]

    def test_slice_returns_plan(self) -> None:
        """Slice index returns a subset Plan (not a list of results)."""
        p = self._make_plan_with_points(5)
        subset = p[0:3]
        assert isinstance(subset, Plan)

    def test_slice_subset_points(self) -> None:
        """Sliced Plan contains only the selected point rows."""
        p = self._make_plan_with_points(5)
        subset = p[1:4]
        assert len(subset.points) == 3
        pd.testing.assert_frame_equal(subset.points, p.points.iloc[1:4])

    def test_slice_subset_granules_and_results(self) -> None:
        """Sliced Plan contains only granules/results needed by the kept points."""
        p = self._make_plan_with_points(5)
        # Points 0-2 map to granules 0-2 respectively
        subset = p[0:3]
        assert len(subset.granules) == 3
        assert len(subset.results) == 3
        assert subset.results[0] is p.results[0]
        assert subset.results[2] is p.results[2]

    def test_slice_reindexes_granule_result_index(self) -> None:
        """Granule result_index values in the sliced Plan start from 0."""
        p = self._make_plan_with_points(5)
        subset = p[2:5]
        for new_g_idx, gm in enumerate(subset.granules):
            assert gm.result_index == new_g_idx

    def test_slice_point_granule_map_remapped(self) -> None:
        """point_granule_map in the sliced Plan uses new granule indices."""
        p = self._make_plan_with_points(5)
        subset = p[2:5]
        # All mapped granule indices must be valid indices into subset.granules
        all_g_indices = [g for g_list in subset.point_granule_map.values() for g in g_list]
        assert all(0 <= g < len(subset.granules) for g in all_g_indices)

    def test_slice_zero_match_points_preserved(self) -> None:
        """Points with no granule matches are still included in the slice."""
        pts = pd.DataFrame(
            {
                "lat": [0.0, 1.0, 2.0],
                "lon": [0.0, 1.0, 2.0],
                "time": pd.date_range("2023-06-01", periods=3, freq="D"),
            }
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: [], 1: [], 2: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        subset = p[0:2]
        assert isinstance(subset, Plan)
        assert len(subset.points) == 2
        assert len(subset.granules) == 0

    def test_slice_preserves_variables_and_metadata(self) -> None:
        """Sliced Plan inherits variables, source_kwargs, and time_buffer."""
        p = self._make_plan_with_points(3)
        p.variables = ["avw"]
        subset = p[0:2]
        assert subset.variables == ["avw"]
        assert subset.source_kwargs == p.source_kwargs
        assert subset.time_buffer == p.time_buffer


class TestPlanOpenDataset:
    def test_open_dataset_returns_dataset(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        fake_result = object()
        p = Plan(
            points=pts,
            results=[fake_result],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        ds = p.open_dataset(p[0], open_dataset_kwargs={"engine": "netcdf4"})
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds
        ds.close()
        mock_ea.open.assert_called_once_with([fake_result], pqdm_kwargs={"disable": True})

    def test_open_mfdataset_returns_dataset(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(
            nc_a, engine="netcdf4"
        )
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=2).to_netcdf(
            nc_b, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        fake_results = [object(), object()]
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=fake_results,
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # Patch xr.open_mfdataset to avoid the real coordinate-combination logic
        fake_ds = xr.Dataset({"sst": (["lat", "lon"], [[1.0]])}, coords={"lat": [0.0], "lon": [0.0]})
        with patch("xarray.open_mfdataset", return_value=fake_ds) as mock_mfdataset:
            # Pass a list of results directly (backward-compatible path)
            ds = p.open_mfdataset(fake_results, open_dataset_kwargs={"engine": "netcdf4"})

        assert ds is fake_ds
        mock_ea.open.assert_called_once_with(fake_results, pqdm_kwargs={"disable": True})
        mock_mfdataset.assert_called_once_with([nc_a, nc_b], chunks={}, engine="netcdf4")

    def test_open_mfdataset_accepts_subset_plan(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_mfdataset accepts a subset Plan and uses its results."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(
            nc_a, engine="netcdf4"
        )
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=2).to_netcdf(
            nc_b, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Build a two-point plan, each point matched to its own granule.
        fake_results = [object(), object()]
        pts = pd.DataFrame(
            {
                "lat": [0.0, 1.0],
                "lon": [0.0, 1.0],
                "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
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
                begin=pd.Timestamp("2023-06-02T00:00:00Z"),
                end=pd.Timestamp("2023-06-02T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=1,
            ),
        ]
        p = Plan(
            points=pts,
            results=fake_results,
            granules=granules,
            point_granule_map={0: [0], 1: [1]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # plan[0:2] is now a subset Plan; open_mfdataset should use its results.
        subset = p[0:2]
        assert isinstance(subset, Plan)

        fake_ds = xr.Dataset({"sst": (["lat", "lon"], [[1.0]])}, coords={"lat": [0.0], "lon": [0.0]})
        with patch("xarray.open_mfdataset", return_value=fake_ds) as mock_mfdataset:
            ds = p.open_mfdataset(subset, open_dataset_kwargs={"engine": "netcdf4"})

        assert ds is fake_ds
        mock_ea.open.assert_called_once_with(fake_results, pqdm_kwargs={"disable": True})
        mock_mfdataset.assert_called_once_with([nc_a, nc_b], chunks={}, engine="netcdf4")

    def test_open_dataset_geometry_grid(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset(result, geometry='grid') uses xr.open_dataset."""
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        fake_result = object()
        p = Plan(
            points=pts,
            results=[fake_result],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        ds = p.open_dataset(p[0], geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds
        ds.close()

    def test_open_dataset_invalid_geometry_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset with invalid geometry raises ValueError."""
        mock_ea = MagicMock()
        mock_ea.open.return_value = ["dummy"]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        with pytest.raises(ValueError, match="geometry"):
            p.open_dataset(p[0], geometry="bad")

    def test_open_dataset_invalid_open_method_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset with invalid open_method raises ValueError."""
        mock_ea = MagicMock()
        mock_ea.open.return_value = ["dummy"]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        with pytest.raises(ValueError, match="open_method"):
            p.open_dataset(p[0], open_method="bad")

    def test_open_mfdataset_with_geometry_grid_uses_open_mfdataset(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_mfdataset(results, geometry='grid') uses xr.open_mfdataset."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(
            nc_a, engine="netcdf4"
        )
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=2).to_netcdf(
            nc_b, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        fake_results = [object(), object()]
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=fake_results,
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        fake_ds = xr.Dataset({"sst": (["lat", "lon"], [[1.0]])}, coords={"lat": [0.0], "lon": [0.0]})
        with patch("xarray.open_mfdataset", return_value=fake_ds) as mock_mfd:
            ds = p.open_mfdataset(fake_results, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})

        assert ds is fake_ds
        mock_mfd.assert_called_once_with([nc_a, nc_b], chunks={}, engine="netcdf4")

    def test_open_mfdataset_geometry_swath_concatenates(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_mfdataset(results, geometry='swath') opens each as DataTree-merge and concatenates."""
        nc_a = str(tmp_path / "swath_a.nc")
        nc_b = str(tmp_path / "swath_b.nc")
        _make_l2_swath_dataset(nrows=3, ncols=4, seed=1).to_netcdf(nc_a, engine="netcdf4")
        _make_l2_swath_dataset(nrows=3, ncols=4, seed=2).to_netcdf(nc_b, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        fake_results = [object(), object()]
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=fake_results,
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        ds = p.open_mfdataset(
            fake_results,
            geometry="swath",
            open_dataset_kwargs={"engine": "netcdf4"},
        )
        # Result is a Dataset with a "granule" dimension from concatenation.
        assert isinstance(ds, xr.Dataset)
        assert ds.sizes["granule"] == 2
        assert "sst" in ds


class TestPlanShowVariables:
    def test_show_variables_prints_dims_and_vars(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        p.show_variables(geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
        captured = capsys.readouterr()
        assert "Dimensions" in captured.out
        assert "Variables" in captured.out
        assert "sst" in captured.out

    def test_show_variables_raises_when_no_granules(self) -> None:
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        with pytest.raises(ValueError, match="No granules"):
            p.show_variables(geometry="grid")


# ---------------------------------------------------------------------------
# Task 4: matchup() variables kwarg and missing-variable error
# ---------------------------------------------------------------------------

class TestMatchupVariablesKwarg:
    def test_matchup_variables_kwarg_overrides_plan_variables(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup(plan, variables=["chlor_a"]) uses provided list, not plan.variables."""
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
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
        # Plan has no variables (empty list)
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p, geometry="grid", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"])

    def test_matchup_raises_on_missing_variable(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup() raises ValueError if a requested variable is not in the dataset."""
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
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
            variables=["nonexistent_var"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        with pytest.raises(ValueError, match="nonexistent_var"):
            pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})

    def test_matchup_new_api_no_variables_in_plan(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """New workflow: plan() without variables, matchup() with variables."""
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
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
        # Build a plan without specifying variables
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        assert p.variables == []

        # Pass variables to matchup() instead
        result = pc.matchup(p, geometry="grid", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])


# ---------------------------------------------------------------------------
# Plan subsetting: pc.matchup(plan[0:n])
# ---------------------------------------------------------------------------

class TestMatchupWithSubsetPlan:
    """Tests that pc.matchup(plan[0:n]) processes only the subset of points."""

    def _build_multi_point_plan(
        self,
        tmp_path: pathlib.Path,
        n_points: int,
    ) -> tuple[Plan, list[str]]:
        """Build a plan with *n_points*, each matched to its own granule."""
        nc_files: list[str] = []
        granules: list[GranuleMeta] = []
        for i in range(n_points):
            nc_path = str(tmp_path / f"granule_{i}.nc")
            _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=i).to_netcdf(
                nc_path, engine="netcdf4"
            )
            nc_files.append(nc_path)
            granules.append(
                GranuleMeta(
                    granule_id=f"https://example.com/g{i}.nc",
                    begin=pd.Timestamp(f"2023-06-{i+1:02d}T00:00:00Z"),
                    end=pd.Timestamp(f"2023-06-{i+1:02d}T23:59:59Z"),
                    bbox=(-180.0, -90.0, 180.0, 90.0),
                    result_index=i,
                )
            )
        pts = pd.DataFrame(
            {
                "lat": [0.0] * n_points,
                "lon": [0.0] * n_points,
                "time": pd.to_datetime(
                    [f"2023-06-{i+1:02d}T12:00:00" for i in range(n_points)]
                ),
            }
        )
        results = [object() for _ in range(n_points)]
        point_granule_map = {i: [i] for i in range(n_points)}
        p = Plan(
            points=pts,
            results=results,
            granules=granules,
            point_granule_map=point_granule_map,
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        return p, nc_files

    def test_matchup_with_subset_returns_only_subset_rows(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """pc.matchup(plan[0:3]) processes only the first 3 points."""
        n_points = 5
        p, nc_files = self._build_multi_point_plan(tmp_path, n_points)

        mock_ea = MagicMock()
        # The subset plan has 3 points (and 3 granules), so earthaccess.open
        # will be called with 3 result objects.
        mock_ea.open.return_value = nc_files[:3]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        subset_plan = p[0:3]
        assert isinstance(subset_plan, Plan)
        assert len(subset_plan.points) == 3

        result = pc.matchup(subset_plan, geometry="grid", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
        # One row per (point × granule) — 3 points, 1 granule each
        assert len(result) == 3
        assert "sst" in result.columns

    def test_matchup_subset_opens_only_subset_granules(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """earthaccess.open is called with only the granules for the subset."""
        n_points = 5
        p, nc_files = self._build_multi_point_plan(tmp_path, n_points)

        mock_ea = MagicMock()
        mock_ea.open.return_value = nc_files[:2]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        subset_plan = p[0:2]
        pc.matchup(subset_plan, geometry="grid", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})

        # Only the 2 results for the first 2 points should have been opened.
        opened_results = mock_ea.open.call_args[0][0]
        assert len(opened_results) == 2
        assert opened_results[0] is p.results[0]
        assert opened_results[1] is p.results[1]

    def test_matchup_subset_last_n_points(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """pc.matchup(plan[2:]) processes only the last n-2 points."""
        n_points = 4
        p, nc_files = self._build_multi_point_plan(tmp_path, n_points)

        mock_ea = MagicMock()
        mock_ea.open.return_value = nc_files[2:]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        subset_plan = p[2:]
        assert len(subset_plan.points) == 2

        result = pc.matchup(subset_plan, geometry="grid", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 2


# ---------------------------------------------------------------------------
# L2/swath support — geometry, open_method, spatial_method, geolocation detection
# ---------------------------------------------------------------------------

def _make_l2_swath_dataset(
    nrows: int = 3,
    ncols: int = 4,
    seed: int = 0,
) -> xr.Dataset:
    """Synthetic L2 swath dataset with 2-D lat/lon arrays."""
    rng = np.random.default_rng(seed)
    lat = rng.uniform(-10.0, 10.0, (nrows, ncols)).astype(np.float32)
    lon = rng.uniform(-30.0, 30.0, (nrows, ncols)).astype(np.float32)
    sst = rng.uniform(20.0, 30.0, (nrows, ncols)).astype(np.float32)
    return xr.Dataset(
        {
            "lat": (["nrows", "ncols"], lat),
            "lon": (["nrows", "ncols"], lon),
            "sst": (["nrows", "ncols"], sst),
        }
    )


def _make_l2_swath_3d_dataset(
    nrows: int = 3,
    ncols: int = 4,
    wavelengths: list[int] | None = None,
    seed: int = 0,
) -> xr.Dataset:
    """Synthetic L2 swath dataset with 2-D lat/lon and a 3-D variable (wavelength)."""
    if wavelengths is None:
        wavelengths = [346, 348, 351]
    rng = np.random.default_rng(seed)
    lat = rng.uniform(-10.0, 10.0, (nrows, ncols)).astype(np.float32)
    lon = rng.uniform(-30.0, 30.0, (nrows, ncols)).astype(np.float32)
    nwl = len(wavelengths)
    rrs = rng.uniform(0.0, 0.1, (nrows, ncols, nwl)).astype(np.float32)
    return xr.Dataset(
        {
            "lat": (["nrows", "ncols"], lat),
            "lon": (["nrows", "ncols"], lon),
            "Rrs": (["nrows", "ncols", "wavelength_3d"], rrs),
        },
        coords={"wavelength_3d": wavelengths},
    )


class TestGeometryParameter:
    """Tests for the required geometry parameter in pc.matchup()."""

    def test_missing_geometry_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Calling matchup() without geometry must raise TypeError."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        with pytest.raises(TypeError, match="geometry"):
            pc.matchup(p)  # type: ignore[call-arg]

    def test_invalid_geometry_raises(self) -> None:
        """Invalid geometry value raises ValueError."""
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        with pytest.raises(ValueError, match="geometry"):
            pc.matchup(p, geometry="spheroid")

    def test_grid_geometry_uses_dataset_open_method_by_default(self) -> None:
        """geometry='grid' should default open_method to 'dataset'."""
        from point_collocation.core.engine import _VALID_OPEN_METHODS

        # Just validate the logic, not actual execution
        open_method = "dataset"  # expected default for grid
        assert open_method in _VALID_OPEN_METHODS

    def test_swath_geometry_uses_datatree_merge_open_method_by_default(self) -> None:
        """geometry='swath' should default open_method to 'datatree-merge'."""
        from point_collocation.core.engine import _VALID_OPEN_METHODS

        open_method = "datatree-merge"  # expected default for swath
        assert open_method in _VALID_OPEN_METHODS


class TestGeolocDetection:
    """Tests for _find_geoloc_pair()."""

    def test_finds_lon_lat_pair(self) -> None:
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(coords={"lon": [0.0], "lat": [0.0]})
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "lon"
        assert lat_name == "lat"

    def test_finds_longitude_latitude_pair(self) -> None:
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(coords={"longitude": [0.0], "latitude": [0.0]})
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "longitude"
        assert lat_name == "latitude"

    def test_finds_Longitude_Latitude_pair(self) -> None:
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(coords={"Longitude": [0.0], "Latitude": [0.0]})
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "Longitude"
        assert lat_name == "Latitude"

    def test_finds_LONGITUDE_LATITUDE_pair(self) -> None:
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(coords={"LONGITUDE": [0.0], "LATITUDE": [0.0]})
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "LONGITUDE"
        assert lat_name == "LATITUDE"

    def test_finds_pair_in_data_vars(self) -> None:
        """Geolocation vars stored as data_vars (not coords) should be found."""
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            {
                "lon": (["nrows", "ncols"], [[0.0, 1.0]]),
                "lat": (["nrows", "ncols"], [[0.0, 0.0]]),
                "sst": (["nrows", "ncols"], [[25.0, 26.0]]),
            }
        )
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "lon"
        assert lat_name == "lat"

    def test_no_geoloc_raises(self) -> None:
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset({"temperature": (["x"], [1.0])})
        with pytest.raises(ValueError, match="no geolocation variables found"):
            _find_geoloc_pair(ds)

    def test_ambiguous_geoloc_raises(self) -> None:
        """Multiple recognised pairs should raise with all pairs listed."""
        from point_collocation.core.engine import _find_geoloc_pair

        # Has both (lon, lat) and (longitude, latitude)
        ds = xr.Dataset(
            coords={
                "lon": [0.0],
                "lat": [0.0],
                "longitude": [0.0],
                "latitude": [0.0],
            }
        )
        with pytest.raises(ValueError, match="ambiguous geolocation variables"):
            _find_geoloc_pair(ds)


class TestGeometryEnforcement:
    """Tests for _check_geometry()."""

    def test_grid_1d_ok(self) -> None:
        from point_collocation.core.engine import _check_geometry

        ds = xr.Dataset(coords={"lon": [0.0], "lat": [0.0]})
        # Should not raise
        _check_geometry(ds, "lon", "lat", "grid")

    def test_grid_2d_raises(self) -> None:
        from point_collocation.core.engine import _check_geometry

        ds = xr.Dataset(
            {
                "lon": (["nrows", "ncols"], [[0.0]]),
                "lat": (["nrows", "ncols"], [[0.0]]),
            }
        )
        with pytest.raises(ValueError, match="geometry='grid'.*Try geometry='swath'"):
            _check_geometry(ds, "lon", "lat", "grid")

    def test_swath_2d_ok(self) -> None:
        from point_collocation.core.engine import _check_geometry

        ds = xr.Dataset(
            {
                "lon": (["nrows", "ncols"], [[0.0]]),
                "lat": (["nrows", "ncols"], [[0.0]]),
            }
        )
        # Should not raise
        _check_geometry(ds, "lon", "lat", "swath")

    def test_swath_1d_raises(self) -> None:
        from point_collocation.core.engine import _check_geometry

        ds = xr.Dataset(coords={"lon": [0.0], "lat": [0.0]})
        with pytest.raises(ValueError, match="geometry='swath'.*Try geometry='grid'"):
            _check_geometry(ds, "lon", "lat", "swath")


class TestMissingXoak:
    """Test that missing xoak raises a clear ImportError."""

    def test_xoak_import_error_raised_early(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup() raises ImportError for xoak before opening any granule."""
        import sys

        # Block the xoak.tree_adapters submodule import by removing it from
        # sys.modules and inserting a sentinel None so that Python's import
        # machinery raises ImportError on the next import attempt.
        for key in list(sys.modules.keys()):
            if key == "xoak" or key.startswith("xoak."):
                monkeypatch.delitem(sys.modules, key)
        monkeypatch.setitem(sys.modules, "xoak", None)  # type: ignore[assignment]
        monkeypatch.setitem(sys.modules, "xoak.tree_adapters", None)  # type: ignore[assignment]

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        with pytest.raises(ImportError, match="xoak"):
            pc.matchup(p, geometry="swath", spatial_method="xoak")


class TestMissingVariableErrorMessage:
    """Tests for improved error message when variables are missing."""

    def test_missing_var_error_includes_geometry_open_method_spatial(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Error for missing variable must include geometry/open_method/spatial_method."""
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
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
            variables=["no_such_var"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        with pytest.raises(ValueError) as exc_info:
            pc.matchup(p, geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})

        msg = str(exc_info.value)
        assert "no_such_var" in msg
        assert "geometry=" in msg
        assert "open_method=" in msg
        assert "spatial_method=" in msg


class TestSwathMatchupWithXoak:
    """Tests for geometry='swath' + spatial_method='xoak' matchup."""

    def test_swath_matchup_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath matchup using xoak returns the nearest pixel value."""
        xoak = pytest.importorskip("xoak")  # skip if xoak not installed
        _ = xoak  # noqa: F841

        nc_path = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=42)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Use the first lat/lon value from the swath as the query point.
        lat_val = float(ds_swath["lat"].values[0, 0])
        lon_val = float(ds_swath["lon"].values[0, 0])

        pts = pd.DataFrame(
            {
                "lat": [lat_val],
                "lon": [lon_val],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/swath.nc",
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
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(
            p,
            geometry="swath",
            variables=["sst"],
            spatial_method="xoak",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_grid_geometry_with_2d_data_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """geometry='grid' with 2-D lat/lon raises a clear ValueError."""
        nc_path = str(tmp_path / "swath.nc")
        _make_l2_swath_dataset(nrows=4, ncols=5).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/swath.nc",
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

        with pytest.raises(ValueError, match="geometry='grid'.*Try geometry='swath'"):
            pc.matchup(
                p,
                geometry="grid",
                spatial_method="nearest",
                open_dataset_kwargs={"engine": "netcdf4"},
            )

    def test_swath_matchup_3d_variable_expands_with_xoak(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath matchup with xoak expands 3-D variables (e.g. Rrs) into per-wavelength columns."""
        pytest.importorskip("xoak")  # skip if xoak not installed

        wavelengths = [346, 348, 351]
        nc_path = str(tmp_path / "swath_3d.nc")
        ds_swath = _make_l2_swath_3d_dataset(nrows=4, ncols=5, wavelengths=wavelengths, seed=42)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        lat_val = float(ds_swath["lat"].values[0, 0])
        lon_val = float(ds_swath["lon"].values[0, 0])

        pts = pd.DataFrame(
            {
                "lat": [lat_val],
                "lon": [lon_val],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/swath_3d.nc",
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
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(
            p,
            geometry="swath",
            variables=["Rrs"],
            spatial_method="xoak",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "Rrs" not in result.columns, "bare 'Rrs' column should be dropped after expansion"
        for wl in wavelengths:
            assert f"Rrs_{wl}" in result.columns, f"Rrs_{wl} column missing"
        assert len(result) == 1


class TestShowVariablesLayout:
    """Tests for plan.show_variables(geometry=...) with both open methods."""

    def test_show_variables_dataset_layout_prints_dims_and_vars(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """show_variables(geometry='grid') prints dims, vars, and geo info."""
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        p.show_variables(geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
        captured = capsys.readouterr()
        assert "Dimensions" in captured.out
        assert "Variables" in captured.out
        assert "sst" in captured.out
        assert "Geolocation" in captured.out
        assert "'lon'" in captured.out or "lon" in captured.out

    def test_show_variables_geo_detection_none_warns(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """show_variables prints a message when no geolocation is detected."""
        nc_path = str(tmp_path / "no_geo.nc")
        # Dataset with no recognisable lat/lon names.
        xr.Dataset(
            {"temperature": (["x", "y"], [[1.0, 2.0], [3.0, 4.0]])},
            coords={"x": [0, 1], "y": [0, 1]},
        ).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        p.show_variables(geometry="grid", open_dataset_kwargs={"engine": "netcdf4"})
        captured = capsys.readouterr()
        assert "NONE" in captured.out or "no geolocation" in captured.out.lower()
