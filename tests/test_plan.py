"""Tests for pc.plan() and pc.matchup(plan) plan-based execution."""

from __future__ import annotations

import datetime
import math
import pathlib
import re
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
    _get_polygon_points,
    _get_umm,
    _match_points_to_granules,
    _parse_time_buffer,
    _plan_normalise_time,
    _point_in_polygon,
    _search_earthaccess,
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
) -> MagicMock:
    """Build a minimal mock earthaccess result supporting dict-access and data_links().

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

    umm: dict = {
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
    mock_result = MagicMock()
    mock_result.__getitem__ = lambda _, key: {"umm": umm}[key]
    mock_result.data_links.return_value = [data_url]
    return mock_result


def _make_global_result(begin: str, end: str, data_url: str = "https://example.com/g.nc") -> MagicMock:
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


class TestExtractGranuleMetaUsesDataLinks:
    """_extract_granule_meta must use result.data_links() for the granule URL."""

    def _make_umm(self, data_url: str) -> dict:
        return {
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": "2023-06-01T00:00:00Z",
                    "EndingDateTime": "2023-06-01T23:59:59Z",
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

    def test_uses_data_links_when_available(self) -> None:
        """granule_id must come from data_links(), not from UMM RelatedUrls."""
        umm_url = "https://umm.example.com/umm_granule.nc"
        links_url = "https://links.example.com/links_granule.nc"

        result = MagicMock()
        result.__getitem__ = lambda _, key: {"umm": self._make_umm(umm_url)}[key]
        result.data_links.return_value = [links_url]

        meta = _extract_granule_meta(result, result_index=0)
        assert meta.granule_id == links_url

    def test_prefers_https_over_s3_from_data_links(self) -> None:
        """When data_links() returns both S3 and HTTPS, prefer HTTPS."""
        result = MagicMock()
        result.__getitem__ = lambda _, key: {"umm": self._make_umm("https://umm.example.com/g.nc")}[key]
        result.data_links.return_value = [
            "s3://bucket/granule.nc",
            "https://https.example.com/granule.nc",
        ]

        meta = _extract_granule_meta(result, result_index=0)
        assert meta.granule_id == "https://https.example.com/granule.nc"

    def test_forwards_data_links_kwargs(self) -> None:
        """data_links_kwargs (access, in_region) are forwarded to data_links()."""
        links_url = "s3://bucket/granule.nc"

        result = MagicMock()
        result.__getitem__ = lambda _, key: {"umm": self._make_umm("https://umm.example.com/g.nc")}[key]
        result.data_links.return_value = [links_url]

        meta = _extract_granule_meta(result, result_index=0, data_links_kwargs={"access": "direct", "in_region": True})
        assert meta.granule_id == links_url
        result.data_links.assert_called_once_with(access="direct", in_region=True)


class TestSearchEarthaccessFiltering:
    """_search_earthaccess must skip granules with empty data_links() and forward data_links kwargs."""

    def _make_points(self) -> pd.DataFrame:
        return pd.DataFrame({
            "lat": [0.0],
            "lon": [0.0],
            "time": pd.to_datetime(["2024-01-01T00:00:00"]),
        })

    def _make_mock_result(self, url: str | None) -> MagicMock:
        """Build a minimal mock earthaccess result."""
        result = MagicMock()
        result.__getitem__ = lambda _, key: {
            "umm": {
                "TemporalExtent": {
                    "RangeDateTime": {
                        "BeginningDateTime": "2024-01-01T00:00:00Z",
                        "EndingDateTime": "2024-01-01T23:59:59Z",
                    }
                },
                "SpatialExtent": {
                    "HorizontalSpatialDomain": {
                        "Geometry": {
                            "BoundingRectangles": [{
                                "WestBoundingCoordinate": -180.0,
                                "SouthBoundingCoordinate": -90.0,
                                "EastBoundingCoordinate": 180.0,
                                "NorthBoundingCoordinate": 90.0,
                            }]
                        }
                    }
                },
                "RelatedUrls": [{"Type": "GET DATA", "URL": url}] if url else [],
            }
        }[key]
        result.data_links.return_value = [url] if url else []
        return result

    def test_granules_with_empty_data_links_are_excluded(self) -> None:
        """Granules whose data_links() returns [] are silently excluded from the plan."""
        good = self._make_mock_result("https://example.com/good.nc")
        empty = self._make_mock_result(None)

        with patch("earthaccess.search_data", return_value=[good, empty]):
            results, metas = _search_earthaccess(
                self._make_points(),
                source_kwargs={"short_name": "TEST"},
            )

        assert len(results) == 1
        assert len(metas) == 1
        assert metas[0].granule_id == "https://example.com/good.nc"

    def test_access_and_in_region_forwarded_to_data_links(self) -> None:
        """'access' and 'in_region' from source_kwargs are passed to data_links()."""
        s3_url = "s3://bucket/granule.nc"
        result = self._make_mock_result(None)
        result.data_links.side_effect = lambda **kw: [s3_url] if kw.get("access") == "direct" else []

        with patch("earthaccess.search_data", return_value=[result]):
            results, metas = _search_earthaccess(
                self._make_points(),
                source_kwargs={"short_name": "TEST", "access": "direct"},
            )

        assert len(results) == 1
        assert metas[0].granule_id == s3_url
        # Verify access="direct" was forwarded to data_links().
        result.data_links.assert_called_with(access="direct")

    def test_access_and_in_region_not_passed_to_search_data(self) -> None:
        """'access' and 'in_region' must not be forwarded to earthaccess.search_data()."""
        result = self._make_mock_result("https://example.com/g.nc")

        with patch("earthaccess.search_data", return_value=[result]) as mock_search:
            _search_earthaccess(
                self._make_points(),
                source_kwargs={"short_name": "TEST", "access": "direct", "in_region": True},
            )

        call_kwargs = mock_search.call_args[1]
        assert "access" not in call_kwargs
        assert "in_region" not in call_kwargs


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

    def test_plan_raises_without_collection_identifier(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_ea = MagicMock()
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        with pytest.raises(ValueError, match="short_name.*concept_id.*doi"):
            plan(pts, source_kwargs={})

    def test_plan_accepts_concept_id(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        result = plan(pts, source_kwargs={"concept_id": "C1234567890-PODAAC"})
        assert isinstance(result, Plan)

    def test_plan_accepts_doi(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mock_ea = MagicMock()
        mock_ea.search_data.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        result = plan(pts, source_kwargs={"doi": "10.5067/PACE/OCI/L3M/RRS/2.0"})
        assert isinstance(result, Plan)


class TestPlanMapping:
    """Tests for the point→granule mapping built by pc.plan()."""

    def _run_plan(
        self,
        monkeypatch: pytest.MonkeyPatch,
        points: pd.DataFrame,
        fake_results: list[Any],
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


def _make_l3_time_dataset(
    lats: list[float],
    lons: list[float],
    times: list[str],
    seed: int = 0,
) -> xr.Dataset:
    """Synthetic L3 dataset with a time dimension: sst has shape (time, lat, lon)."""
    rng = np.random.default_rng(seed)
    lat_arr = np.array(lats)
    lon_arr = np.array(lons)
    time_arr = pd.to_datetime(times)
    sst = rng.uniform(20.0, 30.0, (len(time_arr), lat_arr.size, lon_arr.size)).astype(np.float32)
    return xr.Dataset(
        {"sst": (["time", "lat", "lon"], sst)},
        coords={"time": time_arr, "lat": lat_arr, "lon": lon_arr},
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

        pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        mock_ea.search_data.assert_not_called()

    def test_matchup_with_plan_calls_open(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup(plan) must call earthaccess.open once per batch with that batch's results."""
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

        pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(p, open_method="dataset")  # no variables kwarg
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

        result = pc.matchup(p, open_method="dataset")
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

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
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
        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"chunks": {}, "engine": "netcdf4"})
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
        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"chunks": {}, "engine": "netcdf4"})
        assert len(result) == 1
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"]), (
            "2D variable must return a value when chunks={} (dask) is used, not NaN"
        )

    def test_matchup_silent_false_prints_progress(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """silent=False prints progress after each batch."""
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

        pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"}, silent=False)
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

        pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"}, silent=True)
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
            open_method="dataset",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=True,
            batch_size=1,
            save_dir=save_dir,
        )

        parquet_files = sorted(save_dir.glob("plan_*.parquet"))
        assert len(parquet_files) == 2, f"Expected 2 parquet files, got {len(parquet_files)}"
        # File names must use 1-based granule numbers matching the progress messages.
        assert parquet_files[0].name == "plan_1_1.parquet"
        assert parquet_files[1].name == "plan_2_2.parquet"

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
            open_method="dataset",
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
            open_method="dataset",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=False,
            batch_size=1,
            spatial_method="nearest",
        )
        captured = capsys.readouterr()
        lines = [ln for ln in captured.out.splitlines() if ln.strip()]
        assert len(lines) == 3
        # Each line must follow the documented format
        for line in lines:
            assert "granules" in line
            assert "of 3 processed" in line
            assert "points matched" in line
            assert re.search(r"\d{2}:\d{2}:\d{2}$", line), f"Expected HH:MM:SS at end of: {line!r}"

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
        pc.matchup(p, open_method="dataset", silent=True, save_dir=new_dir)
        assert new_dir.exists()

    def test_matchup_opens_files_per_batch_not_all_at_once(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """earthaccess.open must be called once per batch, not once for all granules.

        This is the core memory-management invariant: opening every file upfront
        causes peak RAM to grow with the total number of granules.  Opening only
        the batch's files lets the OS reclaim handles between batches.
        """
        # Build 3 granule files
        nc_files = []
        for i in range(3):
            nc_path = str(tmp_path / f"g{i}.nc")
            _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=i).to_netcdf(nc_path)
            nc_files.append(nc_path)

        open_call_args: list[list] = []

        mock_ea = MagicMock()

        def fake_open(results, **kwargs):
            open_call_args.append(list(results))
            # Return one file path per result in this batch
            return [nc_files[i] for i in range(len(results))]

        mock_ea.open.side_effect = fake_open
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        fake_results = [object(), object(), object()]
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
            results=fake_results,
            granules=granules,
            point_granule_map={0: [0], 1: [1], 2: [2]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # batch_size=1 → earthaccess.open called 3 times, once per granule
        pc.matchup(
            p,
            open_method="dataset",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=True,
            batch_size=1,
        )

        assert mock_ea.open.call_count == 3, (
            "earthaccess.open should be called once per batch, not once for all granules"
        )
        # Each call should have received exactly 1 result (batch_size=1)
        for call_args in open_call_args:
            assert len(call_args) == 1, (
                f"each open() call should pass 1 result for batch_size=1, got {len(call_args)}"
            )
        # The results passed to each call must be the per-batch results
        assert open_call_args[0] == [fake_results[0]]
        assert open_call_args[1] == [fake_results[1]]
        assert open_call_args[2] == [fake_results[2]]

    def test_swath_gc_called_per_granule_not_per_batch(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """gc.collect() must be called once per granule for datatree-merge (swath).

        With batch_size larger than the number of granules, all granules fall
        into one batch.  DataTree nodes hold parent-child reference cycles that
        Python's reference counting cannot free; they accumulate until
        gc.collect() runs.  The fix calls gc.collect() after *each* granule
        (not just after each batch) so that peak memory is bounded regardless
        of batch_size.
        """
        xoak = pytest.importorskip("xoak")  # noqa: F841

        n = 3
        nc_files = []
        for i in range(n):
            nc_path = str(tmp_path / f"swath{i}.nc")
            _make_l2_swath_dataset(nrows=4, ncols=5, seed=i).to_netcdf(nc_path, engine="netcdf4")
            nc_files.append(nc_path)

        mock_ea = MagicMock()

        def fake_open(results, **kwargs):
            return [nc_files[plan.results.index(r)] for r in results]

        mock_ea.open.side_effect = fake_open
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        fake_results = [object(), object(), object()]
        lat0 = -5.0
        lon0 = -10.0
        pts = pd.DataFrame(
            {
                "lat": [lat0] * n,
                "lon": [lon0] * n,
                "time": pd.to_datetime(
                    [f"2023-06-0{i+1}T12:00:00" for i in range(n)]
                ),
            }
        )
        granules = [
            GranuleMeta(
                granule_id=f"https://example.com/swath{i}.nc",
                begin=pd.Timestamp(f"2023-06-0{i+1}T00:00:00Z"),
                end=pd.Timestamp(f"2023-06-0{i+1}T23:59:59Z"),
                bbox=(-30.0, -10.0, 30.0, 10.0),
                result_index=i,
            )
            for i in range(n)
        ]
        plan = Plan(
            points=pts,
            results=fake_results,
            granules=granules,
            point_granule_map={0: [0], 1: [1], 2: [2]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        gc_call_count = 0

        original_gc_collect = __import__("gc").collect

        def counting_gc_collect(*args, **kwargs):
            nonlocal gc_call_count
            gc_call_count += 1
            return original_gc_collect(*args, **kwargs)

        import point_collocation.core.engine as engine_mod

        monkeypatch.setattr(engine_mod.gc, "collect", counting_gc_collect)

        pc.matchup(
            plan,
            open_method="datatree-merge",
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=True,
            batch_size=1000,  # all 3 granules in one batch
        )

        # gc.collect() should have been called at least once per granule
        # (n per-granule calls inside the inner loop) plus once per batch
        # (1 call at the end of the batch loop) = n + 1 total.
        assert gc_call_count >= n, (
            f"gc.collect() should be called at least {n} times (once per granule) "
            f"for datatree-merge, but was called {gc_call_count} times. "
            "Without per-granule GC, DataTree reference cycles accumulate across "
            "the entire batch, causing memory to scale with batch_size."
        )

    def test_grid_gc_called_per_granule_not_per_batch(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """gc.collect() must be called once per granule for dataset (grid).

        With batch_size larger than the number of granules, all granules fall
        into one batch.  xarray datasets opened with dask (chunks={}) hold
        internal reference cycles that Python's reference counting cannot free;
        they accumulate until gc.collect() runs.  The fix calls gc.collect()
        after *each* granule (not just after each batch) so that peak memory is
        bounded regardless of batch_size.
        """
        n = 3
        nc_files = []
        for i in range(n):
            nc_path = str(tmp_path / f"grid{i}.nc")
            _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=i).to_netcdf(nc_path)
            nc_files.append(nc_path)

        mock_ea = MagicMock()

        def fake_open(results, **kwargs):
            return [nc_files[plan.results.index(r)] for r in results]

        mock_ea.open.side_effect = fake_open
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        fake_results = [object(), object(), object()]
        pts = pd.DataFrame(
            {
                "lat": [0.0] * n,
                "lon": [0.0] * n,
                "time": pd.to_datetime(
                    [f"2023-06-0{i+1}T12:00:00" for i in range(n)]
                ),
            }
        )
        granules = [
            GranuleMeta(
                granule_id=f"https://example.com/grid{i}.nc",
                begin=pd.Timestamp(f"2023-06-0{i+1}T00:00:00Z"),
                end=pd.Timestamp(f"2023-06-0{i+1}T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=i,
            )
            for i in range(n)
        ]
        plan = Plan(
            points=pts,
            results=fake_results,
            granules=granules,
            point_granule_map={0: [0], 1: [1], 2: [2]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        gc_call_count = 0

        original_gc_collect = __import__("gc").collect

        def counting_gc_collect(*args, **kwargs):
            nonlocal gc_call_count
            gc_call_count += 1
            return original_gc_collect(*args, **kwargs)

        import point_collocation.core.engine as engine_mod

        monkeypatch.setattr(engine_mod.gc, "collect", counting_gc_collect)

        pc.matchup(
            plan,
            open_method="dataset",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=True,
            batch_size=1000,  # all 3 granules in one batch
        )

        # gc.collect() should have been called at least once per granule
        # (n per-granule calls inside the inner loop) plus once per batch
        # (1 call at the end of the batch loop) = n + 1 total.
        assert gc_call_count >= n, (
            f"gc.collect() should be called at least {n} times (once per granule) "
            f"for dataset (grid), but was called {gc_call_count} times. "
            "Without per-granule GC, xarray+dask reference cycles accumulate across "
            "the entire batch, causing memory to scale with batch_size."
        )


# ---------------------------------------------------------------------------
# pc_id, granule_lat/lon/time columns and new defaults
# ---------------------------------------------------------------------------


class TestNewOutputColumns:
    """Tests for pc_id, granule_lat, granule_lon, granule_time columns."""

    def _make_plan_single(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        nc_path: str,
    ) -> "Plan":
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
        return Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def _make_plan_zero_match(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> "Plan":
        mock_ea = MagicMock()
        mock_ea.open.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        return Plan(
            points=pts,
            results=[],
            granules=[],
            point_granule_map={0: []},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_pc_id_present_in_matched_output(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """pc_id column contains the original row index for matched points."""
        nc_path = str(tmp_path / "g.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)
        p = self._make_plan_single(tmp_path, monkeypatch, nc_path)

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert "pc_id" in result.columns
        assert result.loc[0, "pc_id"] == 0

    def test_pc_id_present_in_zero_match_output(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """pc_id column is present even for points with zero matching granules."""
        p = self._make_plan_zero_match(monkeypatch)

        result = pc.matchup(p, open_method="dataset")
        assert "pc_id" in result.columns
        assert result.loc[0, "pc_id"] == 0

    def test_pc_id_tracks_original_row_index_for_multi_granule_match(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """pc_id identifies which original point row each matchup row belongs to."""
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

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 2
        # Both output rows trace back to the same input point (row 0)
        assert list(result["pc_id"]) == [0, 0]

    def test_granule_lat_lon_present_in_matched_output(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_lat and granule_lon are the nearest-neighbour grid positions."""
        nc_path = str(tmp_path / "g.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)
        p = self._make_plan_single(tmp_path, monkeypatch, nc_path)

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert "granule_lat" in result.columns
        assert "granule_lon" in result.columns
        # Point is at (0, 0); the nearest grid point in [-90, 0, 90] x [-180, 0, 180]
        # is exactly (0.0, 0.0).
        assert result.loc[0, "granule_lat"] == pytest.approx(0.0)
        assert result.loc[0, "granule_lon"] == pytest.approx(0.0)

    def test_granule_time_from_granule_metadata(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_time is the midpoint of the granule's metadata begin/end times."""
        nc_path = str(tmp_path / "g.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)
        # _make_plan_single uses begin=2023-06-01T00:00:00Z, end=2023-06-01T23:59:59Z
        p = self._make_plan_single(tmp_path, monkeypatch, nc_path)
        begin = pd.Timestamp("2023-06-01T00:00:00Z")
        end = pd.Timestamp("2023-06-01T23:59:59Z")
        expected_time = begin + (end - begin) / 2

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert "granule_time" in result.columns
        assert result.loc[0, "granule_time"] == expected_time

    def test_granule_time_from_metadata_not_dataset_time_coord(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_time uses granule metadata, not the dataset's time coordinate."""
        nc_path = str(tmp_path / "g.nc")
        ds = _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0])
        # Add a scalar time coordinate to the dataset (different from the metadata midpoint).
        ds = ds.assign_coords(time=pd.Timestamp("2000-01-01T00:00:00"))
        ds.to_netcdf(nc_path)

        p = self._make_plan_single(tmp_path, monkeypatch, nc_path)
        begin = pd.Timestamp("2023-06-01T00:00:00Z")
        end = pd.Timestamp("2023-06-01T23:59:59Z")
        expected_time = begin + (end - begin) / 2

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert "granule_time" in result.columns
        # Must match the metadata midpoint, not the dataset's time coordinate.
        assert result.loc[0, "granule_time"] == expected_time

    def test_granule_lat_lon_nan_for_zero_match_points(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_lat, granule_lon are NaN and granule_time is NaT for zero-match rows."""
        p = self._make_plan_zero_match(monkeypatch)

        result = pc.matchup(p, open_method="dataset")
        assert math.isnan(result.loc[0, "granule_lat"])
        assert math.isnan(result.loc[0, "granule_lon"])
        assert pd.isnull(result.loc[0, "granule_time"])

    def test_default_silent_is_true(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """matchup() is silent by default (no progress output)."""
        nc_path = str(tmp_path / "g.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)
        p = self._make_plan_single(tmp_path, monkeypatch, nc_path)

        pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_default_batch_size_processes_all_in_one_batch(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """Default batch_size puts all granules into a single batch (one progress line)."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        nc_c = str(tmp_path / "c.nc")
        for i, nc_path in enumerate([nc_a, nc_b, nc_c]):
            _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=i).to_netcdf(nc_path)

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b, nc_c]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0, 0.0, 0.0],
                "lon": [0.0, 0.0, 0.0],
                "time": pd.to_datetime([
                    "2023-06-01T12:00:00",
                    "2023-06-02T12:00:00",
                    "2023-06-03T12:00:00",
                ]),
            }
        )
        gms = [
            GranuleMeta(
                granule_id=f"https://example.com/{i}.nc",
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
            granules=gms,
            point_granule_map={0: [0], 1: [1], 2: [2]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # With silent=False and default batch_size, only one progress line should appear
        # (all 3 granules processed in a single batch).
        pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"},
                   silent=False, spatial_method="nearest")
        captured = capsys.readouterr()
        lines = [ln for ln in captured.out.strip().splitlines() if ln.strip()]
        assert len(lines) == 1, (
            f"Expected 1 progress line (all granules in one batch), got {len(lines)}: {lines}"
        )


# ---------------------------------------------------------------------------
# User-supplied pc_id and extra columns
# ---------------------------------------------------------------------------


class TestUserPcId:
    """Tests for user-supplied pc_id and extra column behaviour."""

    def _make_plan_multi_point(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        points: pd.DataFrame,
        nc_paths: list[str],
        granule_metas: list[GranuleMeta],
        point_granule_map: dict,
    ) -> "Plan":
        mock_ea = MagicMock()
        mock_ea.open.return_value = nc_paths
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        return Plan(
            points=points,
            results=[object() for _ in nc_paths],
            granules=granule_metas,
            point_granule_map=point_granule_map,
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_user_pc_id_is_used_in_output(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When pc_id is in the input df, output uses those values."""
        nc_path = str(tmp_path / "g.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)

        pts = pd.DataFrame(
            {
                "lat": [0.0],
                "lon": [0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
                "pc_id": [42],
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/g.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = self._make_plan_multi_point(
            tmp_path, monkeypatch, pts, [nc_path], [gm], {0: [0]}
        )

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert "pc_id" in result.columns
        assert result.loc[0, "pc_id"] == 42

    def test_user_pc_id_zero_match(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Zero-match row preserves user-provided pc_id."""
        mock_ea = MagicMock()
        mock_ea.open.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0],
                "lon": [0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
                "pc_id": [99],
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

        result = pc.matchup(p, open_method="dataset")
        assert result.loc[0, "pc_id"] == 99

    def test_user_pc_id_duplicate_raises(self) -> None:
        """Duplicate pc_id values in input df raise a ValueError."""
        pts = pd.DataFrame(
            {
                "lat": [0.0, 1.0],
                "lon": [0.0, 1.0],
                "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
                "pc_id": [5, 5],
            }
        )
        with pytest.raises(ValueError, match="pc_id.*duplicate"):
            from point_collocation.core.plan import _plan_validate_points
            _plan_validate_points(pts)

    def test_extra_columns_retained_in_output(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Extra columns in the user df appear in the matchup output."""
        nc_path = str(tmp_path / "g.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path)

        pts = pd.DataFrame(
            {
                "lat": [0.0],
                "lon": [0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
                "pc_id": [10],
                "pc_label": ["pace_10"],
                "station_name": ["station_A"],
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/g.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = self._make_plan_multi_point(
            tmp_path, monkeypatch, pts, [nc_path], [gm], {0: [0]}
        )

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert "pc_label" in result.columns
        assert result.loc[0, "pc_label"] == "pace_10"
        assert "station_name" in result.columns
        assert result.loc[0, "station_name"] == "station_A"

    def test_output_sorted_by_pc_id_order_without_user_pc_id(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Without user pc_id, output rows are in the same order as the input points."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=0).to_netcdf(nc_a)
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(nc_b)

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Two points: point 0 matches granule 1, point 1 matches granule 0
        # Without sorting the output would be in granule order (point 1, point 0).
        pts = pd.DataFrame(
            {
                "lat": [0.0, 0.0],
                "lon": [0.0, 0.0],
                "time": pd.to_datetime(["2023-06-02T12:00:00", "2023-06-01T12:00:00"]),
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
            # point 0 → granule 1 (b), point 1 → granule 0 (a)
            point_granule_map={0: [1], 1: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        # Output should be sorted by point order: pc_id 0 first, pc_id 1 second
        assert list(result["pc_id"]) == [0, 1]

    def test_output_sorted_by_user_pc_id_order(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With user pc_id, output rows follow the user's pc_id input order."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=0).to_netcdf(nc_a)
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(nc_b)

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0, 0.0],
                "lon": [0.0, 0.0],
                "time": pd.to_datetime(["2023-06-02T12:00:00", "2023-06-01T12:00:00"]),
                "pc_id": [20, 10],
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
            # point 0 (pc_id=20) → granule 1 (b), point 1 (pc_id=10) → granule 0 (a)
            point_granule_map={0: [1], 1: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        # Output should preserve user's pc_id order: 20 first (it was first in input), 10 second
        assert list(result["pc_id"]) == [20, 10]

    def test_multi_granule_same_pc_id_stays_grouped(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A point with user pc_id matching multiple granules produces adjacent rows."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=0).to_netcdf(nc_a)
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=1).to_netcdf(nc_b)

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_a, nc_b]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0],
                "lon": [0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
                "pc_id": [77],
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

        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 2
        assert list(result["pc_id"]) == [77, 77]


# ---------------------------------------------------------------------------
# granule_range: crash recovery
# ---------------------------------------------------------------------------

class TestGranuleRange:
    """Tests for the granule_range parameter of pc.matchup()."""

    def _make_plan(self, tmp_path: pathlib.Path, n: int = 3) -> tuple["Plan", list[str]]:
        """Build a Plan with *n* daily granules, one point per granule."""
        nc_files = []
        for i in range(n):
            nc_path = str(tmp_path / f"g{i}.nc")
            _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=i).to_netcdf(nc_path)
            nc_files.append(nc_path)

        pts = pd.DataFrame(
            {
                "lat": [0.0] * n,
                "lon": [0.0] * n,
                "time": pd.to_datetime(
                    [f"2023-06-{i+1:02d}T12:00:00" for i in range(n)]
                ),
            }
        )
        granules = [
            GranuleMeta(
                granule_id=f"https://example.com/g{i}.nc",
                begin=pd.Timestamp(f"2023-06-{i+1:02d}T00:00:00Z"),
                end=pd.Timestamp(f"2023-06-{i+1:02d}T23:59:59Z"),
                bbox=(-180.0, -90.0, 180.0, 90.0),
                result_index=i,
            )
            for i in range(n)
        ]
        fake_results = [object() for _ in range(n)]
        p = Plan(
            points=pts,
            results=fake_results,
            granules=granules,
            point_granule_map={i: [i] for i in range(n)},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        return p, nc_files

    def _mock_ea(self, monkeypatch: pytest.MonkeyPatch, nc_files: list[str]) -> MagicMock:
        """Patch earthaccess so open() returns the right nc file per result index."""
        mock_ea = MagicMock()

        def fake_open(results, **kwargs):
            # results are the fake result objects; return nc_files in the same order
            return [nc_files[i] for i in range(len(results))]

        mock_ea.open.side_effect = fake_open
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        return mock_ea

    def test_granule_range_returns_only_specified_granules(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_range=(2, 3) should return rows for granules 2 and 3 only."""
        p, nc_files = self._make_plan(tmp_path, n=3)
        mock_ea = MagicMock()

        open_call_results: list[list] = []

        def fake_open(results, **kwargs):
            open_call_results.append(list(results))
            return [nc_files[i] for i in range(len(results))]

        mock_ea.open.side_effect = fake_open
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        df = pc.matchup(
            p,
            open_method="dataset",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=True,
            batch_size=10,
            granule_range=(2, 3),
        )

        # Only granules 2 and 3 (1-based) → 2 rows (one point each)
        assert len(df) == 2
        # earthaccess.open called once with 2 results (granules 2 and 3)
        assert mock_ea.open.call_count == 1
        assert len(open_call_results[0]) == 2
        # The results passed should be for granules at index 1 and 2 (0-based)
        assert open_call_results[0] == [p.results[1], p.results[2]]

    def test_granule_range_progress_shows_absolute_numbers(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """Progress messages must show absolute granule numbers, not relative."""
        p, nc_files = self._make_plan(tmp_path, n=3)
        self._mock_ea(monkeypatch, nc_files)

        pc.matchup(
            p,
            open_method="dataset",
            open_dataset_kwargs={"engine": "netcdf4"},
            silent=False,
            batch_size=1,
            granule_range=(2, 3),
            spatial_method="nearest",
        )
        captured = capsys.readouterr()
        lines = [ln for ln in captured.out.splitlines() if ln.strip()]
        # Two batches of 1 granule each → 2 progress lines
        assert len(lines) == 2
        # Lines must use absolute numbers (2, 3) and report against the full plan total (3)
        assert "granules 2-2 of 3 processed" in lines[0]
        assert "granules 3-3 of 3 processed" in lines[1]

    def test_granule_range_invalid_raises_value_error(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """granule_range values that are invalid raise ValueError."""
        mock_ea = MagicMock()
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p_no_granules = Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # start > end is caught before execution
        with pytest.raises(ValueError, match="granule_range"):
            pc.matchup(p_no_granules, open_method="dataset", silent=True, granule_range=(5, 2))

        # start < 1 is caught before execution
        with pytest.raises(ValueError, match="granule_range"):
            pc.matchup(p_no_granules, open_method="dataset", silent=True, granule_range=(0, 5))

        # start or end exceeds total matched granules (caught at execution time)
        p_with_granules, nc_files = self._make_plan(tmp_path, n=3)
        self._mock_ea(monkeypatch, nc_files)

        with pytest.raises(ValueError, match="granule_range"):
            pc.matchup(
                p_with_granules,
                open_method="dataset",
                open_dataset_kwargs={"engine": "netcdf4"},
                silent=True,
                granule_range=(2, 10),  # end=10 exceeds 3 matched granules
            )


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

        ds = p.open_dataset(0, open_method={"open_kwargs": {"engine": "netcdf4"}}, silent=True)
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
            ds = p.open_mfdataset(fake_results, open_method={"open_kwargs": {"engine": "netcdf4"}}, silent=True)

        assert ds is fake_ds
        mock_ea.open.assert_called_once_with(fake_results, pqdm_kwargs={"disable": True})
        mock_mfdataset.assert_called_once_with([nc_a, nc_b], chunks={}, engine="netcdf4", decode_timedelta=False)

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
            ds = p.open_mfdataset(subset, open_method={"open_kwargs": {"engine": "netcdf4"}}, silent=True)

        assert ds is fake_ds
        mock_ea.open.assert_called_once_with(fake_results, pqdm_kwargs={"disable": True})
        mock_mfdataset.assert_called_once_with([nc_a, nc_b], chunks={}, engine="netcdf4", decode_timedelta=False)

    def test_open_dataset_default_uses_auto(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset(result, open_method='dataset') uses xr.open_dataset."""
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

        ds = p.open_dataset(0, open_method={"xarray_open": "dataset", "open_kwargs": {"engine": "netcdf4"}}, silent=True)
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds
        ds.close()

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
            p.open_dataset(0, open_method="bad", silent=True)

    def test_open_mfdataset_with_open_method_dataset_uses_open_mfdataset(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_mfdataset(results, open_method='dataset') uses xr.open_mfdataset."""
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
            ds = p.open_mfdataset(fake_results, open_method={"xarray_open": "dataset", "open_kwargs": {"engine": "netcdf4"}}, silent=True)

        assert ds is fake_ds
        mock_mfd.assert_called_once_with([nc_a, nc_b], chunks={}, engine="netcdf4", decode_timedelta=False)

    def test_open_mfdataset_datatree_merge_concatenates(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_mfdataset(results, open_method='datatree-merge') opens each as DataTree-merge and concatenates."""
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
            open_method={"xarray_open": "datatree", "merge": "all", "open_kwargs": {"engine": "netcdf4"}},
            silent=True,
        )
        # Result is a Dataset with a "granule" dimension from concatenation.
        assert isinstance(ds, xr.Dataset)
        assert ds.sizes["granule"] == 2
        assert "sst" in ds

    def test_open_dataset_prints_effective_spec_when_not_silent(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """open_dataset prints the effective open_method spec when silent=False."""
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

        ds = p.open_dataset(0, open_method={"open_kwargs": {"engine": "netcdf4"}}, silent=False)
        ds.close()
        captured = capsys.readouterr()
        assert "open_method:" in captured.out
        # The effective spec includes the resolved defaults (chunks, decode_timedelta)
        assert "engine" in captured.out

    def test_open_dataset_silent_suppresses_output(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """open_dataset prints nothing when silent=True."""
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

        ds = p.open_dataset(0, open_method={"open_kwargs": {"engine": "netcdf4"}}, silent=True)
        ds.close()
        captured = capsys.readouterr()
        assert captured.out == ""

    def test_open_dataset_integer_index(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset(0) resolves the integer to plan.results[0]."""
        nc_path = str(tmp_path / "test.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        fake_result = object()
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        p = Plan(
            points=pts,
            results=[fake_result],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        # Using integer index 0 should behave identically to plan[0]
        ds = p.open_dataset(0, open_method={"open_kwargs": {"engine": "netcdf4"}}, silent=True)
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds
        ds.close()
        mock_ea.open.assert_called_once_with([fake_result], pqdm_kwargs={"disable": True})

    def test_open_dataset_integer_index_out_of_range_raises(self) -> None:
        """open_dataset raises IndexError for an out-of-range integer index."""
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

        with pytest.raises(IndexError, match="out of range"):
            p.open_dataset(5, silent=True)

    def test_open_dataset_with_datatree_preset_returns_datatree(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset(0, open_method='datatree') returns a DataTree without merging."""
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

        result = p.open_dataset(0, open_method="datatree", silent=True)
        # Should be a DataTree-like object (has groups), not a flat Dataset
        assert hasattr(result, "groups") or hasattr(result, "subtree") or hasattr(result, "children")
        if hasattr(result, "close"):
            result.close()

    def test_open_dataset_auto_prints_resolved_spec_not_auto(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """open_dataset with open_method='auto' prints the resolved spec, not 'auto'."""
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

        ds = p.open_dataset(0, open_method="auto", silent=False)
        ds.close()
        captured = capsys.readouterr()
        # Should show the resolved mode ("dataset"), not "auto"
        assert "'xarray_open': 'dataset'" in captured.out
        assert "'xarray_open': 'auto'" not in captured.out

    def test_open_dataset_preset_datatree_has_merge_none_in_spec(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """open_method='datatree' preset expands with merge=None in printed spec."""
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

        result = p.open_dataset(0, open_method="datatree", silent=False)
        if hasattr(result, "close"):
            result.close()
        captured = capsys.readouterr()
        # 'datatree' preset should show xarray_open='datatree' and merge=None
        assert "'xarray_open': 'datatree'" in captured.out
        assert "'merge': None" in captured.out

    def test_open_dataset_prints_geolocation_line(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """open_dataset() prints a Geolocation line after the open_method spec."""
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

        ds = p.open_dataset(0, open_method={"open_kwargs": {"engine": "netcdf4"}})
        ds.close()
        captured = capsys.readouterr()
        # First line is the open_method spec
        assert captured.out.splitlines()[0].startswith("open_method:")
        # Second line is the geolocation line
        assert "Geolocation" in captured.out
        assert "lon" in captured.out
        assert "lat" in captured.out

    def test_open_dataset_geolocation_respects_coords_dict(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """open_dataset() prints 'Geolocation specified' when coords dict is given."""
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

        ds = p.open_dataset(
            0,
            open_method={
                "open_kwargs": {"engine": "netcdf4"},
                "coords": {"lat": "lat", "lon": "lon"},
            },
        )
        ds.close()
        captured = capsys.readouterr()
        assert "Geolocation specified" in captured.out

    def test_open_dataset_silent_suppresses_geolocation(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture,
    ) -> None:
        """open_dataset(silent=True) does not print the geolocation line."""
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

        ds = p.open_dataset(0, open_method={"open_kwargs": {"engine": "netcdf4"}}, silent=True)
        ds.close()
        captured = capsys.readouterr()
        assert "Geolocation" not in captured.out
        assert "open_method" not in captured.out


# ---------------------------------------------------------------------------
# Helper: create a grouped NetCDF4 file (HDF5 with subgroups)
# ---------------------------------------------------------------------------

def _make_grouped_nc(path: str) -> None:
    """Write a NetCDF4 file with root and /monthly groups for testing.

    Root '/':      coordinate datasets ``lat``, ``lon``
    Group '/monthly':  variable ``sst`` (lat, lon) plus ``lat``, ``lon`` coords
    """
    import h5py

    with h5py.File(path, "w") as h:
        lat_data = np.array([-90.0, 0.0, 90.0])
        lon_data = np.array([-180.0, 0.0, 180.0])

        lat_ds = h.create_dataset("lat", data=lat_data)
        lat_ds.attrs["CLASS"] = np.bytes_("DIMENSION_SCALE")
        lat_ds.attrs["NAME"] = np.bytes_("lat")

        lon_ds = h.create_dataset("lon", data=lon_data)
        lon_ds.attrs["CLASS"] = np.bytes_("DIMENSION_SCALE")
        lon_ds.attrs["NAME"] = np.bytes_("lon")

        monthly = h.create_group("monthly")
        lat_m = monthly.create_dataset("lat", data=lat_data)
        lat_m.attrs["CLASS"] = np.bytes_("DIMENSION_SCALE")
        lat_m.attrs["NAME"] = np.bytes_("lat")
        lon_m = monthly.create_dataset("lon", data=lon_data)
        lon_m.attrs["CLASS"] = np.bytes_("DIMENSION_SCALE")
        lon_m.attrs["NAME"] = np.bytes_("lon")
        monthly.create_dataset("sst", data=np.ones((3, 3), dtype=np.float32))


def _make_pace_like_grouped_nc(path: str) -> None:
    """Write a PACE OCI L2 AOP-like grouped HDF5 file for testing.

    Root '/':              empty (no variables at root level)
    Group '/geophysical_data': science variable ``Rrs`` with phony dims
    Group '/navigation_data':  ``longitude``, ``latitude`` (2-D swath coords)
                                plus extra 1-D edge coords ``slon``, ``elon``,
                                ``slat``, ``elat`` that share the same CF
                                ``standard_name`` — this triggers cf_xarray
                                "ambiguous geolocation" detection.

    This mimics the grouped structure that caused open_method='auto' to fail:
    the flat dataset is empty so the dataset probe fails, AND the merged
    DataTree has ambiguous lon/lat names so _apply_coords also fails on the
    probe.  The fix is that the DataTree probe in _resolve_auto_spec no longer
    calls _apply_coords — it only checks that the merged dataset is non-empty.
    """
    import h5py

    n_lines, n_pixels = 4, 5

    with h5py.File(path, "w") as h:
        # /geophysical_data — science data only
        geo = h.create_group("geophysical_data")
        geo.create_dataset("Rrs", data=np.ones((n_lines, n_pixels), dtype=np.float32))

        # /navigation_data — 2-D lat/lon plus 1-D edge coords
        nav = h.create_group("navigation_data")
        lon_2d = np.linspace(-120, -100, n_lines * n_pixels).reshape(n_lines, n_pixels).astype(np.float32)
        lat_2d = np.linspace(30, 40, n_lines * n_pixels).reshape(n_lines, n_pixels).astype(np.float32)

        lon_ds = nav.create_dataset("longitude", data=lon_2d)
        lon_ds.attrs["standard_name"] = np.bytes_("longitude")
        lon_ds.attrs["units"] = np.bytes_("degrees_east")

        lat_ds = nav.create_dataset("latitude", data=lat_2d)
        lat_ds.attrs["standard_name"] = np.bytes_("latitude")
        lat_ds.attrs["units"] = np.bytes_("degrees_north")

        # 1-D edge/center coords that share the same standard_name
        for name, std_name, vals in [
            ("slon", "longitude", np.linspace(-120, -100, n_lines).astype(np.float32)),
            ("elon", "longitude", np.linspace(-119, -99, n_lines).astype(np.float32)),
            ("slat", "latitude", np.linspace(30, 38, n_lines).astype(np.float32)),
            ("elat", "latitude", np.linspace(32, 40, n_lines).astype(np.float32)),
        ]:
            ds_var = nav.create_dataset(name, data=vals)
            ds_var.attrs["standard_name"] = np.bytes_(std_name)


# ---------------------------------------------------------------------------
# Tests for dataset-based merge (Task 1) and merge_kwargs (Task 2)
# ---------------------------------------------------------------------------


class TestDatasetMerge:
    """Tests for merge with xarray_open='dataset'."""

    def _make_plan(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, nc_paths: list) -> Plan:
        mock_ea = MagicMock()
        mock_ea.open.return_value = nc_paths
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        return Plan(
            points=pts,
            results=[object() for _ in nc_paths],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_open_dataset_with_merge_list_opens_groups(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset with merge=['/', '/monthly'] merges groups via xr.open_dataset."""
        nc_path = str(tmp_path / "grouped.nc")
        _make_grouped_nc(nc_path)
        p = self._make_plan(tmp_path, monkeypatch, [nc_path])
        open_method = {
            "xarray_open": "dataset",
            "merge": ["/", "/monthly"],
            "open_kwargs": {"engine": "h5netcdf", "phony_dims": "sort"},
            "coords": {"lat": "lat", "lon": "lon"},
        }
        ds = p.open_dataset(p[0], open_method=open_method)
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds

    def test_open_dataset_with_merge_all_uses_h5py(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset with merge='all' discovers groups via h5py."""
        nc_path = str(tmp_path / "grouped.nc")
        _make_grouped_nc(nc_path)
        p = self._make_plan(tmp_path, monkeypatch, [nc_path])
        open_method = {
            "xarray_open": "dataset",
            "merge": "all",
            "open_kwargs": {"engine": "h5netcdf", "phony_dims": "sort"},
            "coords": {"lat": "lat", "lon": "lon"},
        }
        ds = p.open_dataset(p[0], open_method=open_method)
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds

    def test_open_mfdataset_with_merge_list_concatenates(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_mfdataset with dataset+merge=['/', '/monthly'] merges and concatenates."""
        nc_a = str(tmp_path / "a.nc")
        nc_b = str(tmp_path / "b.nc")
        _make_grouped_nc(nc_a)
        _make_grouped_nc(nc_b)
        p = self._make_plan(tmp_path, monkeypatch, [nc_a, nc_b])
        open_method = {
            "xarray_open": "dataset",
            "merge": ["/", "/monthly"],
            "open_kwargs": {"engine": "h5netcdf", "phony_dims": "sort"},
            "coords": {"lat": "lat", "lon": "lon"},
        }
        ds = p.open_mfdataset(p.results, open_method=open_method)
        assert isinstance(ds, xr.Dataset)
        assert ds.sizes["granule"] == 2
        assert "sst" in ds

    def test_merge_kwargs_accepted_in_dataset_spec(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """merge_kwargs in open_method dict is accepted alongside merge for dataset path."""
        nc_path = str(tmp_path / "grouped.nc")
        _make_grouped_nc(nc_path)
        p = self._make_plan(tmp_path, monkeypatch, [nc_path])
        open_method = {
            "xarray_open": "dataset",
            "merge": ["/monthly"],
            "merge_kwargs": {},
            "open_kwargs": {"engine": "h5netcdf", "phony_dims": "sort"},
            "coords": {"lat": "lat", "lon": "lon"},
        }
        ds = p.open_dataset(p[0], open_method=open_method)
        assert isinstance(ds, xr.Dataset)
        assert "sst" in ds

    def test_open_dataset_lazy_access_after_merge_does_not_raise(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Dask lazy arrays from open_dataset+merge can be computed without 'file closed' error."""
        nc_path = str(tmp_path / "grouped.nc")
        _make_grouped_nc(nc_path)
        p = self._make_plan(tmp_path, monkeypatch, [nc_path])
        open_method = {
            "xarray_open": "dataset",
            "merge": ["/", "/monthly"],
            "open_kwargs": {"engine": "h5netcdf", "phony_dims": "sort", "chunks": {}},
            "coords": {"lat": "lat", "lon": "lon"},
        }
        ds = p.open_dataset(p[0], open_method=open_method)
        assert isinstance(ds, xr.Dataset)
        # Computing dask arrays must not raise RuntimeError: file closed
        sst_values = ds["sst"].values
        assert sst_values is not None


# ---------------------------------------------------------------------------
# Tests for open_method="auto" fallback to datatree for PACE-like grouped files
# ---------------------------------------------------------------------------


class TestAutoOpenMethodDatatreeFallback:
    """Test that open_method='auto' falls back to datatree (merge=None) for grouped
    HDF5 files where the flat dataset is empty (root has no variables).

    Regression test for the PACE OCI L2 AOP case where:
    - xr.open_dataset → empty dataset (all vars in subgroups) → probe fails
    - DataTree (merge=None) → has data in non-root nodes → probe succeeds
    - Result is a raw DataTree; user must specify merge groups explicitly.
    """

    def _make_plan(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, nc_path: str) -> Plan:
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01"])}
        )
        return Plan(
            points=pts,
            results=[object()],
            granules=[],
            point_granule_map={0: []},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_open_dataset_auto_falls_back_to_datatree_for_pace_like_file(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_dataset(open_method='auto') returns a raw DataTree for grouped files
        whose root is empty (like PACE OCI L2 AOP).  merge=None so the user can
        inspect the groups and specify merge explicitly.
        """
        nc_path = str(tmp_path / "pace_like.nc")
        _make_pace_like_grouped_nc(nc_path)
        p = self._make_plan(tmp_path, monkeypatch, nc_path)

        result = p.open_dataset(p[0], open_method="auto", silent=True)
        # Should return a DataTree (not a flat Dataset) since root is empty
        assert isinstance(result, xr.DataTree)
        # The navigation_data node must be accessible via the DataTree's subtree
        nav_node = next(
            (node for node in result.subtree if node.name == "navigation_data"), None
        )
        assert nav_node is not None, "Expected 'navigation_data' group in DataTree"
        assert "longitude" in nav_node.ds.data_vars or "longitude" in nav_node.ds.coords
        assert "latitude" in nav_node.ds.data_vars or "latitude" in nav_node.ds.coords

    def test_open_dataset_auto_prints_datatree_spec_for_pace_like_file(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """open_method='auto' prints a resolved spec with xarray_open='datatree'
        and merge=None for a grouped file.
        """
        nc_path = str(tmp_path / "pace_like.nc")
        _make_pace_like_grouped_nc(nc_path)
        p = self._make_plan(tmp_path, monkeypatch, nc_path)

        p.open_dataset(p[0], open_method="auto")
        captured = capsys.readouterr()
        first_line = captured.out.splitlines()[0]
        assert "'xarray_open': 'datatree'" in first_line
        assert "'merge': None" in first_line

    def test_open_dataset_auto_prints_switch_reason_for_grouped_file(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """open_method='auto' prints a switch reason when it falls back to datatree.

        When the flat-dataset probe fails (e.g. no geolocation in root), the
        resolved spec should be accompanied by a line explaining why the mode
        was changed to 'datatree'.
        """
        nc_path = str(tmp_path / "pace_like.nc")
        _make_pace_like_grouped_nc(nc_path)
        p = self._make_plan(tmp_path, monkeypatch, nc_path)

        p.open_dataset(p[0], open_method="auto")
        captured = capsys.readouterr()
        lines = captured.out.splitlines()
        # A line explaining the reason for switching must appear
        assert any("switched to 'datatree'" in line for line in lines), (
            f"Expected switch-reason line in output:\n{captured.out}"
        )


# ---------------------------------------------------------------------------
# Task 3: matchup() with open_method='datatree' (merge=None) must not crash
# ---------------------------------------------------------------------------


class TestMatchupDatatreeMergeNone:
    """Test that open_method='datatree' (merge=None) uses the root dataset.

    Previously _merge_datatree_with_spec raised ValueError for merge=None.
    Now the datatree path in _open_as_flat_dataset uses dt.ds (root) directly.
    """

    def _make_plan(
        self,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        nc_path: str,
    ) -> "Plan":
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
        return Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_matchup_datatree_flat_file_does_not_raise_merge_none_error(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup(open_method='datatree') on a flat NetCDF must not raise.

        open_method='datatree' sets merge=None.  Previously this triggered:
        ValueError: spec['merge']=None is not valid.
        Now it uses the root dataset directly (dt.ds).
        """
        nc_path = str(tmp_path / "flat.nc")
        _make_l3_dataset([-0.5, 0.0, 0.5], [-0.5, 0.0, 0.5]).to_netcdf(
            nc_path, engine="netcdf4"
        )
        p = self._make_plan(tmp_path, monkeypatch, nc_path)
        # Should not raise "spec['merge']=None is not valid"
        result = pc.matchup(
            p, variables=["sst"], open_method={"xarray_open": "datatree", "open_kwargs": {"engine": "netcdf4"}}
        )
        assert result is not None


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

        result = pc.matchup(p, open_method="dataset", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
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
            pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})

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
        result = pc.matchup(p, open_method="dataset", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
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

        result = pc.matchup(subset_plan, open_method="dataset", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
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
        pc.matchup(subset_plan, open_method="dataset", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})

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

        result = pc.matchup(subset_plan, open_method="dataset", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"})
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


class TestOpenMethodParameter:
    """Tests for the open_method parameter in pc.matchup()."""

    def test_invalid_open_method_string_raises(self) -> None:
        """Invalid open_method string raises ValueError mentioning 'open_method'."""
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
        with pytest.raises(ValueError, match="open_method"):
            pc.matchup(p, open_method="invalid")

    def test_invalid_open_method_dict_unknown_key_raises(self) -> None:
        """open_method dict with unknown key raises ValueError mentioning 'unknown keys'."""
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
        with pytest.raises(ValueError, match="unknown keys"):
            pc.matchup(p, open_method={"unknown_key": "val"})

    def test_invalid_open_method_xarray_open_raises(self) -> None:
        """open_method dict with invalid xarray_open value raises ValueError."""
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
        with pytest.raises(ValueError):
            pc.matchup(p, open_method={"xarray_open": "invalid"})

    def test_matchup_without_open_method_uses_auto(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Calling matchup() without open_method uses 'auto' default and does not raise."""
        mock_ea = MagicMock()
        mock_ea.open.return_value = []
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

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
        # Zero-match points return NaN rows immediately without opening granules
        result = pc.matchup(p)
        assert len(result) == 1


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
        """Multiple recognised pairs should raise with all pairs listed.

        Uses a dataset where cf_xarray cannot resolve the ambiguity (no CF
        attributes, and variable names are not recognised CF standard names).
        """
        from point_collocation.core.engine import _find_geoloc_pair

        # Has both (lon, lat) and (Longitude, Latitude) — neither pair uses the
        # exact CF standard name spellings that cf_xarray can resolve by name,
        # so the name-based fallback detects two pairs and raises.
        ds = xr.Dataset(
            coords={
                "lon": [0.0],
                "lat": [0.0],
                "Longitude": [0.0],
                "Latitude": [0.0],
            }
        )
        with pytest.raises(ValueError, match="ambiguous geolocation variables"):
            _find_geoloc_pair(ds)

    def test_lon_lat_plus_longitude_latitude_resolves_via_cf(self) -> None:
        """When cf_xarray is installed, (lon,lat)+(longitude,latitude) resolves.

        Without cf_xarray this would be ambiguous, but cf_xarray correctly
        picks the CF standard-name pair ``(longitude, latitude)``.
        """
        cf_xarray = pytest.importorskip("cf_xarray")  # noqa: F841
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            coords={
                "lon": [0.0],
                "lat": [0.0],
                "longitude": [0.0],
                "latitude": [0.0],
            }
        )
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "longitude"
        assert lat_name == "latitude"


class TestGeolocDetectionCfXarray:
    """Tests for _find_geoloc_pair() using cf_xarray CF-convention detection."""

    def test_finds_via_standard_name(self) -> None:
        pytest.importorskip("cf_xarray")
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            coords={
                "myX": xr.DataArray(
                    [0.0], attrs={"standard_name": "longitude", "units": "degrees_east"}
                ),
                "myY": xr.DataArray(
                    [0.0], attrs={"standard_name": "latitude", "units": "degrees_north"}
                ),
            }
        )
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "myX"
        assert lat_name == "myY"

    def test_finds_via_units(self) -> None:
        pytest.importorskip("cf_xarray")
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            coords={
                "mylon": xr.DataArray([0.0], attrs={"units": "degrees_east"}),
                "mylat": xr.DataArray([0.0], attrs={"units": "degrees_north"}),
            }
        )
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "mylon"
        assert lat_name == "mylat"

    def test_finds_via_long_name(self) -> None:
        pytest.importorskip("cf_xarray")
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            coords={
                "x": xr.DataArray([0.0], attrs={"long_name": "longitude"}),
                "y": xr.DataArray([0.0], attrs={"long_name": "latitude"}),
            }
        )
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "x"
        assert lat_name == "y"

    def test_finds_cf_attrs_in_data_vars(self) -> None:
        """CF-detected geoloc vars stored as data_vars (not coords) should be found."""
        pytest.importorskip("cf_xarray")
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            {
                "nav_lon": xr.DataArray(
                    [[0.0, 1.0]],
                    dims=["y", "x"],
                    attrs={"standard_name": "longitude"},
                ),
                "nav_lat": xr.DataArray(
                    [[0.0, 0.0]],
                    dims=["y", "x"],
                    attrs={"standard_name": "latitude"},
                ),
                "sst": xr.DataArray([[25.0, 26.0]], dims=["y", "x"]),
            }
        )
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "nav_lon"
        assert lat_name == "nav_lat"

    def test_cf_ambiguous_raises(self) -> None:
        """Multiple CF-detected longitude vars should raise ambiguous error."""
        pytest.importorskip("cf_xarray")
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            coords={
                "lon1": xr.DataArray([0.0], attrs={"standard_name": "longitude"}),
                "lon2": xr.DataArray([0.0], attrs={"standard_name": "longitude"}),
                "lat1": xr.DataArray([0.0], attrs={"standard_name": "latitude"}),
            }
        )
        with pytest.raises(ValueError, match="ambiguous geolocation variables"):
            _find_geoloc_pair(ds)

    def test_cf_ambiguous_with_bnds_resolves_via_name_fallback(self) -> None:
        """ECCO-like files: cf_xarray ambiguity (due to _bnds vars) falls back to name search.

        ECCO files have ``longitude_bnds``/``latitude_bnds`` alongside
        ``longitude``/``latitude``, all sharing the same CF standard_name.
        cf_xarray reports this as ambiguous; the name-based fallback resolves to
        ``('longitude', 'latitude')`` without error.
        """
        pytest.importorskip("cf_xarray")
        from point_collocation.core.engine import _find_geoloc_pair

        lon_std = {"standard_name": "longitude", "units": "degrees_east"}
        lat_std = {"standard_name": "latitude", "units": "degrees_north"}
        ds = xr.Dataset(
            coords={
                "longitude": xr.DataArray([0.0], attrs=lon_std),
                "longitude_bnds": xr.DataArray([[-0.5, 0.5]], dims=["longitude", "nv"], attrs={"standard_name": "longitude"}),
                "latitude": xr.DataArray([0.0], attrs=lat_std),
                "latitude_bnds": xr.DataArray([[-0.5, 0.5]], dims=["latitude", "nv"], attrs={"standard_name": "latitude"}),
            }
        )
        lon_name, lat_name = _find_geoloc_pair(ds)
        assert lon_name == "longitude"
        assert lat_name == "latitude"

    def test_cf_partial_detection_raises(self) -> None:
        """CF detects longitude but not latitude — should raise 'no geolocation'."""
        pytest.importorskip("cf_xarray")
        from point_collocation.core.engine import _find_geoloc_pair

        ds = xr.Dataset(
            coords={
                "myX": xr.DataArray([0.0], attrs={"standard_name": "longitude"}),
                "temperature": xr.DataArray([20.0]),
            }
        )
        with pytest.raises(ValueError, match="no geolocation variables found"):
            _find_geoloc_pair(ds)


class TestSpatialCompatCheck:
    """Tests for _check_spatial_compat()."""

    def test_nearest_1d_ok(self) -> None:
        from point_collocation.core.engine import _check_spatial_compat

        ds = xr.Dataset(coords={"lon": [0.0], "lat": [0.0]})
        # Should not raise
        _check_spatial_compat(ds, "lon", "lat", "nearest")

    def test_nearest_2d_raises(self) -> None:
        from point_collocation.core.engine import _check_spatial_compat

        ds = xr.Dataset(
            {
                "lon": (["nrows", "ncols"], [[0.0]]),
                "lat": (["nrows", "ncols"], [[0.0]]),
            }
        )
        with pytest.raises(ValueError, match="spatial_method='nearest'"):
            _check_spatial_compat(ds, "lon", "lat", "nearest")

    def test_nearest_2d_error_mentions_auto(self) -> None:
        from point_collocation.core.engine import _check_spatial_compat

        ds = xr.Dataset(
            {
                "lon": (["nrows", "ncols"], [[0.0]]),
                "lat": (["nrows", "ncols"], [[0.0]]),
            }
        )
        with pytest.raises(ValueError, match="auto"):
            _check_spatial_compat(ds, "lon", "lat", "nearest")

    def test_auto_any_dims_ok(self) -> None:
        from point_collocation.core.engine import _check_spatial_compat

        ds_2d = xr.Dataset(
            {
                "lon": (["nrows", "ncols"], [[0.0]]),
                "lat": (["nrows", "ncols"], [[0.0]]),
            }
        )
        ds_1d = xr.Dataset(coords={"lon": [0.0], "lat": [0.0]})
        # "auto" should never raise from _check_spatial_compat
        _check_spatial_compat(ds_2d, "lon", "lat", "auto")
        _check_spatial_compat(ds_1d, "lon", "lat", "auto")

    def test_xoak_kdtree_any_dims_ok(self) -> None:
        from point_collocation.core.engine import _check_spatial_compat

        ds_2d = xr.Dataset(
            {
                "lon": (["nrows", "ncols"], [[0.0]]),
                "lat": (["nrows", "ncols"], [[0.0]]),
            }
        )
        ds_1d = xr.Dataset(coords={"lon": [0.0], "lat": [0.0]})
        # Should not raise for either dimensionality
        _check_spatial_compat(ds_2d, "lon", "lat", "xoak-kdtree")
        _check_spatial_compat(ds_1d, "lon", "lat", "xoak-kdtree")


class TestMissingXoak:
    """Test that missing xoak raises a clear ImportError for spatial_method="xoak-kdtree"."""

    def test_xoak_kdtree_import_error_raised_early(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup() raises ImportError for xoak-kdtree before opening any granule."""
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

        with pytest.raises(ImportError, match="xoak-kdtree"):
            pc.matchup(p, spatial_method="xoak-kdtree")


class TestMissingVariableErrorMessage:
    """Tests for improved error message when variables are missing."""

    def test_missing_var_error_includes_open_method_and_spatial_method(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Error for missing variable must include open_method/spatial_method."""
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
            pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})

        msg = str(exc_info.value)
        assert "no_such_var" in msg
        assert "open_method=" in msg
        assert "spatial_method=" in msg


class TestXoakSpatialMethod:
    """Tests for spatial_method='xoak-kdtree' with both open_method='datatree-merge' and open_method='dataset'."""

    def test_swath_matchup_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath matchup using xoak-kdtree returns the nearest pixel value."""
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
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_nearest_with_2d_data_raises(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """spatial_method='nearest' with 2-D lat/lon raises a clear ValueError."""
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

        with pytest.raises(ValueError, match="spatial_method='nearest'"):
            pc.matchup(
                p,
                open_method="dataset",
                spatial_method="nearest",
                open_dataset_kwargs={"engine": "netcdf4"},
            )

    def test_swath_matchup_3d_variable_expands_with_xoak_kdtree(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath matchup with xoak-kdtree expands 3-D variables (e.g. Rrs) into per-wavelength columns."""
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
            open_method="datatree-merge",
            variables=["Rrs"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "Rrs" not in result.columns, "bare 'Rrs' column should be dropped after expansion"
        for wl in wavelengths:
            assert f"Rrs_{wl}" in result.columns, f"Rrs_{wl} column missing"
        assert len(result) == 1

    def test_grid_matchup_with_xoak_kdtree_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_method='dataset' + spatial_method='xoak-kdtree' returns the nearest grid value."""
        pytest.importorskip("xoak")  # skip if xoak not installed

        lats = [-90.0, 0.0, 90.0]
        lons = [-180.0, 0.0, 180.0]
        nc_path = str(tmp_path / "grid.nc")
        _make_l3_dataset(lats, lons, seed=7).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0],
                "lon": [0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/grid.nc",
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
            open_method="dataset",
            variables=["sst"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_grid_matchup_xoak_kdtree_global_granule_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_method='dataset' + xoak-kdtree on a global granule slices correctly and returns a value."""
        pytest.importorskip("xoak")  # skip if xoak not installed

        # Large global grid (181 lats × 361 lons) with the query point near centre.
        lats = list(range(-90, 91))        # integers -90, -89, …, 90
        lons = list(range(-180, 181))      # integers -180, -179, …, 180
        nc_path = str(tmp_path / "global_grid.nc")
        _make_l3_dataset(lats, lons, seed=99).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Query a single point at (lat=10, lon=20).
        pts = pd.DataFrame(
            {
                "lat": [10.0],
                "lon": [20.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/global_grid.nc",
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
            open_method="dataset",
            variables=["sst"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_swath_matchup_multiple_points_same_granule_with_xoak_kdtree(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multiple query points mapped to the same granule are processed via a single k-d tree."""
        pytest.importorskip("xoak")  # skip if xoak not installed

        nc_path = str(tmp_path / "swath_multi.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=7)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Use two distinct swath pixels as query points.
        lat0 = float(ds_swath["lat"].values[0, 0])
        lon0 = float(ds_swath["lon"].values[0, 0])
        lat1 = float(ds_swath["lat"].values[2, 3])
        lon1 = float(ds_swath["lon"].values[2, 3])

        pts = pd.DataFrame(
            {
                "lat": [lat0, lat1],
                "lon": [lon0, lon1],
                "time": pd.to_datetime(["2023-06-01T12:00:00", "2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/swath_multi.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0], 1: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(
            p,
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert "sst" in result.columns
        assert len(result) == 2
        assert not result["sst"].isna().any()



    """Unit tests for the _slice_grid_to_points helper."""

    def test_slices_ascending_coords(self) -> None:
        """Dataset with ascending lat/lon is sliced to the point bounding box + buffer."""
        from point_collocation.core.engine import _slice_grid_to_points

        lats = list(range(-90, 91))
        lons = list(range(-180, 181))
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.zeros((len(lats), len(lons))))},
            coords={"lat": lats, "lon": lons},
        )

        sliced = _slice_grid_to_points(ds, [10.0], [20.0], "lat", "lon", buffer_deg=2.0)

        # The slice should cover [8, 12] lat and [18, 22] lon (within 2° buffer).
        assert float(sliced["lat"].min()) >= 8.0
        assert float(sliced["lat"].max()) <= 12.0
        assert float(sliced["lon"].min()) >= 18.0
        assert float(sliced["lon"].max()) <= 22.0
        # Original dataset should be much larger.
        assert sliced.sizes["lat"] < ds.sizes["lat"]
        assert sliced.sizes["lon"] < ds.sizes["lon"]

    def test_slices_descending_lat_coords(self) -> None:
        """Dataset with descending lat (90→-90) is sliced correctly."""
        from point_collocation.core.engine import _slice_grid_to_points

        lats = list(range(90, -91, -1))   # integers 90, 89, …, -90 (descending)
        lons = list(range(-180, 181))
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.zeros((len(lats), len(lons))))},
            coords={"lat": lats, "lon": lons},
        )

        sliced = _slice_grid_to_points(ds, [5.0], [0.0], "lat", "lon", buffer_deg=1.0)

        assert sliced.sizes["lat"] > 0
        assert sliced.sizes["lon"] > 0
        assert sliced.sizes["lat"] < ds.sizes["lat"]

    def test_single_point_uses_buffer(self) -> None:
        """A single query point still produces a non-empty slice thanks to the buffer."""
        from point_collocation.core.engine import _slice_grid_to_points

        lats = list(range(-90, 91))
        lons = list(range(-180, 181))
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.zeros((len(lats), len(lons))))},
            coords={"lat": lats, "lon": lons},
        )

        sliced = _slice_grid_to_points(ds, [0.0], [0.0], "lat", "lon", buffer_deg=1.0)

        # 1° buffer each side → at least 3 lat values and 3 lon values.
        assert sliced.sizes["lat"] >= 3
        assert sliced.sizes["lon"] >= 3

    def test_empty_slice_falls_back_to_full_dataset(self) -> None:
        """If the buffered box is outside the grid, the full dataset is returned."""
        from point_collocation.core.engine import _slice_grid_to_points

        lats = [0.0, 1.0, 2.0]
        lons = [0.0, 1.0, 2.0]
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.zeros((3, 3)))},
            coords={"lat": lats, "lon": lons},
        )

        # Query point far outside the dataset range.
        sliced = _slice_grid_to_points(ds, [50.0], [50.0], "lat", "lon", buffer_deg=0.5)

        # Should fall back to the full dataset unchanged.
        assert sliced.sizes["lat"] == ds.sizes["lat"]
        assert sliced.sizes["lon"] == ds.sizes["lon"]

    def test_2d_coords_returns_unchanged(self) -> None:
        """2-D (swath-style) coordinates are not sliced."""
        from point_collocation.core.engine import _slice_grid_to_points

        lat_2d = np.array([[0.0, 1.0], [2.0, 3.0]])
        lon_2d = np.array([[10.0, 11.0], [12.0, 13.0]])
        ds = xr.Dataset(
            {"sst": (["nrows", "ncols"], np.zeros((2, 2)))},
            coords={
                "lat": (["nrows", "ncols"], lat_2d),
                "lon": (["nrows", "ncols"], lon_2d),
            },
        )

        sliced = _slice_grid_to_points(ds, [1.0], [11.0], "lat", "lon")

        # 2-D coords → no slicing; sizes must be unchanged.
        assert sliced.sizes == ds.sizes

    def test_multiple_points_uses_union_bbox(self) -> None:
        """Multiple query points: slice covers the union bounding box."""
        from point_collocation.core.engine import _slice_grid_to_points

        lats = list(range(-90, 91))
        lons = list(range(-180, 181))
        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.zeros((len(lats), len(lons))))},
            coords={"lat": lats, "lon": lons},
        )

        # Two points that are far apart; the slice must cover both.
        sliced = _slice_grid_to_points(
            ds, [-30.0, 30.0], [-60.0, 60.0], "lat", "lon", buffer_deg=1.0
        )

        assert float(sliced["lat"].min()) <= -30.0
        assert float(sliced["lat"].max()) >= 30.0
        assert float(sliced["lon"].min()) <= -60.0
        assert float(sliced["lon"].max()) >= 60.0
        # Still smaller than the full global grid.
        assert sliced.sizes["lat"] < ds.sizes["lat"]
        assert sliced.sizes["lon"] < ds.sizes["lon"]

    def test_swath_nan_geoloc_pixels_are_ignored(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath pixels with NaN lat/lon (e.g. fill values outside Earth disk) are ignored.

        Regression test for DSCOVR EPIC HE5 data where fill values (~-1.27e30)
        outside the valid Earth disk are converted to NaN by xarray.  Without
        the fix, the xoak-kdtree k-d tree raises ``ValueError`` when building the index
        with NaN coordinates.  The k-d tree must skip those pixels.
        """
        pytest.importorskip("xoak")

        # Build a swath where the last row has NaN lat/lon (simulating fill values).
        rng = np.random.default_rng(42)
        lat = rng.uniform(-10.0, 10.0, (4, 5)).astype(np.float32)
        lon = rng.uniform(-30.0, 30.0, (4, 5)).astype(np.float32)
        sst = rng.uniform(20.0, 30.0, (4, 5)).astype(np.float32)
        # Mark last row as NaN (simulating out-of-swath fill values).
        lat[-1, :] = np.nan
        lon[-1, :] = np.nan

        nc_path = str(tmp_path / "swath_nan.nc")
        xr.Dataset(
            {
                "lat": (["nrows", "ncols"], lat),
                "lon": (["nrows", "ncols"], lon),
                "sst": (["nrows", "ncols"], sst),
            }
        ).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Query the exact location of a valid pixel; expect its sst value back.
        lat_val = float(lat[0, 0])
        lon_val = float(lon[0, 0])
        expected_sst = float(sst[0, 0])

        pts = pd.DataFrame(
            {
                "lat": [lat_val],
                "lon": [lon_val],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/swath_nan.nc",
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
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        # Result must be a finite value from a valid pixel, not NaN.
        assert not math.isnan(result.loc[0, "sst"])
        assert result.loc[0, "sst"] == pytest.approx(expected_sst, rel=1e-4)


class TestMissingNdpoint:
    """Test that missing scipy raises a clear ImportError for spatial_method='kdtree'."""

    def test_kdtree_import_error_raised_early(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup() raises ImportError for kdtree before opening any granule."""
        import sys

        # Block the scipy.spatial submodule import.
        for key in list(sys.modules.keys()):
            if key == "scipy" or key.startswith("scipy."):
                monkeypatch.delitem(sys.modules, key)
        monkeypatch.setitem(sys.modules, "scipy", None)  # type: ignore[assignment]
        monkeypatch.setitem(sys.modules, "scipy.spatial", None)  # type: ignore[assignment]

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

        with pytest.raises(ImportError, match="scipy"):
            pc.matchup(p, spatial_method="kdtree")


class TestMissingHaversine:
    """Test that missing xoak raises a clear ImportError for spatial_method='xoak-haversine'."""

    def test_haversine_import_error_raised_early(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """matchup() raises ImportError for xoak-haversine before opening any granule."""
        import sys

        # Block the xoak.tree_adapters submodule import.
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

        with pytest.raises(ImportError, match="xoak-haversine"):
            pc.matchup(p, spatial_method="xoak-haversine")


class TestHaversineSpatialMethod:
    """Tests for spatial_method='xoak-haversine' (xoak SklearnGeoBallTreeAdapter)."""

    def test_swath_matchup_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath matchup using xoak-haversine returns the nearest pixel value."""
        pytest.importorskip("xoak")  # skip if xoak not installed

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
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="xoak-haversine",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_haversine_returns_same_value_as_xoak_for_normal_latitudes(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """xoak-haversine and xoak-kdtree return the same nearest value for points at normal latitudes."""
        pytest.importorskip("xoak")  # skip if xoak not installed

        nc_path = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=99)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")

        def make_plan() -> "Plan":
            mock_ea = MagicMock()
            mock_ea.open.return_value = [nc_path]
            monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
            lat_val = float(ds_swath["lat"].values[1, 2])
            lon_val = float(ds_swath["lon"].values[1, 2])
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
            return Plan(
                points=pts,
                results=[object()],
                granules=[gm],
                point_granule_map={0: [0]},
                source_kwargs={"short_name": "TEST"},
                time_buffer=pd.Timedelta(0),
            )

        result_xoak_kdtree = pc.matchup(
            make_plan(),
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )
        result_haversine = pc.matchup(
            make_plan(),
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="xoak-haversine",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert result_xoak_kdtree.loc[0, "sst"] == pytest.approx(result_haversine.loc[0, "sst"])

    def test_grid_matchup_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_method='dataset' + spatial_method='xoak-haversine' returns the nearest grid value."""
        pytest.importorskip("xoak")  # skip if xoak not installed

        lats = [-1.0, 0.0, 1.0]
        lons = [-1.0, 0.0, 1.0]
        nc_path = str(tmp_path / "grid.nc")
        _make_l3_dataset(lats, lons, seed=7).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/grid.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-1.0, -1.0, 1.0, 1.0),
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
            open_method="dataset",
            variables=["sst"],
            spatial_method="xoak-haversine",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])


class TestNdpointSpatialMethod:
    """Tests for spatial_method='kdtree' (xarray built-in NDPointIndex with scipy)."""

    def test_swath_matchup_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath matchup using kdtree returns the nearest pixel value."""
        pytest.importorskip("scipy")

        nc_path = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=42)
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
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_swath_matchup_3d_variable_expands(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath matchup with kdtree expands 3-D variables (e.g. Rrs) into per-wavelength columns."""
        pytest.importorskip("scipy")

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
            open_method="datatree-merge",
            variables=["Rrs"],
            spatial_method="kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "Rrs" not in result.columns, "bare 'Rrs' column should be dropped after expansion"
        for wl in wavelengths:
            assert f"Rrs_{wl}" in result.columns, f"Rrs_{wl} column missing"
        assert len(result) == 1

    def test_grid_matchup_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_method='dataset' + spatial_method='kdtree' returns the nearest grid value."""
        pytest.importorskip("scipy")

        lats = [-90.0, 0.0, 90.0]
        lons = [-180.0, 0.0, 180.0]
        nc_path = str(tmp_path / "grid.nc")
        _make_l3_dataset(lats, lons, seed=7).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {
                "lat": [0.0],
                "lon": [0.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/grid.nc",
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
            open_method="dataset",
            variables=["sst"],
            spatial_method="kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_multiple_points_same_granule(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multiple query points mapped to the same granule processed via a single k-d tree."""
        pytest.importorskip("scipy")

        nc_path = str(tmp_path / "swath_multi.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=7)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        lat0 = float(ds_swath["lat"].values[0, 0])
        lon0 = float(ds_swath["lon"].values[0, 0])
        lat1 = float(ds_swath["lat"].values[2, 3])
        lon1 = float(ds_swath["lon"].values[2, 3])

        pts = pd.DataFrame(
            {
                "lat": [lat0, lat1],
                "lon": [lon0, lon1],
                "time": pd.to_datetime(["2023-06-01T12:00:00", "2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/swath_multi.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0], 1: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

        result = pc.matchup(
            p,
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert "sst" in result.columns
        assert len(result) == 2
        assert not result["sst"].isna().any()

    def test_kdtree_and_xoak_kdtree_return_same_values(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """kdtree and xoak-kdtree return identical nearest-neighbour values for the same query."""
        pytest.importorskip("scipy")
        pytest.importorskip("xoak")

        nc_path = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=42)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")

        lat_val = float(ds_swath["lat"].values[1, 2])
        lon_val = float(ds_swath["lon"].values[1, 2])

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

        def make_plan() -> Plan:
            return Plan(
                points=pts,
                results=[object()],
                granules=[gm],
                point_granule_map={0: [0]},
                source_kwargs={"short_name": "TEST"},
                time_buffer=pd.Timedelta(0),
            )

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        result_kdtree = pc.matchup(
            make_plan(),
            open_method="dataset",
            variables=["sst"],
            spatial_method="kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )
        result_xoak_kdtree = pc.matchup(
            make_plan(),
            open_method="dataset",
            variables=["sst"],
            spatial_method="xoak-kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert result_kdtree.loc[0, "sst"] == pytest.approx(result_xoak_kdtree.loc[0, "sst"])

    def test_grid_matchup_global_granule_returns_nearest_value(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_method='dataset' + kdtree on a global granule slices correctly and returns a value."""
        pytest.importorskip("scipy")

        # Large global grid (181 lats × 361 lons) with the query point near centre.
        lats = list(range(-90, 91))        # integers -90, -89, …, 90
        lons = list(range(-180, 181))      # integers -180, -179, …, 180
        nc_path = str(tmp_path / "global_grid.nc")
        _make_l3_dataset(lats, lons, seed=99).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Query a single point at (lat=10, lon=20).
        pts = pd.DataFrame(
            {
                "lat": [10.0],
                "lon": [20.0],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/global_grid.nc",
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
            open_method="dataset",
            variables=["sst"],
            spatial_method="kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_swath_nan_geoloc_pixels_are_ignored(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Swath pixels with NaN lat/lon (e.g. fill values outside Earth disk) are ignored.

        Regression test for DSCOVR EPIC HE5 data where fill values (~-1.27e30)
        outside the valid Earth disk are converted to NaN by xarray.  Without
        the fix, scipy's KD-tree raises ``ValueError`` when building the index
        with NaN coordinates.  The k-d tree must skip those pixels.
        """
        pytest.importorskip("scipy")

        # Build a swath where the last row has NaN lat/lon (simulating fill values).
        rng = np.random.default_rng(42)
        lat = rng.uniform(-10.0, 10.0, (4, 5)).astype(np.float32)
        lon = rng.uniform(-30.0, 30.0, (4, 5)).astype(np.float32)
        sst = rng.uniform(20.0, 30.0, (4, 5)).astype(np.float32)
        # Mark last row as NaN (simulating out-of-swath fill values).
        lat[-1, :] = np.nan
        lon[-1, :] = np.nan

        nc_path = str(tmp_path / "swath_nan.nc")
        xr.Dataset(
            {
                "lat": (["nrows", "ncols"], lat),
                "lon": (["nrows", "ncols"], lon),
                "sst": (["nrows", "ncols"], sst),
            }
        ).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        # Query the exact location of a valid pixel; expect its sst value back.
        lat_val = float(lat[0, 0])
        lon_val = float(lon[0, 0])
        expected_sst = float(sst[0, 0])

        pts = pd.DataFrame(
            {
                "lat": [lat_val],
                "lon": [lon_val],
                "time": pd.to_datetime(["2023-06-01T12:00:00"]),
            }
        )
        gm = GranuleMeta(
            granule_id="https://example.com/swath_nan.nc",
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
            open_method="datatree-merge",
            variables=["sst"],
            spatial_method="kdtree",
            open_dataset_kwargs={"engine": "netcdf4"},
        )

        assert "sst" in result.columns
        assert len(result) == 1
        # Result must be a finite value from a valid pixel, not NaN.
        assert not math.isnan(result.loc[0, "sst"])
        assert result.loc[0, "sst"] == pytest.approx(expected_sst, rel=1e-4)

    def test_swath_nan_geoloc_old_xarray_raises_helpful_message(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When NaN pixels are found and xarray < 2026.2, a clear RuntimeError is raised."""
        pytest.importorskip("scipy")
        import importlib.metadata as _meta

        monkeypatch.setattr(_meta, "version", lambda pkg: "2025.07.1" if pkg == "xarray" else "0")

        import numpy as np
        import xarray as xr
        from point_collocation.core.engine import _drop_nan_geoloc

        lat = np.array([[1.0, 2.0], [np.nan, np.nan]], dtype=np.float32)
        lon = np.array([[10.0, 20.0], [np.nan, np.nan]], dtype=np.float32)
        ds = xr.Dataset(
            {"sst": (["r", "c"], np.ones((2, 2)))},
            coords={"lat": (["r", "c"], lat), "lon": (["r", "c"], lon)},
        )

        with pytest.raises(RuntimeError, match="xarray"):
            _drop_nan_geoloc(ds, "lat", "lon")

    def test_drop_nan_geoloc_bbox_filter_reduces_pixel_count(self) -> None:
        """_slice_2d_to_points removes out-of-bbox rows/cols even when NaN pixels exist."""
        pytest.importorskip("scipy")
        import numpy as np
        import xarray as xr
        from point_collocation.core.engine import _slice_2d_to_points

        # 4x4 swath: last row is NaN (fill values).  Rows 0-2 span a wide area.
        # Row 1 uses lon values close to 0 so a tight bbox around (0, 0) can
        # select only those pixels.
        lat = np.array([
            [5.0, 5.0, 5.0, 5.0],
            [0.0, 0.0, 0.0, 0.0],
            [-5.0, -5.0, -5.0, -5.0],
            [np.nan, np.nan, np.nan, np.nan],
        ], dtype=np.float32)
        lon = np.array([
            [-30.0, -10.0, 10.0, 30.0],
            [-0.5,  -0.2,  0.2,  0.5],   # all within ±1° of lon=0
            [-30.0, -10.0, 10.0, 30.0],
            [np.nan, np.nan, np.nan, np.nan],
        ], dtype=np.float32)
        ds = xr.Dataset(
            {"sst": (["r", "c"], np.ones((4, 4)))},
            coords={"lat": (["r", "c"], lat), "lon": (["r", "c"], lon)},
        )

        # Without bbox filter: full dataset unchanged.
        result_full = ds
        assert result_full.sizes["r"] == 4

        # With bbox filter centred on (0, 0) with 1° pad: only row 1 survives
        # (rows 0 and 2 have lon values ±10° or more, row 3 is all NaN).
        result_bbox = _slice_2d_to_points(ds, [0.0], [0.0], "lat", "lon")
        assert result_bbox.sizes["r"] < 4
        # Every kept row must have at least one pixel within bbox lat/lon.
        kept_lats = result_bbox.coords["lat"].values
        kept_lons = result_bbox.coords["lon"].values
        # row 1 is within bbox (lat=0, lon ∈ {-0.5, -0.2, 0.2, 0.5})
        assert np.any(np.abs(kept_lats) <= 1.0)
        assert np.any(np.abs(kept_lons) <= 1.0)

    def test_slice_2d_to_points_reduces_pixel_count(self) -> None:
        """_slice_2d_to_points filters 2-D swath rows/cols to the query bbox."""
        import numpy as np
        import xarray as xr
        from point_collocation.core.engine import _slice_2d_to_points

        # 5×4 swath: rows 0 and 4 span high/low latitudes far from the query.
        # Only row 2 (lat=0) and its neighbours should survive a bbox at (0, 0).
        lat = np.array([
            [80.0, 80.0, 80.0, 80.0],
            [5.0,  5.0,  5.0,  5.0],
            [0.0,  0.0,  0.0,  0.0],
            [-5.0, -5.0, -5.0, -5.0],
            [-80.0, -80.0, -80.0, -80.0],
        ], dtype=np.float32)
        lon = np.array([
            [-30.0, -10.0, 10.0, 30.0],
            [-0.5,  -0.2,  0.2,  0.5],
            [-0.5,  -0.2,  0.2,  0.5],
            [-0.5,  -0.2,  0.2,  0.5],
            [-30.0, -10.0, 10.0, 30.0],
        ], dtype=np.float32)
        ds = xr.Dataset(
            {"sst": (["r", "c"], np.ones((5, 4)))},
            coords={"lat": (["r", "c"], lat), "lon": (["r", "c"], lon)},
        )

        # With default 1° pad around (0, 0): rows 0 and 4 are far outside.
        result = _slice_2d_to_points(ds, [0.0], [0.0], "lat", "lon")
        assert result.sizes["r"] < 5  # Some rows were dropped.
        assert result.sizes["c"] == 4  # All cols are within lon bbox.

        # The extreme rows (lat=80 and lat=-80) must be gone.
        remaining_lats = result.coords["lat"].values
        assert not np.any(np.abs(remaining_lats) > 10.0)

    def test_slice_2d_to_points_leaves_1d_unchanged(self) -> None:
        """_slice_2d_to_points must not touch datasets with 1-D coordinates."""
        import numpy as np
        import xarray as xr
        from point_collocation.core.engine import _slice_2d_to_points

        ds = xr.Dataset(
            {"sst": (["lat", "lon"], np.ones((5, 4)))},
            coords={"lat": np.linspace(-80, 80, 5), "lon": np.linspace(-30, 30, 4)},
        )
        result = _slice_2d_to_points(ds, [0.0], [0.0], "lat", "lon")
        assert result.identical(ds)


class TestAutoSpatialMethod:

    def _make_granule_meta(self) -> "GranuleMeta":
        return GranuleMeta(
            granule_id="https://example.com/test.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )

    def test_auto_is_default(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Calling matchup() without spatial_method uses 'auto' (1-D coords → nearest)."""
        pytest.importorskip("scipy")
        nc_path = str(tmp_path / "grid.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(nc_path, engine="netcdf4")

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        # No spatial_method → should default to "auto" and succeed with 1-D coords
        result = pc.matchup(p, open_method="dataset", variables=["sst"],
                            open_dataset_kwargs={"engine": "netcdf4"})
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"])

    def test_auto_1d_routes_to_nearest(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """auto with 1-D coords routes to 'nearest' (no scipy/xoak required)."""
        nc_path = str(tmp_path / "grid.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=5).to_netcdf(
            nc_path, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        # auto + 1D coords → should produce the same result as explicit nearest
        result_auto = pc.matchup(
            p, open_method="dataset", variables=["sst"],
            spatial_method="auto", open_dataset_kwargs={"engine": "netcdf4"},
        )
        mock_ea.open.return_value = [nc_path]
        p2 = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        result_nearest = pc.matchup(
            p2, open_method="dataset", variables=["sst"],
            spatial_method="nearest", open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert result_auto.loc[0, "sst"] == pytest.approx(result_nearest.loc[0, "sst"])

    def test_auto_2d_routes_to_kdtree(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """auto with 2-D coords routes to 'kdtree' (scipy required)."""
        pytest.importorskip("scipy")
        nc_path = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=42)
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
        p = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        result = pc.matchup(
            p, open_method="datatree-merge", variables=["sst"],
            spatial_method="auto", open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert "sst" in result.columns
        assert len(result) == 1
        assert not math.isnan(result.loc[0, "sst"])

    def test_auto_2d_matches_kdtree(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """auto with 2-D coords returns the same value as explicit kdtree."""
        pytest.importorskip("scipy")
        nc_path = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=77)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")

        lat_val = float(ds_swath["lat"].values[1, 2])
        lon_val = float(ds_swath["lon"].values[1, 2])

        pts = pd.DataFrame(
            {"lat": [lat_val], "lon": [lon_val], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )

        def make_plan() -> Plan:
            return Plan(
                points=pts,
                results=[object()],
                granules=[self._make_granule_meta()],
                point_granule_map={0: [0]},
                source_kwargs={"short_name": "TEST"},
                time_buffer=pd.Timedelta(0),
            )

        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        result_auto = pc.matchup(
            make_plan(), open_method="dataset", variables=["sst"],
            spatial_method="auto", open_dataset_kwargs={"engine": "netcdf4"},
        )
        mock_ea.open.return_value = [nc_path]  # ensure a fresh list for the second call
        result_kdtree = pc.matchup(
            make_plan(), open_method="dataset", variables=["sst"],
            spatial_method="kdtree", open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert result_auto.loc[0, "sst"] == pytest.approx(result_kdtree.loc[0, "sst"])

    def test_auto_prints_resolved_method(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """auto prints a one-line message showing the resolved spatial method and dims."""
        pytest.importorskip("scipy")
        # Test 1-D path (nearest)
        nc_path_1d = str(tmp_path / "grid.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path_1d, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path_1d]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        pc.matchup(p, open_method="dataset", spatial_method="auto", silent=False,
                   open_dataset_kwargs={"engine": "netcdf4"})
        captured = capsys.readouterr()
        assert "spatial_method='auto'" in captured.out
        assert "'nearest'" in captured.out
        assert "1-D" in captured.out

        # Test 2-D path (kdtree)
        nc_path_2d = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=42)
        ds_swath.to_netcdf(nc_path_2d, engine="netcdf4")
        lat_val = float(ds_swath["lat"].values[0, 0])
        lon_val = float(ds_swath["lon"].values[0, 0])
        pts2 = pd.DataFrame(
            {"lat": [lat_val], "lon": [lon_val], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p2 = Plan(
            points=pts2,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        mock_ea.open.return_value = [nc_path_2d]
        pc.matchup(p2, open_method="dataset", spatial_method="auto", silent=False,
                   open_dataset_kwargs={"engine": "netcdf4"})
        captured2 = capsys.readouterr()
        assert "spatial_method='auto'" in captured2.out
        assert "'kdtree'" in captured2.out
        assert "2-D" in captured2.out

    def test_explicit_method_does_not_print_auto_message(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """Explicit spatial_method does not emit the 'auto' selection message."""
        nc_path = str(tmp_path / "grid.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0]).to_netcdf(
            nc_path, engine="netcdf4"
        )
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        pc.matchup(p, open_method="dataset", spatial_method="nearest", silent=False,
                   open_dataset_kwargs={"engine": "netcdf4"})
        captured = capsys.readouterr()
        assert "spatial_method='auto'" not in captured.out

    def test_auto_invalid_string_raises(self) -> None:
        """An unrecognised spatial_method string raises ValueError early."""
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
        with pytest.raises(ValueError, match="spatial_method"):
            pc.matchup(p, spatial_method="bogus")

    def test_explicit_nearest_with_2d_raises_useful_message(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Explicit nearest with 2-D coords raises ValueError mentioning 'auto'/'kdtree'."""
        nc_path = str(tmp_path / "swath.nc")
        _make_l2_swath_dataset(nrows=4, ncols=5).to_netcdf(nc_path, engine="netcdf4")
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        pts = pd.DataFrame(
            {"lat": [0.0], "lon": [0.0], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        with pytest.raises(ValueError, match="auto"):
            pc.matchup(
                p, open_method="dataset", spatial_method="nearest",
                open_dataset_kwargs={"engine": "netcdf4"},
            )

    def test_auto_xoak_kdtree_never_selected(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """auto never picks xoak even if xoak is installed; xoak must be explicit."""
        pytest.importorskip("scipy")
        nc_path = str(tmp_path / "swath.nc")
        ds_swath = _make_l2_swath_dataset(nrows=4, ncols=5, seed=10)
        ds_swath.to_netcdf(nc_path, engine="netcdf4")
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)

        lat_val = float(ds_swath["lat"].values[0, 0])
        lon_val = float(ds_swath["lon"].values[0, 0])
        pts = pd.DataFrame(
            {"lat": [lat_val], "lon": [lon_val], "time": pd.to_datetime(["2023-06-01T12:00:00"])}
        )
        p = Plan(
            points=pts,
            results=[object()],
            granules=[self._make_granule_meta()],
            point_granule_map={0: [0]},
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )
        # Block xoak — auto should still succeed via kdtree
        import sys
        for key in list(sys.modules.keys()):
            if key == "xoak" or key.startswith("xoak."):
                monkeypatch.delitem(sys.modules, key)
        monkeypatch.setitem(sys.modules, "xoak", None)  # type: ignore[assignment]
        monkeypatch.setitem(sys.modules, "xoak.tree_adapters", None)  # type: ignore[assignment]

        result = pc.matchup(
            p, open_method="dataset", variables=["sst"],
            spatial_method="auto", open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"])


# ---------------------------------------------------------------------------
# Time dimension detection and handling
# ---------------------------------------------------------------------------


class TestFindTimeDim:
    """Tests for _find_time_dim()."""

    def test_returns_none_when_no_time_dim(self) -> None:
        from point_collocation.core.engine import _find_time_dim

        ds = xr.Dataset(coords={"lat": [0.0], "lon": [0.0]})
        assert _find_time_dim(ds) is None

    def test_detects_time_dimension_by_name(self) -> None:
        from point_collocation.core.engine import _find_time_dim

        times = pd.to_datetime(["2023-06-01"])
        ds = xr.Dataset(
            {"sst": (["time", "lat", "lon"], [[[1.0]]])},
            coords={"time": times, "lat": [0.0], "lon": [0.0]},
        )
        assert _find_time_dim(ds) == "time"

    def test_detects_Time_dimension_by_name(self) -> None:
        from point_collocation.core.engine import _find_time_dim

        ds = xr.Dataset(
            {"sst": (["Time", "lat", "lon"], [[[1.0]]])},
            coords={"Time": [0], "lat": [0.0], "lon": [0.0]},
        )
        assert _find_time_dim(ds) == "Time"

    def test_ignores_non_time_3d_dims(self) -> None:
        from point_collocation.core.engine import _find_time_dim

        # 3D variable with wavelength, not time — should return None
        ds = xr.Dataset(
            {"Rrs": (["lat", "lon", "wavelength"], [[[0.001, 0.002]]])},
            coords={"lat": [0.0], "lon": [0.0], "wavelength": [412, 443]},
        )
        assert _find_time_dim(ds) is None


class TestSelectTime:
    """Tests for _select_time()."""

    def test_returns_unchanged_when_no_time_dim(self) -> None:
        from point_collocation.core.engine import _select_time

        da = xr.DataArray([1.0, 2.0], dims=["wavelength"])
        result = _select_time(da, "time", pd.Timestamp("2023-06-01"))
        assert result.dims == ("wavelength",)
        assert list(result.values) == [1.0, 2.0]

    def test_squeezes_single_time_step(self) -> None:
        from point_collocation.core.engine import _select_time

        times = pd.to_datetime(["2023-06-01"])
        da = xr.DataArray([25.0], dims=["time"], coords={"time": times})
        result = _select_time(da, "time", pd.Timestamp("2023-06-01"))
        assert result.ndim == 0
        assert float(result) == pytest.approx(25.0)

    def test_selects_nearest_time_step(self) -> None:
        from point_collocation.core.engine import _select_time

        times = pd.to_datetime(["2023-06-01", "2023-06-02", "2023-06-03"])
        da = xr.DataArray([10.0, 20.0, 30.0], dims=["time"], coords={"time": times})

        # Nearest to 2023-06-02 → middle value
        result = _select_time(da, "time", pd.Timestamp("2023-06-02"))
        assert float(result) == pytest.approx(20.0)

    def test_nearest_time_step_with_close_timestamp(self) -> None:
        from point_collocation.core.engine import _select_time

        times = pd.to_datetime(["2023-06-01", "2023-06-03"])
        da = xr.DataArray([10.0, 30.0], dims=["time"], coords={"time": times})

        # 2023-06-02 is equidistant; xarray picks one; just verify no exception and a real value
        result = _select_time(da, "time", pd.Timestamp("2023-06-01T06:00:00"))
        assert float(result) == pytest.approx(10.0)

    def test_falls_back_to_first_step_when_point_time_is_nat(self) -> None:
        from point_collocation.core.engine import _select_time

        times = pd.to_datetime(["2023-06-01", "2023-06-02"])
        da = xr.DataArray([10.0, 20.0], dims=["time"], coords={"time": times})

        result = _select_time(da, "time", pd.NaT)
        assert float(result) == pytest.approx(10.0)

    def test_falls_back_to_first_step_when_point_time_is_none(self) -> None:
        from point_collocation.core.engine import _select_time

        times = pd.to_datetime(["2023-06-01", "2023-06-02"])
        da = xr.DataArray([10.0, 20.0], dims=["time"], coords={"time": times})

        result = _select_time(da, "time", None)
        assert float(result) == pytest.approx(10.0)


class TestTimeDimMatchup:
    """Integration tests: matchup with (time, lat, lon) variables."""

    def _make_plan_with_nc(
        self,
        nc_path: str,
        monkeypatch: pytest.MonkeyPatch,
        lat: float = 0.0,
        lon: float = 0.0,
        point_time: str = "2023-06-01T12:00:00",
    ) -> "Plan":
        mock_ea = MagicMock()
        mock_ea.open.return_value = [nc_path]
        monkeypatch.setitem(__import__("sys").modules, "earthaccess", mock_ea)
        pts = pd.DataFrame(
            {"lat": [lat], "lon": [lon], "time": pd.to_datetime([point_time])}
        )
        gm = GranuleMeta(
            granule_id="https://example.com/g.nc",
            begin=pd.Timestamp("2023-06-01T00:00:00Z"),
            end=pd.Timestamp("2023-06-01T23:59:59Z"),
            bbox=(-180.0, -90.0, 180.0, 90.0),
            result_index=0,
        )
        return Plan(
            points=pts,
            results=[object()],
            granules=[gm],
            point_granule_map={0: [0]},
            variables=["sst"],
            source_kwargs={"short_name": "TEST"},
            time_buffer=pd.Timedelta(0),
        )

    def test_single_time_step_returns_value_not_nan(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Variable with one time step (time, lat, lon) must return a real value."""
        nc_path = str(tmp_path / "single_time.nc")
        _make_l3_time_dataset(
            [-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0],
            times=["2023-06-01T00:00:00"],
            seed=1,
        ).to_netcdf(nc_path, engine="netcdf4")

        p = self._make_plan_with_nc(nc_path, monkeypatch)
        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 1
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"]), (
            "sst must be a real value, not NaN, when the dataset has a single time step"
        )

    def test_multiple_time_steps_selects_nearest(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """With multiple time steps, the nearest to the point time is selected."""
        lats = [-90.0, 0.0, 90.0]
        lons = [-180.0, 0.0, 180.0]
        times = ["2023-06-01T00:00:00", "2023-06-02T00:00:00"]

        # Build a dataset where time-step 0 has sst=10 and time-step 1 has sst=20 at (0, 0)
        lat_arr = np.array(lats)
        lon_arr = np.array(lons)
        time_arr = pd.to_datetime(times)
        sst = np.zeros((len(time_arr), lat_arr.size, lon_arr.size), dtype=np.float32)
        lat_idx = 1  # lat=0
        lon_idx = 1  # lon=0
        sst[0, lat_idx, lon_idx] = 10.0
        sst[1, lat_idx, lon_idx] = 20.0
        ds = xr.Dataset(
            {"sst": (["time", "lat", "lon"], sst)},
            coords={"time": time_arr, "lat": lat_arr, "lon": lon_arr},
        )
        nc_path = str(tmp_path / "multi_time.nc")
        ds.to_netcdf(nc_path, engine="netcdf4")

        # Point timestamp near 2023-06-02 → nearest is index 1 → sst=20
        p = self._make_plan_with_nc(nc_path, monkeypatch, point_time="2023-06-02T06:00:00")
        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 1
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"])
        assert result.loc[0, "sst"] == pytest.approx(20.0)

    def test_no_time_dim_still_works(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Variables without a time dimension continue to work correctly."""
        nc_path = str(tmp_path / "no_time.nc")
        _make_l3_dataset([-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], seed=3).to_netcdf(
            nc_path, engine="netcdf4"
        )

        p = self._make_plan_with_nc(nc_path, monkeypatch)
        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert len(result) == 1
        assert "sst" in result.columns
        assert not math.isnan(result.loc[0, "sst"])

    def test_3d_wavelength_variable_not_broken_by_time_fix(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """3D variable (lat, lon, wavelength) must still expand to per-wavelength columns."""
        wavelengths = [412, 443, 490]
        nc_path = str(tmp_path / "rrs_no_time.nc")
        _make_l3_3d_dataset(
            [-90.0, 0.0, 90.0], [-180.0, 0.0, 180.0], wavelengths, seed=4
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
        result = pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert "Rrs" not in result.columns
        for wl in wavelengths:
            assert f"Rrs_{wl}" in result.columns
        assert len(result) == 1


# ---------------------------------------------------------------------------
# Tests for the new open_method spec-based pipeline
# ---------------------------------------------------------------------------


class TestOpenMethodNormalization:
    """Tests for _normalize_open_method() - string presets and dict specs."""

    def test_dataset_preset_expands_correctly(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method("dataset")
        assert spec["xarray_open"] == "dataset"
        assert spec["coords"] == "auto"
        assert spec["set_coords"] is True

    def test_datatree_merge_preset_expands_correctly(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method("datatree-merge")
        assert spec["xarray_open"] == "datatree"
        assert spec["merge"] == "all"
        assert spec["coords"] == "auto"

    def test_auto_preset_expands_correctly(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method("auto")
        assert spec["xarray_open"] == "auto"
        assert spec["coords"] == "auto"

    def test_unknown_string_raises(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        with pytest.raises(ValueError, match="open_method"):
            _normalize_open_method("bad_preset")

    def test_dict_spec_fills_missing_keys(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method({"xarray_open": "dataset"})
        assert spec["open_kwargs"] == {}
        assert spec["coords"] == "auto"
        assert spec["set_coords"] is True
        assert spec["dim_renames"] is None

    def test_dict_spec_datatree_fills_merge_defaults(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method({"xarray_open": "datatree"})
        assert spec["merge"] is None  # default is no-merge for plain datatree

    def test_dict_spec_datatree_explicit_merge_all(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method({"xarray_open": "datatree", "merge": "all"})
        assert spec["merge"] == "all"
        assert spec["merge_kwargs"] == {}  # merge_kwargs is set when merge is not None

    def test_dict_spec_unknown_key_raises(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        with pytest.raises(ValueError, match="unknown keys"):
            _normalize_open_method({"xarray_open": "dataset", "bad_key": "value"})

    def test_dict_spec_invalid_xarray_open_raises(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        with pytest.raises(ValueError, match="xarray_open"):
            _normalize_open_method({"xarray_open": "invalid"})

    def test_non_str_non_dict_raises(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        with pytest.raises(TypeError, match="string preset or dict spec"):
            _normalize_open_method(42)  # type: ignore[arg-type]

    def test_open_dataset_kwargs_merges_into_spec(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method("dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert spec["open_kwargs"]["engine"] == "netcdf4"

    def test_open_dataset_kwargs_overrides_spec_open_kwargs(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method

        spec = _normalize_open_method(
            {"xarray_open": "dataset", "open_kwargs": {"engine": "h5netcdf"}},
            open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert spec["open_kwargs"]["engine"] == "netcdf4"


class TestOpenKwargsDefaults:
    """Tests for default open kwargs (chunks, engine, decode_timedelta)."""

    def test_defaults_applied_when_empty(self) -> None:
        from point_collocation.core._open_method import _build_effective_open_kwargs

        result = _build_effective_open_kwargs({})
        assert result["chunks"] == {}
        assert result["engine"] == "h5netcdf"
        assert result["decode_timedelta"] is False

    def test_user_overrides_respected(self) -> None:
        from point_collocation.core._open_method import _build_effective_open_kwargs

        result = _build_effective_open_kwargs({"engine": "netcdf4", "chunks": {"x": 100}})
        assert result["engine"] == "netcdf4"
        assert result["chunks"] == {"x": 100}
        assert result["decode_timedelta"] is False  # default still applied

    def test_decode_timedelta_can_be_overridden(self) -> None:
        from point_collocation.core._open_method import _build_effective_open_kwargs

        result = _build_effective_open_kwargs({"decode_timedelta": True})
        assert result["decode_timedelta"] is True


class TestCoordNormalization:
    """Tests for _apply_coords() coordinate normalization."""

    def test_auto_finds_lon_lat(self) -> None:
        from point_collocation.core._open_method import _apply_coords

        ds = xr.Dataset(coords={"lon": [0.0], "lat": [0.0], "sst": 1.0})
        spec = {"coords": "auto", "set_coords": True}
        ds_out, lon_name, lat_name = _apply_coords(ds, spec)
        assert lon_name == "lon"
        assert lat_name == "lat"

    def test_explicit_coords_dict(self) -> None:
        from point_collocation.core._open_method import _apply_coords

        ds = xr.Dataset({"MyLon": ("x", [0.0]), "MyLat": ("x", [0.0]), "sst": ("x", [1.0])})
        spec = {"coords": {"lat": "MyLat", "lon": "MyLon"}, "set_coords": True}
        ds_out, lon_name, lat_name = _apply_coords(ds, spec)
        assert lon_name == "MyLon"
        assert lat_name == "MyLat"
        # Should be promoted to coords
        assert "MyLon" in ds_out.coords
        assert "MyLat" in ds_out.coords

    def test_explicit_coords_list(self) -> None:
        from point_collocation.core._open_method import _apply_coords

        ds = xr.Dataset({"Longitude": ("x", [0.0]), "Latitude": ("x", [0.0]), "sst": ("x", [1.0])})
        spec = {"coords": ["Latitude", "Longitude"], "set_coords": True}
        ds_out, lon_name, lat_name = _apply_coords(ds, spec)
        assert lon_name == "Longitude"
        assert lat_name == "Latitude"
        assert "Longitude" in ds_out.coords
        assert "Latitude" in ds_out.coords

    def test_missing_coords_dict_raises(self) -> None:
        from point_collocation.core._open_method import _apply_coords

        ds = xr.Dataset({"sst": ("x", [1.0])})
        spec = {"coords": {"lat": "NoLat", "lon": "NoLon"}, "set_coords": True}
        with pytest.raises(ValueError, match="not found"):
            _apply_coords(ds, spec)

    def test_auto_no_geoloc_raises(self) -> None:
        from point_collocation.core._open_method import _apply_coords

        ds = xr.Dataset({"sst": ("x", [1.0])})
        spec = {"coords": "auto", "set_coords": True}
        with pytest.raises(ValueError, match="no geolocation variables found"):
            _apply_coords(ds, spec)

    def test_set_coords_false_does_not_promote(self) -> None:
        from point_collocation.core._open_method import _apply_coords

        ds = xr.Dataset({"lon": ("x", [0.0]), "lat": ("x", [0.0]), "sst": ("x", [1.0])})
        spec = {"coords": "auto", "set_coords": False}
        ds_out, lon_name, lat_name = _apply_coords(ds, spec)
        # Variables not promoted since set_coords=False
        assert "lon" not in ds_out.coords
        assert "lat" not in ds_out.coords
        assert lon_name == "lon"
        assert lat_name == "lat"


class TestOpenAsDatasetFastPath:
    """Tests that open_method='dataset' uses xr.open_dataset (not datatree)."""

    def test_dataset_open_method_calls_open_dataset(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_method='dataset' uses xr.open_dataset (not datatree)."""
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
            granule_id="test.nc",
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

        called_open_dataset = []
        original_open_dataset = xr.open_dataset

        def mock_open_dataset(f, **kwargs):
            called_open_dataset.append(f)
            return original_open_dataset(f, **kwargs)

        monkeypatch.setattr(xr, "open_dataset", mock_open_dataset)

        # Should not fail, and open_dataset must be called
        pc.matchup(p, open_method="dataset", open_dataset_kwargs={"engine": "netcdf4"})
        assert len(called_open_dataset) >= 1

    def test_auto_open_method_succeeds_for_standard_file(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """open_method='auto' succeeds for standard files with lat/lon coords."""
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
            granule_id="test.nc",
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
            p, open_method="auto", variables=["sst"], open_dataset_kwargs={"engine": "netcdf4"}
        )
        assert not result["sst"].isna().all()


class TestProfiles:
    """Tests for the profiles module."""

    def test_pace_l3_profile_importable(self) -> None:
        from point_collocation.profiles import pace_l3

        assert isinstance(pace_l3, dict)
        assert pace_l3["xarray_open"] == "dataset"

    def test_pace_l2_profile_importable(self) -> None:
        from point_collocation.profiles import pace_l2

        assert isinstance(pace_l2, dict)
        assert pace_l2["xarray_open"] == "datatree"
        assert pace_l2["merge"] == "all"

    def test_pace_l3_normalizes_correctly(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method
        from point_collocation.profiles import pace_l3

        spec = _normalize_open_method(pace_l3)
        assert spec["xarray_open"] == "dataset"
        assert spec["coords"] == "auto"
        assert spec["set_coords"] is True

    def test_pace_l2_normalizes_correctly(self) -> None:
        from point_collocation.core._open_method import _normalize_open_method
        from point_collocation.profiles import pace_l2

        spec = _normalize_open_method(pace_l2)
        assert spec["xarray_open"] == "datatree"
        assert spec["merge"] == "all"
        assert spec["coords"] == "auto"

    def test_pace_l3_can_be_used_in_matchup(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """pace_l3 profile can be passed directly to pc.matchup()."""
        from point_collocation.profiles import pace_l3

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
            granule_id="test.nc",
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
            open_method=pace_l3,
            variables=["sst"],
            open_dataset_kwargs={"engine": "netcdf4"},
        )
        assert not result["sst"].isna().all()


class TestAutoAlignPhonyDims:
    """Tests for _safe_align_phony_dims()."""

    def test_aligns_two_datasets_with_matching_phony_dims(self) -> None:
        from point_collocation.core._open_method import _safe_align_phony_dims

        ds1 = xr.Dataset({"a": (["phony_dim_0", "phony_dim_1"], [[1.0, 2.0]])})
        ds2 = xr.Dataset({"b": (["phony_dim_2", "phony_dim_3"], [[3.0, 4.0]])})

        result = _safe_align_phony_dims([ds1, ds2])
        # Both should have been renamed to ("y", "x")
        assert "y" in result[0].dims
        assert "x" in result[0].dims
        assert "y" in result[1].dims
        assert "x" in result[1].dims

    def test_no_change_when_no_phony_dims(self) -> None:
        from point_collocation.core._open_method import _safe_align_phony_dims

        ds1 = xr.Dataset({"a": (["y", "x"], [[1.0, 2.0]])})
        ds2 = xr.Dataset({"b": (["y", "x"], [[3.0, 4.0]])})

        result = _safe_align_phony_dims([ds1, ds2])
        assert "y" in result[0].dims
        assert "x" in result[0].dims


class TestSuppressDaskProgress:
    """Tests for _suppress_dask_progress()."""

    def test_suppresses_stdout_output(self, capsys: pytest.CaptureFixture) -> None:
        """Output written to stdout inside the context is suppressed."""
        from point_collocation.core._open_method import _suppress_dask_progress

        with _suppress_dask_progress():
            print("this should be suppressed")

        captured = capsys.readouterr()
        assert "this should be suppressed" not in captured.out

    def test_suppresses_stderr_output(self, capsys: pytest.CaptureFixture) -> None:
        """Output written to stderr inside the context is suppressed."""
        import sys

        from point_collocation.core._open_method import _suppress_dask_progress

        with _suppress_dask_progress():
            print("stderr output", file=sys.stderr)

        captured = capsys.readouterr()
        assert "stderr output" not in captured.err

    def test_does_not_suppress_after_context(self, capsys: pytest.CaptureFixture) -> None:
        """Stdout/stderr are restored after the context exits."""
        from point_collocation.core._open_method import _suppress_dask_progress

        with _suppress_dask_progress():
            print("inside (suppressed)")

        print("outside (not suppressed)")
        captured = capsys.readouterr()
        assert "outside (not suppressed)" in captured.out
        assert "inside (suppressed)" not in captured.out

    def test_propagates_exceptions(self) -> None:
        """Exceptions raised inside the context are propagated normally."""
        from point_collocation.core._open_method import _suppress_dask_progress

        with pytest.raises(RuntimeError, match="test error"):
            with _suppress_dask_progress():
                raise RuntimeError("test error")

    def test_context_is_reentrant(self, capsys: pytest.CaptureFixture) -> None:
        """_suppress_dask_progress can be nested without error."""
        from point_collocation.core._open_method import _suppress_dask_progress

        with _suppress_dask_progress():
            with _suppress_dask_progress():
                print("nested suppression")

        captured = capsys.readouterr()
        assert "nested suppression" not in captured.out
