"""Tests for the public API surface of earthaccess_matchup."""

from __future__ import annotations

import inspect

import pandas as pd
import pytest

import earthaccess_matchup as eam
from earthaccess_matchup.core.engine import matchup
from earthaccess_matchup.core.types import SourceProtocol
from earthaccess_matchup.diagnostics.report import GranuleSummary, MatchupReport

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_points(**extra: object) -> pd.DataFrame:
    """Minimal valid points DataFrame."""
    data: dict[str, object] = {
        "lat": [34.5, 35.1],
        "lon": [-120.3, -119.8],
        "time": pd.to_datetime(["2023-06-01", "2023-06-02"]),
    }
    data.update(extra)
    return pd.DataFrame(data)


class _StubSource:
    """Minimal SourceProtocol implementation that always raises."""

    def open_dataset(self, **kwargs: object) -> object:  # pragma: no cover
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Import / attribute tests
# ---------------------------------------------------------------------------

class TestPublicAPI:
    def test_matchup_importable_from_top_level(self) -> None:
        assert callable(eam.matchup)

    def test_matchup_is_same_object_as_engine(self) -> None:
        assert eam.matchup is matchup

    def test_all_contains_matchup(self) -> None:
        assert "matchup" in eam.__all__


# ---------------------------------------------------------------------------
# SourceProtocol structural subtyping
# ---------------------------------------------------------------------------

class TestSourceProtocol:
    def test_stub_satisfies_protocol(self) -> None:
        assert isinstance(_StubSource(), SourceProtocol)

    def test_object_without_open_dataset_does_not_satisfy_protocol(self) -> None:
        assert not isinstance(object(), SourceProtocol)


# ---------------------------------------------------------------------------
# matchup() signature & basic behaviour
# ---------------------------------------------------------------------------

class TestMatchupSignature:
    def test_accepts_required_arguments(self) -> None:
        sig = inspect.signature(matchup)
        params = sig.parameters
        assert "points" in params
        assert "source_kwargs" in params
        assert "variables" in params

    def test_data_source_defaults_to_earthaccess(self) -> None:
        sig = inspect.signature(matchup)
        assert sig.parameters["data_source"].default == "earthaccess"


class TestMatchupBehaviour:
    @pytest.fixture(autouse=True)
    def _patch_resolve(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Patch _resolve_earthaccess_sources to return an empty list."""
        monkeypatch.setattr(
            "earthaccess_matchup.core.engine._resolve_earthaccess_sources",
            lambda *args, **kwargs: [],
        )

    def test_returns_dataframe_by_default(self) -> None:
        points = _make_points()
        result = matchup(points, source_kwargs={"short_name": "TEST"}, variables=["sst"])
        assert isinstance(result, pd.DataFrame)

    def test_result_preserves_original_columns(self) -> None:
        points = _make_points(station_id=["A", "B"])
        result = matchup(points, source_kwargs={"short_name": "TEST"}, variables=["sst"])
        for col in ("lat", "lon", "time", "station_id"):
            assert col in result.columns

    def test_result_has_variable_columns(self) -> None:
        points = _make_points()
        result = matchup(points, source_kwargs={"short_name": "TEST"}, variables=["sst", "chlor_a"])
        assert "sst" in result.columns
        assert "chlor_a" in result.columns

    def test_result_rows_match_input(self) -> None:
        points = _make_points()
        result = matchup(points, source_kwargs={"short_name": "TEST"}, variables=["sst"])
        assert len(result) == len(points)

    def test_return_diagnostics_returns_tuple(self) -> None:
        points = _make_points()
        out = matchup(points, source_kwargs={"short_name": "TEST"}, variables=["sst"], return_diagnostics=True)
        assert isinstance(out, tuple)
        df, report = out
        assert isinstance(df, pd.DataFrame)
        assert isinstance(report, MatchupReport)

    def test_raises_on_missing_lat_column(self) -> None:
        bad = pd.DataFrame({"lon": [1.0], "time": pd.to_datetime(["2023-01-01"])})
        with pytest.raises(ValueError, match="lat"):
            matchup(bad, source_kwargs={"short_name": "TEST"}, variables=["sst"])

    def test_raises_on_missing_lon_column(self) -> None:
        bad = pd.DataFrame({"lat": [1.0], "time": pd.to_datetime(["2023-01-01"])})
        with pytest.raises(ValueError, match="lon"):
            matchup(bad, source_kwargs={"short_name": "TEST"}, variables=["sst"])

    def test_raises_on_missing_time_column(self) -> None:
        bad = pd.DataFrame({"lat": [1.0], "lon": [2.0]})
        with pytest.raises(ValueError, match="time"):
            matchup(bad, source_kwargs={"short_name": "TEST"}, variables=["sst"])

    def test_empty_sources_returns_nan_variables(self) -> None:
        import math
        points = _make_points()
        result = matchup(points, source_kwargs={"short_name": "TEST"}, variables=["sst"])
        assert all(math.isnan(v) for v in result["sst"])


# ---------------------------------------------------------------------------
# MatchupReport
# ---------------------------------------------------------------------------

class TestMatchupReport:
    def test_initial_counts_are_zero(self) -> None:
        report = MatchupReport()
        assert report.total == 0
        assert report.succeeded == 0
        assert report.skipped == 0

    def test_elapsed_seconds_is_non_negative(self) -> None:
        report = MatchupReport()
        assert report.elapsed_seconds >= 0

    def test_summary_is_string(self) -> None:
        report = MatchupReport()
        assert isinstance(report.summary(), str)

    def test_add_granule_increments_total(self) -> None:
        report = MatchupReport()
        report._add_granule(GranuleSummary(granule_id="file.nc"))
        assert report.total == 1
        assert report.succeeded == 1
        assert report.skipped == 0

    def test_failed_granule_increments_skipped(self) -> None:
        report = MatchupReport()
        report._add_granule(
            GranuleSummary(granule_id="bad.nc", error="open failed")
        )
        assert report.skipped == 1
        assert report.succeeded == 0
