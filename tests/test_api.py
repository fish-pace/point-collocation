"""Tests for the public API surface of point_collocation."""

from __future__ import annotations

import inspect

import pandas as pd
import pytest

import point_collocation as pc
from point_collocation.core.engine import matchup
from point_collocation.core.types import SourceProtocol
from point_collocation.diagnostics.report import GranuleSummary, MatchupReport

# ---------------------------------------------------------------------------
# Import / attribute tests
# ---------------------------------------------------------------------------

class TestPublicAPI:
    def test_matchup_importable_from_top_level(self) -> None:
        assert callable(pc.matchup)

    def test_matchup_is_same_object_as_engine(self) -> None:
        assert pc.matchup is matchup

    def test_all_contains_matchup(self) -> None:
        assert "matchup" in pc.__all__


# ---------------------------------------------------------------------------
# SourceProtocol structural subtyping
# ---------------------------------------------------------------------------

class TestSourceProtocol:
    def test_stub_satisfies_protocol(self) -> None:
        class _StubSource:
            def open_dataset(self, **kwargs: object) -> object:  # pragma: no cover
                raise NotImplementedError

        assert isinstance(_StubSource(), SourceProtocol)

    def test_object_without_open_dataset_does_not_satisfy_protocol(self) -> None:
        assert not isinstance(object(), SourceProtocol)


# ---------------------------------------------------------------------------
# matchup() signature
# ---------------------------------------------------------------------------

class TestMatchupSignature:
    def test_accepts_plan_argument(self) -> None:
        sig = inspect.signature(matchup)
        params = sig.parameters
        assert "plan" in params

    def test_accepts_open_dataset_kwargs(self) -> None:
        sig = inspect.signature(matchup)
        params = sig.parameters
        assert "open_dataset_kwargs" in params, "matchup() must accept open_dataset_kwargs"

    def test_no_dataframe_parameters(self) -> None:
        sig = inspect.signature(matchup)
        params = sig.parameters
        assert "data_source" not in params
        assert "source_kwargs" not in params
        assert "return_diagnostics" not in params

    def test_accepts_variables(self) -> None:
        sig = inspect.signature(matchup)
        params = sig.parameters
        assert "variables" in params, "matchup() must accept a variables kwarg"


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
