"""Diagnostics and troubleshooting utilities.

Diagnostics are first-class.  Every matchup run produces a
:class:`MatchupReport` that records:

* granules attempted / succeeded / skipped
* I/O timing per granule
* variables found / missing
* any per-granule warnings or errors

The report is returned alongside the results DataFrame when
``return_diagnostics=True`` is passed to :func:`point_collocation.matchup`.
"""

from point_collocation.diagnostics.report import MatchupReport

__all__ = ["MatchupReport"]
