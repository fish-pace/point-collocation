"""MatchupReport — first-class diagnostics for every matchup run.

A ``MatchupReport`` is produced automatically during each call to
:func:`earthaccess_matchup.matchup` and surfaced to the caller when
``return_diagnostics=True``.

Recorded information
--------------------
* Total number of granules attempted, succeeded, and skipped.
* Per-granule I/O timing (seconds to open + extract).
* Variables found versus variables missing for each granule.
* Per-granule warnings and errors (e.g., no temporal overlap).

The report is intentionally lightweight: it stores only plain Python
objects (strings, numbers, dicts) so it can be serialised to JSON
without any additional dependencies.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class GranuleSummary:
    """Diagnostics for a single granule processed during a matchup run."""

    granule_id: str
    """Human-readable identifier (file name or URL)."""

    elapsed_seconds: float = 0.0
    """Wall-clock time in seconds spent opening + extracting this granule."""

    variables_found: list[str] = field(default_factory=list)
    """Variables that were present in the dataset and extracted."""

    variables_missing: list[str] = field(default_factory=list)
    """Variables requested but not present in the dataset."""

    warnings: list[str] = field(default_factory=list)
    """Non-fatal issues encountered (e.g., partial temporal overlap)."""

    error: str | None = None
    """If set, the granule was skipped due to this error message."""

    @property
    def succeeded(self) -> bool:
        """``True`` if the granule was opened and extracted without a fatal error."""
        return self.error is None


class MatchupReport:
    """Collects diagnostics produced during a single matchup run.

    Attributes
    ----------
    granules:
        Ordered list of :class:`GranuleSummary` objects, one per source
        opened during the run.

    Examples
    --------
    ::

        out, report = eam.matchup(..., return_diagnostics=True)
        print(report.summary())
    """

    def __init__(self) -> None:
        self.granules: list[GranuleSummary] = []
        self._start_time: float = time.monotonic()

    # ------------------------------------------------------------------
    # Internal helpers used by the engine
    # ------------------------------------------------------------------

    def _add_granule(self, summary: GranuleSummary) -> None:
        """Append a completed :class:`GranuleSummary`."""
        self.granules.append(summary)

    # ------------------------------------------------------------------
    # Public reporting interface
    # ------------------------------------------------------------------

    @property
    def total(self) -> int:
        """Total number of granules attempted."""
        return len(self.granules)

    @property
    def succeeded(self) -> int:
        """Number of granules opened and extracted without a fatal error."""
        return sum(1 for g in self.granules if g.succeeded)

    @property
    def skipped(self) -> int:
        """Number of granules skipped due to a fatal error."""
        return self.total - self.succeeded

    @property
    def elapsed_seconds(self) -> float:
        """Wall-clock seconds since this report was created."""
        return time.monotonic() - self._start_time

    def summary(self) -> str:
        """Return a human-readable one-line summary of the run."""
        return (
            f"Matchup complete: {self.succeeded}/{self.total} granules succeeded, "
            f"{self.skipped} skipped — {self.elapsed_seconds:.1f}s total"
        )
