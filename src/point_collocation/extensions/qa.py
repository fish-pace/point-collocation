"""Quality-flag (QA) filtering extension.

This extension provides post-extraction hooks that mask or discard
matchup results based on quality-flag variables present in the
source dataset.

Slot in the extension pipeline
--------------------------------
Pass a ``QAFilter`` instance as the ``post_extract`` argument to
:func:`point_collocation.matchup` once the hook API is implemented.

Not yet implemented.
"""

from __future__ import annotations


class QAFilter:
    """Discard matchup results that fail a quality threshold.

    Parameters
    ----------
    flag_variable:
        Name of the quality-flag variable in the source dataset.
    valid_flags:
        Sequence of flag values considered valid (e.g. ``[0, 1]``).
    """

    def __init__(self, flag_variable: str, valid_flags: list[int]) -> None:
        self.flag_variable = flag_variable
        self.valid_flags = valid_flags

    def __call__(self, dataset: object, result: object) -> object:
        """Mask *result* according to quality flags.

        Not yet implemented.
        """
        raise NotImplementedError
