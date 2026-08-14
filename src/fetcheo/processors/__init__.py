"""Processing stage: derived products built from the downloaded files.

See :mod:`fetcheo.processors._processor` for the contract, and
:mod:`fetcheo.registry` for how an external project registers its own processor.
"""

from fetcheo.processors._processor import (
    BaseProcessor,
    InputFile,
    ItemProcessReport,
    ProcessingContext,
)

__all__ = [
    "BaseProcessor",
    "InputFile",
    "ItemProcessReport",
    "ProcessingContext",
]
