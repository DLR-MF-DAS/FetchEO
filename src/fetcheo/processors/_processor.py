"""The processing contract: turning downloaded files into derived products.

FetchEO downloads and catalogues; it deliberately knows nothing about the
science applied afterwards.  This module defines the narrow interface that lets
an external project — DIVE's rain-cell wind analysis is the reference case —
run inside the same pipeline as a first-class stage:

    downloaders  ->  file catalogue  ->  processors  ->  product catalogue

A processor receives a :class:`ProcessingContext` (the polygon and time frame it
was asked about, where to write, and every input file that is available for that
location) and returns one :class:`ItemProcessReport` per product it produced.
It declares the data sources it needs through :attr:`BaseProcessor.required_sources`,
so the loader can skip it when they are missing rather than failing mid-run.

Everything a processor needs is passed in.  It never imports the loader, the
database, or a downloader, which is what keeps the two projects independent.
"""

from __future__ import annotations

import datetime
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence


@dataclass
class ItemProcessReport:
    """One derived product, mirroring ``ItemDownloadReport`` for downloads.

    Attributes:
        processor: Name the processor is registered under.
        product_name: Name of the derived product (the analogue of a variable).
        acquisition_time: Time the product refers to, or None when it aggregates
            a whole period (a composite over many dates, for instance).
        polygon: GeoJSON geometry the product was computed for.
        bbox: ``[xmin, ymin, xmax, ymax]`` of that geometry.
        path: File written.
        process_successful: Whether the product was produced.
        error: Failure message, when it was not.
        metadata: Free-form dict stored as JSON in the catalogue.
        inputs: Input files the product was derived from, for provenance.
    """

    processor: str
    product_name: str
    acquisition_time: Optional[datetime.datetime]
    polygon: dict
    bbox: List[float]
    path: Path
    process_successful: bool
    error: Optional[str] = None
    metadata: Optional[dict] = None
    inputs: List[Path] = field(default_factory=list)


@dataclass
class InputFile:
    """An input file offered to a processor, as recorded in the catalogue."""

    data_source: str
    variable_name: Optional[str]
    acquisition_time: Optional[datetime.datetime]
    path: Path
    metadata: Optional[dict] = None

    @property
    def exists(self) -> bool:
        return self.path.exists()


@dataclass
class ProcessingContext:
    """Everything a processor is given about the run it takes part in.

    Attributes:
        polygon: GeoJSON geometry of the location.
        bbox: ``[xmin, ymin, xmax, ymax]`` of that geometry.
        time_frame: ``(start, end)`` datetimes the run covers.
        location_nickname: Human-readable location key.
        location_id: Catalogue identifier of the location.
        output_dir: Directory the processor writes its products to.
        cache_dir: Scratch directory reserved for this processor.
        inputs: Files available for this location and period.
        query_catalog: Optional callable for a broader catalogue lookup, e.g.
            ``query_catalog(data_source="radar", time_frame=(a, b))``, which lets
            a processor reach files downloaded in an earlier run.
    """

    polygon: dict
    bbox: List[float]
    time_frame: tuple[datetime.datetime, datetime.datetime]
    location_nickname: str
    location_id: Optional[str]
    output_dir: Path
    cache_dir: Path
    inputs: List[InputFile] = field(default_factory=list)
    query_catalog: Optional[Callable[..., List[InputFile]]] = None
    extra: Dict[str, Any] = field(default_factory=dict)

    def inputs_from(self, *data_sources: str,
                    existing_only: bool = True) -> List[InputFile]:
        """Input files belonging to any of *data_sources*, time-ordered.

        Args:
            data_sources: Data-source names to keep; all of them when empty.
            existing_only: Drop entries whose file is missing on disk (a failed
                download still has a catalogue row).
        """
        wanted = set(data_sources)
        selected = [
            item for item in self.inputs
            if (not wanted or item.data_source in wanted)
            and (not existing_only or item.exists)
        ]
        return sorted(
            selected,
            key=lambda item: (item.acquisition_time or datetime.datetime.min,
                              str(item.path)),
        )

    def available_sources(self) -> set[str]:
        """Data sources with at least one usable file in this context."""
        return {item.data_source for item in self.inputs if item.exists}


class BaseProcessor(ABC):
    """Base class for everything that derives a product from downloaded files."""

    #: Data sources the processor cannot work without.  An empty tuple means it
    #: decides for itself, and :meth:`can_run` will always allow it.
    required_sources: Sequence[str] = ()

    #: Data sources the processor uses when they happen to be there.
    optional_sources: Sequence[str] = ()

    @property
    @abstractmethod
    def name(self) -> str:
        """Short name the products are catalogued under."""

    @abstractmethod
    def process(self, context: ProcessingContext,
                show_progress: bool = True) -> List[ItemProcessReport]:
        """Produce the derived products for *context*.

        Implementations should report failures as reports with
        ``process_successful=False`` rather than raising, so that one broken
        product does not abort the whole run.
        """

    def can_run(self, context: ProcessingContext) -> tuple[bool, str]:
        """Whether the required sources are present in *context*.

        Returns:
            ``(True, "")`` when the processor can run, otherwise ``(False,
            reason)`` with the missing sources spelled out.
        """
        if not self.required_sources:
            return True, ""
        missing = sorted(set(self.required_sources) - context.available_sources())
        if missing:
            return False, f"missing required data source(s): {', '.join(missing)}"
        return True, ""

    def _failure(self, context: ProcessingContext, product_name: str,
                 error: str, path: Optional[Path] = None) -> ItemProcessReport:
        """Build the report of a product that could not be produced."""
        return ItemProcessReport(
            processor=self.name,
            product_name=product_name,
            acquisition_time=None,
            polygon=context.polygon,
            bbox=context.bbox,
            path=path if path is not None else context.output_dir / product_name,
            process_successful=False,
            error=error,
        )
