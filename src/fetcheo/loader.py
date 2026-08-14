import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fetcheo.duckdb_helpers import (
    connect_to_db,
    initialise_tables,
    fetch_or_create_location_id,
    query_files,
    upsert_file,
    upsert_product
)
from fetcheo.processors import InputFile, ProcessingContext
from fetcheo.registry import DOWNLOADER_REGISTRY, PROCESSOR_REGISTRY


# Set up module-level logger
logger = logging.getLogger(__name__)


# Kept as a module-level name for backwards compatibility: the authoritative
# list now lives in fetcheo.registry, which external packages can extend.
DOWNLOADER_DICT = DOWNLOADER_REGISTRY.as_dict()


def _extract_bbox(polygon: dict) -> List[float]:
    """Best-effort ``[xmin, ymin, xmax, ymax]`` from any GeoJSON geometry."""
    coords: List[Tuple[float, float]] = []

    def walk(node):
        if isinstance(node, (list, tuple)):
            if node and all(isinstance(v, (int, float)) for v in node[:2]) \
                    and not isinstance(node[0], (list, tuple)):
                coords.append((float(node[0]), float(node[1])))
            else:
                for child in node:
                    walk(child)

    walk(polygon.get("coordinates", []))
    if not coords:
        return []
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    return [min(lons), min(lats), max(lons), max(lats)]


class FetchEOLoader:
    """Runs the enabled downloaders, then the enabled processors, and catalogues both.

    Downloaders and processors are two separate plugin registries
    (:mod:`fetcheo.registry`), so a project can contribute either without being
    part of FetchEO: pass it by name once it is registered, or through a
    ``fetcheo.downloaders`` / ``fetcheo.processors`` entry point.
    """

    def __init__(self,
                 downloader_config: Dict[str, bool],
                 downloader_kwargs: Dict[str, dict] = None,
                 db_path: Path = Path('fetcheo_data.duckdb'),
                 processor_config: Dict[str, bool] = None,
                 processor_kwargs: Dict[str, dict] = None):
        self.downloader_kwargs = downloader_kwargs or {}
        self.processor_kwargs = processor_kwargs or {}
        self.db_path = db_path
        self.downloaders = DOWNLOADER_REGISTRY.create_many(
            downloader_config, self.downloader_kwargs)
        self.processors = PROCESSOR_REGISTRY.create_many(
            processor_config, self.processor_kwargs)

    def fetch(self,
              polygon: dict,
              time_frame: Tuple,
              location_nickname: str,
              output_dir: str = "data",
              cache_dir: str = None,
              show_progress: bool = True) -> List[Any]:
        """
        Fetches data using all enabled downloaders and adds results to DuckDB after each downloader.

        When processors are enabled they run afterwards, on the files this run
        downloaded *and* on anything already catalogued for the same location,
        and their products are recorded in ``product_catalog``.
        """
        # Ensure output_dir is a Path object
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create cache directory if not provided
        if cache_dir is None:
            base_cache_dir = output_dir / location_nickname / "cache"
        else:
            base_cache_dir = Path(cache_dir)

        # Connect to DB and ensure tables are initialised
        db_connection = connect_to_db(str(self.db_path))
        try:
            initialise_tables(db_connection)

            # Get or create location ID for this polygon and nickname
            location_id = fetch_or_create_location_id(db_connection, location_nickname, polygon)

            # Loop through downloaders and fetch data, adding to DB after each downloader
            all_reports = []

            for name, downloader in self.downloaders.items():
                # Set up cache directory for this downloader
                downloader_cache_dir = base_cache_dir / name
                downloader_cache_dir.mkdir(parents=True, exist_ok=True)

                # Fetch data for this downloader
                logger.info(f"Running downloader: {name}")
                reports = downloader.fetch(
                    polygon,
                    time_frame,
                    output_dir,
                    cache_dir=downloader_cache_dir,
                    show_progress=show_progress
                )
                all_reports.extend(reports)

                # Add each report to DB after each downloader
                for r in reports:
                    acq_time = getattr(r, 'acquisition_time', None)
                    year = acq_time.year if acq_time else None
                    month = acq_time.month if acq_time else None
                    upsert_file(
                        db_connection=db_connection,
                        location_id=location_id,
                        location_nickname=location_nickname,
                        data_source=getattr(r, 'data_source', None),
                        variable_name=getattr(r, 'variable_name', None),
                        frequency=getattr(r, 'frequency', None) if hasattr(r, 'frequency') else None,
                        acquisition_time=acq_time,
                        year=year,
                        month=month,
                        root_dir=str(Path(r.path).parent) if hasattr(r, 'path') else None,
                        file_name=str(Path(r.path).name) if hasattr(r, 'path') else None,
                        file_size_bytes=Path(r.path).stat().st_size if hasattr(r, 'path') and Path(r.path).exists() else None,
                        download_status="success" if getattr(r, 'download_successful', False) else "failed",
                        error_message=getattr(r, 'error', None),
                        metadata=getattr(r, 'metadata', None)
                    )

            if self.processors:
                all_reports.extend(self.run_processors(
                    db_connection=db_connection,
                    location_id=location_id,
                    location_nickname=location_nickname,
                    polygon=polygon,
                    time_frame=time_frame,
                    output_dir=output_dir,
                    base_cache_dir=base_cache_dir,
                    show_progress=show_progress,
                ))

            return all_reports
        finally:
            db_connection.close()

    # ------------------------------------------------------------------
    # Processing stage
    # ------------------------------------------------------------------

    def run_processors(self, db_connection, location_id, location_nickname,
                       polygon, time_frame, output_dir, base_cache_dir,
                       show_progress: bool = True) -> List[Any]:
        """Run every enabled processor and catalogue the products it returns."""
        inputs = self._collect_inputs(db_connection, location_id, time_frame)
        bbox = _extract_bbox(polygon)

        def query_catalog(**kwargs):
            kwargs.setdefault("location_id", location_id)
            return [
                InputFile(
                    data_source=row["data_source"],
                    variable_name=row["variable_name"],
                    acquisition_time=row["acquisition_time"],
                    path=Path(row["path"]),
                    metadata=row["metadata"],
                )
                for row in query_files(db_connection, **kwargs)
            ]

        product_reports = []
        for name, processor in self.processors.items():
            processor_output_dir = Path(output_dir) / "products" / name
            processor_output_dir.mkdir(parents=True, exist_ok=True)
            processor_cache_dir = Path(base_cache_dir) / "products" / name
            processor_cache_dir.mkdir(parents=True, exist_ok=True)

            context = ProcessingContext(
                polygon=polygon,
                bbox=bbox,
                time_frame=time_frame,
                location_nickname=location_nickname,
                location_id=location_id,
                output_dir=processor_output_dir,
                cache_dir=processor_cache_dir,
                inputs=inputs,
                query_catalog=query_catalog,
            )

            runnable, reason = processor.can_run(context)
            if not runnable:
                logger.warning("Skipping processor '%s': %s", name, reason)
                continue

            logger.info(f"Running processor: {name}")
            try:
                reports = processor.process(context, show_progress=show_progress)
            except Exception as exc:  # noqa: BLE001 - one processor must not sink the run
                logger.exception("Processor '%s' failed: %s", name, exc)
                reports = [processor._failure(context, name, str(exc))]

            product_reports.extend(reports)
            for r in reports:
                self._catalogue_product(db_connection, location_id,
                                        location_nickname, name, r)
        return product_reports

    def _collect_inputs(self, db_connection, location_id, time_frame) -> List[InputFile]:
        """Every catalogued file available to processors for this location."""
        return [
            InputFile(
                data_source=row["data_source"],
                variable_name=row["variable_name"],
                acquisition_time=row["acquisition_time"],
                path=Path(row["path"]),
                metadata=row["metadata"],
            )
            for row in query_files(db_connection, location_id=location_id,
                                   time_frame=time_frame)
        ]

    @staticmethod
    def _catalogue_product(db_connection, location_id, location_nickname,
                           processor_name, report) -> None:
        """Write one processor report into ``product_catalog``."""
        acq_time = getattr(report, 'acquisition_time', None)
        path = Path(report.path) if getattr(report, 'path', None) else None
        inputs = getattr(report, 'inputs', None)
        metadata = getattr(report, 'metadata', None)
        upsert_product(
            db_connection=db_connection,
            location_id=location_id,
            location_nickname=location_nickname,
            processor=getattr(report, 'processor', processor_name),
            product_name=getattr(report, 'product_name', None),
            acquisition_time=acq_time,
            year=acq_time.year if acq_time else None,
            month=acq_time.month if acq_time else None,
            root_dir=str(path.parent) if path else None,
            file_name=str(path.name) if path else None,
            file_size_bytes=path.stat().st_size if path and path.exists() else None,
            process_status="success" if getattr(report, 'process_successful', False) else "failed",
            error_message=getattr(report, 'error', None),
            inputs=json.dumps([str(p) for p in inputs]) if inputs else None,
            metadata=json.dumps(metadata) if isinstance(metadata, dict) else metadata,
        )
