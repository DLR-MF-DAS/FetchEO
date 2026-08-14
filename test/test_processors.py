import datetime
import json
import tempfile
from pathlib import Path

import pytest

from fetcheo.loader import FetchEOLoader, _extract_bbox
from fetcheo.processors import (
    BaseProcessor,
    InputFile,
    ItemProcessReport,
    ProcessingContext,
)
from fetcheo.registry import PROCESSOR_REGISTRY

TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
}
TIME_FRAME = (datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 31))


def make_context(tmp_path, inputs=None):
    return ProcessingContext(
        polygon=TEST_POLYGON,
        bbox=_extract_bbox(TEST_POLYGON),
        time_frame=TIME_FRAME,
        location_nickname="testloc",
        location_id="loc-1",
        output_dir=tmp_path,
        cache_dir=tmp_path,
        inputs=inputs or [],
    )


def make_input(tmp_path, name, data_source, day, create=True):
    path = tmp_path / name
    if create:
        path.write_text("x", encoding="utf-8")
    return InputFile(
        data_source=data_source,
        variable_name="var",
        acquisition_time=datetime.datetime(2020, 1, day),
        path=path,
    )


# ── Context ────────────────────────────────────────────────────────────────────

def test_extract_bbox_handles_nested_geometries():
    assert _extract_bbox(TEST_POLYGON) == [0, 0, 1, 1]
    multi = {"type": "MultiPolygon",
             "coordinates": [[[[0, 0], [2, 0], [2, 3], [0, 0]]]]}
    assert _extract_bbox(multi) == [0, 0, 2, 3]
    assert _extract_bbox({"coordinates": []}) == []


def test_inputs_from_filters_sorts_and_drops_missing(tmp_path):
    inputs = [
        make_input(tmp_path, "b.nc", "radar", 5),
        make_input(tmp_path, "a.nc", "radar", 2),
        make_input(tmp_path, "c.nc", "sar", 3),
        make_input(tmp_path, "gone.nc", "radar", 1, create=False),
    ]
    context = make_context(tmp_path, inputs)

    radar = context.inputs_from("radar")
    assert [item.path.name for item in radar] == ["a.nc", "b.nc"]

    both = context.inputs_from("radar", "sar")
    assert [item.path.name for item in both] == ["a.nc", "c.nc", "b.nc"]

    # No filter means everything that exists.
    assert len(context.inputs_from()) == 3
    # Opting out of the existence check brings the failed download back.
    assert len(context.inputs_from("radar", existing_only=False)) == 3
    assert context.available_sources() == {"radar", "sar"}


# ── Contract ───────────────────────────────────────────────────────────────────

class DummyProcessor(BaseProcessor):
    required_sources = ("radar",)

    @property
    def name(self):
        return "dummy"

    def process(self, context, show_progress=True):
        out = context.output_dir / "product.json"
        used = context.inputs_from("radar")
        out.write_text(json.dumps({"n_inputs": len(used)}), encoding="utf-8")
        return [ItemProcessReport(
            processor=self.name,
            product_name="dummy_product",
            acquisition_time=context.time_frame[0],
            polygon=context.polygon,
            bbox=context.bbox,
            path=out,
            process_successful=True,
            metadata={"n_inputs": len(used)},
            inputs=[item.path for item in used],
        )]


class ExplodingProcessor(BaseProcessor):
    @property
    def name(self):
        return "boom"

    def process(self, context, show_progress=True):
        raise RuntimeError("this processor is broken")


def test_can_run_reports_missing_sources(tmp_path):
    processor = DummyProcessor()
    runnable, reason = processor.can_run(make_context(tmp_path))
    assert not runnable
    assert "radar" in reason

    context = make_context(tmp_path, [make_input(tmp_path, "a.nc", "radar", 1)])
    assert processor.can_run(context) == (True, "")


def test_processor_without_required_sources_always_runs(tmp_path):
    assert ExplodingProcessor().can_run(make_context(tmp_path)) == (True, "")


def test_failure_helper_builds_a_failed_report(tmp_path):
    report = DummyProcessor()._failure(make_context(tmp_path), "p", "nope")
    assert report.process_successful is False
    assert report.error == "nope"
    assert report.processor == "dummy"


# ── End to end through the loader ──────────────────────────────────────────────

class DummyReport:
    def __init__(self, path):
        self.acquisition_time = datetime.datetime(2020, 1, 2)
        self.path = path
        self.download_successful = True
        self.data_source = "radar"
        self.variable_name = "precip"
        self.frequency = "daily"
        self.error = None
        self.metadata = None
        self.polygon = TEST_POLYGON
        self.bbox = [0, 0, 1, 1]


class DummyDownloader:
    def __init__(self, path):
        self.path = path

    def fetch(self, *args, **kwargs):
        return [DummyReport(self.path)]


def run_pipeline(tmp_path, processors):
    downloaded = tmp_path / "radar_20200102.nc"
    downloaded.write_bytes(b"data")

    loader = FetchEOLoader({}, {}, db_path=tmp_path / "test.duckdb")
    loader.downloaders = {"radar": DummyDownloader(downloaded)}
    loader.processors = processors
    reports = loader.fetch(
        TEST_POLYGON, TIME_FRAME,
        location_nickname="testloc",
        output_dir=tmp_path / "out",
        cache_dir=tmp_path / "cache",
        show_progress=False,
    )
    return reports, tmp_path / "test.duckdb"


def test_loader_runs_processor_and_catalogues_the_product(tmp_path):
    reports, db_path = run_pipeline(tmp_path, {"dummy": DummyProcessor()})

    products = [r for r in reports if isinstance(r, ItemProcessReport)]
    assert len(products) == 1
    product = products[0]
    assert product.process_successful
    assert product.path.exists()
    # The processor saw the file the downloader just fetched, via the catalogue.
    assert product.metadata["n_inputs"] == 1
    assert len(product.inputs) == 1

    import duckdb
    conn = duckdb.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT processor, product_name, process_status FROM product_catalog"
        ).fetchall()
    finally:
        conn.close()
    assert rows == [("dummy", "dummy_product", "success")]


def test_loader_skips_a_processor_whose_sources_are_missing(tmp_path):
    class NeedsLidar(DummyProcessor):
        required_sources = ("lidar",)

        @property
        def name(self):
            return "needs_lidar"

    reports, db_path = run_pipeline(tmp_path, {"needs_lidar": NeedsLidar()})
    assert not [r for r in reports if isinstance(r, ItemProcessReport)]

    import duckdb
    conn = duckdb.connect(str(db_path))
    try:
        assert conn.execute("SELECT count(*) FROM product_catalog").fetchone()[0] == 0
    finally:
        conn.close()


def test_a_broken_processor_is_reported_not_raised(tmp_path):
    reports, db_path = run_pipeline(tmp_path, {"boom": ExplodingProcessor()})

    products = [r for r in reports if isinstance(r, ItemProcessReport)]
    assert len(products) == 1
    assert products[0].process_successful is False
    assert "broken" in products[0].error

    import duckdb
    conn = duckdb.connect(str(db_path))
    try:
        status = conn.execute(
            "SELECT process_status FROM product_catalog").fetchall()
    finally:
        conn.close()
    assert status == [("failed",)]


def test_loader_builds_processors_from_the_registry(tmp_path):
    PROCESSOR_REGISTRY.register("test_dummy", DummyProcessor)
    try:
        loader = FetchEOLoader({}, {}, db_path=tmp_path / "db.duckdb",
                               processor_config={"test_dummy": True})
        assert isinstance(loader.processors["test_dummy"], DummyProcessor)
    finally:
        PROCESSOR_REGISTRY._plugins.pop("test_dummy", None)


def test_loader_without_processors_is_unchanged(tmp_path):
    loader = FetchEOLoader({}, {}, db_path=tmp_path / "db.duckdb")
    assert loader.processors == {}
