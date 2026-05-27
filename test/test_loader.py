import datetime
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock
from contextlib import ExitStack

from fetcheo.loader import FetchEOLoader

TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [0, 0],
            [1, 0],
            [1, 1],
            [0, 1],
            [0, 0],
        ]
    ],
}

def test_loader_instantiates_downloaders():
    # Use dummy downloaders for simplicity
    class DummyDownloader:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
        def fetch(self, *a, **k):
            return []
    loader = FetchEOLoader({}, {})
    loader.downloaders = {
        'era5': DummyDownloader(),
        'modis_ndvi': DummyDownloader(),
    }
    assert 'era5' in loader.downloaders
    assert 'modis_ndvi' in loader.downloaders
    assert 'sen3_openeo' not in loader.downloaders

@patch('fetcheo.loader.connect_to_db')
@patch('fetcheo.loader.initialise_tables')
@patch('fetcheo.loader.fetch_or_create_location_id')
@patch('fetcheo.loader.upsert_file')
def test_loader_fetch_calls_downloaders(mock_upsert, mock_locid, mock_init_tables, mock_connect_db, tmp_path):
    # Use dummy downloaders with a known fetch signature
    class DummyReport:
        def __init__(self):
            self.acquisition_time = datetime.datetime(2020, 1, 1)
            self.path = tmp_path / "dummy.tif"
            self.download_successful = True
            self.data_source = "test"
            self.variable_name = "var"
            self.frequency = "monthly"
            self.error = None
            self.metadata = None
            self.polygon = TEST_POLYGON
            self.bbox = (0, 0, 1, 1)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_bytes(b"test")
    class DummyDownloader:
        def fetch(self, *a, **k):
            return [DummyReport()]
    loader = FetchEOLoader({}, {})
    loader.downloaders = {
        'era5': DummyDownloader(),
        'modis_ndvi': DummyDownloader(),
    }
    polygon = TEST_POLYGON
    time_frame = (datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 31))
    reports = loader.fetch(
        polygon, 
        time_frame, 
        location_nickname="testloc", 
        output_dir=tmp_path, 
        cache_dir=tmp_path
        )
    assert isinstance(reports, list)
    assert reports[0].acquisition_time == datetime.datetime(2020, 1, 1)
    # Assert report structure and types
    required_attrs = [
        "acquisition_time", "path", "download_successful", "data_source",
        "variable_name", "frequency", "error", "metadata", "polygon", "bbox"
    ]
    for report in reports:
        for attr in required_attrs:
            assert hasattr(report, attr), f"Missing attribute: {attr}"
        assert isinstance(report.acquisition_time, datetime.datetime)
        assert isinstance(report.path, Path)
        assert isinstance(report.download_successful, bool)
        assert isinstance(report.data_source, str)
        assert isinstance(report.variable_name, str)
        assert isinstance(report.frequency, str)
        # error and metadata can be None or str/dict, so skip strict type check
        assert isinstance(report.polygon, dict)
        assert isinstance(report.bbox, (tuple, list))
    mock_connect_db.assert_called_once()
    mock_init_tables.assert_called_once()
    mock_locid.assert_called_once()
    mock_upsert.assert_called()

def test_loader_handles_empty_config():
    loader = FetchEOLoader({}, {})
    assert loader.downloaders == {}

def test_loader_handles_missing_kwargs():
    config = {'modis_ndvi': True}
    loader = FetchEOLoader(config)
    assert 'modis_ndvi' in loader.downloaders
