import datetime
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

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
    config = {
        'era5': True,
        'modis_ndvi': True,
        'sen3_openeo': False,
    }
    kwargs = {
        'era5': {'variables_dict': {'t2m': '2m_temperature'}},
        'modis_ndvi': {},
    }
    loader = FetchEOLoader(config, kwargs)
    assert 'era5' in loader.downloaders
    assert 'modis_ndvi' in loader.downloaders
    assert 'sen3_openeo' not in loader.downloaders

@patch('fetcheo.loader.connect_to_db')
@patch('fetcheo.loader.initialise_tables')
@patch('fetcheo.loader.fetch_or_create_location_id')
@patch('fetcheo.loader.upsert_file')
def test_loader_fetch_calls_downloaders(mock_upsert, mock_locid, mock_init_tables, mock_connect_db, tmp_path):
    # Patch the downloader fetch methods to return dummy reports
    dummy_report = MagicMock()
    dummy_report.acquisition_time = datetime.datetime(2020, 1, 1)
    dummy_report.path = tmp_path / "dummy.tif"
    dummy_report.download_successful = True
    dummy_report.data_source = "test"
    dummy_report.variable_name = "var"
    dummy_report.frequency = "monthly"
    dummy_report.error = None
    dummy_report.metadata = None
    dummy_report.polygon = TEST_POLYGON
    dummy_report.bbox = (0, 0, 1, 1)
    dummy_report.path.parent.mkdir(parents=True, exist_ok=True)
    dummy_report.path.write_bytes(b"test")

    config = {'era5': True}
    kwargs = {'era5': {'variables_dict': {'t2m': '2m_temperature'}}}
    loader = FetchEOLoader(config, kwargs)
    # Patch the fetch method
    for d in loader.downloaders.values():
        d.fetch = MagicMock(return_value=[dummy_report])

    polygon = TEST_POLYGON
    time_frame = (datetime.datetime(2020, 1, 1), datetime.datetime(2020, 1, 31))
    reports = loader.fetch(polygon, time_frame, location_nickname="testloc", output_dir=tmp_path)
    assert isinstance(reports, list)
    assert reports[0].acquisition_time == datetime.datetime(2020, 1, 1)
    for d in loader.downloaders.values():
        d.fetch.assert_called_once()
    mock_connect_db.assert_called_once()
    mock_init_tables.assert_called_once()
    mock_locid.assert_called_once()
    mock_upsert.assert_called()

def test_loader_handles_empty_config():
    loader = FetchEOLoader({}, {})
    assert loader.downloaders == {}

def test_loader_handles_missing_kwargs():
    config = {'era5': True}
    loader = FetchEOLoader(config)
    assert 'era5' in loader.downloaders
