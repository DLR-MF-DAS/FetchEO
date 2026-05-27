import json
import os
import pytest
import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

import numpy as np
import rasterio
from rasterio.transform import from_bounds

from fetcheo.downloaders._downloader import ItemDownloadReport
from fetcheo.downloaders.sen3_openeo import Sen3WaterOpenEODownloader


# Global test variables for consistency
TEST_START_DATE = datetime.datetime(2021, 1, 1)
TEST_END_DATE = datetime.datetime(2021, 1, 5)

# Test is california bounding box
TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-124.0, 32.9],
            [-123.9, 32.9],
            [-123.9, 33.0],
            [-124.0, 33.0],
            [-124.0, 32.9],
        ]
    ],
}



@patch("fetcheo.downloaders.sen3_openeo.openeo.connect")
def test_sen3_openeo_core(mock_connect, tmp_path):
    """Test Sen3WaterOpenEODownloader: fetch (mocked), _validate_geotiff."""
    # Mock the openeo connection and its authenticate_oidc method
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.authenticate_oidc.return_value = None

    # Simulate two bands, expect two reports
    dummy_reports = [
        ItemDownloadReport(
            data_source="Sentinel3Water-openeo",
            variable_name="B01",
            acquisition_time=datetime.datetime(2021, 1, 1),
            polygon=TEST_POLYGON,
            bbox=[-124.0, 32.0, -123.0, 33.0],
            path=tmp_path / "S3_WATER_20210101T000000_B01.tif",
            download_successful=True,
            error=None,
            metadata=None,
        ),
        ItemDownloadReport(
            data_source="Sentinel3Water-openeo",
            variable_name="B02",
            acquisition_time=datetime.datetime(2021, 1, 1),
            polygon=TEST_POLYGON,
            bbox=[-124.0, 32.0, -123.0, 33.0],
            path=tmp_path / "S3_WATER_20210101T000000_B02.tif",
            download_successful=True,
            error=None,
            metadata=None,
        ),
    ]
    with patch("fetcheo.downloaders.sen3_openeo.Sen3WaterOpenEODownloader.fetch") as mock_fetch:
        mock_fetch.return_value = dummy_reports
        downloader = Sen3WaterOpenEODownloader(bands=["B01", "B02"])
        reports = downloader.fetch(
            polygon=TEST_POLYGON,
            time_frame=(TEST_START_DATE, TEST_END_DATE),
            output_dir=tmp_path,
            cache_dir=tmp_path,
        )
        mock_fetch.assert_called_once_with(
            polygon=TEST_POLYGON,
            time_frame=(TEST_START_DATE, TEST_END_DATE),
            output_dir=tmp_path,
            cache_dir=tmp_path,
        )
        assert isinstance(reports, list)
        assert len(reports) == 2
        assert all(isinstance(item, ItemDownloadReport) for item in reports)
        assert all(item.download_successful for item in reports)
        assert {r.variable_name for r in reports} == {"B01", "B02"}
        assert downloader.frequency == "daily"


def test_sen3_openeo_integration(tmp_path):
    """
    Integration test for Sen3WaterOpenEODownloader: only runs if RUN_INTEGRATION=1 is set.
    This test is mocked for safety; remove the patch to run a real integration test.
    """
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to run this test (requires openEO credentials and internet).")

    downloader = Sen3WaterOpenEODownloader(bands=["B01", "B02"])
    report = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        cache_dir=tmp_path,
        show_progress=False,
    )
    assert isinstance(report, list)
    for item in report:
        assert item.download_successful, f"Download failed: {item.error}"
        assert Path(item.path).exists(), f"GeoTIFF not found: {item.path}"
    # Clean up
    for f in tmp_path.iterdir():
        f.unlink()


# ---------------------------------------------------------------------------
# Helpers for splitting tests
# ---------------------------------------------------------------------------

_TIF_FILENAME = "S3_out_20210101.tif"
_STAC_JSON = {
    "links": [
        {
            "rel": "derived_from",
            "href": "https://example.com/S3A_OL_2_WFR_20210101T000000_END.SAFE",
        }
    ],
    "assets": {_TIF_FILENAME: {"href": f"https://example.com/{_TIF_FILENAME}"}},
}


def _write_multiband_tiff(path, n_bands):
    """Write a tiny valid multi-band GeoTIFF with n_bands bands."""
    transform = from_bounds(-124.0, 32.9, -123.9, 33.0, 2, 2)
    profile = {
        "driver": "GTiff",
        "dtype": "float32",
        "width": 2,
        "height": 2,
        "count": n_bands,
        "crs": "EPSG:4326",
        "transform": transform,
    }
    with rasterio.open(path, "w", **profile) as dst:
        for b in range(1, n_bands + 1):
            dst.write(np.ones((2, 2), dtype=np.float32) * b, b)


def _make_download_files_side_effect(tmp_path, tif_writer):
    """Return a callable that populates the raw temp dir when `download_files` is called."""

    def _side_effect(dest_dir):
        dest = Path(dest_dir)
        dest.mkdir(parents=True, exist_ok=True)
        (dest / "result.json").write_text(json.dumps(_STAC_JSON))
        tif_writer(dest / _TIF_FILENAME)

    return _side_effect


def _configure_mock_connection(mock_connection, download_files_side_effect):
    """Wire the mock openEO connection chain."""
    mock_datacube = MagicMock()
    mock_saved_cube = MagicMock()
    mock_batch_job = MagicMock()
    mock_results = MagicMock()
    mock_connection.load_collection.return_value = mock_datacube
    mock_datacube.save_result.return_value = mock_saved_cube
    mock_saved_cube.create_job.return_value = mock_batch_job
    mock_batch_job.start_and_wait.return_value = None
    mock_batch_job.get_results.return_value = mock_results
    mock_results.download_files.side_effect = download_files_side_effect


# ---------------------------------------------------------------------------
# Splitting unit tests
# ---------------------------------------------------------------------------

@patch("fetcheo.downloaders.sen3_openeo.openeo.connect")
def test_split_multiband_tiff_success(mock_connect, tmp_path):
    """A valid 2-band GeoTIFF is split into 2 single-band files with successful reports."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.authenticate_oidc.return_value = None

    _configure_mock_connection(
        mock_connection,
        _make_download_files_side_effect(tmp_path, lambda p: _write_multiband_tiff(p, 2)),
    )

    downloader = Sen3WaterOpenEODownloader(bands=["B01", "B02"])
    reports = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        cache_dir=tmp_path,
        show_progress=False,
    )

    assert len(reports) == 2
    assert all(isinstance(r, ItemDownloadReport) for r in reports)
    assert {r.variable_name for r in reports} == {"B01", "B02"}
    assert all(r.download_successful for r in reports)
    assert all(r.error is None for r in reports)
    for r in reports:
        assert Path(r.path).exists(), f"Expected output file not found: {r.path}"


@patch("fetcheo.downloaders.sen3_openeo.openeo.connect")
def test_split_corrupt_tiff_yields_failed_reports(mock_connect, tmp_path):
    """A corrupt (non-TIFF) file produces failed reports for all bands without crashing."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.authenticate_oidc.return_value = None

    def _write_corrupt(path):
        Path(path).write_bytes(b"NOT_A_VALID_TIFF")

    _configure_mock_connection(
        mock_connection,
        _make_download_files_side_effect(tmp_path, _write_corrupt),
    )

    downloader = Sen3WaterOpenEODownloader(bands=["B01", "B02"])
    reports = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        cache_dir=tmp_path,
        show_progress=False,
    )

    assert len(reports) == 2
    assert all(isinstance(r, ItemDownloadReport) for r in reports)
    assert {r.variable_name for r in reports} == {"B01", "B02"}
    assert not any(r.download_successful for r in reports)
    assert all(r.error is not None for r in reports)


@patch("fetcheo.downloaders.sen3_openeo.openeo.connect")
def test_split_band_count_mismatch_yields_failed_reports(mock_connect, tmp_path):
    """When the TIFF band count doesn't match the requested bands, all reports fail."""
    mock_connection = MagicMock()
    mock_connect.return_value = mock_connection
    mock_connection.authenticate_oidc.return_value = None

    # Write a 1-band TIFF but request 2 bands
    _configure_mock_connection(
        mock_connection,
        _make_download_files_side_effect(tmp_path, lambda p: _write_multiband_tiff(p, 1)),
    )

    downloader = Sen3WaterOpenEODownloader(bands=["B01", "B02"])
    reports = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        cache_dir=tmp_path,
        show_progress=False,
    )

    assert len(reports) == 2
    assert all(isinstance(r, ItemDownloadReport) for r in reports)
    assert {r.variable_name for r in reports} == {"B01", "B02"}
    assert not any(r.download_successful for r in reports)
    assert all("mismatch" in (r.error or "").lower() for r in reports)
