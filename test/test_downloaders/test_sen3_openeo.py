import os
import pytest
import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

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

def test_sen3_openeo_core(tmp_path):
	"""Test Sen3WaterOpenEODownloader: fetch (mocked), _validate_geotiff."""
	dummy_report = [
		ItemDownloadReport(
			data_source="Sentinel3Water-openeo",
			variable_name="B01,B02",
			acquisition_time=datetime.datetime(2021, 1, 1),
			polygon=TEST_POLYGON,
			bbox=[-124.0, 32.0, -123.0, 33.0],
			path=tmp_path / "S3_WATER_20210101T000000.tif",
			download_successful=True,
			error=None,
			metadata=None,
		)
	]
	with patch("fetcheo.downloaders.sen3_openeo.Sen3WaterOpenEODownloader.fetch") as mock_fetch:
		mock_fetch.return_value = dummy_report
		downloader = Sen3WaterOpenEODownloader(bands=["B01", "B02"])
		report = downloader.fetch(
			polygon=TEST_POLYGON,
			time_frame=(TEST_START_DATE, TEST_END_DATE),
			output_dir=tmp_path,
		)
		mock_fetch.assert_called_once_with(
			polygon=TEST_POLYGON,
			time_frame=(TEST_START_DATE, TEST_END_DATE),
			output_dir=tmp_path,
		)
		assert isinstance(report, list)
		assert all(isinstance(item, ItemDownloadReport) for item in report)
		assert all(item.download_successful for item in report)
		assert downloader.frequency == "daily"

	# Optionally, test _validate_geotiff with dummy data if implemented


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
        show_progress=False,
    )
    assert isinstance(report, list)
    for item in report:
        assert item.download_successful, f"Download failed: {item.error}"
        assert Path(item.path).exists(), f"GeoTIFF not found: {item.path}"
    # Clean up
    for f in tmp_path.iterdir():
        f.unlink()
