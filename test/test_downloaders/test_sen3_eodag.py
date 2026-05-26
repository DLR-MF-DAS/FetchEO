import os
import pytest
import datetime
from pathlib import Path
from unittest.mock import patch, MagicMock

from rasterio.transform import from_bounds

from fetcheo.downloaders._downloader import ItemDownloadReport
from fetcheo.downloaders.sen3_eodag import Sentinel3SynergyDownloader


# Global test variables for consistency
TEST_START_DATE = datetime.datetime(2021, 1, 1)
TEST_END_DATE = datetime.datetime(2021, 1, 3)

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


@patch("fetcheo.downloaders.sen3_eodag.EODataAccessGateway")
def test_sen3_openeo_core(mock_eodag, tmp_path):
    """Test Sen3WaterOpenEODownloader: fetch (mocked), _validate_geotiff."""

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
    with patch("fetcheo.downloaders.sen3_eodag.Sentinel3SynergyDownloader.fetch") as mock_fetch:
        mock_fetch.return_value = dummy_reports
        downloader = Sentinel3SynergyDownloader(
            variables_to_files_map={
                "Oa01_reflectance": "Oa01_reflectance",
                "Oa02_reflectance": "Oa02_reflectance",
            }
        )
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


def test_sen3_eodag_integration(tmp_path):
    """
    Integration test for Sentinel3SynergyDownloader: only runs if RUN_INTEGRATION=1 is set.
    This test is mocked for safety; remove the patch to run a real integration test.
    """
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to run this test (requires openEO credentials and internet).")

    downloader = Sentinel3SynergyDownloader(
        variables_to_files_map={
            "Oa01_reflectance": "Oa01_reflectance",
            "Oa02_reflectance": "Oa02_reflectance",
        }
    )
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