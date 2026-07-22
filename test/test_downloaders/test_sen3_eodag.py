import os
import pytest
import datetime
import numpy as np
import rasterio
import xarray as xr
from pathlib import Path
from unittest.mock import patch

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
def test_sen3_eodag_fetch_skips_duplicate_output_for_same_acquisition(mock_eodag, tmp_path, monkeypatch):
    """`fetch` should emit one raster per same-time swath using the source product id."""
    monkeypatch.setenv("CDSE_USERNAME", "dummy_user")
    monkeypatch.setenv("CDSE_PASSWORD", "dummy_pass")

    acquisition_time = "2021-01-01T00:00:00Z"
    tile_dirs = [tmp_path / "tile_a", tmp_path / "tile_b"]
    for tile_dir in tile_dirs:
        tile_dir.mkdir()

    class FakeItem:
        def __init__(self, tile_id):
            self.properties = {"datetime": acquisition_time, "id": tile_id}

    fake_items = [FakeItem("tile-a"), FakeItem("tile-b")]
    mock_gateway = mock_eodag.return_value
    mock_gateway.search_all.return_value = fake_items
    mock_gateway.download.side_effect = [str(tile_dirs[0]), str(tile_dirs[1])]

    downloader = Sentinel3SynergyDownloader(
        variables_to_files_map={"Oa01_reflectance": "Oa01_reflectance"}
    )

    def fake_process_swath_to_grid(cache_dir, sen3_dir, nc_filename, var_name, bbox, area_def):
        value = 1.0 if sen3_dir.name == "tile_a" else 2.0
        data = xr.DataArray(
            [np.full((2, 2), value, dtype=np.float32)],
            dims=["band", "y", "x"],
            coords={
                "band": [1],
                "x": [-124.0, -123.99],
                "y": [33.0, 32.99],
            },
            name=var_name,
        )
        data.rio.write_crs("EPSG:4326", inplace=True)
        return data

    monkeypatch.setattr(downloader, "_process_swath_to_grid", fake_process_swath_to_grid)

    reports = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        cache_dir=tmp_path,
        show_progress=False,
    )

    assert downloader.frequency == "daily"
    assert len(reports) == 2
    assert mock_gateway.download.call_count == 2

    first_report, second_report = reports
    assert first_report.download_successful is True
    assert second_report.download_successful is True
    assert first_report.variable_name == "Oa01_reflectance"
    assert second_report.variable_name == "Oa01_reflectance"
    assert first_report.path != second_report.path
    assert first_report.path.name == "S3_20210101_000000_tile-a_Oa01_reflectance.tif"
    assert second_report.path.name == "S3_20210101_000000_tile-b_Oa01_reflectance.tif"
    assert first_report.path.exists()
    assert second_report.path.exists()
    assert first_report.metadata == {"note": "Exact timestamp preserved"}
    assert second_report.metadata == {"note": "Exact timestamp preserved"}

    with rasterio.open(first_report.path) as dataset:
        data = dataset.read(1)
        assert dataset.count == 1
        assert dataset.width == 2
        assert dataset.height == 2
        assert 1.0 in data

    with rasterio.open(second_report.path) as dataset:
        data = dataset.read(1)
        assert dataset.count == 1
        assert dataset.width == 2
        assert dataset.height == 2
        assert 2.0 in data


def test_sen3_eodag_integration(tmp_path):
    """
    Integration test for Sentinel3SynergyDownloader: only runs if RUN_INTEGRATION=1 is set.
    This test performs real network calls and requires credentials; it should remain skipped in CI.
    """
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to run this test (requires Copernicus Data Space credentials and internet).")
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
        if f.is_file():
            f.unlink()


@patch("fetcheo.downloaders.sen3_eodag.EODataAccessGateway")
def test_sen3_eodag_fetch_reports_failed_swath_download(mock_eodag, tmp_path, monkeypatch):
    """`fetch` should return an error report when a swath download fails."""
    monkeypatch.setenv("CDSE_USERNAME", "dummy_user")
    monkeypatch.setenv("CDSE_PASSWORD", "dummy_pass")

    class FakeItem:
        properties = {
            "datetime": "2021-01-01T12:34:56Z",
            "id": "tile-a",
        }

    mock_gateway = mock_eodag.return_value
    mock_gateway.search_all.return_value = [FakeItem()]
    mock_gateway.download.side_effect = RuntimeError("service unavailable")

    downloader = Sentinel3SynergyDownloader(
        variables_to_files_map={"Oa01_reflectance": "Oa01_reflectance"}
    )

    reports = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        cache_dir=tmp_path,
        show_progress=False,
    )

    assert len(reports) == 1
    report = reports[0]
    assert report.download_successful is False
    assert report.variable_name == "Synergy_Product"
    assert report.acquisition_time == datetime.datetime(2021, 1, 1, 12, 34, 56, tzinfo=datetime.timezone.utc)
    assert report.path == tmp_path
    assert report.error == "Download failed for swath tile-a at 20210101_123456: service unavailable"