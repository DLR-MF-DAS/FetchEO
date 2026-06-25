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
def test_sen3_eodag_fetch_mosaics_tiles_for_same_acquisition(mock_eodag, tmp_path, monkeypatch):
    """`fetch` should mosaic multiple tiles for the same acquisition into one output."""
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
    mock_gateway.search.return_value = fake_items
    mock_gateway.download.side_effect = [str(tile_dirs[0]), str(tile_dirs[1])]

    downloader = Sentinel3SynergyDownloader(
        variables_to_files_map={"Oa01_reflectance": "Oa01_reflectance"}
    )

    def make_tile(x_start, value):
        data = xr.DataArray(
            np.full((2, 2), value, dtype=float),
            dims=["y", "x"],
            coords={
                "x": [x_start, x_start + 0.01],
                "y": [33.0, 32.99],
            },
            name="Oa01_reflectance",
        )
        data.rio.set_spatial_dims(x_dim="x", y_dim="y", inplace=True)
        data.rio.write_crs("EPSG:4326", inplace=True)
        return data

    def fake_clip(da, sen3_dir, polygon):
        if sen3_dir.name == "tile_a":
            return make_tile(-124.00, 1.0)
        return make_tile(-123.98, 2.0)

    monkeypatch.setattr(
        downloader,
        "_load_netcdf_as_array",
        lambda cache_dir, sen3_dir, nc_filename, variable_name: xr.DataArray([1.0]),
    )
    monkeypatch.setattr(downloader, "_clip_array_to_grid", fake_clip)

    reports = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        cache_dir=tmp_path,
        show_progress=False,
    )

    assert downloader.frequency == "daily"
    assert len(reports) == 1

    report = reports[0]
    assert report.download_successful is True
    assert report.variable_name == "Oa01_reflectance"
    assert report.path.name == "S3_20210101_000000_Oa01_reflectance.tif"
    assert report.path.exists()
    assert report.metadata is not None
    assert len(report.metadata["source_products"]) == 2

    with rasterio.open(report.path) as dataset:
        data = dataset.read(1)
        assert dataset.width == 4
        assert dataset.height == 2
        assert 1.0 in data
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
        f.unlink()