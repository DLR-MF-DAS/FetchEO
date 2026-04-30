import os
import pytest
import datetime
from pathlib import Path
from unittest.mock import patch

import numpy as np
import xarray as xr

from fetcheo.downloaders._downloader import ItemDownloadReport
from fetcheo.downloaders.era5 import ERA5Downloader


# Global test variables for consistency
TEST_START_DATE = datetime.datetime(2021, 1, 1)
TEST_END_DATE = datetime.datetime(2021, 1, 31)

# Test is california bounding box
TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-124.0, 32.0],
            [-123.0, 32.0],
            [-123.0, 33.0],
            [-124.0, 33.0],
            [-124.0, 32.0],
        ]
    ],
}


def _dummy_da():
    """Small dummy DataArray with proper x/y spatial dims."""
    data = np.zeros((1, 2, 2), dtype=float)
    coords = {
        "time": [0],
        "y": [0.0, 1.0],
        "x": [0.0, 1.0],
    }
    da = xr.DataArray(data, coords=coords, dims=("time", "y", "x"))

    try:
        import rioxarray  # noqa: F401

        da = da.rio.write_crs("EPSG:4326")
        da = da.rio.set_spatial_dims(x_dim="x", y_dim="y")
    except Exception:
        # If rioxarray or rio accessor isn't available, we still get a usable object.
        pass

    return da


def _dummy_era5_ds():
    """Dummy ERA5-like Dataset with t2m and ssrd variables."""
    da = _dummy_da()
    return xr.Dataset({"t2m": da, "ssrd": da, "tp": da, "swvl1": da})


@patch("fetcheo.downloaders.era5.cdsapi.Client")
def test_era5_core(mock_client, tmp_path):
    """Custom test for ERA5Downloader: download (mocked), _save_geotiff, _validate_geotiff."""
    # Set up the mock to simulate .retrieve() behavior
    instance = mock_client.return_value
    instance.retrieve.return_value = None

    # Prepare dummy report for download
    dummy_report = [
        ItemDownloadReport(
            data_source="test_source",
            variable_name="test_variable",
            acquisition_time=datetime.datetime(2020, 1, 1),
            polygon=TEST_POLYGON,
            bbox=[-124.0, 32.0, -123.0, 33.0],
            path=tmp_path / "ERA5_202001.tif",
            download_successful=True,
            error=None,
            metadata=None,
        )
    ]

    with patch("fetcheo.downloaders.era5.ERA5Downloader.fetch") as mock_download:
        mock_download.return_value = dummy_report
        downloader = ERA5Downloader(cache_dir=tmp_path)
        report = downloader.fetch(
            polygon=TEST_POLYGON,
            time_frame=(TEST_START_DATE, TEST_END_DATE),
            output_dir=tmp_path,
        )
        mock_download.assert_called_once_with(
            polygon=TEST_POLYGON,
            time_frame=(TEST_START_DATE, TEST_END_DATE),
            output_dir=tmp_path,
        )
        assert isinstance(report, list)
        assert all(isinstance(item, ItemDownloadReport) for item in report)
        assert all(item.download_successful for item in report)
        assert downloader.frequency == "monthly"

    # Now test _save_geotiff and _validate_geotiff with dummy data
    da = _dummy_era5_ds()
    save_paths = downloader._save_geotiff(
        data=da,
        output_dir=tmp_path,
        basename="era5_test"
    )
    for path in save_paths.values():
        assert Path(path).exists()
    validate_paths = downloader._validate_geotiff(
        output_dir=tmp_path,
        basename="era5_test"
    )
    assert all(validate_paths.values())
    assert len(validate_paths) == len(save_paths)


def test_era5_integration(tmp_path):
    """
    Integration test for ERA5Downloader: actually downloads a small ERA5 file from CDS.
    Only runs if RUN_INTEGRATION=1 is set in the environment.
    """
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to run this test (requires CDS credentials and internet).")

    # Use the global test variables for consistency
    variables_dict = {"t2m": "2m_temperature"}
    downloader = ERA5Downloader(variables_dict=variables_dict, cache_dir=tmp_path)
    report = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=(TEST_START_DATE, TEST_END_DATE),
        output_dir=tmp_path,
        show_progress=False,
    )
    assert isinstance(report, list)
    assert len(report) == ((TEST_END_DATE.year - TEST_START_DATE.year) * 12 + (TEST_END_DATE.month - TEST_START_DATE.month) + 1)
    for item in report:
        assert item.download_successful, f"Download failed: {item.error}"
        assert Path(item.path).exists(), f"GeoTIFF not found: {item.path}"
    # Clean up
    for f in tmp_path.iterdir():
        f.unlink()
