import importlib
import inspect
import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import numpy as np
import xarray as xr

from fetcheo.downloaders._downloader import ItemDownloadReport


downloader_classes = [
    ("fetcheo.downloaders.era5", "ERA5Downloader", "monthly"),
    ("fetcheo.downloaders.modis_ndvi", "MODISNDVIDownloader", "monthly"),
    ("fetcheo.downloaders.spei", "SPEIDownloader", "monthly"),
    ("fetcheo.downloaders.esacci_landcover", "ESACCILandCoverDownloader", "yearly"),
    ("fetcheo.downloaders.ecira", "ECIRADownloader", "yearly"),
    ("fetcheo.downloaders.sen2_aws", "Sen2AWSDownloader", "daily"),
    ("fetcheo.downloaders.sen3_cdse", "Sen3CDSEDownloader", "daily"),
    ("fetcheo.downloaders.sen2_openeo", "Sen2OpenEODownloader", "daily"),
    ("fetcheo.downloaders.sen3_openeo", "Sen3WaterOpenEODownloader", "daily"),
]


# Global test variables for consistency
TEST_START_DATE = datetime.datetime(2021, 1, 1)
TEST_END_DATE = datetime.datetime(2021, 3, 31)

# Test is california bounding box
TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [
            [-124.0, 32.0],
            [-114.0, 32.0],
            [-114.0, 42.0],
            [-124.0, 42.0],
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


@pytest.mark.parametrize("modname,class_name,expected_freq", downloader_classes)
def test_downloader_generalized(tmp_path, modname, class_name, expected_freq, mock_fetch=True):
    # Import class dynamically
    mod = importlib.import_module(modname)
    Downloader = getattr(mod, class_name)
    time_frame = (TEST_START_DATE, TEST_END_DATE)

    # Dummy report for mocking
    dummy_report = [
        ItemDownloadReport(
            data_source="test_source",
            variable_name="test_variable",
            acquisition_time=datetime.datetime(2021, 2, 1),
            polygon=TEST_POLYGON,
            bbox=(-124.0, 32.0, -114.0, 42.0),
            path=tmp_path / f"{class_name}_202102.tif",
            download_successful=True,
            error=None,
            metadata=None,
        ),
        ItemDownloadReport(
            data_source="test_source",
            variable_name="test_variable",
            acquisition_time=datetime.datetime(2021, 3, 1),
            polygon=TEST_POLYGON,
            bbox=(-124.0, 32.0, -114.0, 42.0),
            path=tmp_path / f"{class_name}_202103.tif",
            download_successful=True,
            error=None,
            metadata=None,
        ),
        ItemDownloadReport(
            data_source="test_source",
            variable_name="test_variable",
            acquisition_time=datetime.datetime(2021, 4, 1),
            polygon=TEST_POLYGON,
            bbox=(-124.0, 32.0, -114.0, 42.0),
            path=tmp_path / f"{class_name}_202104.tif",
            download_successful=True,
            error=None,
            metadata=None,
        )
    ]

    # Patch fetch if requested
    fetch_path = f"{modname}.{class_name}.fetch"
    init_path = f"{modname}.{class_name}.__init__"
    if mock_fetch:
        with patch(init_path, return_value=None), patch(fetch_path, return_value=dummy_report):
            _run_downloader_test(Downloader, tmp_path, time_frame, expected_freq, dummy_report)
    else:
        _run_downloader_test(Downloader, tmp_path, time_frame, expected_freq, None)

def _run_downloader_test(Downloader, tmp_path, time_frame, expected_freq, dummy_report):
    # Instantiate
    downloader = Downloader(cache_dir=tmp_path) if 'cache_dir' in inspect.signature(Downloader).parameters else Downloader()
    # Patch required attributes for ERA5Downloader and similar classes
    if Downloader.__name__ == "ERA5Downloader":
        downloader.engine = "netcdf4"
        downloader.variables_dict = {"t2m": "2m_temperature", "ssrd": "surface_solar_radiation_downwards", "tp": "total_precipitation", "swvl1": "volumetric_soil_water_layer_1"}
        downloader.product_type = "monthly_averaged_reanalysis"
        downloader.dataset = "reanalysis-era5-land-monthly-means"
        downloader.time_key = "time"
        downloader.cache_dir = tmp_path
        def _dummy_era5_ds():
            """Dummy ERA5-like Dataset with t2m and ssrd variables."""
            da = _dummy_da()
            return xr.Dataset({"t2m": da, "ssrd": da, "tp": da, "swvl1": da})
    # ...add similar blocks for other downloaders if needed...
    # Run fetch
    report = downloader.fetch(
        polygon=TEST_POLYGON,
        time_frame=time_frame,
        output_dir=tmp_path,
    )
    assert isinstance(report, list)
    assert all(isinstance(item, ItemDownloadReport) for item in report)
    assert all(item.download_successful for item in report)

    # Frequency property
    assert hasattr(downloader, "frequency")
    assert downloader.frequency == expected_freq

    # Test _save_geotiff/_validate_geotiff if present
    if hasattr(downloader, "_save_geotiff") and hasattr(downloader, "_validate_geotiff"):
        if Downloader.__name__ == "ERA5Downloader":
            da = _dummy_era5_ds()
        else:
            da = _dummy_da()
        save_paths = downloader._save_geotiff(data=da, output_dir=tmp_path, basename=f"{Downloader.__name__}_test")
        for path in save_paths.values():
            assert Path(path).exists()
        validate_paths = downloader._validate_geotiff(output_dir=tmp_path, basename=f"{Downloader.__name__}_test")
        assert all(validate_paths.values())
        assert len(validate_paths) == len(save_paths)