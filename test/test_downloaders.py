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
            _run_downloader_test(Downloader, 
                                 tmp_path, 
                                 time_frame, 
                                 expected_freq, 
                                 dummy_report)
    else:
        _run_downloader_test(Downloader, tmp_path, time_frame, expected_freq, None)

def _run_downloader_test(Downloader, tmp_path, time_frame, expected_freq, dummy_report):
    import shutil
    # Instantiate
    downloader = Downloader(cache_dir=tmp_path) if 'cache_dir' in inspect.signature(Downloader).parameters else Downloader()
    try:
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

    finally:
        # Clean up the tmp_path directory after test
        shutil.rmtree(tmp_path, ignore_errors=True)
