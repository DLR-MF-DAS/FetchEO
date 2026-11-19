from __future__ import annotations

import datetime
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import rasterio
import xarray as xr

from fetcheo.downloaders.dive_era5 import DIVEERA5Downloader


TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [[
        [20.0, 65.0],
        [20.0833333, 65.0],
        [20.0833333, 65.0833333],
        [20.0, 65.0833333],
        [20.0, 65.0],
    ]],
}


def _write_dataset(path: Path) -> Path:
    """Write a compact fixture with the Zenodo record's coordinate names and variables."""
    # Mirror the real Zenodo file's decoded timestamps and time-origin metadata.
    times = np.array(["2097-12-31", "2098-01-01"], dtype="datetime64[ns]")
    latitudes = np.array([65.0, 65.0833333, 65.1666667])
    longitudes = np.array([20.0, 20.0833333, 20.1666667])
    shape = (len(times), len(latitudes), len(longitudes))
    dataset = xr.Dataset(
        {
            "spar": (("time", "latitude", "longitude"), np.arange(np.prod(shape), dtype=np.float32).reshape(shape), {"units": "W m-2"}),
            "tcc": (("time", "latitude", "longitude"), np.full(shape, 0.5, dtype=np.float32), {"units": "1"}),
            "tp": (("time", "latitude", "longitude"), np.ones(shape, dtype=np.float32), {"units": "mm"}),
        },
        coords={"time": times, "latitude": latitudes, "longitude": longitudes},
    )
    dataset["time"].attrs["timeorigin"] = "1900.01.01"
    dataset.to_netcdf(path)
    return path


def test_constructor_defaults_to_zenodo_record_and_expected_fields():
    downloader = DIVEERA5Downloader(time_batch_size=0)

    assert downloader.source_url_or_path == "https://zenodo.org/records/23416271"
    assert downloader.data_source_name == "DIVE_ERA5"
    assert downloader.variables == ["spar", "tcc", "tp"]
    assert downloader.time_batch_size == 1
    assert downloader.verify_md5 is True
    assert downloader.frequency == "daily"


def test_fetch_exports_each_surface_field(tmp_path):
    netcdf_path = _write_dataset(tmp_path / "era5.nc")
    downloader = DIVEERA5Downloader(source_url_or_path=str(netcdf_path), time_batch_size=1)

    reports = downloader.fetch(
        TEST_POLYGON,
        (datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 1)),
        tmp_path / "output",
        show_progress=False,
    )

    assert {report.variable_name for report in reports} == {"spar", "tcc", "tp"}
    assert len(reports) == 3
    assert all(report.download_successful for report in reports)
    assert all(report.acquisition_time == datetime.datetime(2008, 1, 1) for report in reports)
    assert all(report.path.is_file() for report in reports)

    spar_report = next(report for report in reports if report.variable_name == "spar")
    with rasterio.open(spar_report.path) as raster:
        assert raster.count == 1
        assert raster.crs.to_string() == "EPSG:4326"
        assert raster.tags()["source_doi"] == "10.5281/zenodo.23416271"
        np.testing.assert_array_equal(
            raster.read(1), np.array([[3, 4], [0, 1]], dtype=np.float32)
        )


def test_zenodo_era5_download_integration(tmp_path):
    """Download the ~2.8 GB source and check its schema plus a small raster extraction."""
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to download the 2.8 GB Zenodo dataset.")

    downloader = DIVEERA5Downloader(time_batch_size=1)
    cache_dir = tmp_path / "cache"
    netcdf_path = downloader._ensure_local_netcdf(cache_dir, show_progress=False)

    assert netcdf_path.name == "dive_inputdata_svar_spartcctp_20w29e_65n79n_0083deg.nc"
    assert netcdf_path.stat().st_size > 2_000_000_000
    assert not netcdf_path.with_name(f"{netcdf_path.name}.part").exists()

    with xr.open_dataset(netcdf_path) as dataset:
        assert {"time", "latitude", "longitude"}.issubset(dataset.coords)
        assert {"spar", "tcc", "tp"}.issubset(dataset.data_vars)
        for variable in ("spar", "tcc", "tp"):
            assert dataset[variable].dims == ("time", "latitude", "longitude")
        latitude = float(dataset.latitude.values[0])
        longitude = float(dataset.longitude.values[0])
        lat_resolution = float(np.median(np.abs(np.diff(dataset.latitude.values))))
        lon_resolution = float(np.median(np.abs(np.diff(dataset.longitude.values))))
        timestamp = pd.Timestamp(dataset.time.values[0]).to_pydatetime()

    epsilon = 0.1
    polygon = {
        "type": "Polygon",
        "coordinates": [[
            [longitude - lon_resolution * epsilon, latitude - lat_resolution * epsilon],
            [longitude + lon_resolution * epsilon, latitude - lat_resolution * epsilon],
            [longitude + lon_resolution * epsilon, latitude + lat_resolution * epsilon],
            [longitude - lon_resolution * epsilon, latitude + lat_resolution * epsilon],
            [longitude - lon_resolution * epsilon, latitude - lat_resolution * epsilon],
        ]],
    }
    reports = downloader.fetch(
        polygon,
        (timestamp, timestamp),
        tmp_path / "output",
        cache_dir=cache_dir,
        show_progress=False,
    )

    assert {report.variable_name for report in reports} == {"spar", "tcc", "tp"}
    assert all(report.download_successful for report in reports)
    assert all(report.path.is_file() and report.path.stat().st_size > 0 for report in reports)
    for report in reports:
        with rasterio.open(report.path) as raster:
            assert raster.count == 1
            assert raster.width >= 1 and raster.height >= 1
            assert raster.crs.to_string() == "EPSG:4326"
            assert raster.tags()["source_doi"] == "10.5281/zenodo.23416271"
