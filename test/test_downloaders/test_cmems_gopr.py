import os
import pytest
import datetime
from pathlib import Path

import rasterio
import numpy as np
import xarray as xr

from fetcheo.downloaders import cmems_gopr
from fetcheo.downloaders.cmems_gopr import CMEMSGlobalOceanPhysicsReanalysisDownloader


TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [[[-1.0, 49.0], [1.0, 49.0], [1.0, 51.0], [-1.0, 51.0], [-1.0, 49.0]]],
}


def _write_subset(path):
    dataset = xr.Dataset(
        {
            "thetao": (
                ("time", "depth", "latitude", "longitude"),
                np.arange(36, dtype=np.float32).reshape(2, 2, 3, 3),
            )
        },
        coords={
            "time": [np.datetime64("2021-01-01", "ns"), np.datetime64("2021-01-02", "ns")],
            "depth": [0.0, 5.0],
            "latitude": [49.0, 50.0, 51.0],
            "longitude": [-1.0, 0.0, 1.0],
        },
    )
    dataset.to_netcdf(path)


def test_cmems_fetch_subsets_clips_and_reports_each_timestamp(tmp_path, monkeypatch):
    calls = []

    def fake_subset(**kwargs):
        calls.append(kwargs)
        result = tmp_path / kwargs["output_directory"] / f"{kwargs['output_filename']}.nc"
        _write_subset(result)
        return result

    monkeypatch.setattr(cmems_gopr.copernicusmarine, "subset", fake_subset)
    downloader = CMEMSGlobalOceanPhysicsReanalysisDownloader(
        dataset_id="example-dataset",
        variables=["thetao"],
        min_depth=0.0,
        max_depth=5.0,
        username="user",
        password="secret",
    )
    reports = downloader.fetch(
        TEST_POLYGON,
        (datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 2)),
        tmp_path / "output",
        cache_dir=tmp_path / "cache",
        show_progress=False,
    )

    assert len(reports) == 4
    assert all(report.download_successful for report in reports)
    assert [report.acquisition_time for report in reports] == [
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
        datetime.datetime(2021, 1, 1),
        datetime.datetime(2021, 1, 2),
    ]
    assert [report.metadata["depth_m"] for report in reports] == [0.0, 0.0, 5.0, 5.0]
    assert all(
        report.metadata["standard_name"] == "sea_water_potential_temperature"
        for report in reports
    )
    assert all(report.path.is_file() for report in reports)
    assert all(report.data_source == "CMEMS" for report in reports)
    assert calls[0]["dataset_id"] == "example-dataset"
    assert calls[0]["variables"] == ["thetao"]
    assert calls[0]["minimum_longitude"] == -1.0
    assert calls[0]["maximum_latitude"] == 51.0
    assert calls[0]["username"] == "user"

    # The cached subset is reused on subsequent fetches.
    downloader.fetch(
        TEST_POLYGON,
        (datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 2)),
        tmp_path / "output",
        cache_dir=tmp_path / "cache",
        show_progress=False,
    )
    assert len(calls) == 1


def test_cmems_fetch_reports_missing_variable(tmp_path, monkeypatch):
    subset = tmp_path / "subset.nc"
    _write_subset(subset)
    downloader = CMEMSGlobalOceanPhysicsReanalysisDownloader(
        dataset_id="example-dataset", variables=["not_present"]
    )
    monkeypatch.setattr(downloader, "_ensure_downloaded", lambda *args: subset)

    reports = downloader.fetch(
        TEST_POLYGON,
        (datetime.datetime(2021, 1, 1), datetime.datetime(2021, 1, 2)),
        tmp_path / "output",
        show_progress=False,
    )

    assert len(reports) == 1
    assert not reports[0].download_successful
    assert "missing" in reports[0].error


def test_cmems_requires_dataset_and_variables():
    with pytest.raises(ValueError, match="dataset_id"):
        CMEMSGlobalOceanPhysicsReanalysisDownloader(dataset_id="", variables=["water_temp"])
    with pytest.raises(ValueError, match="variables"):
        CMEMSGlobalOceanPhysicsReanalysisDownloader(dataset_id="example-dataset", variables=[])


def test_cmems_defaults_to_all_glorys_variables():
    downloader = CMEMSGlobalOceanPhysicsReanalysisDownloader()
    assert downloader.variables == list(downloader.GLORYS_VARIABLES)
    assert len(downloader.variables) == 11
    assert downloader.GLORYS_VARIABLES["thetao"] == "sea_water_potential_temperature"


def test_cmems_integration_downloads_valid_geotiffs(tmp_path):
    """Live download check; requires network access and Copernicus Marine credentials."""
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to run this test (requires internet and CMEMS credentials).")

    polygon = {
        "type": "Polygon",
        "coordinates": [[[5, 65], [25, 65], [25, 75], [5, 75], [5, 65]]],
    }
    time_frame = (datetime.datetime(2023, 6, 1), datetime.datetime(2023, 6, 3))
    output_dir = Path(tmp_path) / "cmems_output"
    cache_dir = Path(tmp_path) / "cmems_cache"

    downloader = CMEMSGlobalOceanPhysicsReanalysisDownloader(
        variables=["thetao", "zos"],
        frequency="daily",
    )
    reports = downloader.fetch(
        polygon=polygon,
        time_frame=time_frame,
        output_dir=output_dir,
        cache_dir=cache_dir,
        show_progress=False,
    )

    assert reports, "CMEMS returned no download reports"
    assert {report.variable_name for report in reports} == {"thetao", "zos"}
    expected_dates = {
        datetime.datetime(2023, 6, day).date()
        for day in range(time_frame[0].day, time_frame[1].day + 1)
    }
    for variable in ("thetao", "zos"):
        actual_dates = {
            report.acquisition_time.date()
            for report in reports
            if report.variable_name == variable and report.download_successful
        }
        assert actual_dates == expected_dates, f"Unexpected dates for {variable}: {actual_dates}"
    for report in reports:
        assert report.download_successful, (
            f"{report.variable_name} | {report.acquisition_time.date()} | {report.error}"
        )
        assert report.path.is_file(), f"GeoTIFF does not exist: {report.path}"
        with rasterio.open(report.path) as raster:
            assert raster.count >= 1
            assert raster.width > 0 and raster.height > 0
            assert raster.crs is not None
            assert raster.read(1).size > 0
