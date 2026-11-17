from __future__ import annotations

import datetime
import os
from pathlib import Path

import numpy as np
import pytest
import rasterio
import xarray as xr

from fetcheo.downloaders.dive_pa import DIVEPlanktonAbundanceDownloader


TEST_POLYGON = {
    "type": "Polygon",
    "coordinates": [
        [[20.0, 65.0], [20.0833333, 65.0], [20.0833333, 65.0833333], [20.0, 65.0833333], [20.0, 65.0]]
    ],
}
TEST_FRAME = (datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 2))


def _write_dataset(path: Path, *, descending: bool = False) -> Path:
    """Write a small DIVE-like file with (lat, lon, time, feedingmode) ordering."""
    latitudes = np.array([65.0, 65.0833333, 65.1666667])
    longitudes = np.array([20.0, 20.0833333, 20.1666667])
    times = np.array(["2008-01-01", "2008-01-02"], dtype="datetime64[ns]")
    values = np.arange(3 * 3 * 2 * 3, dtype=np.float32).reshape(3, 3, 2, 3)
    if descending:
        latitudes = latitudes[::-1]
        longitudes = longitudes[::-1]
        values = values[::-1, ::-1, :, :]

    dataset = xr.Dataset(
        {
            "abundance": (
                ("lat", "lon", "time", "feedingmode"),
                values,
                {"units": "individuals/m3", "long_name": "Zooplankton abundance"},
            )
        },
        coords={
            "lat": latitudes,
            "lon": longitudes,
            "time": times,
            "feedingmode": ("feedingmode", [0, 1, 2], {"unique_vals": "0:undefined, 1:herbivore, 2:carnivore"}),
        },
    )
    dataset.to_netcdf(path)
    return path


def test_constructor_defaults_and_datetime_normalization():
    downloader = DIVEPlanktonAbundanceDownloader(time_batch_size=0)

    assert downloader.frequency == "daily"
    assert downloader.variables is None
    assert downloader.split_extra_dim_as_variables is True
    assert downloader.time_batch_size == 1
    assert downloader.verify_md5 is True
    assert downloader._to_naive_datetime64(
        datetime.datetime(2008, 1, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
    ) == np.datetime64("2008-01-01T00:00:00", "ns")


def test_parse_extra_dimension_labels_handles_malformed_entries():
    dataset = xr.Dataset(coords={"feedingmode": [0, 1, 2]})
    dataset["feedingmode"].attrs["unique_vals"] = "0: undefined, bad, 1:herbivore, nope:nope, 2:carnivore"

    assert DIVEPlanktonAbundanceDownloader._parse_extra_dim_labels(dataset, "feedingmode") == {
        0: "undefined",
        1: "herbivore",
        2: "carnivore",
    }


def test_fetch_splits_extra_dimension_and_writes_north_up_geotiffs(tmp_path):
    netcdf_path = _write_dataset(tmp_path / "dive.nc")
    output_dir = tmp_path / "output"
    downloader = DIVEPlanktonAbundanceDownloader(source_url_or_path=str(netcdf_path), time_batch_size=1)

    reports = downloader.fetch(TEST_POLYGON, TEST_FRAME, output_dir, show_progress=False)

    assert len(reports) == 6
    assert [report.variable_name for report in reports] == [
        "abundance_undefined", "abundance_herbivore", "abundance_carnivore",
        "abundance_undefined", "abundance_herbivore", "abundance_carnivore",
    ]
    assert all(report.download_successful for report in reports)
    assert all(report.path.is_file() for report in reports)
    assert all(report.metadata["units"] == "individuals/m3" for report in reports)
    assert all(report.bbox == pytest.approx([19.95833335, 64.95833335, 20.125, 65.125]) for report in reports)

    with rasterio.open(reports[1].path) as raster:
        assert raster.count == 1
        assert (raster.width, raster.height) == (2, 2)
        assert raster.crs.to_string() == "EPSG:4326"
        assert raster.descriptions == ("abundance_herbivore",)
        assert raster.tags()["units"] == "individuals/m3"
        assert raster.tags()["source_doi"] == "10.5281/zenodo.22899602"
        # Source values increase south-to-north; the output raster is north-up.
        np.testing.assert_array_equal(raster.read(1), np.array([[19, 25], [1, 7]], dtype=np.float32))


def test_fetch_exports_extra_dimension_as_multiband_geotiff(tmp_path):
    netcdf_path = _write_dataset(tmp_path / "dive.nc")
    downloader = DIVEPlanktonAbundanceDownloader(
        source_url_or_path=str(netcdf_path), split_extra_dim_as_variables=False
    )

    reports = downloader.fetch(
        TEST_POLYGON,
        (TEST_FRAME[0], TEST_FRAME[0]),
        tmp_path / "output",
        show_progress=False,
    )

    assert len(reports) == 1
    assert reports[0].variable_name == "abundance"
    assert reports[0].download_successful
    assert reports[0].metadata["bands"] == 3
    assert reports[0].metadata["band_names"] == ["undefined", "herbivore", "carnivore"]
    with rasterio.open(reports[0].path) as raster:
        assert raster.count == 3
        assert raster.descriptions == ("undefined", "herbivore", "carnivore")


def test_fetch_sorts_descending_spatial_coordinates(tmp_path):
    netcdf_path = _write_dataset(tmp_path / "descending.nc", descending=True)
    downloader = DIVEPlanktonAbundanceDownloader(
        source_url_or_path=str(netcdf_path), variables=["abundance"],
    )

    reports = downloader.fetch(
        TEST_POLYGON,
        (TEST_FRAME[0], TEST_FRAME[0]),
        tmp_path / "output",
        show_progress=False,
    )

    assert len(reports) == 3
    assert all(report.download_successful for report in reports)
    with rasterio.open(reports[1].path) as raster:
        np.testing.assert_array_equal(raster.read(1), np.array([[19, 25], [1, 7]], dtype=np.float32))


def test_fetch_reuses_valid_outputs_without_rewriting(tmp_path):
    netcdf_path = _write_dataset(tmp_path / "dive.nc")
    output_dir = tmp_path / "output"
    downloader = DIVEPlanktonAbundanceDownloader(
        source_url_or_path=str(netcdf_path), variables=["abundance"]
    )
    arguments = {
        "polygon": TEST_POLYGON,
        "time_frame": (TEST_FRAME[0], TEST_FRAME[0]),
        "output_dir": output_dir,
        "show_progress": False,
    }

    first_reports = downloader.fetch(**arguments)
    expected_variables = {
        "abundance_undefined",
        "abundance_herbivore",
        "abundance_carnivore",
    }
    assert len(first_reports) == len(expected_variables)
    assert {report.variable_name for report in first_reports} == expected_variables
    assert all(report.download_successful for report in first_reports)
    first_mtimes = {report.path: report.path.stat().st_mtime_ns for report in first_reports}
    second_reports = downloader.fetch(**arguments)

    assert len(second_reports) == len(expected_variables)
    assert {report.variable_name for report in second_reports} == expected_variables
    assert all(report.download_successful for report in second_reports)
    assert all(report.metadata["cached"] is True for report in second_reports)
    assert {report.path for report in second_reports} == set(first_mtimes)
    assert all(report.path.stat().st_mtime_ns == first_mtimes[report.path] for report in second_reports)


def test_fetch_returns_empty_for_time_or_area_without_data(tmp_path):
    netcdf_path = _write_dataset(tmp_path / "dive.nc")
    downloader = DIVEPlanktonAbundanceDownloader(source_url_or_path=str(netcdf_path))

    no_time = downloader.fetch(
        TEST_POLYGON,
        (datetime.datetime(2010, 1, 1), datetime.datetime(2010, 1, 2)),
        tmp_path / "output",
        show_progress=False,
    )
    no_area = downloader.fetch(
        {"type": "Polygon", "coordinates": [[[40, 80], [41, 80], [41, 81], [40, 81], [40, 80]]]},
        TEST_FRAME,
        tmp_path / "output",
        show_progress=False,
    )

    assert no_time == []
    assert no_area == []


def test_fetch_reports_netcdf_acquisition_error(tmp_path, monkeypatch):
    downloader = DIVEPlanktonAbundanceDownloader()

    def fail_to_acquire(*args, **kwargs):
        raise OSError("network unavailable")

    monkeypatch.setattr(downloader, "_ensure_local_netcdf", fail_to_acquire)
    cache_dir = tmp_path / "cache"
    reports = downloader.fetch(
        TEST_POLYGON,
        TEST_FRAME,
        tmp_path / "output",
        cache_dir=cache_dir,
        show_progress=False,
    )

    assert len(reports) == 1
    assert not reports[0].download_successful
    assert reports[0].data_source == downloader.data_source_name
    assert reports[0].variable_name == "ALL"
    assert reports[0].path == cache_dir
    assert "network unavailable" in reports[0].error


def test_resolve_zenodo_record_extracts_netcdf_metadata(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "files": [
                    {"key": "README.txt", "links": {"self": "https://example/readme"}},
                    {
                        "key": "dataset.nc",
                        "links": {"self": "https://example/dataset.nc"},
                        "checksum": "md5:abc123",
                    },
                ]
            }

    class FakeSession:
        def get(self, url, timeout):
            assert url == "https://zenodo.org/api/records/1234"
            assert timeout == 30
            return FakeResponse()

    monkeypatch.setattr(
        DIVEPlanktonAbundanceDownloader, "_build_retry_session", staticmethod(lambda: FakeSession())
    )
    downloader = DIVEPlanktonAbundanceDownloader()

    assert downloader._resolve_zenodo_record("1234") == (
        "https://example/dataset.nc", "dataset.nc", "abc123"
    )


def test_resolve_zenodo_record_raises_without_netcdf(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"files": [{"key": "README.txt"}]}

    class FakeSession:
        def get(self, *args, **kwargs):
            return FakeResponse()

    monkeypatch.setattr(
        DIVEPlanktonAbundanceDownloader, "_build_retry_session", staticmethod(lambda: FakeSession())
    )

    with pytest.raises(FileNotFoundError, match=r"No \.nc file"):
        DIVEPlanktonAbundanceDownloader()._resolve_zenodo_record("1234")


@pytest.mark.parametrize(
    ("record_id", "filename"),
    [
        ("22899602", "cprdata_dive_v4_20w29e_65n79n_0083deg.nc"),
        ("23416271", "dive_inputdata_svar_spartcctp_20w29e_65n79n_0083deg.nc"),
    ],
)
def test_ensure_local_netcdf_uses_cached_default_record_without_api(
    tmp_path, monkeypatch, record_id, filename
):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cached_path = cache_dir / filename
    cached_path.touch()
    downloader = DIVEPlanktonAbundanceDownloader(
        source_url_or_path=f"https://zenodo.org/records/{record_id}"
    )

    def fail_to_resolve(*args, **kwargs):
        pytest.fail("Zenodo metadata should not be requested for an existing cached default file")

    monkeypatch.setattr(downloader, "_resolve_zenodo_record", fail_to_resolve)

    assert downloader._ensure_local_netcdf(cache_dir) == cached_path


def test_ensure_local_netcdf_downloads_direct_url(tmp_path, monkeypatch):
    class FakeResponse:
        status_code = 200
        headers = {"content-length": "8"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            assert chunk_size == 16 * 1024 * 1024
            yield b"netcdf!!"

    class FakeSession:
        def get(self, url, headers, stream, timeout):
            assert url == "https://example.test/data.nc"
            assert headers == {}
            assert stream is True
            assert timeout == 60
            return FakeResponse()

    monkeypatch.setattr(
        DIVEPlanktonAbundanceDownloader, "_build_retry_session", staticmethod(lambda: FakeSession())
    )
    downloader = DIVEPlanktonAbundanceDownloader(
        source_url_or_path="https://example.test/data.nc", verify_md5=False
    )

    result = downloader._ensure_local_netcdf(tmp_path / "cache", show_progress=False)

    assert result == tmp_path / "cache" / "data.nc"
    assert result.read_bytes() == b"netcdf!!"
    assert not result.with_suffix(".nc.part").exists()


def test_zenodo_download_integration(tmp_path):
    """Download and exercise the real ~12 MB NetCDF file when explicitly enabled."""
    if os.environ.get("RUN_INTEGRATION") != "1":
        pytest.skip("Set RUN_INTEGRATION=1 to download the Zenodo dataset.")

    cache_dir = tmp_path / "cache"
    downloader = DIVEPlanktonAbundanceDownloader(time_batch_size=1)
    netcdf_path = downloader._ensure_local_netcdf(cache_dir, show_progress=False)

    assert netcdf_path.name == "cprdata_dive_v4_20w29e_65n79n_0083deg.nc"
    assert netcdf_path.stat().st_size > 1_000_000
    assert not netcdf_path.with_name(f"{netcdf_path.name}.part").exists()

    with xr.open_dataset(netcdf_path) as dataset:
        assert {"lat", "lon", "time"}.issubset(dataset.coords)
        assert "abundance" in dataset.data_vars
        abundance = dataset["abundance"]
        assert {"lat", "lon", "time"}.issubset(abundance.dims)

        latitudes = dataset["lat"].values
        longitudes = dataset["lon"].values
        lat_resolution = float(np.median(np.abs(np.diff(latitudes))))
        lon_resolution = float(np.median(np.abs(np.diff(longitudes))))
        latitude = float(latitudes[0])
        longitude = float(longitudes[0])
        timestamp = np.datetime64(dataset["time"].values[0], "us").astype(datetime.datetime)

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

    assert reports, "The downloaded dataset should produce at least one raster report."
    assert all(report.download_successful for report in reports)
    assert all(report.path.is_file() and report.path.stat().st_size > 0 for report in reports)
    with rasterio.open(reports[0].path) as raster:
        assert raster.count >= 1
        assert raster.width >= 1 and raster.height >= 1
        assert raster.crs.to_string() == "EPSG:4326"
        assert raster.tags()["source_doi"] == "10.5281/zenodo.22899602"
        assert raster.read(1).shape == (raster.height, raster.width)
