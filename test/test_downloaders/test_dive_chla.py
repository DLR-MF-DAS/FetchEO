import datetime
import os

import numpy as np
import pandas as pd
import pytest
import rasterio
import requests
import xarray as xr

from fetcheo.downloaders.dive_chla import DIVEChlADownloader


TEST_POLYGON = {
	"type": "Polygon",
	"coordinates": [[[20.0, 65.0], [20.1, 65.0], [20.1, 65.1], [20.0, 65.1], [20.0, 65.0]]],
}


def _write_chla_dataset(path, times=None):
	"""Write a compact NetCDF fixture with the downloader's expected dimensions."""
	if times is None:
		times = np.array(["2008-01-01", "2008-01-02"], dtype="datetime64[ns]")
	values = np.arange(len(times) * 3 * 3 * 3, dtype=np.float32).reshape(
		len(times), 3, 3, 3
	)
	dataset = xr.Dataset(
		{
			"chl": (
				("time", "depth", "lat", "lon"),
				values,
				{"units": "mg/m3", "long_name": "Chlorophyll-a concentration"},
			)
		},
		coords={
			"time": times,
			"depth": [0.51, 1.56, 2.67],
			"lat": [65.0, 65.0833333, 65.1666667],
			"lon": [20.0, 20.0833333, 20.1666667],
		},
	)
	dataset.to_netcdf(path)


def test_dive_chla_constructor_and_datetime_conversion():
	downloader = DIVEChlADownloader(
		split_depths_as_variables=False,
		depth_indices=[0, 2],
		time_batch_size=0,
	)

	assert downloader.frequency == "daily"
	assert downloader.split_depths_as_variables is False
	assert downloader.depth_indices == [0, 2]
	assert downloader.time_batch_size == 1
	assert downloader._to_naive_datetime64(
		datetime.datetime(2021, 1, 1, tzinfo=datetime.timezone.utc)
	) == np.datetime64("2021-01-01T00:00:00", "ns")

def test_fix_chla_time_origin_shifts_future_decoded_dates():
	dataset = xr.Dataset(
		coords={"time": pd.date_range("2098-01-02", periods=2, freq="D")}
	)
	dataset["time"].attrs["units"] = "days since 1900-01-01"

	fixed = DIVEChlADownloader._fix_chla_time_origin(dataset)

	assert fixed.time.values[0] == np.datetime64("2008-01-01T00:00:00", "ns")
	assert fixed.time.values[1] == np.datetime64("2008-01-02T00:00:00", "ns")
	assert fixed.time.attrs["units"] == "days since 1900-01-01"


def test_fetch_exports_selected_depths_to_split_geotiffs(tmp_path):
	netcdf_path = tmp_path / "tiny_chla.nc"
	_write_chla_dataset(netcdf_path)
	output_dir = tmp_path / "output"
	downloader = DIVEChlADownloader(
		local_nc_path=netcdf_path,
		depth_indices=[0, 2],
		time_batch_size=1,
	)

	reports = downloader.fetch(
		polygon=TEST_POLYGON,
		time_frame=(datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 1)),
		output_dir=output_dir,
		show_progress=False,
	)

	assert len(reports) == 2
	assert [report.variable_name for report in reports] == ["chl_0p51m", "chl_2p67m"]
	assert all(report.download_successful for report in reports)
	assert all(report.path.is_file() for report in reports)
	assert all(report.metadata["units"] == "mg/m3" for report in reports)
	assert [report.metadata["depths_m"] for report in reports] == [[0.51], [2.67]]

	with rasterio.open(reports[0].path) as raster:
		assert raster.count == 1
		assert raster.width == 2
		assert raster.height == 2
		assert raster.crs.to_string() == "EPSG:4326"
		assert raster.descriptions == ("chl_0.51m",)
		assert raster.tags()["units"] == "mg/m3"
		# Source values increase from south to north; output is north-up.
		np.testing.assert_array_equal(
			raster.read(1), np.array([[3, 4], [0, 1]], dtype=np.float32)
		)


def test_fetch_exports_selected_depths_as_multiband_geotiff(tmp_path):
	netcdf_path = tmp_path / "tiny_chla.nc"
	_write_chla_dataset(netcdf_path)
	downloader = DIVEChlADownloader(
		local_nc_path=netcdf_path,
		split_depths_as_variables=False,
		depth_indices=[0, 2],
		time_batch_size=2,
	)

	reports = downloader.fetch(
		polygon=TEST_POLYGON,
		time_frame=(datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 1)),
		output_dir=tmp_path / "output",
		show_progress=False,
	)

	assert len(reports) == 1
	assert reports[0].variable_name == "chl"
	assert reports[0].download_successful
	assert reports[0].metadata["bands"] == 2
	assert reports[0].metadata["depths_m"] == [0.51, 2.67]
	with rasterio.open(reports[0].path) as raster:
		assert raster.count == 2
		assert raster.descriptions == ("chl_0.51m", "chl_2.67m")


def test_fetch_reuses_valid_output_without_rewriting(tmp_path):
	netcdf_path = tmp_path / "tiny_chla.nc"
	_write_chla_dataset(netcdf_path)
	output_dir = tmp_path / "output"
	downloader = DIVEChlADownloader(local_nc_path=netcdf_path, depth_indices=[0])
	arguments = {
		"polygon": TEST_POLYGON,
		"time_frame": (datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 1)),
		"output_dir": output_dir,
		"show_progress": False,
	}

	first_reports = downloader.fetch(**arguments)
	first_mtime = first_reports[0].path.stat().st_mtime_ns
	second_reports = downloader.fetch(**arguments)

	assert len(second_reports) == 1
	assert second_reports[0].download_successful
	assert second_reports[0].metadata["cached"] is True
	assert second_reports[0].path.stat().st_mtime_ns == first_mtime


def test_fetch_returns_no_reports_when_time_or_area_has_no_data(tmp_path):
	netcdf_path = tmp_path / "tiny_chla.nc"
	_write_chla_dataset(netcdf_path)
	downloader = DIVEChlADownloader(local_nc_path=netcdf_path)

	reports = downloader.fetch(
		polygon=TEST_POLYGON,
		time_frame=(datetime.datetime(2010, 1, 1), datetime.datetime(2010, 1, 2)),
		output_dir=tmp_path / "output",
		show_progress=False,
	)

	assert reports == []


def test_fetch_reports_local_netcdf_acquisition_error(tmp_path, monkeypatch):
	downloader = DIVEChlADownloader()

	def fail_to_acquire(*args, **kwargs):
		raise OSError("network unavailable")

	monkeypatch.setattr(downloader, "_ensure_local_netcdf", fail_to_acquire)
	reports = downloader.fetch(
		polygon=TEST_POLYGON,
		time_frame=(datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 1)),
		output_dir=tmp_path / "output",
		cache_dir=tmp_path / "cache",
		show_progress=False,
	)

	assert len(reports) == 1
	assert not reports[0].download_successful
	assert reports[0].data_source == "DIVE_CHLA"
	assert reports[0].variable_name == "chl"
	assert "network unavailable" in reports[0].error
	assert reports[0].path == tmp_path / "cache" / downloader.FILENAME


def test_dive_chla_zenodo_integration_metadata():
	"""Check the live Zenodo record without downloading its large NetCDF file."""
	if os.environ.get("RUN_INTEGRATION") != "1":
		pytest.skip("Set RUN_INTEGRATION=1 to check the live Zenodo record.")

	downloader = DIVEChlADownloader()
	response = requests.get(
		f"https://zenodo.org/api/records/{downloader.ZENODO_RECORD_ID}",
		timeout=30,
	)
	response.raise_for_status()
	record = response.json()

	file_metadata = next(
		file_info
		for file_info in record["files"]
		if file_info["key"] == downloader.FILENAME
	)
	assert file_metadata["checksum"].endswith(downloader.EXPECTED_MD5)
	assert file_metadata["size"] > 1_000_000_000


def test_fetch_cache_distinguishes_ordered_depth_selections(tmp_path):
	netcdf_path = tmp_path / "tiny_chla.nc"
	_write_chla_dataset(netcdf_path)
	output_dir = tmp_path / "output"
	downloader = DIVEChlADownloader(
		local_nc_path=netcdf_path,
		depth_indices=[0, 2],
		time_batch_size=1,
	)

	initial_reports = downloader.fetch(
		polygon=TEST_POLYGON,
		time_frame=(datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 1)),
		output_dir=output_dir,
		show_progress=False,
	)

	assert len(initial_reports) == 2
	assert [report.variable_name for report in initial_reports] == ["chl_0p51m", "chl_2p67m"]
	assert all(report.download_successful for report in initial_reports)
	assert all(report.path.is_file() for report in initial_reports)
	assert all(report.metadata["units"] == "mg/m3" for report in initial_reports)
	assert [report.metadata["depths_m"] for report in initial_reports] == [[0.51], [2.67]]

	with rasterio.open(initial_reports[0].path) as raster:
		assert raster.count == 1
		assert raster.width == 2
		assert raster.height == 2
		assert raster.crs.to_string() == "EPSG:4326"
		assert raster.descriptions == ("chl_0.51m",)
		assert raster.tags()["units"] == "mg/m3"
		# Source values increase from south to north; output is north-up.
		np.testing.assert_array_equal(
			raster.read(1), np.array([[3, 4], [0, 1]], dtype=np.float32)
		)

	# Request in reverse order.
	downloader.depth_indices = [2, 0]
	reversed_reports = downloader.fetch(
		polygon=TEST_POLYGON,
		time_frame=(datetime.datetime(2008, 1, 1), datetime.datetime(2008, 1, 1)),
		output_dir=output_dir,
		show_progress=False,
	)

	assert len(reversed_reports) == 2
	assert [report.variable_name for report in reversed_reports] == ["chl_2p67m", "chl_0p51m"]
	assert all(report.download_successful for report in reversed_reports)
	assert all(report.path.is_file() for report in reversed_reports)
	assert all(report.metadata["units"] == "mg/m3" for report in reversed_reports)
	assert [report.metadata["depths_m"] for report in reversed_reports] == [[2.67], [0.51]]
	assert all(report.metadata.get("cached", False) is False for report in reversed_reports)
	assert {report.path for report in initial_reports}.isdisjoint(
		{report.path for report in reversed_reports}
	)

	with rasterio.open(reversed_reports[0].path) as raster:
		assert raster.count == 1
		assert raster.width == 2
		assert raster.height == 2
		assert raster.crs.to_string() == "EPSG:4326"
		assert raster.descriptions == ("chl_2.67m",)
		assert raster.tags()["units"] == "mg/m3"
		# Source values increase from south to north; output is north-up.
		np.testing.assert_array_equal(
			raster.read(1), np.array([[21, 22], [18, 19]], dtype=np.float32)
		)

