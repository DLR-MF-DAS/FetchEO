from __future__ import annotations

import datetime
import hashlib
import logging
from pathlib import Path
from typing import Optional, List, Sequence

import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_bounds
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
import xarray as xr

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport

logger = logging.getLogger(__name__)


class DIVEChlADownloader(BaseDownloader):
    """
    Dedicated downloader for the 14 GB DIVE Chlorophyll-a dataset on Zenodo (Record 23392001):
    `dive_inputdata_bio_chla_20w29e_65n79n_15z_0083deg.nc`

    Dataset structure:
      - Variable: `chl` (mg/m3), float32
      - Dimensions: (time: 5479, depth: 10, lat: 169, lon: 589)
      - Depth levels (10): 0.51m, 1.56m, 2.67m, 3.86m, 5.14m, 6.54m, 8.09m, 9.82m, 11.77m, 13.99m
      - Time coverage: 2008-01-01 to 2022-12-31 (daily, 5,479 steps)
    """

    ZENODO_RECORD_ID = "23392001"
    FILENAME = "dive_inputdata_bio_chla_20w29e_65n79n_15z_0083deg.nc"
    DOWNLOAD_URL = f"https://zenodo.org/records/{ZENODO_RECORD_ID}/files/{FILENAME}?download=1"
    DATA_SOURCE_NAME = "DIVE_CHLA"

    def __init__(
        self,
        local_nc_path: Optional[Path | str] = None,
        split_depths_as_variables: bool = True,
        depth_indices: Optional[Sequence[int]] = None,
        time_batch_size: int = 365,
        verify_md5: bool = True,
    ):
        """
        Args:
            local_nc_path: Optional path if the 14 GB .nc file is already on disk.
                           If None, downloads/caches from Zenodo into `cache_dir`.
            split_depths_as_variables:
                - True (default): Writes 1 single-band GeoTIFF per depth per day
                  (e.g., `DIVE_CHLA_chl_0p51m_20150601.tif`, reported as variable `chl_0p51m`).
                - False: Writes 1 multi-band GeoTIFF per day containing all selected depths
                  (e.g., `DIVE_CHLA_chl_20150601.tif`, reported as variable `chl`).
            depth_indices: Optional list of depth indices (0 to 9) to export.
                           e.g. `[0]` for surface only (0.51m), or `None` for all 10 depths.
            time_batch_size: Number of days to read into RAM per batch (default 365).
            verify_md5: Verify the MD5 checksum after downloading from Zenodo.
        """
        super().__init__()
        self.local_nc_path = Path(local_nc_path) if local_nc_path else None
        self.split_depths_as_variables = split_depths_as_variables
        self.depth_indices = list(depth_indices) if depth_indices is not None else None
        self.time_batch_size = max(1, int(time_batch_size))
        self.verify_md5 = verify_md5

    @property
    def frequency(self) -> str:
        return "daily"

    @staticmethod
    def _to_naive_datetime64(dt: datetime.datetime) -> np.datetime64:
        if dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return np.datetime64(dt, "ns")

    @staticmethod
    def _fix_chla_time_origin(ds: xr.Dataset) -> xr.Dataset:
        """
        Correct the known `timeorigin = 1900.01.01` inconsistency in the Chl-a source.

        Its time coordinate can decode to 2098-01-02..2113-01-01 instead of the
        documented 2008-01-01..2022-12-31. Require both the known marker and a
        future-dated coordinate so unrelated datasets are left unchanged.
        """
        if "time" not in ds.coords or ds.sizes.get("time", 0) == 0:
            return ds

        time_vals = pd.to_datetime(ds["time"].values)
        time_origin = ds["time"].attrs.get("timeorigin")
        if time_vals[0].year > 2050 and time_origin == "1900.01.01":
            shift = time_vals[0] - pd.Timestamp("2008-01-01")
            corrected = (time_vals - shift).values.astype("datetime64[ns]")
            attrs = ds["time"].attrs.copy()
            ds = ds.assign_coords(time=corrected)
            ds["time"].attrs = attrs
        return ds

    @staticmethod
    def _build_retry_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=2.0,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _resolve_zenodo_file(
        self, session: Optional[requests.Session] = None
    ) -> tuple[str, Optional[str]]:
        """Query the Zenodo record API for the current download URL and MD5 checksum."""
        active_session = session or self._build_retry_session()
        resp = active_session.get(
            f"https://zenodo.org/api/records/{self.ZENODO_RECORD_ID}", timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        for file_info in data.get("files", []):
            if file_info.get("key") == self.FILENAME:
                download_url = file_info.get("links", {}).get("self", self.DOWNLOAD_URL)
                checksum = file_info.get("checksum", "")
                md5 = checksum.split("md5:")[-1] if checksum.startswith("md5:") else None
                return download_url, md5

        raise FileNotFoundError(
            f"{self.FILENAME} not found in Zenodo record {self.ZENODO_RECORD_ID}"
        )

    def _ensure_local_netcdf(self, cache_dir: Path, show_progress: bool = True) -> Path:
        """Streams the 14 GB NetCDF from Zenodo with HTTP Range resume support."""
        if self.local_nc_path and self.local_nc_path.exists():
            return self.local_nc_path

        cache_dir.mkdir(parents=True, exist_ok=True)
        nc_path = cache_dir / self.FILENAME
        if nc_path.exists():
            return nc_path

        temp_path = cache_dir / f"{self.FILENAME}.part"
        resume_byte_pos = temp_path.stat().st_size if temp_path.exists() else 0

        headers = {"Range": f"bytes={resume_byte_pos}-"} if resume_byte_pos > 0 else {}
        session = self._build_retry_session()
        expected_md5: Optional[str] = None
        if self.verify_md5:
            download_url, expected_md5 = self._resolve_zenodo_file(session)
        else:
            download_url = self.DOWNLOAD_URL

        with session.get(download_url, headers=headers, stream=True, timeout=60) as response:
            if response.status_code == 416:
                expected_range = f"bytes */{resume_byte_pos}"
                if not resume_byte_pos or response.headers.get("Content-Range") != expected_range:
                    temp_path.unlink(missing_ok=True)
                    response.raise_for_status()
            else:
                response.raise_for_status()
                if response.status_code == 206:
                    mode = "ab"
                    initial_bytes = resume_byte_pos
                else:
                    mode = "wb"
                    initial_bytes = 0

                content_length = int(response.headers.get("content-length", 0))
                total_size = content_length + initial_bytes if content_length else 0

                with open(temp_path, mode) as f, tqdm(
                    total=total_size,
                    initial=initial_bytes,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=f"Downloading {self.FILENAME}",
                    disable=not show_progress,
                ) as pbar:
                    for chunk in response.iter_content(chunk_size=16 * 1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            pbar.update(len(chunk))

        if self.verify_md5 and expected_md5:
            hash_md5 = hashlib.md5()
            with open(temp_path, "rb") as f:
                for chunk in iter(lambda: f.read(16 * 1024 * 1024), b""):
                    hash_md5.update(chunk)
            actual_md5 = hash_md5.hexdigest()
            if actual_md5 != expected_md5:
                quarantine_path = cache_dir / f"{self.FILENAME}.corrupt.{actual_md5[:8]}"
                temp_path.replace(quarantine_path)
                raise ValueError(
                    f"MD5 mismatch for {self.FILENAME}: expected {expected_md5}, got {actual_md5}. "
                    f"Downloaded data was quarantined at {quarantine_path}; remove it after inspection if unneeded."
                )

        temp_path.replace(nc_path)
        return nc_path

    def fetch(
        self,
        polygon: dict,
        time_frame: tuple[datetime.datetime, datetime.datetime],
        output_dir: Path,
        cache_dir: Optional[Path] = None,
        show_progress: bool = True,
    ) -> list[ItemDownloadReport]:
        output_dir.mkdir(parents=True, exist_ok=True)
        effective_cache_dir = cache_dir if cache_dir is not None else output_dir / ".cache"

        bbox = self._extract_bbox(polygon)
        xmin, ymin, xmax, ymax = bbox
        start_dt, end_dt = time_frame

        try:
            nc_path = self._ensure_local_netcdf(effective_cache_dir, show_progress=show_progress)
        except Exception as exc:
            logger.exception("Failed to acquire the DIVE Chl-a NetCDF from %s", effective_cache_dir)
            return [
                ItemDownloadReport(
                    data_source=self.DATA_SOURCE_NAME,
                    variable_name="chl",
                    acquisition_time=start_dt,
                    polygon=polygon,
                    bbox=bbox,
                    path=effective_cache_dir / self.FILENAME,
                    download_successful=False,
                    error=f"Failed to acquire NetCDF: {exc}",
                )
            ]

        reports: List[ItemDownloadReport] = []

        with xr.open_dataset(nc_path) as ds:
            ds = self._fix_chla_time_origin(ds)

            # Native grid step is 1/12 degree (0.0833333 deg)
            full_lons = ds["lon"].values
            full_lats = ds["lat"].values
            res_x = float(np.median(np.abs(np.diff(full_lons))))
            res_y = float(np.median(np.abs(np.diff(full_lats))))

            t_start = self._to_naive_datetime64(start_dt)
            t_end = self._to_naive_datetime64(end_dt)

            ds_sub = ds.sel(
                time=slice(t_start, t_end),
                lat=slice(ymin - res_y / 2.0, ymax + res_y / 2.0),
                lon=slice(xmin - res_x / 2.0, xmax + res_x / 2.0),
            )

            if self.depth_indices is not None:
                ds_sub = ds_sub.isel(depth=self.depth_indices)

            timestamps = ds_sub["time"].values
            depths = ds_sub["depth"].values
            lats = ds_sub["lat"].values
            lons = ds_sub["lon"].values

            if len(timestamps) == 0 or len(lats) == 0 or len(lons) == 0 or len(depths) == 0:
                return reports

            west = float(lons.min()) - res_x / 2.0
            east = float(lons.max()) + res_x / 2.0
            south = float(lats.min()) - res_y / 2.0
            north = float(lats.max()) + res_y / 2.0
            clipped_bbox = [west, south, east, north]

            width = len(lons)
            height = len(lats)
            transform = from_bounds(west, south, east, north, width, height)
            cache_identity = hashlib.sha256()
            for name, values in (("lons", lons), ("lats", lats), ("depths", depths)):
                values_array = np.asarray(values, dtype="<f8")
                cache_identity.update(name.encode())
                cache_identity.update(len(values_array).to_bytes(8, "little"))
                cache_identity.update(values_array.tobytes())
            cache_key = cache_identity.hexdigest()[:16]

            da_chl = ds_sub["chl"]
            units = str(da_chl.attrs.get("units", "mg/m3"))
            long_name = str(da_chl.attrs.get("long_name", "Chlorophyll-a concentration"))

            # Build export configurations for the 10 depth levels
            if self.split_depths_as_variables:
                var_configs = []
                for d_idx, d_val in enumerate(depths):
                    depth_tag = f"{float(d_val):.2f}m".replace(".", "p")
                    var_name = f"chl_{depth_tag}"
                    var_configs.append(
                        {
                            "report_var_name": var_name,
                            "depth_idx": d_idx,
                            "band_names": [f"chl_{float(d_val):.2f}m"],
                            "depths_m": [float(d_val)],
                        }
                    )
            else:
                var_configs = [
                    {
                        "report_var_name": "chl",
                        "depth_idx": None,
                        "band_names": [f"chl_{float(d):.2f}m" for d in depths],
                        "depths_m": [float(d) for d in depths],
                    }
                ]

            pbar = tqdm(
                total=len(timestamps) * len(var_configs),
                desc="Exporting Chl-a GeoTIFFs",
                disable=not show_progress,
            )

            # Read in batches along the leading `time` dimension: (time, depth, lat, lon)
            for batch_start in range(0, len(timestamps), self.time_batch_size):
                batch_end = min(batch_start + self.time_batch_size, len(timestamps))
                batch_ts = timestamps[batch_start:batch_end]

                need_io = False
                for ts in batch_ts:
                    date_str = pd.to_datetime(ts).strftime("%Y%m%d")
                    for cfg in var_configs:
                        basename = (
                            f"{self.DATA_SOURCE_NAME}_{cfg['report_var_name']}_{date_str}_{cache_key}"
                        )
                        tif_path = self._get_filepaths(output_dir, basename)[0]
                        if not self._validate_geotiff(output_dir, basename).get(tif_path, False):
                            need_io = True
                            break
                    if need_io:
                        break

                batch_array = None
                if need_io:
                    # Native order in file is already (time, depth, lat, lon)
                    da_batch = da_chl.isel(time=slice(batch_start, batch_end))
                    batch_array = da_batch.transpose("time", "depth", "lat", "lon").values
                    # Flip latitude North-up (79.0°N at row 0, 65.0°N at bottom row)
                    batch_array = np.flip(batch_array, axis=2)
                    batch_array = np.asarray(batch_array, dtype=np.float32)

                for t_idx, ts in enumerate(batch_ts):
                    acq_time = pd.to_datetime(ts).to_pydatetime()
                    date_str = acq_time.strftime("%Y%m%d")

                    for cfg in var_configs:
                        report_var = cfg["report_var_name"]
                        basename = (
                            f"{self.DATA_SOURCE_NAME}_{report_var}_{date_str}_{cache_key}"
                        )
                        tif_path = self._get_filepaths(output_dir, basename)[0]

                        if self._validate_geotiff(output_dir, basename).get(tif_path, False):
                            reports.append(
                                ItemDownloadReport(
                                    data_source=self.DATA_SOURCE_NAME,
                                    variable_name=report_var,
                                    acquisition_time=acq_time,
                                    polygon=polygon,
                                    bbox=clipped_bbox,
                                    path=tif_path,
                                    download_successful=True,
                                    metadata={
                                        "cached": True,
                                        "units": units,
                                        "depths_m": cfg["depths_m"],
                                    },
                                )
                            )
                            pbar.update(1)
                            continue

                        tmp_tif_path = tif_path.with_suffix(".tif.tmp")
                        try:
                            if cfg["depth_idx"] is not None:
                                arr = batch_array[
                                    t_idx, cfg["depth_idx"] : cfg["depth_idx"] + 1, :, :
                                ]
                            else:
                                arr = batch_array[t_idx]

                            profile = {
                                "driver": "GTiff",
                                "height": height,
                                "width": width,
                                "count": arr.shape[0],
                                "dtype": "float32",
                                "crs": "EPSG:4326",
                                "transform": transform,
                                "nodata": np.nan,
                                "compress": "deflate",
                            }

                            with rasterio.open(tmp_tif_path, "w", **profile) as dst:
                                dst.write(arr)
                                for b_idx, b_name in enumerate(cfg["band_names"], start=1):
                                    dst.set_band_description(b_idx, b_name)
                                dst.update_tags(
                                    variable=report_var,
                                    long_name=long_name,
                                    units=units,
                                    depths_m=",".join(f"{d:.2f}" for d in cfg["depths_m"]),
                                    acquisition_time=acq_time.isoformat(),
                                    source_doi="10.5281/zenodo.23392001",
                                )

                            tmp_tif_path.replace(tif_path)

                            is_valid = self._validate_geotiff(output_dir, basename).get(
                                tif_path, False
                            )
                            reports.append(
                                ItemDownloadReport(
                                    data_source=self.DATA_SOURCE_NAME,
                                    variable_name=report_var,
                                    acquisition_time=acq_time,
                                    polygon=polygon,
                                    bbox=clipped_bbox,
                                    path=tif_path,
                                    download_successful=is_valid,
                                    error=None
                                    if is_valid
                                    else "GeoTIFF validation failed after write.",
                                    metadata={
                                        "cached": False,
                                        "units": units,
                                        "bands": arr.shape[0],
                                        "depths_m": cfg["depths_m"],
                                    },
                                )
                            )
                        except Exception as exc:
                            tmp_tif_path.unlink(missing_ok=True)
                            error = (
                                f"Failed to write {report_var} for {date_str} "
                                f"to {tif_path}: {exc}"
                            )
                            logger.exception(error)
                            reports.append(
                                ItemDownloadReport(
                                    data_source=self.DATA_SOURCE_NAME,
                                    variable_name=report_var,
                                    acquisition_time=acq_time,
                                    polygon=polygon,
                                    bbox=clipped_bbox,
                                    path=tif_path,
                                    download_successful=False,
                                    error=error,
                                )
                            )
                        finally:
                            pbar.update(1)

            pbar.close()

        return reports
