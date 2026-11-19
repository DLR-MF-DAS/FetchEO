from __future__ import annotations

import datetime
import hashlib
import re
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


class DIVEPlanktonAbundanceDownloader(BaseDownloader):
    """
    Downloader for Akvaplan-niva DIVE project NetCDF datasets, configured for
    Zenodo Record 22899602 (Abundance of holo- and mero-zooplankton grouped by feeding modes).

    Handles DIVE-specific conventions:
      - Dimension ordering: (lat, lon, time, feedingmode)
      - Daily timestamps (datetime64[ns]) from 2008 to 2022
      - Ascending lat (65.0 -> 78.9944) and lon (-20.0 -> 27.9808) at 0.0833° resolution
      - Custom attributes such as `unique_vals` and `fillval: "numpy.ndarray.nan"`
    """

    SOURCE_DOI = "10.5281/zenodo.22899602"
    ZENODO_RECORD_FILES = {
        "22899602": "cprdata_dive_v4_20w29e_65n79n_0083deg.nc",
        "23416271": "dive_inputdata_svar_spartcctp_20w29e_65n79n_0083deg.nc",
    }

    def __init__(
        self,
        source_url_or_path: str = "https://zenodo.org/records/22899602",
        data_source_name: str = "DIVE_CPR_ZOOPLANKTON",
        variables: Optional[Sequence[str]] = None,
        split_extra_dim_as_variables: bool = True,
        time_batch_size: int = 365,
        verify_md5: bool = True,
    ):
        super().__init__()
        self.source_url_or_path = source_url_or_path
        self.data_source_name = data_source_name
        self.variables = list(variables) if variables else None
        self.split_extra_dim_as_variables = split_extra_dim_as_variables
        self.time_batch_size = max(1, int(time_batch_size))
        self.verify_md5 = verify_md5

    @property
    def frequency(self) -> str:
        """DIVE gridded datasets have an exact 86,400s (1-day) step."""
        return "daily"

    @staticmethod
    def _to_naive_datetime64(dt: datetime.datetime) -> np.datetime64:
        if dt.tzinfo is not None:
            dt = dt.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return np.datetime64(dt, "ns")

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

    def _resolve_zenodo_record(self, record_id: str) -> tuple[str, str, Optional[str]]:
        """Queries the Zenodo REST API for a record ID and returns (download_url, filename, md5)."""
        session = self._build_retry_session()
        resp = session.get(f"https://zenodo.org/api/records/{record_id}", timeout=30)
        resp.raise_for_status()
        data = resp.json()

        for file_info in data.get("files", []):
            key = file_info.get("key", "")
            if key.endswith(".nc"):
                download_url = file_info.get("links", {}).get(
                    "self", f"https://zenodo.org/records/{record_id}/files/{key}?download=1"
                )
                checksum = file_info.get("checksum", "")
                md5 = checksum.split("md5:")[-1] if checksum.startswith("md5:") else None
                return download_url, key, md5

        raise FileNotFoundError(f"No .nc file found in Zenodo record {record_id}")

    def _ensure_local_netcdf(self, cache_dir: Path, show_progress: bool = True) -> Path:
        """Resolves a local path or downloads and caches the NetCDF file with HTTP Range resume support."""
        local_candidate = Path(self.source_url_or_path)
        if local_candidate.exists() and local_candidate.is_file():
            return local_candidate

        cache_dir.mkdir(parents=True, exist_ok=True)

        zenodo_match = re.search(r"zenodo\.org/records/(\d+)(?:$|/)", self.source_url_or_path)
        expected_md5 = None
        if zenodo_match and "/files/" not in self.source_url_or_path:
            record_id = zenodo_match.group(1)
            cached_filename = self.ZENODO_RECORD_FILES.get(record_id)
            if cached_filename:
                cached_path = cache_dir / cached_filename
                if cached_path.exists():
                    return cached_path
            download_url, filename, expected_md5 = self._resolve_zenodo_record(record_id)
        else:
            download_url = self.source_url_or_path
            filename = download_url.split("?")[0].rstrip("/").split("/")[-1]
            if not filename.endswith(".nc"):
                filename = "dive_dataset.nc"

        nc_path = cache_dir / filename
        if nc_path.exists():
            return nc_path

        temp_path = cache_dir / f"{filename}.part"
        resume_byte_pos = temp_path.stat().st_size if temp_path.exists() else 0

        headers = {"Range": f"bytes={resume_byte_pos}-"} if resume_byte_pos > 0 else {}
        session = self._build_retry_session()

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
                    desc=f"Downloading {filename}",
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
                temp_path.unlink(missing_ok=True)
                raise ValueError(f"MD5 mismatch for {filename}: expected {expected_md5}, got {actual_md5}")

        temp_path.replace(nc_path)
        return nc_path

    @staticmethod
    def _parse_extra_dim_labels(ds: xr.Dataset, dim_name: str) -> dict[int, str]:
        """
        Parses DIVE `unique_vals` attribute if present, e.g.:
        '0:undefined, 1:herbivore, 2:carnivore, 3:onmivore, 4:detritivore'
        """
        coord = ds[dim_name]
        unique_vals_str = coord.attrs.get("unique_vals", "")
        label_map: dict[int, str] = {}

        if unique_vals_str:
            for item in unique_vals_str.split(","):
                if ":" in item:
                    k_str, v_str = item.split(":", 1)
                    try:
                        clean_label = re.sub(r"[^a-zA-Z0-9_-]", "", v_str.strip())
                        label_map[int(k_str.strip())] = clean_label
                    except ValueError:
                        continue

        return label_map

    def _prepare_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        """Apply dataset-specific coordinate corrections before selection."""
        return ds

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

        bbox = self._extract_bbox(polygon)  # [xmin, ymin, xmax, ymax]
        xmin, ymin, xmax, ymax = bbox
        start_dt, end_dt = time_frame

        # 1. Obtain local or cached NetCDF
        try:
            nc_path = self._ensure_local_netcdf(effective_cache_dir, show_progress=show_progress)
        except Exception as exc:
            return [
                ItemDownloadReport(
                    data_source=self.data_source_name,
                    variable_name="ALL",
                    acquisition_time=start_dt,
                    polygon=polygon,
                    bbox=bbox,
                    path=effective_cache_dir,
                    download_successful=False,
                    error=f"Failed to acquire NetCDF: {exc}",
                )
            ]

        reports: List[ItemDownloadReport] = []

        # 2. Open NetCDF and slice by polygon bbox & time_frame
        with xr.open_dataset(nc_path) as ds:
            ds = self._prepare_dataset(ds)
            lat_dim = "lat" if "lat" in ds.dims else "latitude"
            lon_dim = "lon" if "lon" in ds.dims else "longitude"
            time_dim = "time"

            # Guarantee ascending coordinate order before slicing
            if ds[lat_dim].size > 1 and ds[lat_dim].values[0] > ds[lat_dim].values[-1]:
                ds = ds.sortby(lat_dim)
            if ds[lon_dim].size > 1 and ds[lon_dim].values[0] > ds[lon_dim].values[-1]:
                ds = ds.sortby(lon_dim)
            if ds[time_dim].size > 1 and ds[time_dim].values[0] > ds[time_dim].values[-1]:
                ds = ds.sortby(time_dim)

            full_lons = ds[lon_dim].values
            full_lats = ds[lat_dim].values
            res_x = float(np.median(np.abs(np.diff(full_lons)))) if len(full_lons) > 1 else 0.0833
            res_y = float(np.median(np.abs(np.diff(full_lats)))) if len(full_lats) > 1 else 0.0833

            t_start = self._to_naive_datetime64(start_dt)
            t_end = self._to_naive_datetime64(end_dt)

            ds_sub = ds.sel(
                {
                    lat_dim: slice(ymin - res_y / 2.0, ymax + res_y / 2.0),
                    lon_dim: slice(xmin - res_x / 2.0, xmax + res_x / 2.0),
                    time_dim: slice(t_start, t_end),
                }
            )

            timestamps = ds_sub[time_dim].values
            lats = ds_sub[lat_dim].values
            lons = ds_sub[lon_dim].values

            if len(timestamps) == 0 or len(lats) == 0 or len(lons) == 0:
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
            for name, values in (("lons", lons), ("lats", lats)):
                values_array = np.asarray(values, dtype="<f8")
                cache_identity.update(name.encode())
                cache_identity.update(len(values_array).to_bytes(8, "little"))
                cache_identity.update(values_array.tobytes())
            cache_key = cache_identity.hexdigest()[:16]

            target_vars = self.variables if self.variables else list(ds_sub.data_vars)

            # 3. Export GeoTIFFs per variable & timestamp
            for base_var in target_vars:
                da = ds_sub[base_var]
                units = str(da.attrs.get("units", ""))
                long_name = str(da.attrs.get("long_name", base_var))

                # Identify 4th dimension (e.g. 'feedingmode')
                extra_dims = [d for d in da.dims if d not in (lat_dim, lon_dim, time_dim) and d != "spatial_ref"]
                extra_dim = extra_dims[0] if extra_dims else None

                if extra_dim and self.split_extra_dim_as_variables:
                    label_map = self._parse_extra_dim_labels(ds_sub, extra_dim)
                    coord_vals = ds_sub[extra_dim].values
                    var_configs = []
                    for idx, val in enumerate(coord_vals):
                        if np.issubdtype(coord_vals.dtype, np.integer):
                            suffix = label_map.get(int(val), f"{extra_dim}{int(val)}")
                        else:
                            val_str = f"{float(val):g}".replace(".", "p")
                            suffix = f"{extra_dim}{val_str}"
                        var_configs.append(
                            {
                                "report_var_name": f"{base_var}_{suffix}",
                                "extra_idx": idx,
                                "band_names": [f"{base_var}_{suffix}"],
                            }
                        )
                elif extra_dim and not self.split_extra_dim_as_variables:
                    label_map = self._parse_extra_dim_labels(ds_sub, extra_dim)
                    coord_vals = ds_sub[extra_dim].values
                    band_names = [
                        label_map.get(int(v), f"{extra_dim}_{int(v)}")
                        if np.issubdtype(coord_vals.dtype, np.integer)
                        else f"{extra_dim}_{float(v):g}"
                        for v in coord_vals
                    ]
                    var_configs = [{"report_var_name": base_var, "extra_idx": None, "band_names": band_names}]
                else:
                    var_configs = [{"report_var_name": base_var, "extra_idx": None, "band_names": [base_var]}]

                pbar = tqdm(
                    total=len(timestamps) * len(var_configs),
                    desc=f"Exporting {base_var} GeoTIFFs",
                    disable=not show_progress,
                )

                # Process in time batches to avoid random disk seeks across (lat, lon, time, feedingmode) layout
                for batch_start in range(0, len(timestamps), self.time_batch_size):
                    batch_end = min(batch_start + self.time_batch_size, len(timestamps))
                    batch_ts = timestamps[batch_start:batch_end]

                    need_io = False
                    for ts in batch_ts:
                        date_str = pd.to_datetime(ts).strftime("%Y%m%d")
                        for cfg in var_configs:
                            basename = f"{self.data_source_name}_{cfg['report_var_name']}_{date_str}_{cache_key}"
                            tif_path = self._get_filepaths(output_dir, basename)[0]
                            if not self._validate_geotiff(output_dir, basename).get(tif_path, False):
                                need_io = True
                                break
                        if need_io:
                            break

                    batch_array = None
                    if need_io:
                        da_batch = da.isel({time_dim: slice(batch_start, batch_end)})
                        if extra_dim:
                            # Transpose to (time, extra_dim, lat, lon)
                            batch_array = da_batch.transpose(time_dim, extra_dim, lat_dim, lon_dim).values
                        else:
                            batch_array = da_batch.transpose(time_dim, lat_dim, lon_dim).values[:, np.newaxis, :, :]
                        # Flip latitude North-up (row 0 = North, bottom row = South)
                        batch_array = np.flip(batch_array, axis=2)
                        batch_array = np.asarray(batch_array, dtype=np.float32)

                    for t_idx, ts in enumerate(batch_ts):
                        acq_time = pd.to_datetime(ts).to_pydatetime()
                        date_str = acq_time.strftime("%Y%m%d")

                        for cfg in var_configs:
                            report_var = cfg["report_var_name"]
                            basename = f"{self.data_source_name}_{report_var}_{date_str}_{cache_key}"
                            tif_path = self._get_filepaths(output_dir, basename)[0]

                            if self._validate_geotiff(output_dir, basename).get(tif_path, False):
                                reports.append(
                                    ItemDownloadReport(
                                        data_source=self.data_source_name,
                                        variable_name=report_var,
                                        acquisition_time=acq_time,
                                        polygon=polygon,
                                        bbox=clipped_bbox,
                                        path=tif_path,
                                        download_successful=True,
                                        metadata={"cached": True, "units": units},
                                    )
                                )
                                pbar.update(1)
                                continue

                            tmp_tif_path = tif_path.with_suffix(".tif.tmp")
                            try:
                                if cfg["extra_idx"] is not None:
                                    arr = batch_array[t_idx, cfg["extra_idx"] : cfg["extra_idx"] + 1, :, :]
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
                                        acquisition_time=acq_time.isoformat(),
                                        source_doi=self.SOURCE_DOI,
                                    )

                                tmp_tif_path.replace(tif_path)

                                is_valid = self._validate_geotiff(output_dir, basename).get(tif_path, False)
                                reports.append(
                                    ItemDownloadReport(
                                        data_source=self.data_source_name,
                                        variable_name=report_var,
                                        acquisition_time=acq_time,
                                        polygon=polygon,
                                        bbox=clipped_bbox,
                                        path=tif_path,
                                        download_successful=is_valid,
                                        error=None if is_valid else "GeoTIFF validation failed after write.",
                                        metadata={
                                            "units": units,
                                            "bands": arr.shape[0],
                                            "band_names": cfg["band_names"],
                                        },
                                    )
                                )
                            except Exception as exc:
                                tmp_tif_path.unlink(missing_ok=True)
                                reports.append(
                                    ItemDownloadReport(
                                        data_source=self.data_source_name,
                                        variable_name=report_var,
                                        acquisition_time=acq_time,
                                        polygon=polygon,
                                        bbox=clipped_bbox,
                                        path=tif_path,
                                        download_successful=False,
                                        error=str(exc),
                                    )
                                )
                            finally:
                                pbar.update(1)

                pbar.close()

        return reports