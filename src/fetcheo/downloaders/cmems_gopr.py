from __future__ import annotations

import os
import re
import hashlib
import logging
import inspect
import datetime
import importlib
from tqdm import tqdm
from pathlib import Path
from typing import Optional, Union

import rioxarray  
import xarray as xr
import pandas as pd
import copernicusmarine
from rasterio.crs import CRS

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport


logger = logging.getLogger(__name__)


class CMEMSGlobalOceanPhysicsReanalysisDownloader(BaseDownloader):
    """Subset Copernicus Marine data and write one GeoTIFF per variable, time, and depth level."""

    GLORYS_DATASETS = {
        "daily": "cmems_mod_glo_phy_my_0.083deg_P1D-m",
        "monthly": "cmems_mod_glo_phy_my_0.083deg_P1M-m",
    }

    def __init__(
        self,
        dataset_id: Optional[str] = None,
        variables: Optional[list[str]] = None,
        frequency: str = "daily",
        min_depth: float = 0.0,
        max_depth: Optional[float] = 16.0,
        username: Optional[str] = None,
        password: Optional[str] = None,
    ) -> None:
        super().__init__()
        if not frequency.strip():
            raise ValueError("frequency must not be empty")

        if dataset_id is None:
            if frequency not in self.GLORYS_DATASETS:
                raise ValueError(
                    f"No default GLORYS dataset for frequency '{frequency}'. "
                    f"Provide dataset_id explicitly or use {list(self.GLORYS_DATASETS.keys())}."
                )
            dataset_id = self.GLORYS_DATASETS[frequency]

        if not dataset_id.strip():
            raise ValueError("dataset_id must not be empty")
        if not variables or any(not variable.strip() for variable in variables):
            raise ValueError("variables must contain at least one non-empty name")

        self.dataset_id = dataset_id
        self.variables = list(dict.fromkeys(variables))
        self._frequency = frequency
        self.min_depth = min_depth
        self.max_depth = max_depth
        self.username = username or os.environ.get("CMEMS_USERNAME")
        self.password = password or os.environ.get("CMEMS_PASSWORD")

    @property
    def frequency(self) -> str:
        return self._frequency

    def fetch(
        self,
        polygon: dict,
        time_frame: tuple[datetime.datetime, datetime.datetime],
        output_dir: Path,
        cache_dir: Optional[Union[str, Path]] = None,
        show_progress: bool = True,
    ) -> list[ItemDownloadReport]:
        """Download a temporal/spatial subset and export individual GeoTIFFs per depth, time, and variable."""
        start, end = time_frame
        if start > end:
            raise ValueError("time_frame start must be earlier than or equal to end")

        output_dir = Path(output_dir)
        cache_dir = Path(cache_dir) if cache_dir is not None else output_dir / "cache" / "cmems"
        output_dir.mkdir(parents=True, exist_ok=True)
        cache_dir.mkdir(parents=True, exist_ok=True)
        bbox = self._extract_bbox(polygon)

        try:
            nc_path = self._ensure_downloaded(polygon, time_frame, cache_dir, show_progress)
            with xr.open_dataset(nc_path) as dataset:
                return self._export_dataset(dataset, polygon, bbox, time_frame, output_dir, show_progress)
        except Exception as exc:
            logger.exception("CMEMS subset/export failed for %s", self.dataset_id)
            return [
                self._report(
                    variable,
                    start,
                    polygon,
                    bbox,
                    output_dir / f"{self._file_stem(variable, start, depth=self.min_depth)}.tif",
                    False,
                    str(exc),
                )
                for variable in self.variables
            ]

    def _ensure_downloaded(
        self,
        polygon: dict,
        time_frame: tuple[datetime.datetime, datetime.datetime],
        cache_dir: Path,
        show_progress: bool = True,
    ) -> Path:
        """Return a cached subset, downloading it with the official client if needed."""
        bbox = self._extract_bbox(polygon)
        start, end = time_frame
        depth_spec = f"depth_{self.min_depth:.1f}_to_{self.max_depth if self.max_depth is not None else 'max'}"
        identity = "|".join(
            [
                self.dataset_id,
                *self.variables,
                *(f"{value:.8f}" for value in bbox),
                start.isoformat(),
                end.isoformat(),
                depth_spec,
            ]
        )
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
        dataset_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.dataset_id).strip("_.-") or "dataset"
        filename = f"cmems_{dataset_name}_{digest}.nc"
        target = cache_dir / filename

        if target.is_file() and target.stat().st_size:
            try:
                with xr.open_dataset(target) as cached:
                    if all(name in cached.data_vars for name in self.variables):
                        return target
            except (OSError, ValueError):
                pass
            target.unlink(missing_ok=True)

        request: dict = {
            "dataset_id": self.dataset_id,
            "variables": self.variables,
            "minimum_longitude": bbox[0],
            "maximum_longitude": bbox[2],
            "minimum_latitude": bbox[1],
            "maximum_latitude": bbox[3],
            "start_datetime": start.isoformat(),
            "end_datetime": end.isoformat(),
            "output_directory": str(cache_dir),
            "output_filename": filename,
            "minimum_depth": self.min_depth,
        }
        if self.max_depth is not None:
            request["maximum_depth"] = self.max_depth

        sig_params = inspect.signature(copernicusmarine.subset).parameters
        if "overwrite" in sig_params:
            request["overwrite"] = True
        elif "force_download" in sig_params:
            request["force_download"] = True

        if "disable_progress_bar" in sig_params:
            request["disable_progress_bar"] = not show_progress

        if self.username:
            request["username"] = self.username
        if self.password:
            request["password"] = self.password

        result = copernicusmarine.subset(**request)
        if not target.is_file() and result is not None:
            returned_path = getattr(result, "file_path", result)
            if isinstance(returned_path, (str, Path)) and Path(returned_path).is_file():
                returned_path = Path(returned_path)
                if returned_path.resolve() != target.resolve():
                    returned_path.replace(target)

        if not target.is_file() or target.stat().st_size == 0:
            raise FileNotFoundError(f"Copernicus Marine did not create the expected subset: {target}")
        return target

    def _export_dataset(
        self,
        dataset: xr.Dataset,
        polygon: dict,
        bbox: list[float],
        time_frame: tuple[datetime.datetime, datetime.datetime],
        output_dir: Path,
        show_progress: bool,
    ) -> list[ItemDownloadReport]:
        lat_name = self._find_coordinate(dataset, ("latitude", "lat"))
        lon_name = self._find_coordinate(dataset, ("longitude", "lon"))
        depth_name = self._find_coordinate(dataset, ("depth", "z", "elevation", "lev", "level"), optional=True)
        reports: list[ItemDownloadReport] = []

        for variable in tqdm(self.variables, desc="CMEMS variables", disable=not show_progress):
            if variable not in dataset.data_vars:
                reports.append(
                    self._report(
                        variable,
                        time_frame[0],
                        polygon,
                        bbox,
                        output_dir / f"{self._file_stem(variable, time_frame[0], depth=self.min_depth)}.tif",
                        False,
                        f"Variable '{variable}' is missing from the CMEMS subset.",
                    )
                )
                continue

            try:
                da = dataset[variable]

                # Filter by depth range if depth dimension exists
                if depth_name and depth_name in da.dims:
                    if self.max_depth is not None:
                        da = da.sel({depth_name: slice(self.min_depth, self.max_depth)})
                    else:
                        da = da.sel({depth_name: slice(self.min_depth, None)})
                    
                    if da.sizes[depth_name] == 0:
                        error = (
                            f"No depth levels found in range [{self.min_depth}, {self.max_depth}] "
                            f"for variable '{variable}'."
                        )
                        logger.warning(error)
                        reports.append(
                            self._report(
                                variable,
                                time_frame[0],
                                polygon,
                                bbox,
                                output_dir / f"{self._file_stem(variable, time_frame[0], depth=self.min_depth)}.tif",
                                False,
                                error,
                            )
                        )
                        continue
                    depth_values = [float(d) for d in da[depth_name].values]
                else:
                    depth_values = [0.0]

                # Loop through each depth level
                for d_val in depth_values:
                    if depth_name and depth_name in da.dims:
                        sub_da = da.sel({depth_name: d_val}, method="nearest")
                        actual_depth = float(sub_da[depth_name].values)
                    else:
                        sub_da = da
                        actual_depth = 0.0

                    sub_da = (
                        sub_da.rio.set_spatial_dims(x_dim=lon_name, y_dim=lat_name, inplace=False)
                        .rio.write_crs(CRS.from_epsg(4326), inplace=False)
                        .load()
                    )
                    clipped = sub_da.rio.clip([polygon], crs=CRS.from_epsg(4326), drop=True, all_touched=True)

                    time_dim = self._find_time_dimension(clipped)
                    if time_dim is None:
                        slices = [(time_frame[0], clipped)]
                    else:
                        slices = [
                            (self._as_datetime(clipped[time_dim].values[index]), clipped.isel({time_dim: index}))
                            for index in range(clipped.sizes[time_dim])
                        ]

                    for acquisition_time, data_slice in slices:
                        path = output_dir / f"{self._file_stem(variable, acquisition_time, depth=actual_depth)}.tif"
                        data_slice.rio.to_raster(path, compress="deflate")
                        valid = self._validate_geotiff(output_dir, path.stem).get(path, False)
                        reports.append(
                            self._report(
                                variable,
                                acquisition_time,
                                polygon,
                                bbox,
                                path,
                                valid,
                                None if valid else "GeoTIFF validation failed",
                                {"dataset_id": self.dataset_id, "depth_m": actual_depth},
                            )
                        )

            except Exception as exc:
                logger.exception("Failed to export CMEMS variable %s", variable)
                reports.append(
                    self._report(
                        variable,
                        time_frame[0],
                        polygon,
                        bbox,
                        output_dir / f"{self._file_stem(variable, time_frame[0], depth=self.min_depth)}.tif",
                        False,
                        str(exc),
                    )
                )
        return reports

    @staticmethod
    def _find_coordinate(dataset: xr.Dataset, prefixes: tuple[str, ...], optional: bool = False) -> Optional[str]:
        for name in dataset.coords:
            if any(name.lower() == prefix or name.lower().startswith(prefix) for prefix in prefixes):
                return name
        for name in dataset.dims:
            if any(name.lower() == prefix or name.lower().startswith(prefix) for prefix in prefixes):
                return name
        if optional:
            return None
        raise ValueError(f"Could not identify coordinates matching {prefixes} in CMEMS dataset")

    @staticmethod
    def _find_time_dimension(array: xr.DataArray) -> Optional[str]:
        for name in array.dims:
            if name.lower() == "time" or name.lower().startswith("time"):
                return name
        return None

    @staticmethod
    def _as_datetime(value: object) -> datetime.datetime:
        return pd.Timestamp(value).to_pydatetime().replace(tzinfo=None)

    def _file_stem(self, variable: str, acquisition_time: datetime.datetime, depth: float) -> str:
        dataset_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", self.dataset_id).strip("_.-") or "dataset"
        variable_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", variable).strip("_.-") or "variable"
        timestamp = acquisition_time.strftime("%Y%m%dT%H%M%S")
        return f"{dataset_name}_{variable_name}_depth_{depth:06.1f}m_{timestamp}"

    def _report(
        self,
        variable: str,
        acquisition_time: datetime.datetime,
        polygon: dict,
        bbox: list[float],
        path: Path,
        successful: bool,
        error: Optional[str],
        metadata: Optional[dict] = None,
    ) -> ItemDownloadReport:
        return ItemDownloadReport(
            data_source="CMEMS",
            variable_name=variable,
            acquisition_time=acquisition_time,
            polygon=polygon,
            bbox=bbox,
            path=path,
            download_successful=successful,
            error=error,
            metadata=metadata,
        )