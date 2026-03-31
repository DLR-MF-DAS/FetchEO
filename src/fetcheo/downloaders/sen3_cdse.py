import os
import math
import uuid
import shutil
import zipfile
import logging
import requests
import datetime
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

# Geospatial imports
from shapely.geometry import shape
import xarray as xr
import rioxarray
from satpy import Scene
from pyresample import create_area_def
import numpy as np
from pyresample.geometry import SwathDefinition, AreaDefinition
from pyresample.kd_tree import resample_nearest
from tqdm import tqdm

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport


class Sen3CDSEDownloader(BaseDownloader):
    def __init__(self, 
                 product_type: str = "OL_2_WFR", 
                 cache_dir: Optional[Path] = "sen3_cache"):
        """
        Direct OData Downloader for Copernicus Data Space Ecosystem.
        
        Args:
            product_type: E.g., 'OL_2_LFR' for Land or 'OL_2_WFR' for Water.
            cache_dir: Directory to store the raw .zip archives.
        """
        super().__init__()
        self.product_type = product_type
        
        # Set up the cache directory
        self.cache_dir = Path(f"./{cache_dir}") or Path("./.cdse_cache")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Test authentication on init to fail fast if credentials are wrong
        try:
            self._get_token()
        except Exception as e:
            self.logger.error(f"Authentication failed: {e}")
            raise 
       
    @property
    def frequency(self) -> str:
        return "daily"
    
    def fetch(self, 
              polygon: dict, 
              time_frame: tuple[datetime.datetime, datetime.datetime],
              output_dir: Path,
              show_progress: bool = True,
              max_workers: int = 4) -> list[ItemDownloadReport]:
        
        output_dir.mkdir(parents=True, exist_ok=True)
        reports: list[ItemDownloadReport] = []
        
        # Calculate Bounding Box from Polygon [min_lon, min_lat, max_lon, max_lat]
        aoi_shape = shape(polygon)
        aoi_bbox = list(aoi_shape.bounds) 
        aoi_wkt = aoi_shape.wkt
        
        self.logger.info("Authenticating with Copernicus...")
        token = self._get_token()
        
        date_start = time_frame[0].strftime("%Y-%m-%dT%H:%M:%S.000Z")
        date_end = time_frame[1].strftime("%Y-%m-%dT%H:%M:%S.000Z")
        
        search_url = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
        headers = {"Authorization": f"Bearer {token}"}
        
        filter_query = (
            "Collection/Name eq 'SENTINEL-3' "
            f"and contains(Name,'{self.product_type}') "
            "and contains(Name,'_NT_') "
            f"and ContentDate/Start ge {date_start} "
            f"and ContentDate/End le {date_end} "
            f"and OData.CSC.Intersects(area=geography'SRID=4326;{aoi_wkt}')"
        )
        
        all_products = []
        skip = 0
        
        self.logger.info(f"Querying OData API for {self.product_type} products...")
        while True:
            params = {"$filter": filter_query, "$top": 100, "$skip": skip}
            response = requests.get(search_url, headers=headers, params=params)
            response.raise_for_status()
            
            products = response.json().get("value", [])
            if not products:
                break
            
            all_products.extend(products)
            skip += 100

        self.logger.info(f"Found {len(all_products)} matching products.")
        if not all_products:
            return reports

        # Build baseline reports
        for product in all_products:
            report = ItemDownloadReport(
                data_source="CDSE OData API",
                variable_name=self.product_type,
                acquisition_time=datetime.datetime.fromisoformat(product["ContentDate"]["Start"].replace('Z', '+00:00')),
                polygon=polygon,
                bbox=aoi_bbox,
                path=Path(""),
                download_successful=False,
                metadata=product
            )
            reports.append(report)

        self.logger.info(f"Starting parallel pipeline with {max_workers} workers...")
        
        # Download images (multiple in parallel)
        final_reports: list[ItemDownloadReport] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._download_worker, token, report, output_dir)
                for report in reports
            ]
            
            completed_iterator = as_completed(futures)
            if show_progress:
                completed_iterator = tqdm(completed_iterator, total=len(reports), desc="Sentinel-3 CDSE", unit="image")
                
            for future in completed_iterator:
                # Extend flattens the list of reports returned by the worker
                final_reports.extend(future.result())
        return final_reports

    def _get_token(self) -> str:
        self.logger.debug("Requesting new CDSE access token...")
        url = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
        data = {
            "client_id": "cdse-public",
            "username": os.getenv("CDSE_USER"),
            "password": os.getenv("CDSE_PASS"),
            "grant_type": "password",
        }
        response = requests.post(url, data=data)
        response.raise_for_status()
        return response.json()["access_token"]


    def _convert_to_individual_tiffs(self, zip_path: Path, base_output_dir: Path, target_bbox: list[float]) -> dict[str, Path]:
        """
        Native Xarray implementation. Returns a dictionary of {band_name: uuid_tif_path}.
        Saves all files to a flat directory structure.
        """
        extracted_sen3_dir = base_output_dir / zip_path.stem 
        final_scene_name = zip_path.stem.replace(".SEN3", "")
        
        self.logger.info(f"[{final_scene_name}] Extracting zip archive...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Extracts to base_output_dir/[Product_Name].SEN3
            zip_ref.extractall(base_output_dir)
            
        # 1. Setup Swath and Grid Geometries
        coords_ds = xr.open_dataset(extracted_sen3_dir / "geo_coordinates.nc")
        lons = coords_ds['longitude'].values
        lats = coords_ds['latitude'].values
        swath_def = SwathDefinition(lons=lons, lats=lats)
        
        # --- NEW DYNAMIC PIXEL SIZE LOGIC ---
        # 1. Calculate center latitude of the bounding box
        center_lat = (target_bbox[1] + target_bbox[3]) / 2.0
        
        # 2. Define physical pixel size in meters (Sentinel-3 is 300m)
        pixel_size_m = 300.0
        
        # 3. Calculate dynamic degree resolution
        res_y = pixel_size_m / 111320.0 
        res_x = pixel_size_m / (111320.0 * math.cos(math.radians(center_lat)))
        
        # 4. Apply to map grid configuration
        area_id = 'duckdb_grid'
        proj_dict = {'proj': 'longlat', 'datum': 'WGS84'}
        width = int((target_bbox[2] - target_bbox[0]) / res_x)
        height = int((target_bbox[3] - target_bbox[1]) / res_y)
        area_def = AreaDefinition(area_id, 'WGS84', area_id, proj_dict, width, height, target_bbox)
        
        # 2. Target files
        target_files = [f"Oa{str(i).zfill(2)}_reflectance.nc" for i in range(1, 22)]
        target_files.extend([
            "chl_nn.nc", "chl_oc4me.nc", "tsm_nn.nc", 
            "iop_nn.nc", "w_aer.nc", "iwv.nc", "wqsf.nc",
            "lqsf.nc",
        ])
        
        exported_tiffs: dict[str, Path] = {}
        
        # 3. Process each file
        for filename in target_files:
            nc_path = extracted_sen3_dir / filename
            if not nc_path.exists():
                continue
                
            ds = xr.open_dataset(nc_path)
            data_vars = [v for v in ds.data_vars if 'columns' in ds[v].dims and 'rows' in ds[v].dims]
            
            for var_name in data_vars:
                # SKIP UNCERTAINTY FILES to save disk space
                if "_unc" in var_name.lower():
                    continue
                    
                data_array = ds[var_name].values
                fill_val = 0 if var_name == "WQSF" else np.nan
                
                gridded_data = resample_nearest(
                    swath_def, data_array, area_def, 
                    radius_of_influence=1500, fill_value=fill_val
                )
                
                x = np.linspace(target_bbox[0], target_bbox[2], width)
                y = np.linspace(target_bbox[3], target_bbox[1], height)
                
                da = xr.DataArray(gridded_data, coords=[('y', y), ('x', x)])
                da.rio.write_crs("EPSG:4326", inplace=True)
                
                # --- FLAT FILE UUID LOGIC ---
                file_uuid = str(uuid.uuid4())
                tif_path = base_output_dir / f"{file_uuid}.tif"
                
                da.rio.to_raster(tif_path, driver="COG", compress="DEFLATE")
                
                exported_tiffs[var_name] = tif_path
                
            ds.close()
            
        coords_ds.close()
        shutil.rmtree(extracted_sen3_dir)
        
        return exported_tiffs


    def _download_worker(self, token: str, base_report: ItemDownloadReport, output_dir: Path) -> list[ItemDownloadReport]:
        """Worker function for threading. Returns a list of reports per band."""
        product_id = base_report.metadata["Id"]
        product_name = base_report.metadata["Name"]
        
        download_url = f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"
        headers = {"Authorization": f"Bearer {token}"}
        
        # --- CACHE ROUTING ---
        # Save the zip to the dedicated cache directory, not the final COG output directory
        zip_path = self.cache_dir / f"{product_name}.zip"
        
        band_reports = []
        
        try:
            # 1. Download or Use Cache
            if zip_path.exists() and zip_path.stat().st_size > 100_000_000:
                self.logger.info(f"[{product_name}] Found in cache! Skipping download.")
            else:
                self.logger.info(f"[{product_name}] Initiating download to cache...")
                response = requests.get(download_url, headers=headers, stream=True)
                response.raise_for_status()
                with open(zip_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

            # 2. Process into COGs (It reads from cache, but extracts to output_dir)
            exported_tiffs = self._convert_to_individual_tiffs(zip_path, output_dir, base_report.bbox)
            
            # 3. Generate granular reports
            for band_name, tif_path in exported_tiffs.items():
                report = ItemDownloadReport(
                    data_source="Sen3_OLCI",
                    variable_name=band_name, 
                    acquisition_time=base_report.acquisition_time,
                    polygon=base_report.polygon,
                    bbox=base_report.bbox,
                    path=tif_path,
                    download_successful=True,
                    metadata=base_report.metadata
                )
                band_reports.append(report)
                
            return band_reports
            
        except Exception as e:
            error_msg = f"Failed processing {product_name}: {str(e)}"
            self.logger.error(error_msg)
            base_report.error = error_msg
            return [base_report]
