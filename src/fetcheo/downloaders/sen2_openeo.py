from __future__ import annotations

import datetime
import json
import shutil
from pathlib import Path
from abc import ABC, abstractmethod
from typing import Optional, List
from dataclasses import dataclass

import rasterio
from rasterio.errors import RasterioIOError
import openeo
from openeo.processes import ProcessBuilder

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport


class Sen2OpenEODownloader(BaseDownloader):
    def __init__(self):
        super().__init__()
        # Initialize connection. OIDC device flow is used by default.
        self.connection = openeo.connect("https://openeo.dataspace.copernicus.eu")
        self.connection.authenticate_oidc()

    def get_all_bands(self) -> List[str]:
        """Returns a list of all available Sentinel-2 bands in the openEO collection."""
        return ["B01", "B02", "B03", "B04", 
                "B05", "B06", "B07", "B08", 
                "B8A", "B09", "B11", "B12"]

    def fetch(self, 
              polygon: dict, 
              time_frame: tuple[datetime.datetime, datetime.datetime], 
              output_dir: Path, 
              show_progress: bool = True, 
              bands: list = None, 
              max_cloud_cover: float = 30.0) -> list[ItemDownloadReport]:
        # Create list to hold download reports       
        reports = []

        # Create temporary and final output directories
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        temp_dir = output_dir / ".openeo_raw"
        temp_dir.mkdir(exist_ok=True)

        # Get bounding box from polygon and start dates
        bbox = self._extract_bbox(polygon)
        spatial_extent = {"west": bbox[0], "south": bbox[1], "east": bbox[2], "north": bbox[3]}
        
        start_str = time_frame[0].strftime("%Y-%m-%d")
        end_str = time_frame[1].strftime("%Y-%m-%d")

        if show_progress:
            print(f"Submitting Sentinel-2 processing job for {start_str} to {end_str}...")

        # Build datacube using openeo servers
        if bands is None:
            bands = self.get_all_bands()
        datacube = self.connection.load_collection(
            "SENTINEL2_L2A",
            spatial_extent=spatial_extent,
            temporal_extent=[start_str, end_str],
            bands=bands,
            max_cloud_cover=max_cloud_cover 
        )

        def scale_function(x: ProcessBuilder):
            return x * 0.0001
        reflectance_cube = datacube.apply(scale_function)

        # Execute batch job to collect data
        saved_cube = reflectance_cube.save_result(format="GTiff")
        batch_job = saved_cube.create_job(title="FetchEO_S2_Download")
        batch_job.start_and_wait() 
        
        if show_progress:
            print("Job complete. Downloading assets and parsing metadata...")
        
        # Download image files and metadata json
        batch_job.get_results().download_files(str(temp_dir))

        # 3. Parse STAC JSON
        json_files = list(temp_dir.glob("*.json"))
        if not json_files:
            raise ValueError("No STAC metadata JSON found in openEO output.")
            
        with open(json_files[0], 'r') as f:
            stac_data = json.load(f)
        
        # Map original .SAFE filenames to dates
        derived_links = [link["href"] for link in stac_data.get("links", []) if link.get("rel") == "derived_from"]
        date_to_true_acq = {}
        for link in derived_links:
            # Example: S2A_MSIL2A_20260320T101051_N0512_R022_T32UPU_20260320T171910
            parts = link.split("_")
            if len(parts) > 2:
                datetime_str = parts[2] # e.g., 20260320T101051
                date_key = datetime_str[:8] # e.g., 20260320
                date_to_true_acq[date_key] = datetime_str

        assets = stac_data.get("assets", {})

        # 4. Process each downloaded TIFF asset
        for asset_filename, asset_meta in assets.items():
            if not asset_filename.endswith(".tif"):
                continue

            raw_filepath = temp_dir / asset_filename
            
            if raw_filepath.exists():
                # Extract the date from the openEO filename (e.g., test_2026-03-20Z.tif)
                # This relies on the openEO naming convention in the JSON
                raw_date_str = asset_filename.replace("openEO_", "").replace("Z.tif", "").replace("-", "")
                
                # Match it to the true acquisition time from the derived links
                true_acq_str = date_to_true_acq.get(raw_date_str)
                
                if true_acq_str:
                    # Parse the true timestamp (YYYYMMDDTHHMMSS)
                    exact_dt = datetime.datetime.strptime(true_acq_str, "%Y%m%dT%H%M%S")
                else:
                    # Fallback to midnight if matching fails
                    fallback_str = asset_filename.replace("test_", "").replace("Z.tif", "")
                    exact_dt = datetime.datetime.strptime(fallback_str, "%Y-%m-%d")

                # Standardize basename: S2_L2A_20260320T101051
                basename = f"S2_L2A_{exact_dt.strftime('%Y%m%dT%H%M%S')}"
                final_path = output_dir / f"{basename}.tif"
                
                shutil.move(str(raw_filepath), str(final_path))
                is_valid = self._validate_geotiff(output_dir, basename)[final_path]
                
                report = ItemDownloadReport(
                    data_source="Sentinel2-openeo",
                    variable_name=", ".join(bands),
                    acquisition_time=exact_dt,
                    polygon=polygon,
                    bbox=bbox,
                    path=final_path,
                    download_successful=is_valid,
                    error=None if is_valid else "Corrupted or unreadable GeoTIFF",
                    metadata={"original_stac_href": asset_meta.get("href", ""), "cloud_cover_max": max_cloud_cover}
                )
                reports.append(report)
            
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        return reports
    
    @property
    def frequency(self) -> str:
        return "daily"

    def _extract_bbox(self, polygon: dict) -> list[float]:
        """Helper method to extract [xmin, ymin, xmax, ymax] from a GeoJSON polygon."""
        lons = [pt[0] for pt in polygon["coordinates"][0]]
        lats = [pt[1] for pt in polygon["coordinates"][0]]
        return [min(lons), min(lats), max(lons), max(lats)]