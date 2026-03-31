from __future__ import annotations

import datetime
import json
import shutil
import pyproj
from tqdm import tqdm
from pathlib import Path

import openeo
from openeo.processes import ProcessBuilder

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport


class Sen3OpenEODownloader(BaseDownloader):
    def __init__(self):
        super().__init__()
        self.connection = openeo.connect("https://openeo.dataspace.copernicus.eu")
        self.connection.authenticate_oidc()

    @property
    def frequency(self) -> str:
        return "1-day"

    def fetch(self, 
              polygon: dict, 
              time_frame: tuple[datetime.datetime, datetime.datetime],
              output_dir: Path,
              show_progress: bool = True,
              bands: list = ["B08", "B04", "B03"]) -> list[ItemDownloadReport]:
        
        reports = []
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        temp_dir = output_dir / ".openeo_raw"
        temp_dir.mkdir(exist_ok=True)

        # 1. Standard WGS84 bounding box (The "Old Way" that worked)
        bbox_deg = self._extract_bbox(polygon)
        search_extent = {
            "west": bbox_deg[0], "south": bbox_deg[1], 
            "east": bbox_deg[2], "north": bbox_deg[3], 
            "crs": "EPSG:4326"
        }

        start_str = time_frame[0].strftime("%Y-%m-%d")
        end_str = time_frame[1].strftime("%Y-%m-%d")

        if show_progress:
            print(f"Submitting Sentinel-3 processing job for {start_str} to {end_str}...")

        # 2. Load datacube using WGS84 (Finds the actual data!)
        if bands is None:
            bands = self._get_all_bands()
        datacube = self.connection.load_collection(
            "SENTINEL3_OLCI_L2_WATER", 
            spatial_extent=search_extent,
            temporal_extent=[start_str, end_str],
            bands=bands,
        )

        # # 3. Resample and force the exact metric crop
        # if target_res_m:
        #     resample_method = "bilinear" if target_res_m < 300 else "average"
            
        #     # First, project the data into the local UTM metric grid
        #     datacube = datacube.resample_spatial(
        #         resolution=target_res_m,
        #         projection=target_crs,
        #         method=resample_method 
        #     )
            
        #     # What you write:
        #     datacube = datacube.filter_bbox(**metric_extent)
        
        # 4. Execute batch job
        saved_cube = datacube.save_result(format="GTiff", options={"filename_prefix": "S3_out"})
        batch_job = saved_cube.create_job(title="FetchEO_S3_Download")
        batch_job.start_and_wait() 
        
        if show_progress:
            print("Job complete. Downloading assets and parsing metadata...")
        
        batch_job.get_results().download_files(str(temp_dir))

        # 5. Parse STAC JSON
        json_files = list(temp_dir.glob("*.json"))
        if not json_files:
            raise ValueError("No STAC metadata JSON found in openEO output.")
            
        with open(json_files[0], 'r') as f:
            stac_data = json.load(f)
        
        # 6. Map S3 SAFE filenames to true acquisition datetimes
        derived_links = [link["href"] for link in stac_data.get("links", []) if link.get("rel") == "derived_from"]
        date_to_true_acq = {}
        for link in derived_links:
            parts = link.split("_")
            for part in parts:
                if len(part) == 15 and "T" in part:
                    date_key = part[:8] 
                    date_to_true_acq[date_key] = part
                    break

        assets = stac_data.get("assets", {})

        # 7. Process downloaded TIFF assets
        for asset_filename, asset_meta in tqdm(assets.items()):
            if not asset_filename.endswith(".tif"):
                continue

            raw_filepath = temp_dir / asset_filename
            
            clean_str = asset_filename.replace("S3_out_", "").replace("Z.tif", "").replace(".tif", "").replace("-", "")
            date_key = clean_str[:8]
            true_acq_str = date_to_true_acq.get(date_key)
            
            try:
                if true_acq_str:
                    exact_dt = datetime.datetime.strptime(true_acq_str, "%Y%m%dT%H%M%S").replace(tzinfo=datetime.timezone.utc)
                else:
                    if "T" in clean_str:
                        exact_dt = datetime.datetime.strptime(clean_str, "%Y%m%dT%H%M%S").replace(tzinfo=datetime.timezone.utc)
                    else:
                        exact_dt = datetime.datetime.strptime(clean_str, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)
            except ValueError as e:
                print(f"Warning: Could not parse exact time for {asset_filename}. Defaulting to start of day. Error: {e}")
                exact_dt = datetime.datetime.strptime(date_key, "%Y%m%d").replace(tzinfo=datetime.timezone.utc)

            basename = f"S3_WATER_{exact_dt.strftime('%Y%m%dT%H%M%S')}"
            final_path = output_dir / f"{basename}.tif"
            
            is_valid = False
            error_msg = None

            if raw_filepath.exists():
                shutil.move(str(raw_filepath), str(final_path))
                is_valid = self._validate_geotiff(output_dir, basename)[final_path]
                if not is_valid:
                    error_msg = "Corrupted or unreadable GeoTIFF"
            else:
                error_msg = "File missing from download stream"
            
            report = ItemDownloadReport(
                data_source="Sentinel3-openeo",
                variable_name=", ".join(bands),
                acquisition_time=exact_dt,
                polygon=polygon,
                bbox=self._extract_bbox(polygon), 
                path=final_path,
                download_successful=is_valid,
                error=error_msg,
                metadata={"original_stac_href": asset_meta.get("href", "")}
            )
            reports.append(report)

        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        return reports
    
    def _get_all_bands(self) -> list[str]:
        """Returns all available bands for Sentinel-3 OLCI Level-2 Water."""
        return [
            "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B09", 
            "B10", "B11", "B12", "B13", "B14", "B15", "B16", "B17", "B18",
            "B19", "B20", "B21", "FLAGS", "IWV", "CHL_OC4ME", "TSM_NN", "PAR",
            "KD490_M07", "A865", "T865", "CHL_NN", "ADG443_NN"
        ]

    def _extract_bbox(self, polygon: dict) -> list[float]:
        """Extracts [xmin, ymin, xmax, ymax] from a GeoJSON polygon."""
        lons = [pt[0] for pt in polygon["coordinates"][0]]
        lats = [pt[1] for pt in polygon["coordinates"][0]]
        return [min(lons), min(lats), max(lons), max(lats)]

    def _calculate_utm_epsg(self, bbox: list[float]) -> int:
        """Dynamically calculates the local UTM EPSG code based on the bounding box centroid."""
        centroid_lon = (bbox[0] + bbox[2]) / 2.0
        centroid_lat = (bbox[1] + bbox[3]) / 2.0
        utm_zone = int((centroid_lon + 180) / 6) + 1
        return 32600 + utm_zone if centroid_lat >= 0 else 32700 + utm_zone

    def get_metric_bounds(self, polygon: dict, patch_size_meters: int = 10240) -> dict:
        """Calculates a perfect square metric bounding box centered on a WGS84 polygon."""
        bbox = self._extract_bbox(polygon)
        centroid_lon = (bbox[0] + bbox[2]) / 2.0
        centroid_lat = (bbox[1] + bbox[3]) / 2.0
        epsg_code = self._calculate_utm_epsg(bbox)
        
        transformer = pyproj.Transformer.from_crs("EPSG:4326", f"EPSG:{epsg_code}", always_xy=True)
        centroid_x, centroid_y = transformer.transform(centroid_lon, centroid_lat)
        half_size = patch_size_meters / 2.0
        
        return {
            "crs": f"EPSG:{epsg_code}",
            "west": centroid_x - half_size,
            "south": centroid_y - half_size,
            "east": centroid_x + half_size,
            "north": centroid_y + half_size
        }