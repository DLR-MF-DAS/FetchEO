import logging
import requests
import datetime
from tqdm import tqdm
from pathlib import Path
import concurrent.futures
from pystac_client import Client
from typing import List, Optional
import uuid

import pyproj
import rasterio
from affine import Affine
from rasterio.mask import mask
from shapely.geometry import shape
from rasterio.enums import Resampling
from shapely.ops import transform as shapely_transform

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport


class Sen2AWSDownloader(BaseDownloader):

    def __init__(self): 
        super().__init__()
        self.stac_url = "https://earth-search.aws.element84.com/v1"
        self.catalog = Client.open(self.stac_url)

        # Maps standard Sentinel-2 band names to Element84 STAC asset keys
        self.S2_BAND_MAP = {
            'B1': 'coastal',    
            'B2': 'blue',       
            'B3': 'green',
            'B4': 'red',        
            'B5': 'rededge1',   
            'B6': 'rededge2',
            'B7': 'rededge3',   
            'B8': 'nir',        
            'B8A': 'nir08',
            'B9': 'nir09',
            'B10': 'cirrus',      
            'B11': 'swir16',    
            'B12': 'swir22',
            'AOT': 'aot',       
            'WVP': 'wvp',       
            'SCL': 'scl',
            'VISUAL': 'visual'
        }

    @property
    def frequency(self) -> str:
        return "daily"

    def fetch(
        self,
        polygon: dict,
        time_frame: tuple[datetime.datetime, datetime.datetime],
        output_dir: Path,
        show_progress: bool = True,
        data_type: str = "l2a",
        bands: List[str] = ['VISUAL'], 
        target_resolution_m: Optional[float] = None,
        max_workers: int = 5,
    ) -> List[ItemDownloadReport]:
        # Ensure output directory exists
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # Extract datetime (STAC expects YYYY-MM-DD/YYYY-MM-DD)
        start_dt, end_dt = time_frame
        time_range = f"{start_dt.strftime('%Y-%m-%d')}/{end_dt.strftime('%Y-%m-%d')}"

        # Map GEE collection name to STAC collection name
        collection = f"sentinel-2-{data_type}"

        # Search the Catalog for all images within the polygon and time frame.
        search = self.catalog.search(
            collections=[collection],
            intersects=polygon,
            datetime=time_range,
            # Optional: Add a cloud cover filter here if you want to skip completely cloudy tiles
            # query={"eo:cloud_cover": {"lt": 100}}
        )
        
        items = list(search.items())
        if items:
            logging.info(f"Found {len(items)} images on AWS for the specified parameters.")
        else:
            logging.warning("No images found for the specified parameters.")
            return []        
        
        # 3. Multithreaded Download Execution
        # We use a ThreadPoolExecutor to download multiple images at once.
        reports = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all items to the thread pool
            images4dl = {
                executor.submit(self._download_single_image, 
                                item, 
                                output_dir, 
                                collection, 
                                bands,
                                polygon,
                                target_resolution_m): item 
                for item in items
            }
            
            # Wrap the 'as_completed' iterator in tqdm for the progress bar
            iterator = tqdm(
                concurrent.futures.as_completed(images4dl), 
                total=len(images4dl), 
                desc="Sen2_AWS", 
                unit="image", 
                disable=not show_progress
            )
            
            # Run download
            for image4dl in iterator:
                reports.append(image4dl.result())

        return reports
    
    def _ensure_download(self, item, bands, tile_name: str, cache_dir: Path, requests_timeout: int = 120) -> list:
        """
        Checks if requested bands for a tile are cached in cache_dir. Downloads missing bands and returns paths.
        """
        # Create cache directory if it doesn't exist
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Download each band if not cached, and collect paths
        band_files = []
        for band in bands:
            mapped_key = self.S2_BAND_MAP.get(band.upper(), band.lower())
            current_band_path = cache_dir / f"{tile_name}_{mapped_key}.tif"

            # Check if file exists and is valid
            if current_band_path.exists() and current_band_path.stat().st_size > 0:
                band_files.append(current_band_path)
                continue

            # Download if not cached
            if mapped_key not in item.assets:
                raise ValueError(f"Band {band} (mapped to '{mapped_key}') not found in STAC assets.")
            url = item.assets[mapped_key].href
            r = requests.get(url, stream=True, timeout=requests_timeout)
            r.raise_for_status()
            with open(current_band_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
            band_files.append(current_band_path)
        return band_files
    
    def _crop_tile_to_polygon(self, tile_path: Path, polygon: dict) -> Path:
        """Crops a raster file to a GeoJSON polygon, handling CRS reprojection."""
        with rasterio.open(tile_path) as src:
            # 1. Reproject the Lat/Lon polygon to the image's native CRS
            geom_4326 = shape(polygon)
            project = pyproj.Transformer.from_crs("epsg:4326", src.crs, always_xy=True).transform
            geom_native = shapely_transform(project, geom_4326)
            
            # 2. Crop using the reprojected geometry
            clipped, clipped_transform = mask(src, [geom_native], crop=True)
            clipped_meta = src.meta.copy()
            clipped_meta.update({
                "height": clipped.shape[1],
                "width": clipped.shape[2],
                "transform": clipped_transform,
                "tiled": True
            })
            
            # 3. Extract descriptions BEFORE the file closes
            band_descriptions = [src.tags(i).get('DESCRIPTION', '') for i in range(1, src.count + 1)]
        
        # Write to a hidden temporary file first
        temp_path = tile_path.with_name(tile_path.stem + "_cropped.tif")
        with rasterio.open(temp_path, 'w', **clipped_meta) as dst:
            dst.write(clipped)
            # Apply the descriptions from our saved list
            for i, desc in enumerate(band_descriptions, start=1):
                dst.set_band_description(i, desc)
        
        # Safely overwrite the original file and return the clean path
        temp_path.replace(tile_path)
        return tile_path


    def _downsample_raster(self, raster_path: Path, target_resolution_m: float) -> Path:
        """Resamples a multi-band raster file to a specific physical resolution in meters."""
        with rasterio.open(raster_path) as src:
            # 1. Calculate new dimensions based on target resolution in meters
            physical_width = src.bounds.right - src.bounds.left
            physical_height = src.bounds.top - src.bounds.bottom
            
            new_width = max(int(physical_width / target_resolution_m), 1)
            new_height = max(int(physical_height / target_resolution_m), 1)
            
            # 2. Read all bands at once, resampling them on the fly
            data = src.read(
                out_shape=(src.count, new_height, new_width),
                resampling=Resampling.average
            )
            
            # 3. Create a perfect geographic transform for the new resolution
            new_transform = Affine(
                target_resolution_m, 0.0, src.bounds.left,
                0.0, -target_resolution_m, src.bounds.top
            )
            
            profile = src.profile.copy()
            profile.update({
                "height": new_height,
                "width": new_width,
                "transform": new_transform,
                "tiled": True
            })
            
            # 4. Extract descriptions BEFORE the file closes
            band_descriptions = [src.tags(i).get('DESCRIPTION', '') for i in range(1, src.count + 1)]
        
        temp_path = raster_path.with_name(raster_path.stem + "_downsampled.tif")
        with rasterio.open(temp_path, 'w', **profile) as dst:
            dst.write(data) 
            # Apply the descriptions to the new downsampled file
            for i, desc in enumerate(band_descriptions, start=1):
                dst.set_band_description(i, desc)
                
        temp_path.replace(raster_path)
        return raster_path

    def _download_single_image(
        self, 
        item, 
        output_dir: Path, 
        collection: str, 
        bands: List[str],
        polygon: dict,                               
        target_resolution_m: Optional[float] = None
    ) -> ItemDownloadReport:
        # Extract acquisition time and metadata
        acq_time_str = item.properties["datetime"].replace('Z', '+00:00')
        acqusition_dt = datetime.datetime.fromisoformat(acq_time_str)
        metadata = item.properties
        
        # Generate a UUID for the polygon (once per download call)
        polygon_uuid = uuid.uuid5(uuid.NAMESPACE_URL, str(polygon))

        # Get MGRS tile from metadata to construct a meaningful filename
        mgrs_tile = metadata.get("s2:mgrs_tile")
        if not mgrs_tile:
            grid_code = metadata.get("grid:code", "UNKNOWN_GRID")
            mgrs_tile = grid_code.replace("MGRS-", "") if grid_code.startswith("MGRS-") else grid_code
        tile_name = f"S2_{mgrs_tile}_{acqusition_dt.strftime('%Y%m%dT%H%M%S')}"
        out_path = output_dir / f"{tile_name}_{polygon_uuid}.tif"
        
        band_files = []
        try:
            # 1. Download each band as a temporary file
            band_files = self._ensure_download(item, bands, tile_name, cache_dir=output_dir / "s2_cache")
            
            # 2. Stack the temporary files into a single multi-band GeoTIFF
            # First, find the file with the highest resolution (most pixels) to act as the master
            master_file = band_files[0]
            max_pixels = 0
            
            for band_path in band_files:
                with rasterio.open(band_path) as src:
                    total_pixels = src.height * src.width
                    if total_pixels > max_pixels:
                        max_pixels = total_pixels
                        master_file = band_path
            
            # Now, use that highest-resolution file to define the master profile and shape
            with rasterio.open(master_file) as src_master:
                profile = src_master.profile.copy()
                profile.update(count=len(bands))
                master_shape = (src_master.height, src_master.width)
                
            with rasterio.open(out_path, 'w', **profile) as dst:
                for idx, band_path in enumerate(band_files):
                    with rasterio.open(band_path) as src:
                        # Auto-resample lower-resolution bands UP to match the highest-resolution band
                        if (src.height, src.width) != master_shape:
                            arr = src.read(
                                1, 
                                out_shape=master_shape,
                                resampling=Resampling.bilinear
                            )
                        else:
                            arr = src.read(1)
                        
                        dst.write(arr, idx + 1)
                        dst.set_band_description(idx + 1, bands[idx])
            
            # ---------------------------------------------------------
            # 4. POST-PROCESSING: CROP & DOWNSAMPLE
            # ---------------------------------------------------------
            final_path = out_path
            
            # Step A: Crop ONLY if the tile is not fully contained by the requested polygon
            if polygon:
                from shapely.geometry import shape
                request_geom = shape(polygon)
                tile_geom = shape(item.geometry)
                
                if not request_geom.contains(tile_geom):
                    # This function safely overwrites final_path in place
                    self._crop_tile_to_polygon(final_path, polygon)
                
            # Step B: Downsample the image to target resolution
            if target_resolution_m:
                # This function safely overwrites final_path in place
                self._downsample_raster(final_path, target_resolution_m)
            # ---------------------------------------------------------
                    
            return ItemDownloadReport(
                data_source="S2",
                variable_name=collection,
                acquisition_time=acqusition_dt,
                polygon=item.geometry,
                bbox=item.bbox,
                path=final_path, # <--- Updated to return the post-processed file
                download_successful=True,
                metadata=metadata
            )
            
        except Exception as e:
            logging.error(f"Failed to process image {item.id}: {e}")
            
            # Clean up all temp files if failure occurs
            for band_path in band_files:
                if band_path.exists():
                    band_path.unlink(missing_ok=True)
            # Also clean up the main file if it failed during post-processing
            if 'out_path' in locals() and out_path.exists():
                out_path.unlink(missing_ok=True)
                    
            return ItemDownloadReport(
                data_source="S2",
                variable_name=collection,
                acquisition_time=acqusition_dt,
                polygon=item.geometry,
                bbox=item.bbox,
                path=out_path, 
                download_successful=False,
                error=str(e),
                metadata=metadata
            )
