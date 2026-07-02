import os
import datetime
from tqdm import tqdm
from pathlib import Path
from typing import Optional
from collections import defaultdict

import rioxarray
import numpy as np
import xarray as xr
from eodag import EODataAccessGateway
from pyresample.kd_tree import resample_nearest
from pyresample.geometry import SwathDefinition, create_area_def
from rioxarray.merge import merge_arrays

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport


class Sentinel3SynergyDownloader(BaseDownloader):

    def __init__(
        self,
        username: Optional[str] = None,
        password: Optional[str] = None,
        variables_to_files_map: Optional[dict[str, str]] = None,
    ):
        """
        Streamlined downloader for Sentinel-3 OLCI Level-2 Water (S3_OLCI_L2WRR) products.
        Checks for required credentials in arguments or environment variables.
        """
        super().__init__()
        self.variables_to_files_map = variables_to_files_map
        if variables_to_files_map is None:
            self.variables_to_files_map = self._get_all_variables_to_files_map()

        # Check for credentials: use arguments if provided, else fall back to env vars
        username = username or os.environ.get("CDSE_USERNAME")
        password = password or os.environ.get("CDSE_PASSWORD")

        if not username or not password:
            raise ValueError(
                "CDSE credentials required: provide username and password as arguments or set CDSE_USERNAME and CDSE_PASSWORD environment variables."
            )

        # Set EODAG environment variables for credentials
        os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__USERNAME"] = username
        os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__PASSWORD"] = password

        # Initialize EODAG
        self.dag = EODataAccessGateway()
        self.dag.set_preferred_provider("cop_dataspace")


    @property
    def frequency(self) -> str:
        return "daily"


    def fetch(self,
              polygon: dict,
              time_frame: tuple[datetime.datetime, datetime.datetime],
              output_dir: Path,
              cache_dir: Optional[Path] = None,
              show_progress: bool = True,
              ) -> list[ItemDownloadReport]:

        # Normalize path inputs and make output/cache directories
        output_dir = Path(output_dir)
        if cache_dir is None:
            cache_dir = output_dir / "cache"
        else:
            cache_dir = Path(cache_dir)

        output_dir.mkdir(parents=True, exist_ok=True)
        cache_dir.mkdir(parents=True, exist_ok=True)
        os.environ["EODAG__COP_DATASPACE__DOWNLOAD__OUTPUT_DIR"] = str(cache_dir)
        bbox = self._extract_bbox(polygon)

        # The CDSE STAC backend requires the 'collection' key
        search_criteria = {
            "collection": "S3_OLCI_L2WRR",
            "start": time_frame[0].strftime("%Y-%m-%dT%H:%M:%S"),
            "end": time_frame[1].strftime("%Y-%m-%dT%H:%M:%S"),
            "geom": {"lonmin": bbox[0], "latmin": bbox[1], "lonmax": bbox[2], "latmax": bbox[3]}
        }

        # Search catalog for Sentinel-3 water images
        reports = []
        grouped_tiles: dict[tuple[datetime.datetime, str], dict] = {}
        try:
            search_results = self.dag.search(**search_criteria)
        except Exception as e:
            return [
                ItemDownloadReport(
                    data_source="Sentinel-3",
                    variable_name="Synergy_Product",
                    acquisition_time=time_frame[0],
                    polygon=polygon,
                    bbox=bbox,
                    path=output_dir,
                    download_successful=False,
                    error=f"Search failed: {str(e)}"
                )
            ]

        reports = []

        # ---------------------------------------------------------
        # 1. GROUP SEARCH RESULTS BY DATE
        # ---------------------------------------------------------
        items_by_date = defaultdict(list)
        for item in search_results:
            acq_time_raw = item.properties.get("datetime")
            if not acq_time_raw:
                continue
            acq_time = datetime.datetime.fromisoformat(acq_time_raw.replace("Z", "+00:00"))
            date_str = acq_time.strftime('%Y%m%d')
            items_by_date[date_str].append((item, acq_time))

        # ---------------------------------------------------------
        # 2. PROCESS DAY BY DAY
        # ---------------------------------------------------------
        for date_str, daily_items in tqdm(items_by_date.items(), desc="Processing days", disable=not show_progress):
            
            # Dictionary to temporarily hold swath file paths for each variable
            swaths_to_merge = defaultdict(list)
            first_acq_time = daily_items[0][1] # Use the first swath's time for the final report
            
            # A. DOWNLOAD AND CLIP ALL SWATHS FOR THIS DAY
            for item, acq_time in daily_items:
                try:
                    product_dir_path = self.dag.download(item, extract=True)
                    sen3_dir = Path(product_dir_path)

                    for variable_name, nc_filename in self.variables_to_files_map.items():
                        # Save swaths with a temporary 'part' suffix
                        basename = f"S3_{acq_time.strftime('%Y%m%d_%H%M%S')}_{variable_name}_part"
                        temp_tif_path = output_dir / f"{basename}.tif"

                        # Load, clip, and save the individual swath
                        da = self._load_netcdf_as_array(cache_dir, sen3_dir, nc_filename, variable_name)
                        self._clip_and_save_array_as_cog(da, sen3_dir, polygon, temp_tif_path)
                        
                        swaths_to_merge[variable_name].append(temp_tif_path)

                except Exception as e:
                    print(f"Failed to process a swath on {date_str}: {e}")
                    continue # Skip this swath but continue with others for the day

            # B. MERGE SWATHS AND CREATE ONE REPORT PER VARIABLE
            for variable_name, tif_paths in swaths_to_merge.items():
                if not tif_paths:
                    continue
                
                final_basename = f"S3_{date_str}_merged_{variable_name}.tif"
                final_path = output_dir / final_basename

                try:
                    if len(tif_paths) == 1:
                        # Only one swath today, just rename the temp file
                        tif_paths[0].rename(final_path)
                        metadata = {"notes": "Single swath"}
                    else:
                        # Multiple swaths: Merge them
                        datasets = [xr.open_dataset(p, engine="rasterio") for p in tif_paths]
                        merged = merge_arrays(datasets)
                        
                        merged.rio.to_raster(final_path, driver="COG", compress="DEFLATE", tiled=True)
                        
                        for ds in datasets:
                            ds.close()
                        for p in tif_paths:
                            p.unlink() # Clean up temp files
                            
                        metadata = {"notes": f"Merged from {len(tif_paths)} swaths"}

                    # Validate and create the single report
                    success = final_path.exists()
                    reports.append(
                        ItemDownloadReport(
                            data_source="Sentinel-3",
                            variable_name=variable_name,
                            acquisition_time=first_acq_time,
                            polygon=polygon,
                            bbox=bbox,
                            path=final_path,
                            download_successful=success,
                            error=None if success else "Final GeoTIFF missing.",
                            metadata=metadata
                        )
                    )

                except Exception as e:
                    reports.append(
                        ItemDownloadReport(
                            data_source="Sentinel-3",
                            variable_name=variable_name,
                            acquisition_time=first_acq_time,
                            polygon=polygon,
                            bbox=bbox,
                            path=output_dir, # fallback path
                            download_successful=False,
                            error=f"Merge failed: {e}"
                        )
                    )

        return reports


    def _get_all_variables_to_files_map(self):
        return {
          "Oa01_reflectance": "Oa01_reflectance",
          "Oa02_reflectance": "Oa02_reflectance",
          "Oa03_reflectance": "Oa03_reflectance",
          "Oa04_reflectance": "Oa04_reflectance",
          "Oa05_reflectance": "Oa05_reflectance",
          "Oa06_reflectance": "Oa06_reflectance",
          "Oa07_reflectance": "Oa07_reflectance",
          "Oa08_reflectance": "Oa08_reflectance",
          "Oa09_reflectance": "Oa09_reflectance",
          "Oa10_reflectance": "Oa10_reflectance",
          "Oa11_reflectance": "Oa11_reflectance",
          "Oa12_reflectance": "Oa12_reflectance",
          "Oa16_reflectance": "Oa16_reflectance",
          "Oa17_reflectance": "Oa17_reflectance",
          "Oa18_reflectance": "Oa18_reflectance",
          "Oa21_reflectance": "Oa21_reflectance",
          "CHL_NN": "chl_nn",
          "CHL_OC4ME": "chl_oc4me",
          "ADG443_NN": "iop_nn",
          "IWV": "iwv",
          "KD490_M07": "trsp",
          "TSM_NN": "tsm_nn",
          "A865": "w_aer",
          "T865": "w_aer",
          "WQSF": "wqsf"
          }


    def _load_netcdf_as_array(self, cache_dir, sen3_dir, nc_filename, variable_name) -> None:
        # Check that file exists
        nc_path = cache_dir/ sen3_dir / f"{nc_filename}.nc"
        if not nc_path.exists():
            raise FileNotFoundError(f"Missing {nc_filename} in {sen3_dir.name}")

        # Load netcdf file and return as data array
        with xr.open_dataset(nc_path) as array:
            return array[variable_name].load()


    def _build_group_metadata(self, metadata_items: list[dict]) -> Optional[dict]:
        if not metadata_items:
            return None
        if len(metadata_items) == 1:
            return metadata_items[0]
        return {"source_products": metadata_items}
    

    def _clip_and_save_array_as_cog(self, da, sen3_dir, polygon, output_path):
        """
        Pipeline for degree-based mapping (EPSG:4326): 
        1. Trims distorted swath edges.
        2. Crops to bounding box to save RAM.
        3. Resamples to a rigid 0.01 degree latitude/longitude grid.
        """
        # 1. Extract geographic bounding box (Degrees)
        min_lon, min_lat, max_lon, max_lat = self._extract_bbox(polygon)

        # 2. Load the 2D Geolocation arrays
        geo_file = sen3_dir / "geo_coordinates.nc"
        if not geo_file.exists():
            raise FileNotFoundError(f"Missing geolocation file: {geo_file}")
            
        with xr.open_dataset(geo_file) as geo_ds:
            lons = geo_ds['longitude'].values
            lats = geo_ds['latitude'].values

        # ---------------------------------------------------------
        # STEP 1: TRIM THE SWATH EDGES (Quality Control)
        # ---------------------------------------------------------
        # Drop the outer 15% on both sides to remove atmospheric distortion
        total_cols = lons.shape[1]
        trim_amount = int(total_cols * 0.15) 

        lons = lons[:, trim_amount:-trim_amount]
        lats = lats[:, trim_amount:-trim_amount]
        
        dim_y, dim_x = da.dims[0], da.dims[1]
        da_trimmed = da.isel({dim_x: slice(trim_amount, -trim_amount)})

        # ---------------------------------------------------------
        # STEP 2: ROUGH CROP BY LAT/LON (Save Memory)
        # ---------------------------------------------------------
        valid_pixels = (lats >= min_lat) & (lats <= max_lat) & (lons >= min_lon) & (lons <= max_lon)
        
        if not valid_pixels.any():
            raise ValueError(f"No data found in bounds after edge trimming. Variable: {da.name}")

        valid_rows = np.any(valid_pixels, axis=1)
        valid_cols = np.any(valid_pixels, axis=0)
        
        rmin, rmax = np.where(valid_rows)[0][[0, -1]]
        cmin, cmax = np.where(valid_cols)[0][[0, -1]]

        # Add a 10-pixel buffer
        rmin, rmax = max(0, rmin - 10), min(lats.shape[0], rmax + 10)
        cmin, cmax = max(0, cmin - 10), min(lats.shape[1], cmax + 10)

        lats_cropped = lats[rmin:rmax, cmin:cmax]
        lons_cropped = lons[rmin:rmax, cmin:cmax]
        da_cropped = da_trimmed.isel({dim_y: slice(rmin, rmax), dim_x: slice(cmin, cmax)})

        # ---------------------------------------------------------
        # STEP 3: DEFINE THE EPSG:4326 GRID
        # ---------------------------------------------------------
        proj_dict = {"proj": "longlat", "datum": "WGS84"}

        swath_def = SwathDefinition(lons=lons_cropped, lats=lats_cropped)

        # Change resolution to match your framework's degree requirements
        # 0.01 degrees ~= 1.1km. (Change to 0.05 if matching MODIS exactly)
        area_def = create_area_def(
            area_id="epsg4326_grid",
            projection=proj_dict,
            area_extent=[min_lon, min_lat, max_lon, max_lat],
            resolution=0.01  
        )

        # ---------------------------------------------------------
        # STEP 4: RESAMPLE TO THE DEGREE GRID
        # ---------------------------------------------------------
        resampled_data = resample_nearest(
            swath_def, 
            da_cropped.values, 
            area_def, 
            radius_of_influence=4000, # Max search distance in meters (still required in meters)
            fill_value=np.nan
        )

        # ---------------------------------------------------------
        # STEP 5: SAVE GEOTIFF
        # ---------------------------------------------------------
        # get_proj_coords() now returns 2D X/Y arrays in DEGREES
        target_lon, target_lat = area_def.get_proj_coords()
        
        da_gridded = xr.DataArray(
            resampled_data,
            dims=["y", "x"],
            coords={
                "x": target_lon[0, :],  # 1D Longitudes
                "y": target_lat[:, 0]   # 1D Latitudes
            }
        )
        
        # Write the standard EPSG:4326 CRS
        da_gridded.rio.write_crs("EPSG:4326", inplace=True)
        
        da_gridded.rio.to_raster(
            output_path,
            driver="COG",
            compress="DEFLATE",
            tiled=True
        )
        
        return da_gridded

