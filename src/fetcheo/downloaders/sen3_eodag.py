import os
import datetime
from tqdm import tqdm
from pathlib import Path
from typing import Optional

import rioxarray
import numpy as np
import xarray as xr
from eodag import EODataAccessGateway
from pyresample.kd_tree import resample_nearest
from pyresample.geometry import SwathDefinition, create_area_def

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
        """
        Fetches Sentinel-3 swaths and maps them to a unified regional grid.
        Every swath is saved independently with its exact timestamp to preserve temporal integrity.
        """
        output_dir = Path(output_dir)
        cache_dir = Path(cache_dir) if cache_dir else output_dir / "cache"
        output_dir.mkdir(parents=True, exist_ok=True)
        cache_dir.mkdir(parents=True, exist_ok=True)

        os.environ["EODAG__COP_DATASPACE__DOWNLOAD__OUTPUT_DIR"] = str(cache_dir)
        bbox = self._extract_bbox(polygon)

        # 1. CREATE MASTER GRID FOR THE REGION (The "Empty Canvas")
        area_def = self._create_master_grid(bbox, resolution=0.01)

        search_criteria = {
            "collection": "S3_OLCI_L2WRR",
            "start": time_frame[0].strftime("%Y-%m-%dT%H:%M:%S"),
            "end": time_frame[1].strftime("%Y-%m-%dT%H:%M:%S"),
            "geom": {"lonmin": bbox[0], "latmin": bbox[1], "lonmax": bbox[2], "latmax": bbox[3]}
        }

        reports = []
        try:
            search_results = self.dag.search(**search_criteria)
        except Exception as e:
            return [self._create_error_report(time_frame[0], polygon, bbox, output_dir, f"Search failed: {e}")]

        # 2. PROCESS EVERY SWATH INDEPENDENTLY 
        for item in tqdm(search_results, desc="Processing swaths", disable=not show_progress):
            acq_time_raw = item.properties.get("datetime")
            if not acq_time_raw:
                continue
            
            acq_time = datetime.datetime.fromisoformat(acq_time_raw.replace("Z", "+00:00"))
            exact_time_str = acq_time.strftime('%Y%m%d_%H%M%S')

            try:
                # EODAG handles caching automatically if the file is already downloaded
                product_path = self.dag.download(item, extract=True)
                sen3_dir = Path(product_path)
            except Exception as e:
                print(f"Download failed for swath at {exact_time_str}: {e}")
                continue

            for var_name, nc_filename in self.variables_to_files_map.items():
                product_id = item.properties.get("id", "unknown")
                final_basename = f"S3_{exact_time_str}_{product_id}_{var_name}"
                final_tif_path = output_dir / f"{final_basename}.tif"
                
                # Skip if this specific swath variable is already processed
                if final_tif_path.exists():
                    reports.append(self._create_success_report(var_name, acq_time, polygon, bbox, final_tif_path, "Already exists"))
                    continue

                try:
                    # Load, trim, and place on the Master Grid
                    da_gridded = self._process_swath_to_grid(cache_dir, sen3_dir, nc_filename, var_name, bbox, area_def)
                    
                    if da_gridded is not None:
                        # Since WQSF is now a float mask, we can safely write to GeoTIFF without corruption
                        da_gridded.rio.to_raster(final_tif_path, driver="COG", compress="DEFLATE", tiled=True)
                        reports.append(self._create_success_report(var_name, acq_time, polygon, bbox, final_tif_path, "Exact timestamp preserved"))
                        
                        # Clean up RAM immediately
                        del da_gridded
                    else:
                        pass # Swath was entirely outside the bounding box

                except Exception as e:
                    reports.append(self._create_error_report(acq_time, polygon, bbox, output_dir, f"Processing failed: {e}", var_name))

        return reports

    # ---------------------------------------------------------
    # HELPER METHODS
    # ---------------------------------------------------------
    def _create_master_grid(self, bbox, resolution):
        """Defines the static EPSG:4326 grid for the entire region."""
        proj_dict = {"proj": "longlat", "datum": "WGS84"}
        return create_area_def(
            area_id="epsg4326_grid",
            projection=proj_dict,
            area_extent=[bbox[0], bbox[1], bbox[2], bbox[3]], 
            resolution=resolution  
        )

    def _process_swath_to_grid(self, cache_dir, sen3_dir, nc_filename, var_name, bbox, area_def):
        """Loads a single swath, trims distorted edges, and projects it onto the master grid."""
        nc_path = cache_dir / sen3_dir / f"{nc_filename}.nc"
        geo_file = cache_dir / sen3_dir / "geo_coordinates.nc"
        
        if not nc_path.exists() or not geo_file.exists():
            raise FileNotFoundError(f"Missing NetCDF or Geo file in {sen3_dir.name}")

        # Load data and coordinates
        with xr.open_dataset(nc_path) as ds, xr.open_dataset(geo_file) as geo_ds:
            da = ds[var_name].load() 
            lons = geo_ds['longitude'].values
            lats = geo_ds['latitude'].values

        # 1. Trim Edges (15%) to remove atmospheric bowtie distortion
        trim = int(lons.shape[1] * 0.15)
        if trim > 0:
            lons = lons[:, trim:-trim]
            lats = lats[:, trim:-trim]
            da = da.isel({da.dims[1]: slice(trim, -trim)})

        # 2. Check Valid Pixels (Are we actually inside the box?)
        valid = (lats >= bbox[1]) & (lats <= bbox[3]) & (lons >= bbox[0]) & (lons <= bbox[2])
        if not valid.any():
            return None 

        # ---------------------------------------------------------
        # 3. DIRECT MASKING: Filter Good Pixels using provided WQSF bits
        # ---------------------------------------------------------
        if var_name == "WQSF":
            wqsf_vals = da.values.astype(np.uint64)
            
            # Derived explicitly from the provided NetCDF flag_masks and flag_meanings
            WATER_MASK = 2  
            
            BAD_MASK = (
                1 |         # INVALID
                4 |         # LAND
                8 |         # CLOUD
                8388608 |   # CLOUD_AMBIGUOUS
                16777216 |  # CLOUD_MARGIN
                16 |        # SNOW_ICE
                32 |        # INLAND_WATER
                64 |        # COASTLINE
                128 |       # TIDAL
                512 |       # SUSPECT
                1024 |      # HISOLZEN
                2048 |      # SATURATED
                8192 |      # HIGHGLINT
                65536 |     # WV_FAIL
                131072 |    # PAR_FAIL
                262144 |    # AC_FAIL
                524288 |    # OC4ME_FAIL
                1048576 |   # OCNN_FAIL
                2097152     # KDM_FAIL
            )
            
            # Determine good pixels: Must be water, must NOT have bad flags
            is_water = (wqsf_vals & WATER_MASK) > 0
            is_bad = (wqsf_vals & BAD_MASK) > 0
            is_clean = is_water & ~is_bad
            
            # Convert to float32: 1.0 is a good pixel, 0.0 is a bad pixel
            da_processed = np.where(is_clean, 1.0, 0.0).astype(np.float32)
            fill_val = 0.0
        else:
            da_processed = da.values.astype(np.float32)
            fill_val = np.nan

        # 4. Resample onto the Master Canvas
        swath_def = SwathDefinition(lons=lons, lats=lats)
        resampled_data = resample_nearest(
            swath_def, 
            da_processed, 
            area_def, 
            radius_of_influence=4000, 
            fill_value=fill_val
        )

        # 5. Construct valid 3D DataArray
        target_lon, target_lat = area_def.get_proj_coords()
        da_gridded = xr.DataArray(
            [resampled_data], 
            dims=["band", "y", "x"],
            coords={"band": [1], "x": target_lon[0, :], "y": target_lat[:, 0]}
        )
        
        da_gridded.rio.write_crs("EPSG:4326", inplace=True)
        da_gridded.rio.write_nodata(fill_val, inplace=True)
        
        return da_gridded

    def _create_success_report(self, var_name, time, poly, bbox, path, note):
        return ItemDownloadReport(data_source="Sentinel-3", 
                                  variable_name=var_name, 
                                  acquisition_time=time, 
                                  polygon=poly, 
                                  bbox=bbox, 
                                  path=path, 
                                  download_successful=True, 
                                  error=None, 
                                  metadata={"note": note})

    def _create_error_report(self, time, poly, bbox, path, error, var_name="Synergy_Product"):
        return ItemDownloadReport(data_source="Sentinel-3", 
                                  variable_name=var_name, 
                                  acquisition_time=time, 
                                  polygon=poly, 
                                  bbox=bbox, 
                                  path=path, 
                                  download_successful=False, 
                                  error=error)

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