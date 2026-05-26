import os
import datetime
from tqdm import tqdm
from pathlib import Path
from typing import Optional, List

import rioxarray
import numpy as np
import xarray as xr
from eodag import EODataAccessGateway
from pyresample.kd_tree import resample_nearest
from pyresample.geometry import SwathDefinition, create_area_def

from fetcheo.downloaders._downloader import BaseDownloader, ItemDownloadReport


class Sentinel3SynergyDownloader(BaseDownloader):
    def __init__(self, username: Optional[str] = None, password: Optional[str] = None, variables_to_files_map: Optional[List[str]] = None):
        """
        Streamlined downloader for Sentinel-3 Synergy (S3_SY_2_SYN) products.
        """
        super().__init__()
        self.variables_to_files_map = variables_to_files_map
        if variables_to_files_map is None:
          self.variables_to_files_map = self._get_all_variables_to_files_map()

        # 1. SET CREDENTIALS FIRST so EODAG sees them when it boots up
        if username and password:
            os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__USERNAME"] = username
            os.environ["EODAG__COP_DATASPACE__AUTH__CREDENTIALS__PASSWORD"] = password

        # 2. NOW initialize EODAG
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

        # Make output directory
        output_dir.mkdir(parents=True, exist_ok=True)
        if cache_dir:
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

        # Loop through all images and download them
        for item in tqdm(search_results, desc="Downloading Sentinel-3 images", disable=not show_progress):
            acq_time_str = item.properties.get("datetime", "").split(".")[0]
            self.acq_time_str = item.properties
            acq_time = datetime.datetime.strptime(acq_time_str, "%Y-%m-%dT%H:%M:%S")

            # Download current image file
            try:
                # EODAG downloads and extracts, returning the path to the extracted product directory.
                product_dir_path = self.dag.download(item, extract=True)
                if not product_dir_path:
                    raise RuntimeError("EODAG download returned an empty path after extraction.")

                # Get the directory to the downloaded sen3 data
                sen3_dir = Path(product_dir_path)
                if not sen3_dir.exists() or not sen3_dir.is_dir():
                    raise FileNotFoundError(f"Expected extracted directory {sen3_dir} does not exist or is not a directory.")

                # Process every requested variable
                for variable_name, nc_filename in self.variables_to_files_map.items():
                    basename = f"S3_{acq_time.strftime('%Y%m%d_%H%M%S')}_{variable_name}"
                    expected_tif = self._get_filepaths(output_dir, basename)[0]

                    # Load netcdf using xarray
                    try:
                        da = self._load_netcdf_as_array(cache_dir=cache_dir,
                                                        sen3_dir=sen3_dir,
                                                        nc_filename=nc_filename,
                                                        variable_name=variable_name)

                        da_coords = self._load_netcdf_as_array(cache_dir=cache_dir,
                                                        sen3_dir=sen3_dir,
                                                        nc_filename=nc_filename,
                                                        variable_name=variable_name)

                        # Clip data array to region and save as geotiff
                        self._clip_and_save_array_as_cog(da=da,
                                                         sen3_dir=sen3_dir,
                                                         polygon=polygon,
                                                         output_path=output_dir / f"{basename}.tif")

                        # Validate the file is loadable
                        success = self._validate_geotiff(output_dir, basename).get(expected_tif, False)
                        reports.append(
                            ItemDownloadReport(
                                data_source="Sentinel-3",
                                variable_name=variable_name,
                                acquisition_time=acq_time,
                                polygon=polygon,
                                bbox=bbox,
                                path=expected_tif,
                                download_successful=success,
                                error=None if success else "GeoTIFF validation failed.",
                                metadata=item.properties
                            )
                        )

                    except Exception as e:
                      reports.append(
                            ItemDownloadReport(
                                data_source="Sentinel-3",
                                variable_name=variable_name,
                                acquisition_time=acq_time,
                                polygon=polygon,
                                bbox=bbox,
                                path=expected_tif,
                                download_successful=False,
                                error=str(e),
                                metadata=item.properties
                            )
                        )

            except Exception as e:
                for variable_name, _ in self.variables_to_files_map.items():
                    reports.append(
                        ItemDownloadReport(
                            data_source="Sentinel-3",
                            variable_name=variable_name,
                            acquisition_time=acq_time,
                            polygon=polygon,
                            bbox=bbox,
                            path=output_dir,
                            download_successful=False,
                            error=str(e)
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
        array = xr.open_dataset(nc_path)
        return array[variable_name]


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