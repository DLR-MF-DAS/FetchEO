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

        # Loop through all images and download them
        for item in tqdm(search_results, desc="Downloading Sentinel-3 images", disable=not show_progress):
            acq_time_raw = item.properties.get("datetime")
            if not acq_time_raw:
                raise ValueError("Missing 'datetime' in product properties")
            acq_time = datetime.datetime.fromisoformat(acq_time_raw.replace("Z", "+00:00"))

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
                    group_key = (acq_time, variable_name)
                    basename = f"S3_{acq_time.strftime('%Y%m%d_%H%M%S')}_{variable_name}"
                    expected_tif = self._get_filepaths(output_dir, basename)[0]
                    grouped_entry = grouped_tiles.setdefault(
                        group_key,
                        {
                            "basename": basename,
                            "expected_tif": expected_tif,
                            "arrays": [],
                            "errors": [],
                            "metadata": [],
                        },
                    )
                    grouped_entry["metadata"].append(item.properties)

                    # Load netcdf using xarray
                    try:
                        da = self._load_netcdf_as_array(cache_dir=cache_dir,
                                                        sen3_dir=sen3_dir,
                                                        nc_filename=nc_filename,
                                                        variable_name=variable_name)

                        clipped_array = self._clip_array_to_grid(da=da,
                                                                 sen3_dir=sen3_dir,
                                                                 polygon=polygon)
                        grouped_entry["arrays"].append(clipped_array)

                    except Exception as e:
                        grouped_entry["errors"].append(str(e))

            except Exception as e:
                for variable_name, _ in self.variables_to_files_map.items():
                    group_key = (acq_time, variable_name)
                    basename = f"S3_{acq_time.strftime('%Y%m%d_%H%M%S')}_{variable_name}"
                    expected_tif = self._get_filepaths(output_dir, basename)[0]
                    grouped_entry = grouped_tiles.setdefault(
                        group_key,
                        {
                            "basename": basename,
                            "expected_tif": expected_tif,
                            "arrays": [],
                            "errors": [],
                            "metadata": [],
                        },
                    )
                    grouped_entry["errors"].append(str(e))

        reports = []
        for (acq_time, variable_name), grouped_entry in grouped_tiles.items():
            basename = grouped_entry["basename"]
            expected_tif = grouped_entry["expected_tif"]
            tile_arrays = grouped_entry["arrays"]
            tile_errors = grouped_entry["errors"]

            if tile_arrays:
                try:
                    self._merge_and_save_arrays_as_cog(
                        arrays=tile_arrays,
                        output_path=expected_tif,
                    )
                    success = self._validate_geotiff(output_dir, basename).get(expected_tif, False)
                    error_message = None
                    if tile_errors:
                        error_message = "Some tiles failed before mosaicking: " + "; ".join(tile_errors)
                    elif not success:
                        error_message = "GeoTIFF validation failed."

                    reports.append(
                        ItemDownloadReport(
                            data_source="Sentinel-3",
                            variable_name=variable_name,
                            acquisition_time=acq_time,
                            polygon=polygon,
                            bbox=bbox,
                            path=expected_tif,
                            download_successful=success and not tile_errors,
                            error=error_message,
                            metadata=self._build_group_metadata(grouped_entry["metadata"]),
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
                            metadata=self._build_group_metadata(grouped_entry["metadata"]),
                        )
                    )
            else:
                error_message = "; ".join(tile_errors) if tile_errors else "No tiles were available for mosaicking."
                reports.append(
                    ItemDownloadReport(
                        data_source="Sentinel-3",
                        variable_name=variable_name,
                        acquisition_time=acq_time,
                        polygon=polygon,
                        bbox=bbox,
                        path=expected_tif,
                        download_successful=False,
                        error=error_message,
                        metadata=self._build_group_metadata(grouped_entry["metadata"]),
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


    def _merge_and_save_arrays_as_cog(self, arrays: list[xr.DataArray], output_path: Path) -> xr.DataArray:
        if len(arrays) == 1:
            merged_array = arrays[0]
        else:
            merged_array = merge_arrays(arrays, nodata=np.nan)

        merged_array.rio.write_crs("EPSG:4326", inplace=True)
        merged_array.rio.to_raster(
            output_path,
            driver="COG",
            compress="DEFLATE",
            tiled=True,
        )
        return merged_array


    def _clip_array_to_grid(self, da, sen3_dir, polygon):
        """
        Pipeline for degree-based mapping (EPSG:4326): 
        1. Crops to bounding box to save RAM.
        2. Resamples to a rigid 0.01 degree latitude/longitude grid.
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

        dim_y, dim_x = da.dims[0], da.dims[1]

        # ---------------------------------------------------------
        # STEP 1: ROUGH CROP BY LAT/LON (Save Memory)
        # ---------------------------------------------------------
        valid_pixels = (lats >= min_lat) & (lats <= max_lat) & (lons >= min_lon) & (lons <= max_lon)
        
        if not valid_pixels.any():
            raise ValueError(f"No data found in bounds. Variable: {da.name}")

        valid_rows = np.any(valid_pixels, axis=1)
        valid_cols = np.any(valid_pixels, axis=0)
        
        rmin, rmax = np.where(valid_rows)[0][[0, -1]]
        cmin, cmax = np.where(valid_cols)[0][[0, -1]]

        # Add a 10-pixel buffer
        rmin, rmax = max(0, rmin - 10), min(lats.shape[0], rmax + 10)
        cmin, cmax = max(0, cmin - 10), min(lats.shape[1], cmax + 10)

        lats_cropped = lats[rmin:rmax, cmin:cmax]
        lons_cropped = lons[rmin:rmax, cmin:cmax]
        da_cropped = da.isel({dim_y: slice(rmin, rmax), dim_x: slice(cmin, cmax)})

        # ---------------------------------------------------------
        # STEP 2: DEFINE THE EPSG:4326 GRID
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
        # STEP 3: RESAMPLE TO THE DEGREE GRID
        # ---------------------------------------------------------
        resampled_data = resample_nearest(
            swath_def, 
            da_cropped.values, 
            area_def, 
            radius_of_influence=4000, # Max search distance in meters (still required in meters)
            fill_value=np.nan
        )

        # ---------------------------------------------------------
        # STEP 4: BUILD GRIDDED ARRAY
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
        return da_gridded