import datetime
import importlib
from pathlib import Path
from typing import Dict, List, Tuple, Any


from fetcheo.duckdb_helpers import (connect_to_db, 
                                    initialise_tables, 
                                    fetch_or_create_location_id, 
                                    upsert_file)


# Import the base downloader and report
DOWNLOADER_DICT = {
    'era5': 'fetcheo.downloaders.era5.ERA5Downloader',
    'modis_ndvi': 'fetcheo.downloaders.modis.MODISDownloader',
    'sen3_openeo': 'fetcheo.downloaders.sen3_openeo.Sen3WaterOpenEODownloader',
    # Add more as needed
}


class FetchEOLoader:
    def __init__(self, 
                 downloader_config: Dict[str, bool], 
                 downloader_kwargs: Dict[str, dict] = None, 
                 db_path: Path = Path('fetcheo_data.duckdb')):
        self.downloaders = {}
        self.downloader_kwargs = downloader_kwargs or {}
        self.db_path = db_path
        for name, enabled in downloader_config.items():
            if enabled and name in DOWNLOADER_DICT:
                module_path, class_name = DOWNLOADER_DICT[name].rsplit('.', 1)
                module = importlib.import_module(module_path)
                klass = getattr(module, class_name)
                kwargs = self.downloader_kwargs.get(name, {})
                self.downloaders[name] = klass(**kwargs)

    def fetch(self, 
              polygon: dict, 
              time_frame: Tuple, 
              location_nickname: str, 
              output_dir: str = "data", 
              show_progress: bool = True) -> List[Any]:
        """
        Fetches data using all enabled downloaders and adds results to DuckDB after each downloader.
        """
        #
        output_dir = Path(output_dir)
        
        # Connect to DB and ensure tables are initialised
        db_connection = connect_to_db(str(self.db_path))
        initialise_tables(db_connection)

        # Get or create location ID for this polygon and nickname
        location_id = fetch_or_create_location_id(db_connection, location_nickname, polygon)

        # Loop through downloaders and fetch data, adding to DB after each downloader
        all_reports = []
        for name, downloader in self.downloaders.items():
            # Fetch data for this downloader
            print(f"Running downloader: {name}")
            reports = downloader.fetch(polygon, 
                                       time_frame, 
                                       output_dir, 
                                       show_progress=show_progress)
            all_reports.extend(reports)

            # Add each report to DB after each downloader
            for r in reports:
                acq_time = getattr(r, 'acquisition_time', None)
                year = acq_time.year if acq_time else None
                month = acq_time.month if acq_time else None
                upsert_file(
                    db_connection=db_connection,
                    location_id=location_id,
                    location_nickname=location_nickname,
                    data_source=getattr(r, 'data_source', None),
                    variable_name=getattr(r, 'variable_name', None),
                    frequency=getattr(r, 'frequency', None) if hasattr(r, 'frequency') else None,
                    year=year,
                    month=month,
                    root_dir=str(Path(r.path).parent) if hasattr(r, 'path') else None,
                    file_name=str(Path(r.path).name) if hasattr(r, 'path') else None,
                    file_size_bytes=Path(r.path).stat().st_size if hasattr(r, 'path') and Path(r.path).exists() else None,
                    download_status="success" if getattr(r, 'download_successful', False) else "failed",
                    error_message=getattr(r, 'error', None),
                    metadata=getattr(r, 'metadata', None)
                )

        # Close DB connection and return all reports
        db_connection.close()
        return all_reports

# Example usage:
if __name__ == "__main__":
    # User specifies which downloaders to use
    config = {
        'era5': True,
        'ecira': False,
        'sen3_openeo': True,
        # ...
    }
    # Optionally, pass custom kwargs for each downloader
    kwargs = {
        'era5': {'variables_dict': {'t2m': '2m_temperature'}},
        # ...
    }
    loader = FetchEOLoader(config, kwargs)
    # Example polygon and time frame
    polygon = {"type": "Polygon", "coordinates": [[[0,0],[1,0],[1,1],[0,1],[0,0]]]}  # Replace with real geojson
    time_frame = (datetime.datetime(2020,1,1), datetime.datetime(2020,1,31))
    output_dir = Path('output')
    reports = loader.fetch(polygon, time_frame, location_nickname="example_location")
