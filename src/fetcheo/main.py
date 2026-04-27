import os
import json
import click
import logging
from pathlib import Path
from datetime import datetime
from fetcheo.loader import FetchEOLoader

# Set up basic logging config for CLI
logging.basicConfig(level=logging.INFO)

# Map available downloaders (for validation/help)
from fetcheo.loader import DOWNLOADER_DICT
AVAILABLE_DOWNLOADERS = list(DOWNLOADER_DICT.keys())


def validate_downloaders(downloaders):
	if not downloaders:
		return AVAILABLE_DOWNLOADERS
	invalid = [d for d in downloaders if d not in AVAILABLE_DOWNLOADERS]
	if invalid:
		raise click.ClickException(f"Unrecognised downloaders: {invalid}. Should be from {AVAILABLE_DOWNLOADERS}.")
	return list(downloaders)


def parse_and_validate_inputs(
        geojson_path: str,
        location_nickname: str,
        downloaders: tuple,
        start_date: str, 
        end_date: str,
        output_folder: str
    ):
    """
    Parse and validate input parameters.
    """
    # Convert start_date and end_date to datetime for comparison and downstream use
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    assert start_date_dt <= end_date_dt

    downloaders = validate_downloaders(downloaders)
	#logging.info(f"Downloaders to be used: {downloaders}")

    # Load GeoJSON file
    json_path = Path(geojson_path)
    with open(json_path, 'r') as f:
        geojson_dict = json.load(f)
    polygon = geojson_dict['features'][0]['geometry']
    
    # If no nickname provided, use the geojson filename (without extension)
    if not location_nickname:
        location_nickname = json_path.stem
    logging.info(f'Loaded {json_path}')

    # Create a cache directory for the temporary/reusable files
    cache_dir = Path(os.getcwd()) / f"{output_folder}/{location_nickname}/cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return start_date_dt, end_date_dt, downloaders, geojson_dict, polygon, location_nickname, cache_dir


@click.command()
@click.option('--downloader', '-d', multiple=True, help=f"Downloader(s) to use. Available: {AVAILABLE_DOWNLOADERS}")
@click.option('--polygon', required=True, help='Polygon as GeoJSON string or path to GeoJSON file')
@click.option('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
@click.option('--location-nickname', type=str, default=None, help='Location nickname (default: polygon file name or "location")')
@click.option('--output-dir', type=str, default='data', show_default=True, help='Output directory')
@click.option('--show-progress/--no-show-progress', default=True, show_default=True, help='Show progress bar')
@click.option('--db-path', type=str, default='fetcheo_data.duckdb', show_default=True, help='Path to DuckDB database file')
def main(downloader, polygon, start_date, end_date, location_nickname, output_dir, show_progress, db_path):
    """Run FetchEOLoader from the command line."""
    #
    start_dt, end_dt, downloaders, geojson_dict, polygon, location_nickname, cache_dir = parse_and_validate_inputs(
        geojson_path=polygon,
        location_nickname=location_nickname,
        downloaders=downloader,
        start_date=start_date,
        end_date=end_date,
        output_folder=output_dir
    )

    # Set up loader with enabled downloaders (default kwargs for now)
    downloader_config = {name: True for name in downloaders}
    loader = FetchEOLoader(
        downloader_config=downloader_config,
        downloader_kwargs=None,
        db_path=Path(db_path)
    )

    # Place output in a subfolder under the location nickname
    data_output_dir = str(Path(output_dir) / location_nickname)

    # Download data and add to DB
    loader.fetch(
        polygon=polygon,
        time_frame=(start_dt, end_dt),
        location_nickname=location_nickname,
        output_dir=data_output_dir,
        show_progress=show_progress
    )


if __name__ == '__main__':
	main()

