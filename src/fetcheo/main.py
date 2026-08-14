import os
import json
import click
import logging
from pathlib import Path
from datetime import datetime
from fetcheo.loader import FetchEOLoader

# Set up basic logging config for CLI
logging.basicConfig(level=logging.INFO)

# Map available plugins (for validation/help).  Both lists include anything an
# external package registered through a fetcheo.downloaders / fetcheo.processors
# entry point, so `fetcheo --help` shows plugins FetchEO does not ship.
from fetcheo.registry import available_downloaders, available_processors
AVAILABLE_DOWNLOADERS = available_downloaders()
AVAILABLE_PROCESSORS = available_processors()


def validate_downloaders(downloaders):
    if not downloaders:
        return AVAILABLE_DOWNLOADERS
    invalid = [d for d in downloaders if d not in AVAILABLE_DOWNLOADERS]
    if invalid:
        raise click.ClickException(f"Unrecognised downloaders: {invalid}. Should be from {AVAILABLE_DOWNLOADERS}.")
    return list(downloaders)


def validate_processors(processors):
    """Processors are opt-in: nothing runs unless it is asked for by name."""
    if not processors:
        return []
    invalid = [p for p in processors if p not in AVAILABLE_PROCESSORS]
    if invalid:
        raise click.ClickException(f"Unrecognised processors: {invalid}. Should be from {AVAILABLE_PROCESSORS}.")
    return list(processors)


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
    if start_date_dt > end_date_dt:
        raise click.BadParameter("start_date must be on or before end_date.")

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
@click.option('--processor', '-p', multiple=True, help=f"Processor(s) to run on the downloaded files (none by default). Available: {AVAILABLE_PROCESSORS}")
@click.option('--geojson_path', required=True, help='Path to GeoJSON file')
@click.option('--start-date', type=str, required=True, help='Start date (YYYY-MM-DD)')
@click.option('--end-date', type=str, required=True, help='End date (YYYY-MM-DD)')
@click.option('--location-nickname', type=str, default=None, help='Location nickname (default: polygon file name or "location")')
@click.option('--output-dir', type=str, default='data', show_default=True, help='Output directory')
@click.option('--show-progress/--no-show-progress', default=True, show_default=True, help='Show progress bar')
@click.option('--db-path', type=str, default='fetcheo_data.duckdb', show_default=True, help='Path to DuckDB database file')
def main(downloader, processor, geojson_path, start_date, end_date, location_nickname, output_dir, show_progress, db_path):
    """Run FetchEOLoader from the command line."""
    #
    processors = validate_processors(processor)
    start_dt, end_dt, downloaders, geojson_dict, polygon, location_nickname, cache_dir = parse_and_validate_inputs(
        geojson_path=geojson_path,
        location_nickname=location_nickname,
        downloaders=downloader,
        start_date=start_date,
        end_date=end_date,
        output_folder=output_dir
    )

    # Set up loader with enabled downloaders and processors (default kwargs for now)
    downloader_config = {name: True for name in downloaders}
    processor_config = {name: True for name in processors}
    loader = FetchEOLoader(
        downloader_config=downloader_config,
        downloader_kwargs=None,
        db_path=Path(db_path),
        processor_config=processor_config,
        processor_kwargs=None
    )

    # Place output in a subfolder under the location nickname
    data_output_dir = str(Path(output_dir) / location_nickname)
    cache_dir = str(Path(output_dir) / location_nickname / "cache")

    # Download data and add to DB
    loader.fetch(
        polygon=polygon,
        time_frame=(start_dt, end_dt),
        location_nickname=location_nickname,
        output_dir=data_output_dir,
        cache_dir=cache_dir,
        show_progress=show_progress
    )


if __name__ == '__main__':
	main()

