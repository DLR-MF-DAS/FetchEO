import os
import json
import uuid
import duckdb


def connect_to_db(db_path: str = "db.duckdb"):
    """Connect to DuckDB database and return the connection object."""
    return duckdb.connect(db_path)


def initialise_tables(db_connection):
    """Initialise database tables if they don't exist."""
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS locations (
            location_id TEXT PRIMARY KEY,
            first_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            location_nickname TEXT UNIQUE,
            geojson JSON
        )
    """)
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS geotiff_catalog (
            catalog_id TEXT PRIMARY KEY,
            first_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            location_id TEXT,
            location_nickname TEXT,
            data_source TEXT,
            variable_name TEXT,  
            frequency TEXT,
            acquisition_time TIMESTAMP,
            year INT,
            month INT,
            root_dir TEXT,
            file_name TEXT,
            file_size_bytes INT,
            download_status TEXT,
            error_message TEXT,
            metadata JSON,
            CONSTRAINT geotiff_unique UNIQUE (location_id, data_source, variable_name, frequency, acquisition_time, year, month)
        )
    """)
    db_connection.execute("""
        CREATE TABLE IF NOT EXISTS product_catalog (
            product_id TEXT PRIMARY KEY,
            first_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            location_id TEXT,
            location_nickname TEXT,
            processor TEXT,
            product_name TEXT,
            acquisition_time TIMESTAMP,
            year INT,
            month INT,
            root_dir TEXT,
            file_name TEXT,
            file_size_bytes INT,
            process_status TEXT,
            error_message TEXT,
            inputs JSON,
            metadata JSON,
            CONSTRAINT product_unique UNIQUE (location_id, processor, product_name, acquisition_time)
        )
    """)


def fetch_or_create_location_id(db_connection, location_nickname, geojson):
    """
    Fetch a location ID or insert if new.
    This allows users to reuse geojsons with different location names for multiple experiments.
    """
    # Store canonical JSON string (sorted keys)
    geojson_str = json.dumps(geojson, sort_keys=True)
    row = db_connection.execute(
        "SELECT location_id, geojson FROM locations WHERE location_nickname = ?",
        [location_nickname]
    ).fetchone()

    # If found, verify geojson matches (compare parsed objects)
    if row:
        existing_id, existing_geojson = row
        try:
            existing_geojson_obj = json.loads(existing_geojson)
        except Exception:
            # If for some reason the DB value is not valid JSON, fallback to string compare
            existing_geojson_obj = existing_geojson
        if existing_geojson_obj == geojson:
            return existing_id
        else:
            raise ValueError(f"Location nickname '{location_nickname}' already exists with a different geojson.")
    else:
        new_location_id = str(uuid.uuid4())
        db_connection.execute(
            "INSERT INTO locations (location_id, location_nickname, geojson) VALUES (?, ?, ?)",
            [new_location_id, location_nickname, geojson_str]
        )
        return new_location_id


def upsert_file(
        db_connection,
        location_id,
        location_nickname,
        data_source,
        variable_name,
        frequency,
        acquisition_time,
        year,
        month,
        root_dir,
        file_name,
        file_size_bytes,
        download_status,
        error_message,
        metadata=None
    ):
    new_catalog_id = str(uuid.uuid4())
    db_connection.execute("""
        INSERT INTO geotiff_catalog (
            catalog_id,
            location_id,
            location_nickname,
            data_source,
            variable_name,  
            frequency,
            acquisition_time,
            year,
            month,
            root_dir,
            file_name,
            file_size_bytes,
            download_status,
            error_message,
            metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_id, data_source, variable_name, frequency, acquisition_time, year, month) DO UPDATE SET
            root_dir=excluded.root_dir,
            file_name=excluded.file_name,
            file_size_bytes=excluded.file_size_bytes,
            download_status=excluded.download_status,
            error_message=excluded.error_message,
            metadata=excluded.metadata,
            last_updated=now()
    """,
    [new_catalog_id,
     location_id,
     location_nickname,
     data_source,
     variable_name,
     frequency,
     acquisition_time,
     year,
     month,
     root_dir,
     file_name,
     file_size_bytes,
     download_status,
     error_message,
     metadata])


def upsert_product(
        db_connection,
        location_id,
        location_nickname,
        processor,
        product_name,
        acquisition_time,
        year,
        month,
        root_dir,
        file_name,
        file_size_bytes,
        process_status,
        error_message,
        inputs=None,
        metadata=None
    ):
    """Record a derived product built by a processor.

    The product catalogue is the download catalogue's counterpart: same idea,
    but keyed on the processor that produced the file instead of the data source
    it came from, and carrying the list of input files for provenance.
    """
    new_product_id = str(uuid.uuid4())
    db_connection.execute("""
        INSERT INTO product_catalog (
            product_id,
            location_id,
            location_nickname,
            processor,
            product_name,
            acquisition_time,
            year,
            month,
            root_dir,
            file_name,
            file_size_bytes,
            process_status,
            error_message,
            inputs,
            metadata
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(location_id, processor, product_name, acquisition_time) DO UPDATE SET
            root_dir=excluded.root_dir,
            file_name=excluded.file_name,
            file_size_bytes=excluded.file_size_bytes,
            process_status=excluded.process_status,
            error_message=excluded.error_message,
            inputs=excluded.inputs,
            metadata=excluded.metadata,
            last_updated=now()
    """,
    [new_product_id,
     location_id,
     location_nickname,
     processor,
     product_name,
     acquisition_time,
     year,
     month,
     root_dir,
     file_name,
     file_size_bytes,
     process_status,
     error_message,
     inputs,
     metadata])


def query_files(
        db_connection,
        location_id=None,
        data_sources=None,
        variable_names=None,
        time_frame=None,
        successful_only=True
    ):
    """Look up catalogued downloads, for processors that need their inputs.

    This is what lets a processor reach files fetched during an **earlier** run:
    the catalogue, not the current run's reports, is the source of truth.

    Args:
        db_connection: Open DuckDB connection.
        location_id: Restrict to one location.
        data_sources: Iterable of data-source names to keep.
        variable_names: Iterable of variable names to keep.
        time_frame: ``(start, end)`` bounds on ``acquisition_time`` (inclusive).
        successful_only: Drop rows whose download failed.

    Returns:
        list[dict]: One dict per file with ``data_source``, ``variable_name``,
        ``acquisition_time``, ``path`` and ``metadata``.
    """
    clauses = []
    params = []
    if location_id is not None:
        clauses.append("location_id = ?")
        params.append(location_id)
    if data_sources:
        data_sources = list(data_sources)
        clauses.append(f"data_source IN ({', '.join('?' * len(data_sources))})")
        params.extend(data_sources)
    if variable_names:
        variable_names = list(variable_names)
        clauses.append(f"variable_name IN ({', '.join('?' * len(variable_names))})")
        params.extend(variable_names)
    if time_frame:
        clauses.append("(acquisition_time IS NULL OR acquisition_time BETWEEN ? AND ?)")
        params.extend([time_frame[0], time_frame[1]])
    if successful_only:
        clauses.append("download_status = 'success'")

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = db_connection.execute(f"""
        SELECT data_source, variable_name, acquisition_time, root_dir, file_name, metadata
        FROM geotiff_catalog
        {where}
        ORDER BY acquisition_time, file_name
    """, params).fetchall()

    results = []
    for data_source, variable_name, acquisition_time, root_dir, file_name, metadata in rows:
        if not file_name:
            continue
        results.append({
            "data_source": data_source,
            "variable_name": variable_name,
            "acquisition_time": acquisition_time,
            "path": os.path.join(root_dir or "", file_name),
            "metadata": json.loads(metadata) if isinstance(metadata, str) else metadata,
        })
    return results
