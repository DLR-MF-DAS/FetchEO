import os
import tempfile
import json
import pytest
from pathlib import Path
from fetcheo.duckdb_helpers import connect_to_db, initialise_tables, fetch_or_create_location_id, upsert_file

def test_connect_and_initialise_tables():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        conn = connect_to_db(db_path)
        initialise_tables(conn)
        # Check that tables exist
        tables = set(row[0] for row in conn.execute("SHOW TABLES").fetchall())
        assert "locations" in tables
        assert "geotiff_catalog" in tables
        conn.close()

def test_fetch_or_create_location_id():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        conn = connect_to_db(db_path)
        initialise_tables(conn)
        geojson = {"type": "Point", "coordinates": [0, 0]}
        loc_id1 = fetch_or_create_location_id(conn, "loc1", geojson)
        assert isinstance(loc_id1, str)
        # Should return same id for same nickname and geojson
        loc_id2 = fetch_or_create_location_id(conn, "loc1", geojson)
        assert loc_id1 == loc_id2
        # Should raise for same nickname but different geojson
        with pytest.raises(ValueError):
            fetch_or_create_location_id(conn, "loc1", {"type": "Point", "coordinates": [1, 1]})
        # Should allow new nickname
        loc_id3 = fetch_or_create_location_id(conn, "loc2", geojson)
        assert loc_id3 != loc_id1
        conn.close()

def test_upsert_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.duckdb")
        conn = connect_to_db(db_path)
        initialise_tables(conn)
        geojson = {"type": "Point", "coordinates": [0, 0]}
        loc_id = fetch_or_create_location_id(conn, "loc1", geojson)
        upsert_file(
            db_connection=conn,
            location_id=loc_id,
            location_nickname="loc1",
            data_source="testsrc",
            variable_name="var",
            frequency="monthly",
            acquisition_time="2020-01-01T00:00:00",
            year=2020,
            month=1,
            root_dir="/tmp",
            file_name="file.tif",
            file_size_bytes=123,
            download_status="success",
            error_message=None,
            metadata=json.dumps({"meta": 1})
        )
        # Should insert a row
        rows = conn.execute("SELECT * FROM geotiff_catalog").fetchall()
        assert len(rows) == 1
        # Upsert with same unique key should update, not add
        upsert_file(
            db_connection=conn,
            location_id=loc_id,
            location_nickname="loc1",
            data_source="testsrc",
            variable_name="var",
            frequency="monthly",
            acquisition_time="2020-01-01T00:00:00",
            year=2020,
            month=1,
            root_dir="/tmp",
            file_name="file2.tif",
            file_size_bytes=456,
            download_status="updated",
            error_message="err",
            metadata=json.dumps({"meta": 2})
        )
        rows2 = conn.execute("SELECT * FROM geotiff_catalog").fetchall()
        assert len(rows2) == 1
        # Check updated fields
        row = rows2[0]
        assert row[12] == "file2.tif"  # file_name
        assert row[13] == 456  # file_size_bytes
        assert row[14] == "updated"  # download_status
        assert row[15] == "err"  # error_message
        conn.close()
