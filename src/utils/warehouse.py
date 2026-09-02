"""
Redshift stand-in for local dev: DuckDB reading Parquet directly from S3
(MiniStack), the same access pattern as a Redshift COPY / Spectrum query.

WAREHOUSE_BACKEND=duckdb (default, free, local) is the only backend this
repo implements — the real DDL for an actual Redshift deployment lives in
sql/redshift/schema.sql for reference, not for execution here.
"""

import os

import duckdb


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4581")
    host = endpoint.replace("http://", "").replace("https://", "")
    con.execute(f"""
        SET s3_endpoint='{host}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='{os.environ.get("AWS_ACCESS_KEY_ID", "test")}';
        SET s3_secret_access_key='{os.environ.get("AWS_SECRET_ACCESS_KEY", "test")}';
    """)
    return con


def read_parquet(con: duckdb.DuckDBPyConnection, s3_glob: str, view_name: str) -> None:
    """Register an S3 Parquet glob (e.g. 's3://txn-curated/**/*.parquet') as a queryable view."""
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{s3_glob}')")
