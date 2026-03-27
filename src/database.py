import os
import glob
import duckdb
import pandas as pd
from src.config import DATA_DIR


def get_connection() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with all CSVs registered as tables."""
    con = duckdb.connect(database=":memory:")
    csv_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))
    for path in csv_files:
        table_name = os.path.splitext(os.path.basename(path))[0]
        con.execute(f"CREATE VIEW {table_name} AS SELECT * FROM read_csv_auto('{path}')")
    return con


def get_schema(con: duckdb.DuckDBPyConnection) -> dict[str, list[dict]]:
    """Return schema as {table_name: [{column, type}, ...]}."""
    tables = con.execute("SHOW TABLES").fetchall()
    schema = {}
    for (table,) in tables:
        cols = con.execute(f"DESCRIBE {table}").fetchall()
        schema[table] = [{"column": col[0], "type": col[1]} for col in cols]
    return schema


def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> pd.DataFrame:
    """Execute SQL and return a DataFrame."""
    return con.execute(sql).df()
