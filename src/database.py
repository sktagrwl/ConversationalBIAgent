import os
import re
import glob
import collections
import duckdb
import pandas as pd
from src.config import DATA_DIR, DB_PATH, MAX_ROWS


QueryResult = collections.namedtuple("QueryResult", ["df", "truncated"])


def _load_derived_table_sql(data_dir: str) -> list[tuple[str, str]]:
    """
    Load derived table SQL definitions from data/derived_tables.sql.
    Returns list of (table_name, sql) tuples in file order.
    Returns [] if the file does not exist.

    File format — each statement preceded by a sentinel comment:
        -- TABLE: <table_name>
        CREATE TABLE IF NOT EXISTS <table_name> AS ...;
    """
    sql_file = os.path.join(data_dir, "derived_tables.sql")
    if not os.path.exists(sql_file):
        return []
    with open(sql_file) as f:
        content = f.read()
    results = []
    parts = re.split(r'--\s*TABLE:\s*(\w+)', content)
    # parts[0] is preamble/empty; then alternates: table_name, sql_body
    for i in range(1, len(parts) - 1, 2):
        table_name = parts[i].strip()
        sql_body = parts[i + 1].strip().rstrip(';')
        if table_name and sql_body:
            results.append((table_name, sql_body))
    return results


# Names of every derived/pre-aggregated table defined in data/derived_tables.sql.
# Exported so agent._schema_to_text() can classify tables without extra DB queries.
# Empty frozenset when no derived_tables.sql exists (fresh clone / new dataset).
DERIVED_TABLES: frozenset[str] = frozenset(
    name for name, _ in _load_derived_table_sql(DATA_DIR)
)


def _build_derived_tables(con: duckdb.DuckDBPyConnection) -> None:
    """
    Build derived tables from raw tables using definitions in data/derived_tables.sql.
    Uses CREATE TABLE IF NOT EXISTS — safe to call on every startup; no-op when tables exist.
    If no derived_tables.sql exists, skips silently — app works on raw tables.
    Order is significant: each statement may depend on the previous one.
    """
    derived_sql = _load_derived_table_sql(DATA_DIR)
    if not derived_sql:
        print("[database] No derived_tables.sql found — skipping derived table build.")
        return
    for table_name, sql in derived_sql:
        try:
            print(f"[database] Ensuring derived table '{table_name}' exists ...")
            con.execute(sql)
        except duckdb.Error as e:
            print(f"[database] Skipping derived table '{table_name}' (missing base tables or columns?): {e}")
            break
    print("[database] Derived tables build finished.")


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Connect to (or create) a persistent DuckDB file at DB_PATH.

    Phase 1 — CSV materialization (first run only, skipped thereafter):
      Reads CSVs from DATA_DIR, creates native DuckDB tables. Auto-converts to Parquet if needed.

    Phase 2 — Derived table build (first run only, no-op thereafter):
      Builds product_metrics, order_metrics, user_metrics, department_metrics
      from the raw tables. These are preferred by the agent over raw tables.
    """
    con = duckdb.connect(database=DB_PATH)

    # Phase 1: Materialize raw source files. Auto-convert loose CSVs to Parquet.
    existing_tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    
    parquet_files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    parquet_basenames = {os.path.basename(p) for p in parquet_files}
    
    for csv_file in glob.glob(os.path.join(DATA_DIR, "*.csv")):
        name = os.path.splitext(os.path.basename(csv_file))[0]
        expected_parquet = f"{name}.parquet"
        
        if expected_parquet not in parquet_basenames:
            parquet_path = os.path.join(DATA_DIR, expected_parquet)
            print(f"[database] Auto-converting {os.path.basename(csv_file)} to Parquet ...")
            try:
                con.execute(
                    f"COPY (SELECT * FROM read_csv_auto('{csv_file.replace(chr(39), chr(39)+chr(39))}')) TO '{parquet_path.replace(chr(39), chr(39)+chr(39))}' "
                    f"(FORMAT PARQUET, COMPRESSION ZSTD)"
                )
                parquet_files.append(parquet_path)
                parquet_basenames.add(expected_parquet)
            except duckdb.Error as e:
                print(f"[database] Warning: Failed to convert {os.path.basename(csv_file)} to Parquet: {e}")

    # Materialize from Parquet files
    for path in parquet_files:
        table_name = os.path.splitext(os.path.basename(path))[0]
        if table_name in existing_tables:
            continue
        print(f"[database] Materializing '{table_name}' from {os.path.basename(path)} ...")
        con.execute(f"CREATE TABLE \"{table_name}\" AS SELECT * FROM read_parquet('{path}')")
        print(f"[database] Done: '{table_name}'")

    # Phase 2: Build derived tables (no-op if already up to date, skipped if no derived_tables.sql)
    _build_derived_tables(con)

    return con


def get_schema(con: duckdb.DuckDBPyConnection) -> dict[str, dict]:
    """
    Return schema as:
      { table_name: { "columns": [{"column": str, "type": str}, ...], "row_count": int } }
    """
    tables = con.execute("SHOW TABLES").fetchall()
    schema = {}
    for (table,) in tables:
        cols = con.execute(f"DESCRIBE {table}").fetchall()
        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        schema[table] = {
            "columns": [{"column": col[0], "type": col[1]} for col in cols],
            "row_count": row_count,
        }
    return schema


# Matches SELECT * FROM <table_name> but NOT SELECT * FROM (<subquery>).
# \w after FROM requires a table name character, so SELECT * FROM (... is excluded.
_BARE_SELECT_STAR_RE = re.compile(r'\bSELECT\s+\*\s+FROM\s+\w', re.IGNORECASE)


def validate_sql(sql: str) -> tuple[bool, str | None]:
    """
    Validate LLM-generated SQL before execution.
    Rejects bare SELECT * at the top level of a query.
    Allows SELECT * inside subqueries (paren depth > 0) and SELECT COUNT(*).

    Returns (is_valid, error_message). error_message is None when is_valid is True.
    """
    stripped = sql.strip()
    for match in _BARE_SELECT_STAR_RE.finditer(stripped):
        pos = match.start()
        depth = stripped.count('(', 0, pos) - stripped.count(')', 0, pos)
        if depth > 0:
            continue  # inside a subquery — acceptable
        return False, (
            "Query rejected: bare SELECT * is not allowed. "
            "Please select specific columns or use aggregation (GROUP BY + COUNT/SUM/AVG). "
            "Add a WHERE clause with LIMIT if you need a sample."
        )
    return True, None


def run_query(con: duckdb.DuckDBPyConnection, sql: str) -> QueryResult:
    """
    Validate and execute SQL, returning QueryResult(df, truncated).

    Validation rejects bare SELECT * at the query top level (Rule 1).
    Results are capped at MAX_ROWS rows; truncated=True signals the caller to warn the user.
    SQL is subquery-wrapped (not string-appended) to handle CTEs and existing LIMITs correctly.
    """
    is_valid, error = validate_sql(sql)
    if not is_valid:
        raise ValueError(error)

    capped_sql = f"SELECT * FROM ({sql}) AS __q LIMIT {MAX_ROWS + 1}"
    raw_df = con.execute(capped_sql).df()

    if len(raw_df) > MAX_ROWS:
        return QueryResult(df=raw_df.iloc[:MAX_ROWS], truncated=True)
    return QueryResult(df=raw_df, truncated=False)
