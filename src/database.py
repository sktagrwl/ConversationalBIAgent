import os
import re
import glob
import collections
import duckdb
import pandas as pd
from src.config import DATA_DIR, DB_PATH, MAX_ROWS


QueryResult = collections.namedtuple("QueryResult", ["df", "truncated"])

# Names of every derived/pre-aggregated table this module creates.
# Exported so agent._schema_to_text() can classify tables without extra DB queries.
DERIVED_TABLES: frozenset[str] = frozenset({
    "order_products",
    "fact_orders",
    "product_metrics",
    "aisle_metrics",
    "order_metrics",
    "user_metrics",
    "department_metrics",
})

# Build order:
#   1. order_products     — unified prior+train product rows (test excluded: no basket data)
#   2. fact_orders        — full hierarchy denormalized (depends on order_products + lookups)
#   3. aisle_metrics      — depends on fact_orders
#   4. product_metrics    — depends on order_products
#   5. order_metrics      — depends on order_products
#   6. user_metrics       — depends on order_metrics
#   7. department_metrics — depends on fact_orders (fixed: correct reorder_rate formula)
_DERIVED_TABLE_SQL: list[tuple[str, str]] = [
    (
        "order_products",
        """
        CREATE TABLE IF NOT EXISTS order_products AS
        SELECT order_id, product_id, add_to_cart_order, reordered FROM order_products__prior
        UNION ALL
        SELECT order_id, product_id, add_to_cart_order, reordered FROM order_products__train
        """,
    ),
    (
        "fact_orders",
        """
        CREATE TABLE IF NOT EXISTS fact_orders AS
        SELECT
            op.order_id,
            op.product_id,
            p.product_name,
            a.aisle_id,
            a.aisle,
            d.department_id,
            d.department,
            op.reordered,
            op.add_to_cart_order
        FROM order_products op
        JOIN products p    ON op.product_id   = p.product_id
        JOIN aisles a      ON p.aisle_id      = a.aisle_id
        JOIN departments d ON p.department_id = d.department_id
        """,
    ),
    (
        "aisle_metrics",
        """
        CREATE TABLE IF NOT EXISTS aisle_metrics AS
        SELECT
            aisle_id,
            aisle,
            department_id,
            department,
            COUNT(DISTINCT product_id)                  AS product_count,
            COUNT(*)                                    AS total_orders,
            ROUND(SUM(reordered) * 1.0 / COUNT(*), 4)  AS reorder_rate,
            ROUND(AVG(add_to_cart_order), 2)            AS avg_cart_position
        FROM fact_orders
        GROUP BY aisle_id, aisle, department_id, department
        """,
    ),
    (
        "product_metrics",
        """
        CREATE TABLE IF NOT EXISTS product_metrics AS
        SELECT
            p.product_id,
            p.product_name,
            a.aisle_id,
            a.aisle,
            d.department_id,
            d.department,
            COUNT(op.order_id)                      AS total_orders,
            SUM(op.reordered)                       AS reorder_count,
            ROUND(AVG(op.reordered), 4)             AS reorder_rate,
            ROUND(AVG(op.add_to_cart_order), 2)     AS avg_cart_position
        FROM products p
        JOIN aisles a ON p.aisle_id = a.aisle_id
        JOIN departments d ON p.department_id = d.department_id
        JOIN order_products op ON p.product_id = op.product_id
        GROUP BY p.product_id, p.product_name, a.aisle_id, a.aisle, d.department_id, d.department
        """,
    ),
    (
        "order_metrics",
        """
        CREATE TABLE IF NOT EXISTS order_metrics AS
        SELECT
            o.order_id,
            o.user_id,
            o.order_number,
            o.order_dow,
            o.order_hour_of_day,
            o.days_since_prior_order,
            COUNT(op.product_id)        AS basket_size,
            SUM(op.reordered)           AS reordered_items,
            ROUND(AVG(op.reordered), 4) AS reorder_rate
        FROM orders o
        JOIN order_products op ON o.order_id = op.order_id
        WHERE o.eval_set IN ('prior', 'train')
        GROUP BY
            o.order_id, o.user_id, o.order_number,
            o.order_dow, o.order_hour_of_day, o.days_since_prior_order
        """,
    ),
    (
        "user_metrics",
        # Joins order_metrics (not raw 35M table) — derived-from-derived pattern.
        """
        CREATE TABLE IF NOT EXISTS user_metrics AS
        SELECT
            o.user_id,
            COUNT(DISTINCT o.order_id)              AS total_orders,
            ROUND(AVG(om.basket_size), 2)           AS avg_basket_size,
            ROUND(AVG(o.days_since_prior_order), 2) AS avg_days_between_orders,
            SUM(om.basket_size)                     AS total_items_purchased,
            ROUND(AVG(om.reorder_rate), 4)          AS avg_reorder_rate
        FROM orders o
        JOIN order_metrics om ON o.order_id = om.order_id
        WHERE o.eval_set IN ('prior', 'train')
        GROUP BY o.user_id
        """,
    ),
    (
        "department_metrics",
        # Built from fact_orders so reorder_rate is computed correctly at department level.
        # AVG(product_metrics.reorder_rate) was wrong — it averaged product-level rates.
        """
        CREATE TABLE IF NOT EXISTS department_metrics AS
        SELECT
            department_id,
            department,
            COUNT(DISTINCT product_id)                  AS product_count,
            COUNT(*)                                    AS total_orders,
            ROUND(SUM(reordered) * 1.0 / COUNT(*), 4)  AS avg_reorder_rate,
            ROUND(AVG(add_to_cart_order), 2)            AS avg_cart_position
        FROM fact_orders
        GROUP BY department_id, department
        """,
    ),
]


def _migrate_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    One-time migration: if fact_orders is missing, drop stale derived tables
    so _build_derived_tables() rebuilds them with updated SQL.
    Safe to call on every startup — no-op once fact_orders exists.
    """
    existing = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    if "fact_orders" not in existing:
        print("[database] Migrating schema: dropping stale derived tables to rebuild ...")
        for tbl in ("department_metrics", "aisle_metrics"):
            con.execute(f"DROP TABLE IF EXISTS {tbl}")


def _build_derived_tables(con: duckdb.DuckDBPyConnection) -> None:
    """
    Build derived tables from raw CSV tables.
    Uses CREATE TABLE IF NOT EXISTS — safe to call on every startup; no-op when tables exist.
    Order is significant: order_products first, then product_metrics/order_metrics, then user_metrics/department_metrics.
    """
    for table_name, sql in _DERIVED_TABLE_SQL:
        print(f"[database] Ensuring derived table '{table_name}' exists ...")
        con.execute(sql)
    print("[database] All derived tables ready.")


def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Connect to (or create) a persistent DuckDB file at DB_PATH.

    Phase 1 — CSV materialization (first run only, skipped thereafter):
      Reads CSVs from DATA_DIR, creates native DuckDB tables.

    Phase 2 — Derived table build (first run only, no-op thereafter):
      Builds product_metrics, order_metrics, user_metrics, department_metrics
      from the raw tables. These are preferred by the agent over raw tables.
    """
    con = duckdb.connect(database=DB_PATH)

    # Phase 1: Materialize raw source files (Parquet preferred, CSV fallback)
    existing_tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    source_files = glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    if not source_files:
        source_files = glob.glob(os.path.join(DATA_DIR, "*.csv"))

    for path in source_files:
        table_name = os.path.splitext(os.path.basename(path))[0]
        if table_name in existing_tables:
            continue
        ext = os.path.splitext(path)[1].lower()
        read_fn = f"read_parquet('{path}')" if ext == ".parquet" else f"read_csv_auto('{path}')"
        print(f"[database] Materializing '{table_name}' from {os.path.basename(path)} ...")
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {read_fn}")
        print(f"[database] Done: '{table_name}'")

    # Phase 2: Migrate schema (no-op if already up to date), then build derived tables
    _migrate_schema(con)
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
