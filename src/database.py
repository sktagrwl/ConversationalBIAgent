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
    "user_order_timeline",
    "fact_orders",
    "product_metrics",
    "aisle_metrics",
    "order_metrics",
    "user_metrics",
    "department_metrics",
    "product_pairs",
})

# Build order:
#   1. order_products        — unified prior+train product rows (test excluded: no basket data)
#   2. user_order_timeline   — cumulative days_since_first_order per user (depends on orders)
#   3. fact_orders           — full hierarchy + temporal columns (depends on order_products + user_order_timeline)
#   4. aisle_metrics         — depends on fact_orders
#   5. product_metrics       — depends on order_products
#   6. order_metrics         — depends on order_products
#   7. user_metrics          — depends on order_metrics
#   8. department_metrics    — depends on fact_orders (fixed: correct reorder_rate formula)
#   9. product_pairs         — reordered product co-occurrence for top 200 products (depends on fact_orders + product_metrics)
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
        "user_order_timeline",
        """
        CREATE TABLE IF NOT EXISTS user_order_timeline AS
        SELECT
            order_id,
            user_id,
            order_number,
            order_dow,
            order_hour_of_day,
            SUM(COALESCE(days_since_prior_order, 0)) OVER (
                PARTITION BY user_id
                ORDER BY order_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS days_since_first_order
        FROM orders
        WHERE eval_set IN ('prior', 'train')
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
            op.add_to_cart_order,
            t.user_id,
            t.order_number,
            t.order_dow,
            t.order_hour_of_day,
            t.days_since_first_order
        FROM order_products op
        JOIN products p             ON op.product_id  = p.product_id
        JOIN aisles a               ON p.aisle_id     = a.aisle_id
        JOIN departments d          ON p.department_id = d.department_id
        JOIN user_order_timeline t  ON op.order_id    = t.order_id
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
    (
        "product_pairs",
        # Basket affinity: which reordered products are bought together most often.
        # Scoped to top 200 products by order volume to keep the self-join tractable.
        # a.product_id < b.product_id deduplicates pairs and removes self-pairs.
        """
        CREATE TABLE IF NOT EXISTS product_pairs AS
        WITH top_products AS (
            SELECT product_id
            FROM product_metrics
            ORDER BY total_orders DESC
            LIMIT 200
        ),
        reordered AS (
            SELECT order_id, product_id
            FROM fact_orders
            WHERE reordered = 1
              AND product_id IN (SELECT product_id FROM top_products)
        )
        SELECT
            a.product_id        AS product_1_id,
            b.product_id        AS product_2_id,
            pm1.product_name    AS product_1,
            pm2.product_name    AS product_2,
            COUNT(*)            AS co_occurrence_count
        FROM reordered a
        JOIN reordered b
            ON a.order_id = b.order_id
           AND a.product_id < b.product_id
        JOIN product_metrics pm1 ON a.product_id = pm1.product_id
        JOIN product_metrics pm2 ON b.product_id = pm2.product_id
        GROUP BY a.product_id, b.product_id, pm1.product_name, pm2.product_name
        ORDER BY co_occurrence_count DESC
        """,
    ),
]


def _migrate_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    One-time migrations: drop stale derived tables so _build_derived_tables() rebuilds them.
    Safe to call on every startup — each check is a no-op once the target table exists.
    """
    existing = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
    # Migration 1: first-time fact_orders (pre-existing stale aisle/department tables)
    if "fact_orders" not in existing:
        print("[database] Migrating schema: dropping stale derived tables to rebuild ...")
        for tbl in ("department_metrics", "aisle_metrics"):
            con.execute(f"DROP TABLE IF EXISTS {tbl}")
    # Migration 2: user_order_timeline missing → fact_orders lacks temporal columns → rebuild
    if "user_order_timeline" not in existing:
        print("[database] Migrating schema: adding user_order_timeline, rebuilding fact_orders ...")
        for tbl in ("department_metrics", "aisle_metrics", "fact_orders"):
            con.execute(f"DROP TABLE IF EXISTS {tbl}")


def _build_derived_tables(con: duckdb.DuckDBPyConnection) -> None:
    """
    Build derived tables from raw tables.
    Uses CREATE TABLE IF NOT EXISTS — safe to call on every startup; no-op when tables exist.
    Order is significant. Caught exceptions gracefully skip building if required base tables (like Instacart specific rows) are totally missing.
    """
    for table_name, sql in _DERIVED_TABLE_SQL:
        try:
            print(f"[database] Ensuring derived table '{table_name}' exists ...")
            con.execute(sql)
        except duckdb.Error as e:
            print(f"[database] Skipping derived table '{table_name}' (missing base tables or columns?): {e}")
            break
    print("[database] Derived tables materialization finished.")


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
