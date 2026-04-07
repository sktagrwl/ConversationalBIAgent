"""
setup_dataset.py — Dataset onboarding for the Conversational BI Agent

Usage:
  # Drop CSVs into data/ then run:
  python setup_dataset.py

  # Load CSVs from another directory:
  python setup_dataset.py --source /path/to/csvs

  # Download Instacart from Kaggle (requires ~/.kaggle/kaggle.json):
  python setup_dataset.py --kaggle psparks/instacart-market-basket-analysis

  # Delete warehouse + parquets and start fresh:
  python setup_dataset.py --reset

  # Skip AI analysis (keep existing derived_tables.sql):
  python setup_dataset.py --skip-analysis
"""

import argparse
import glob
import os
import shutil

import anthropic
import duckdb

from src.config import ANTHROPIC_API_KEY, DATA_DIR, DB_PATH, MODEL

# Instacart-specific derived tables SQL — written to data/derived_tables.sql
# when --kaggle psparks/instacart-market-basket-analysis is used.
_INSTACART_DERIVED_SQL = """\
-- TABLE: order_products
CREATE TABLE IF NOT EXISTS order_products AS
SELECT order_id, product_id, add_to_cart_order, reordered FROM order_products__prior
UNION ALL
SELECT order_id, product_id, add_to_cart_order, reordered FROM order_products__train;

-- TABLE: user_order_timeline
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
WHERE eval_set IN ('prior', 'train');

-- TABLE: fact_orders
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
JOIN products p             ON op.product_id   = p.product_id
JOIN aisles a               ON p.aisle_id      = a.aisle_id
JOIN departments d          ON p.department_id = d.department_id
JOIN user_order_timeline t  ON op.order_id     = t.order_id;

-- TABLE: aisle_metrics
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
GROUP BY aisle_id, aisle, department_id, department;

-- TABLE: product_metrics
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
JOIN aisles a              ON p.aisle_id      = a.aisle_id
JOIN departments d         ON p.department_id = d.department_id
JOIN order_products op     ON p.product_id    = op.product_id
GROUP BY p.product_id, p.product_name, a.aisle_id, a.aisle, d.department_id, d.department;

-- TABLE: order_metrics
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
    o.order_dow, o.order_hour_of_day, o.days_since_prior_order;

-- TABLE: user_metrics
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
GROUP BY o.user_id;

-- TABLE: department_metrics
CREATE TABLE IF NOT EXISTS department_metrics AS
SELECT
    department_id,
    department,
    COUNT(DISTINCT product_id)                  AS product_count,
    COUNT(*)                                    AS total_orders,
    ROUND(SUM(reordered) * 1.0 / COUNT(*), 4)  AS avg_reorder_rate,
    ROUND(AVG(add_to_cart_order), 2)            AS avg_cart_position
FROM fact_orders
GROUP BY department_id, department;

-- TABLE: product_pairs
CREATE TABLE IF NOT EXISTS product_pairs AS
WITH top_products AS (
    SELECT product_id FROM product_metrics ORDER BY total_orders DESC LIMIT 200
),
reordered AS (
    SELECT order_id, product_id FROM fact_orders
    WHERE reordered = 1 AND product_id IN (SELECT product_id FROM top_products)
)
SELECT
    a.product_id     AS product_1_id,
    b.product_id     AS product_2_id,
    pm1.product_name AS product_1,
    pm2.product_name AS product_2,
    COUNT(*)         AS co_occurrence_count
FROM reordered a
JOIN reordered b         ON a.order_id = b.order_id AND a.product_id < b.product_id
JOIN product_metrics pm1 ON a.product_id = pm1.product_id
JOIN product_metrics pm2 ON b.product_id = pm2.product_id
GROUP BY a.product_id, b.product_id, pm1.product_name, pm2.product_name
ORDER BY co_occurrence_count DESC;
"""

_INSTACART_KAGGLE_SLUG = "psparks/instacart-market-basket-analysis"


def _reset(dest_dir: str) -> None:
    db_path = DB_PATH
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Deleted {db_path}")
    for p in glob.glob(os.path.join(dest_dir, "*.parquet")):
        os.remove(p)
        print(f"Deleted {os.path.basename(p)}")
    derived_sql = os.path.join(dest_dir, "derived_tables.sql")
    if os.path.exists(derived_sql):
        os.remove(derived_sql)
        print(f"Deleted {derived_sql}")


def _convert_csvs_to_parquet(dest_dir: str) -> list[str]:
    """Convert any loose CSVs in dest_dir to Parquet. Returns list of parquet paths."""
    parquet_files = glob.glob(os.path.join(dest_dir, "*.parquet"))
    parquet_basenames = {os.path.basename(p) for p in parquet_files}

    con = duckdb.connect()
    for csv_file in glob.glob(os.path.join(dest_dir, "*.csv")):
        name = os.path.splitext(os.path.basename(csv_file))[0]
        expected = f"{name}.parquet"
        if expected in parquet_basenames:
            print(f"Skipping {os.path.basename(csv_file)} (Parquet already exists)")
            continue
        out_path = os.path.join(dest_dir, expected)
        print(f"Converting {os.path.basename(csv_file)} → {expected} ...")
        con.execute(
            f"COPY (SELECT * FROM read_csv_auto('{csv_file}')) "
            f"TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        parquet_files.append(out_path)
        parquet_basenames.add(expected)
    con.close()
    return glob.glob(os.path.join(dest_dir, "*.parquet"))


def _collect_schema_and_samples(parquet_files: list[str]) -> tuple[str, str]:
    """
    Register parquet files in a temp DuckDB connection and collect:
    - schema text (table name, columns, row count)
    - sample rows (up to 3 per table)
    """
    con = duckdb.connect()
    schema_lines = []
    sample_lines = []

    for path in sorted(parquet_files):
        table_name = os.path.splitext(os.path.basename(path))[0]
        con.execute(f'CREATE VIEW "{table_name}" AS SELECT * FROM read_parquet(\'{path}\')')

        cols = con.execute(f'DESCRIBE "{table_name}"').fetchall()
        row_count = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
        col_str = ", ".join(f"{c[0]} ({c[1]})" for c in cols)
        schema_lines.append(f"  {table_name} [{row_count:,} rows]: {col_str}")

        samples = con.execute(f'SELECT * FROM "{table_name}" LIMIT 3').fetchall()
        col_names = [c[0] for c in cols]
        sample_lines.append(f"  {table_name}:")
        for row in samples:
            sample_lines.append(f"    {dict(zip(col_names, row))}")

    con.close()
    return "\n".join(schema_lines), "\n".join(sample_lines)


def _generate_derived_tables_sql(schema_text: str, samples_text: str) -> str:
    """Call Claude to generate derived_tables.sql content for this dataset."""
    print("\nAnalyzing schema with Claude to generate derived_tables.sql ...")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    prompt = f"""You are setting up a DuckDB analytics warehouse for a conversational BI agent.

Here is the schema of the raw tables loaded from CSV files:
{schema_text}

Here are sample rows from each table (up to 3 rows):
{samples_text}

Your task: generate a derived_tables.sql file that pre-aggregates this data for fast analytical queries.

Rules:
1. Each derived table statement must be preceded by: -- TABLE: <table_name>
2. Use CREATE TABLE IF NOT EXISTS for each statement
3. Separate statements with a blank line
4. Focus on: joining related tables, computing useful metrics (counts, averages, rates), denormalizing hierarchies into fact tables
5. Only create derived tables that make genuine analytical sense for this data
6. Tables with fewer than 100,000 rows likely do not need a derived table
7. Tables with more than 1,000,000 rows definitely need pre-aggregation
8. Use DuckDB SQL syntax

Return ONLY the SQL content — no explanation, no markdown code fences."""

    response = client.messages.create(
        model=MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    return response.content[0].text.strip()


def _build_derived_tables(dest_dir: str) -> None:
    """Connect to the persistent warehouse and execute derived_tables.sql."""
    from src.database import get_connection
    print("\nBuilding derived tables in warehouse ...")
    get_connection()
    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Onboard a dataset into the Conversational BI Agent warehouse."
    )
    parser.add_argument(
        "--source", default=None,
        help="Directory containing CSV files to copy into data/ (default: use data/ directly)"
    )
    parser.add_argument(
        "--kaggle", default=None, metavar="SLUG",
        help="Kaggle dataset slug to download (e.g. psparks/instacart-market-basket-analysis)"
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete warehouse.duckdb, parquet files, and derived_tables.sql before loading"
    )
    parser.add_argument(
        "--skip-analysis", action="store_true",
        help="Skip AI-generated derived_tables.sql (keep existing file)"
    )
    args = parser.parse_args()

    dest_dir = DATA_DIR
    os.makedirs(dest_dir, exist_ok=True)

    # Optional reset
    if args.reset:
        _reset(dest_dir)

    # Optional Kaggle download
    if args.kaggle:
        try:
            import kagglehub
        except ImportError:
            print("kagglehub not installed. Run: pip install kagglehub")
            return
        print(f"Downloading Kaggle dataset: {args.kaggle} ...")
        source_path = kagglehub.dataset_download(args.kaggle)
        print(f"Downloaded to: {source_path}")
        args.source = source_path

    # Copy CSVs from external source if provided
    if args.source and os.path.abspath(args.source) != os.path.abspath(dest_dir):
        csv_files = glob.glob(os.path.join(args.source, "*.csv"))
        if not csv_files:
            csv_files = glob.glob(os.path.join(args.source, "**", "*.csv"), recursive=True)
        if not csv_files:
            print(f"No CSV files found in {args.source}. Nothing to do.")
            return
        for f in csv_files:
            dest_file = os.path.join(dest_dir, os.path.basename(f))
            shutil.copy2(f, dest_file)
            print(f"Copied {os.path.basename(f)} → {dest_dir}/")

    # Phase 1: Convert CSVs to Parquet
    parquet_files = _convert_csvs_to_parquet(dest_dir)
    if not parquet_files:
        print(f"\nNo CSV or Parquet files found in {dest_dir}/. Add your CSVs and re-run.")
        return
    print(f"\n{len(parquet_files)} Parquet file(s) ready in {dest_dir}/")

    # Phase 2: Generate derived_tables.sql
    derived_sql_path = os.path.join(dest_dir, "derived_tables.sql")

    if args.skip_analysis:
        if os.path.exists(derived_sql_path):
            print(f"\nSkipping analysis — using existing {derived_sql_path}")
        else:
            print("\nSkipping analysis — no derived_tables.sql will be created.")
            print("The app will work on raw tables. Re-run without --skip-analysis to generate one.")
    else:
        # Use the known-good Instacart SQL if that dataset was requested
        if args.kaggle == _INSTACART_KAGGLE_SLUG:
            print("\nInstacart dataset detected — using pre-defined derived tables SQL.")
            sql_content = _INSTACART_DERIVED_SQL
        else:
            schema_text, samples_text = _collect_schema_and_samples(parquet_files)
            sql_content = _generate_derived_tables_sql(schema_text, samples_text)

        with open(derived_sql_path, "w") as f:
            f.write(sql_content)

        print(f"\nGenerated {derived_sql_path}:")
        print("-" * 60)
        print(sql_content)
        print("-" * 60)

    # Phase 3: Build derived tables
    answer = input("\nBuild derived tables in the warehouse now? [Y/n]: ").strip().lower()
    if answer in ("", "y", "yes"):
        _build_derived_tables(dest_dir)
    else:
        print("Skipped. Run 'streamlit run app.py' — tables will be built on first startup.")

    print("\nSetup complete. Run: streamlit run app.py")


if __name__ == "__main__":
    main()
