# Data Directory

Drop your e-commerce CSV files here. Each file becomes a queryable table.

**Naming convention:** The filename (without `.csv`) becomes the table name.

Examples:
- `orders.csv` → `orders` table
- `order_items.csv` → `order_items` table
- `customers.csv` → `customers` table
- `products.csv` → `products` table
- `categories.csv` → `categories` table
- `returns.csv` → `returns` table

After adding or changing CSV files, restart the Streamlit app to reload the schema.

**Note on Parquet Conversion:** For performance, any `.csv` dropped in this folder will be automatically compressed and converted to a `.parquet` file the next time the app starts. If you modify the original CSV later, make sure to delete its generated `.parquet` file and the `warehouse.duckdb` file before restarting the app so the new data gets ingested.
