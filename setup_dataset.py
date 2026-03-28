import kagglehub
import glob
import os
import duckdb

print("Downloading dataset...")
path = kagglehub.dataset_download("psparks/instacart-market-basket-analysis")
print(f"Downloaded to: {path}")

dest_dir = "data"
os.makedirs(dest_dir, exist_ok=True)

csv_files = glob.glob(os.path.join(path, "*.csv"))
if not csv_files:
    csv_files = glob.glob(os.path.join(path, "**", "*.csv"), recursive=True)

con = duckdb.connect()
for file in csv_files:
    name = os.path.splitext(os.path.basename(file))[0]
    out_path = os.path.join(dest_dir, f"{name}.parquet")
    print(f"Converting {os.path.basename(file)} → {name}.parquet ...")
    con.execute(
        f"COPY (SELECT * FROM read_csv_auto('{file}')) TO '{out_path}' "
        f"(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
con.close()

print("Done. Parquet files written to data/.")
print("If warehouse.duckdb already exists, delete it to re-materialize from Parquet.")
