import kagglehub
import shutil
import glob
import os

print("Downloading dataset...")
path = kagglehub.dataset_download("psparks/instacart-market-basket-analysis")
print(f"Downloaded to: {path}")

dest_dir = "data"
os.makedirs(dest_dir, exist_ok=True)

csv_files = glob.glob(os.path.join(path, "*.csv"))

if not csv_files:
    # Some datasets contain subdirectories, check recursively just in case
    csv_files = glob.glob(os.path.join(path, "**", "*.csv"), recursive=True)

for file in csv_files:
    filename = os.path.basename(file)
    print(f"Copying {filename} to {dest_dir}...")
    shutil.copy(file, os.path.join(dest_dir, filename))

print("Dataset setup complete! The app can now load these files.")
