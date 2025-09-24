import pandas as pd
import glob
import os
 
# -------- CONFIG --------
BASE_DIR = r"Path to Csv's"   # Folder that contains many subfolders
OUTPUT_FILE = os.path.join(BASE_DIR, "final_merged.parquet")
 
# Band columns to check
band_cols = ["B1","B2","B3","B4","B5","B6","B7","B8"]
 
# Empty list to store DataFrames
all_dfs = []
 
# Iterate through all subfolders
for folder in os.listdir(BASE_DIR):
    subfolder_path = os.path.join(BASE_DIR, folder)
   
    if os.path.isdir(subfolder_path):  # only process directories
        print(f"📂 Processing folder: {subfolder_path}")
       
        # Collect CSVs in this subfolder
        csv_files = glob.glob(os.path.join(subfolder_path, "*.csv"))
       
        if not csv_files:
            continue
       
        # Read and merge CSVs inside this folder
        df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
 
        # Drop duplicates
        df = df.drop_duplicates()
 
        # Drop empty/null columns
        df = df.dropna(axis=1, how="all")
 
        # Drop rows where all bands are 0
        if all(col in df.columns for col in band_cols):
            df = df[~(df[band_cols].sum(axis=1) == 0)]
 
        # Drop tiff_file column if it exists
        if "tiff_file" in df.columns:
            df = df.drop(columns=["tiff_file"])
 
        # ✅ Save cleaned CSV inside the same subfolder
        cleaned_csv_path = os.path.join(subfolder_path, "merged_cleaned.csv")
        df.to_csv(cleaned_csv_path, index=False)
        print(f"   ➡️ Cleaned CSV saved at: {cleaned_csv_path}")
 
        # Append to list for global parquet
        all_dfs.append(df)
 
# Merge all cleaned DataFrames
if all_dfs:
    final_df = pd.concat(all_dfs, ignore_index=True)
 
    # Save as parquet
    final_df.to_parquet(OUTPUT_FILE, index=False)
    print(f"✅ Final Parquet saved at: {OUTPUT_FILE}")
else:
    print("⚠️ No CSV files found in subfolders.")
