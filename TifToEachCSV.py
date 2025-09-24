import os
import glob
import json
import rasterio
import pandas as pd
import pygeohash as pgh
from rasterio.transform import rowcol
from rasterio.warp import transform, transform_bounds
from rasterio.windows import Window
from tqdm import tqdm
 
# ---------------- CONFIG ----------------
BASE_TIFF_DIR = r'Path to folders contatining tif'   # parent folder containing subfolders like 4_unzipped_data, 5_unzipped_data
PARQUET_PATH = r'Parquet file'                  #this file is used for matching lat lon
OUTPUT_BASE_DIR = r"output folder"               # parent for per_tiff_csvs_N folders
WGS84 = "EPSG:4326"
 
# ---------------- HELPERS ----------------
def normalize_parquet(df: pd.DataFrame) -> pd.DataFrame:
    colmap = {c.lower(): c for c in df.columns}
    class_key = next((k for k in colmap if k in ("classification", "class", "label")), None)
    if class_key is None:
        raise ValueError(f"Could not find classification column in parquet. Columns: {list(df.columns)}")
 
    lat_key = None
    for k in ("lat", "latitude"):
        if k in colmap:
            lat_key = k
            break
    if lat_key is None and "y" in colmap:
        lat_key = "y"
 
    lon_key = None
    for k in ("lon", "long", "longitude"):
        if k in colmap:
            lon_key = k
            break
    if lon_key is None and "x" in colmap:
        lon_key = "x"
 
    if lat_key is None or lon_key is None:
        raise ValueError(f"Could not find lat/lon columns in parquet. Columns: {list(df.columns)}")
 
    df = df.rename(columns={
        colmap[lat_key]: "Lat",
        colmap[lon_key]: "Lon",
        colmap[class_key]: "classification"
    })
 
    df["Lat"] = pd.to_numeric(df["Lat"], errors="coerce")
    df["Lon"] = pd.to_numeric(df["Lon"], errors="coerce")
    df = df.dropna(subset=["Lat", "Lon", "classification"]).reset_index(drop=True)
 
    return df[["Lat", "Lon", "classification"]]
 
 
def load_all_json_metadata(json_dir):
    out = []
    for jf in glob.glob(os.path.join(json_dir, "*.json")):
        try:
            with open(jf, "r") as f:
                meta = json.load(f)
        except Exception:
            continue
        props = meta.get("properties") or {}
        acq_time = props.get("acquired")
        if not acq_time:
            continue
        try:
            date, time = acq_time.split("T")[0], acq_time.split("T")[1].split("Z")[0]
        except Exception:
            continue
        out.append({
            "json_file": os.path.basename(jf),
            "date": date,
            "time": time,
            "cloud_cover": props.get("cloud_cover")
        })
    return out
 
 
def process_one_tiff(tif_path, pq_df):
    try:
        with rasterio.open(tif_path) as src:
            crs_tiff = src.crs
            transform_aff = src.transform
            width, height = src.width, src.height
 
            if crs_tiff and crs_tiff.to_string() != WGS84:
                minx, miny, maxx, maxy = transform_bounds(crs_tiff, WGS84, *src.bounds, densify_pts=21)
            else:
                minx, miny, maxx, maxy = src.bounds.left, src.bounds.bottom, src.bounds.right, src.bounds.top
 
            cand = pq_df[(pq_df["Lon"] >= minx) & (pq_df["Lon"] <= maxx) &
                        (pq_df["Lat"] >= miny) & (pq_df["Lat"] <= maxy)].copy()
 
            if cand.empty:
                return None
 
            if crs_tiff and crs_tiff.to_string() != WGS84:
                xs, ys = transform(WGS84, crs_tiff, cand["Lon"].values, cand["Lat"].values)
            else:
                xs, ys = cand["Lon"].values, cand["Lat"].values
 
            rows, cols = rowcol(transform_aff, xs, ys)
            rows = pd.Series(rows, index=cand.index)
            cols = pd.Series(cols, index=cand.index)
            mask = (rows >= 0) & (rows < height) & (cols >= 0) & (cols < width)
 
            matched = cand.loc[mask].copy()
            rows, cols = rows[mask], cols[mask]
 
            if matched.empty:
                return None
 
            matched["geohash"] = [
                pgh.encode(lat, lon, precision=7) for lat, lon in zip(matched["Lat"], matched["Lon"])
            ]
 
            # Add band values (B1-B8)
            for b in range(1, min(9, src.count + 1)):
                band_vals = []
                for r, c in zip(rows, cols):
                    try:
                        val = src.read(b, window=Window(c, r, 1, 1))
                        band_vals.append(val[0, 0])
                    except Exception:
                        band_vals.append(float("nan"))
                matched[f"B{b}"] = band_vals
 
            json_dir = os.path.dirname(tif_path)
            meta_list = load_all_json_metadata(json_dir)
            if meta_list:
                meta = meta_list[0]
                matched["date"] = meta["date"]
                matched["time"] = meta["time"]
                matched["cloud_cover"] = meta["cloud_cover"]
            else:
                matched["date"] = None
                matched["time"] = None
                matched["cloud_cover"] = None
 
            matched["tiff_file"] = os.path.basename(tif_path)
            return matched
    except Exception as e:
        print(f"⚠️ Skipping {tif_path}, could not open ({e})")
        return None
 
# ---------------- MAIN ----------------
def main():
    print("Loading parquet...")
    pq_raw = pd.read_parquet(PARQUET_PATH)
    pq_df = normalize_parquet(pq_raw)
    print(f"Parquet points loaded: {len(pq_df)} | Columns: {pq_df.columns.tolist()}")
 
    # Loop over folders starting at 4
        # Loop over folders starting at 4
    for folder_num in range(50,51):   # 4 .. 60
        tiff_dir = os.path.join(BASE_TIFF_DIR, f"{folder_num}_unzipped_data")
        output_dir = os.path.join(OUTPUT_BASE_DIR, f"per_tiff_csvs_{folder_num}")
        os.makedirs(output_dir, exist_ok=True)
 
        # Get all TIFFs except those with 'udm2' in name
        tiff_paths = [
            p for p in glob.glob(os.path.join(tiff_dir, "**", "*.tif"), recursive=True)
            if "udm2" not in os.path.basename(p).lower()
        ]
 
        print(f"\n📂 Folder {folder_num}: Found {len(tiff_paths)} TIFFs (excluding 'udm2')")
 
        for tif in tqdm(tiff_paths):
            df = process_one_tiff(tif, pq_df)
 
            # use TIFF filename instead of folder name to avoid overwriting
            parent_folder = os.path.basename(os.path.dirname(tif))
 
            # Combine parent folder and tif name
            tif_name = f"{parent_folder}_{os.path.splitext(os.path.basename(tif))[0]}"
 
            if df is not None and not df.empty:
                out_csv = os.path.join(output_dir, f"{tif_name}.csv")
                df.to_csv(out_csv, index=False)
                print(f"  ✅ Saved {len(df)} rows -> {out_csv}")
            else:
                print(f"  ⚠️ No matches for {tif}")
 
    print("\n🎉 All TIFFs processed!")
 
 
if __name__ == "__main__":
    main()
