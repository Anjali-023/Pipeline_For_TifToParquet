import os
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
 
def unzip_file(zip_path, extract_path):
    try:
        # ❌ removed the "skip if already extracted" check
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for member in zip_ref.namelist():
                if member.lower().endswith(".tif") or member.lower().endswith("_metadata.json"):
                    filename = os.path.basename(member)  # flatten structure
                    target_path = os.path.join(extract_path, filename)
 
                    # overwrite existing file if it already exists
                    with zip_ref.open(member) as src, open(target_path, "wb") as dst:
                        dst.write(src.read())
 
        return f"✅ Unzipped (always): {zip_path} → {extract_path}"
 
    except zipfile.BadZipFile:
        return f"❌ Bad zip file skipped: {zip_path}"
    except Exception as e:
        return f"⚠️ Error with {zip_path}: {e}"
 
 
def unzip_all_in_range(source_root, folder_start, folder_end, max_workers=8):
    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for dirpath, _, filenames in os.walk(source_root):
            current_folder = os.path.basename(dirpath.rstrip(os.sep))
 
            # ✅ Only process folders in the assigned range
            if not current_folder.isdigit():
                continue
            folder_num = int(current_folder)
            if folder_num < folder_start or folder_num > folder_end:
                continue
 
            # Process zip files in this folder
            zip_files = [f for f in filenames if f.lower().endswith('.zip')]
            if not zip_files:
                continue
 
            unzipped_folder = os.path.join(os.path.dirname(dirpath), f"{current_folder}_unzipped_data")
            os.makedirs(unzipped_folder, exist_ok=True)
 
            for zip_file in zip_files:
                zip_path = os.path.join(dirpath, zip_file)
                unzip_dir_name = os.path.splitext(zip_file)[0]
                extract_path = os.path.join(unzipped_folder, unzip_dir_name)
                os.makedirs(extract_path, exist_ok=True)
 
                tasks.append(executor.submit(unzip_file, zip_path, extract_path))
 
        # Print results as tasks finish
        for future in as_completed(tasks):
            print(future.result())
 
 
# -------------------------
# Example usage
# -------------------------
source_path = r"Path to all the zip folders"
 
# Always re-unzips, overwriting files if already present
unzip_all_in_range(source_path, 5,10, max_workers=2) #range of folders from start to end
