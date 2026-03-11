import os
import re
import hashlib
from datetime import datetime

from pyspark.sql.functions import lit
from transform import transform

DATE_PATTERN = re.compile(r"(\d{8})")


def extract_date(file_path: str) -> str:
    base = os.path.basename(file_path)
    m = DATE_PATTERN.search(base)
    return m.group(1) if m else "unknown"


def build_unique_tag(existing_files, meta=None) -> str:
    """
    Build a unique tag for output folder to avoid overwrite collisions.

    Tag format:
      <mode>_<min>_<max>_<n>files_<hash6>

    Example:
      range_20220401_20220409_9files_a83f2b
      files_20220401_20220409_2files_19c4d1
    """
    mode = "range"
    if meta and meta.get("mode") == "file_list":
        mode = "files"
    elif meta and meta.get("mode") == "date_range":
        mode = "range"

    dates = [extract_date(p) for p in existing_files]
    dates = [d for d in dates if d != "unknown"]

    if dates:
        dmin, dmax = min(dates), max(dates)
    else:
        # fallback if cannot infer date from filenames
        now_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
        dmin, dmax = now_tag, now_tag

    nfiles = len(existing_files)

    # Hash based on the exact selected file set (stable + unique)
    joined = "|".join(sorted(os.path.basename(p) for p in existing_files))
    hash6 = hashlib.md5(joined.encode("utf-8")).hexdigest()[:6]

    return f"{mode}_{dmin}_{dmax}_{nfiles}files_{hash6}"


def write_output_csv(df, output_path: str):
    """Write output as CSV (Spark writes a folder; coalesce(1) -> 1 part file)."""
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )


def run_method2(spark, selected_paths, output_dir, meta=None):
    """
    Method 2:
    - Read multiple files at once
    - Transform once
    - Write one output folder (unique tag to avoid overwrite collisions)
    """
    if not selected_paths:
        print("[STOP] No files selected.")
        return

    existing_files = [p for p in selected_paths if os.path.exists(p)]
    missing_files = [p for p in selected_paths if not os.path.exists(p)]

    if missing_files:
        print(f"[WARN] Missing {len(missing_files)} file(s). Showing up to 10:")
        for p in missing_files[:10]:
            print(" -", p)

    if not existing_files:
        print("[STOP] No valid files found.")
        return

    tag = build_unique_tag(existing_files, meta)

    print(f"\n[INFO] Running Method 2 on {len(existing_files)} file(s)")
    print(f"[INFO] Output tag: {tag}")

    start_time = datetime.now()

    # Read all files at once (Spark unions internally)
    raw_df = spark.read.json(existing_files)

    # Transform
    result = transform(raw_df)

    # Output path
    out_path = os.path.join(output_dir, "method2_clean", f"run={tag}")

    # Write
    write_output_csv(result, out_path)

    end_time = datetime.now()
    print(f"[OK] Output saved to: {out_path}")
    print("[DONE] Total seconds:", (end_time - start_time).total_seconds())