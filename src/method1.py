import os
from datetime import datetime

from transform import transform


def run_method1(spark, selected_paths):
    """Read all files at once and transform in a single pass."""
    if not selected_paths:
        print("[STOP] No files selected.")
        return None

    existing = [p for p in selected_paths if os.path.exists(p)]
    missing  = [p for p in selected_paths if not os.path.exists(p)]

    if missing:
        print(f"[WARN] {len(missing)} file(s) not found, skipping.")
    if not existing:
        print("[STOP] No valid files found.")
        return None

    print(f"\n[INFO] Method 1 — reading {len(existing)} file(s) at once")
    start_time = datetime.now()

    raw_df = spark.read.json(existing)
    print(f"[INFO] Total rows: {raw_df.count():,}")

    result = transform(raw_df)
    print(f"[INFO] Total contracts: {result.count():,}")
    result.show(10)

    end_time = datetime.now()
    print("[DONE] Total seconds:", (end_time - start_time).total_seconds())
    return result
