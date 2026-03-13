import os
from datetime import datetime

from pyspark.sql.functions import sum as fsum
from transform import transform, PIVOT_TYPES, add_most_watched, add_customer_taste


def run_method2(spark, selected_paths):
    """Transform each file individually, union results, then re-aggregate by Contract."""
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

    print(f"\n[INFO] Method 2 — transforming {len(existing)} file(s) individually")
    start_time = datetime.now()

    accumulated = None
    for i, file_path in enumerate(existing, 1):
        fname = os.path.basename(file_path)
        print(f"[{i:02d}/{len(existing)}] Processing: {fname}")
        raw_df = spark.read.json(file_path)
        df_transformed = transform(raw_df)
        if accumulated is None:
            accumulated = df_transformed
        else:
            accumulated = accumulated.union(df_transformed)
        accumulated.cache()

    print("\n[INFO] Aggregating after union...")
    result = (
        accumulated
        .groupBy("Contract")
        .agg(
            *[fsum(t).alias(t) for t in PIVOT_TYPES],
            fsum("TotalDevices").alias("TotalDevices"),
        )
    )
    result = add_most_watched(result)
    result = add_customer_taste(result)
    print(f"[INFO] Total contracts: {result.count():,}")
    result.show(10)

    end_time = datetime.now()
    print("[DONE] Total seconds:", (end_time - start_time).total_seconds())
    return result