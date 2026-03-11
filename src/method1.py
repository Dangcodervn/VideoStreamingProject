import os
import re
from datetime import datetime

from pyspark.sql.functions import lit
from transform import transform

DATE_PATTERN = re.compile(r"(\d{8})")


def extract_date_from_filename(file_path: str) -> str:
    base = os.path.basename(file_path)
    match = DATE_PATTERN.search(base)
    return match.group(1) if match else "unknown"


def write_output(df, output_path: str):
    (
        df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(output_path)
    )


def run_method1(spark, selected_paths, output_dir):

    if not selected_paths:
        print("[STOP] No files selected.")
        return

    print(f"\n[INFO] Running Method 1 on {len(selected_paths)} file(s)")

    start_time = datetime.now()

    for file_path in selected_paths:

        if not os.path.exists(file_path):
            print(f"[SKIP] File not found: {file_path}")
            continue

        dt = extract_date_from_filename(file_path)

        print(f"\n[RUN] Processing file: {file_path}")
        print(f"[INFO] Detected date: {dt}")

        raw_df = spark.read.json(file_path)

        result = transform(raw_df)

        result = result.withColumn("Date", lit(dt))

        out_path = os.path.join(output_dir, "method1_clean", f"dt={dt}")

        write_output(result, out_path)

        print(f"[OK] Saved to: {out_path}")

    end_time = datetime.now()
    print("\n[DONE] Total seconds:", (end_time - start_time).total_seconds())