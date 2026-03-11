import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from method1 import run_method1
from method2 import run_method2


# ==============================
# COMMON HELPERS
# ==============================

def ask_dir(prompt: str, must_exist=True, create_if_missing=False):
    p = input(prompt).strip().strip('"').strip("'")
    p = os.path.normpath(p)

    if must_exist and not os.path.isdir(p):
        raise ValueError(f"Folder not found: {p}")

    if create_if_missing and not os.path.isdir(p):
        os.makedirs(p, exist_ok=True)

    return p


def build_date_list(start_yyyymmdd: str, end_yyyymmdd: str):
    start = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    end = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()

    if start > end:
        raise ValueError("start_date must be <= end_date")

    out = []
    cur = start
    while cur <= end:
        out.append(cur.strftime("%Y%m%d"))
        cur += timedelta(days=1)

    return out


def select_inputs(input_dir: str):
    print("\nSelect input range for ETL:")
    print("1) Date range (start/end yyyymmdd)")
    print("2) Specific file list")

    mode = input("Mode (1/2): ").strip()

    paths = []
    meta = {}

    if mode == "1":
        start_date = input("Start date (yyyymmdd): ").strip()
        end_date = input("End date (yyyymmdd): ").strip()

        date_list = build_date_list(start_date, end_date)

        paths = [
            os.path.join(input_dir, f"{d}.json")
            for d in date_list
        ]
        paths = [p for p in paths if os.path.exists(p)]

        meta = {
            "mode": "date_range",
            "start": start_date,
            "end": end_date
        }

    elif mode == "2":
        raw = input("Enter file names (comma separated, e.g., 20220401.json,20220405.json): ").strip()
        names = [x.strip() for x in raw.split(",") if x.strip()]
        paths = [os.path.join(input_dir, n) for n in names]
        paths = [p for p in paths if os.path.exists(p)]

        meta = {"mode": "file_list"}

    else:
        raise ValueError("Invalid mode")

    return paths, meta


# ==============================
# MAIN
# ==============================

def main():
    print("=== ETL APPLICATION ===")

    input_dir = ask_dir(
        "Enter input data folder path: ",
        must_exist=True
    )

    output_dir = ask_dir(
        "Enter output folder path: ",
        must_exist=False,
        create_if_missing=True
    )

    selected_paths, meta = select_inputs(input_dir)

    if not selected_paths:
        print("[STOP] No files selected.")
        return

    print("\nSelected files:")
    for p in selected_paths[:10]:
        print(" -", p)
    if len(selected_paths) > 10:
        print(" ...")
    print(f"Total: {len(selected_paths)} file(s)")

    print("\nChoose processing method:")
    print("1) Method 1 - Process each file separately")
    print("2) Method 2 - Merge files and process once")

    method = input("Method (1/2): ").strip()

    spark = (
        SparkSession.builder
        .appName("ETL_APP")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    if method == "1":
        run_method1(spark, selected_paths, output_dir)

    elif method == "2":
        run_method2(spark, selected_paths, output_dir, meta)

    else:
        print("Invalid method selected.")

if __name__ == "__main__":
    main()