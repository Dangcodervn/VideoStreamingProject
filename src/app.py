import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from method1 import run_method1
from method2 import run_method2

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(_ROOT, ".env"))

DATABASE_NAME     = os.getenv("DATABASE_NAME")
DATABASE_USER     = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")


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


def import_to_mysql(result):

    url = f"jdbc:mysql://localhost:3306/{DATABASE_NAME}"
    driver = "com.mysql.cj.jdbc.Driver"

    result.write \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", "customer_content_stats") \
        .option("user", DATABASE_USER) \
        .option("password", DATABASE_PASSWORD) \
        .mode("append") \
        .save()

    print("Data Import Successfully")


def main():
    print("=== ETL APPLICATION ===")

    input_dir = ask_dir(
        "Enter input data folder path: ",
        must_exist=True
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
    print("1) Method 1 - Read all files at once → Transform once")
    print("2) Method 2 - Transform each file separately → Union")

    method = input("Method (1/2): ").strip()

    spark = (
        SparkSession.builder
        .appName("ETL_APP")
        .config("spark.driver.memory", "8g")
        .getOrCreate()
    )

    if method == "1":
        result = run_method1(spark, selected_paths)
    elif method == "2":
        result = run_method2(spark, selected_paths)
    else:
        print("Invalid method selected.")
        return

    if result is None:
        return

    print("\nChoose storage destination:")
    print("1) CSV   - Save to local folder")
    print("3) MySQL - Import to MySQL database")

    storage = input("Storage (1/3): ").strip()

    if storage == "1":
        output_dir = ask_dir(
            "Enter output folder path: ",
            must_exist=False,
            create_if_missing=True
        )
        method_folder = "method1_result" if method == "1" else "method2_result"
        out_path = os.path.join(output_dir, method_folder)
        (
            result.coalesce(1)
                  .write
                  .mode("overwrite")
                  .option("header", "true")
                  .csv(out_path)
        )
        print(f"[OK] Saved to: {out_path}")

    elif storage == "3":
        import_to_mysql(result)

    else:
        print("Invalid storage option.")

if __name__ == "__main__":
    main()