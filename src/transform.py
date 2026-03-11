from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum as fsum

# Danh sách Type cố định để pivot ổn định schema
PIVOT_TYPES = [
    "Truyền Hình",
    "Phim Truyện",
    "Giải Trí",
    "Thiếu Nhi",
    "Thể Thao"
]


# -----------------------------
# 1. Select _source.*
# -----------------------------
def select_source(df: DataFrame) -> DataFrame:
    """
    JSON log có cấu trúc:
    {
        "_source": {...}
    }

    Lấy toàn bộ field bên trong _source.
    """
    return df.select("_source.*")


# -----------------------------
# 2. Map AppName -> Type
# -----------------------------
def add_type(df: DataFrame) -> DataFrame:
    """
    Phân loại AppName thành nhóm nội dung.
    """
    return df.withColumn(
        "Type",
        when(col("AppName").isin("CHANNEL", "DSHD", "KPLUS", "KPlus"), "Truyền Hình")
        .when(col("AppName").isin(
            "VOD", "FIMS_RES", "BHD_RES",
            "VOD_RES", "FIMS", "BHD", "DANET"
        ), "Phim Truyện")
        .when(col("AppName") == "RELAX", "Giải Trí")
        .when(col("AppName") == "CHILD", "Thiếu Nhi")
        .when(col("AppName") == "SPORT", "Thể Thao")
        .otherwise("Error")
    )


# -----------------------------
# 3. Clean + Aggregate base
# -----------------------------
def clean_base(df: DataFrame) -> DataFrame:
    """
    - Giữ các cột cần thiết
    - Loại Contract = 0
    - Loại Type = Error
    - Cast TotalDuration về double
    - Aggregate theo Contract + Type
    """

    df = df.select("Contract", "Type", "TotalDuration")

    df = df.filter(col("Contract") != "0")
    df = df.filter(col("Type") != "Error")

    df = df.withColumn(
        "TotalDuration",
        col("TotalDuration").cast("double")
    )

    return (
        df.groupBy("Contract", "Type")
          .agg(fsum("TotalDuration").alias("TotalDuration"))
    )


# -----------------------------
# 4. Pivot
# -----------------------------
def pivot_contract_type(df: DataFrame) -> DataFrame:
    """
    Pivot Type thành các cột.
    Kết quả: 1 dòng / Contract
    """

    return (
        df.groupBy("Contract")
          .pivot("Type", PIVOT_TYPES)
          .agg(fsum("TotalDuration"))
          .na.fill(0)
    )


# -----------------------------
# 5. Main transform
# -----------------------------
def transform(df: DataFrame) -> DataFrame:
    """
    Hàm transform chính.
    Nhận DataFrame raw -> trả DataFrame summary đã pivot.
    """

    df = select_source(df)
    df = add_type(df)
    df = clean_base(df)
    df = pivot_contract_type(df)

    return df