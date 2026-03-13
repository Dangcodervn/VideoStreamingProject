from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, sum as fsum, count, greatest, concat_ws

# Fixed list to ensure stable pivot schema
PIVOT_TYPES = [
    "Truyền Hình",
    "Phim Truyện",
    "Giải Trí",
    "Thiếu Nhi",
    "Thể Thao"
]


def select_source(df: DataFrame) -> DataFrame:
    return df.select("_source.*")


def add_type(df: DataFrame) -> DataFrame:
    """Map AppName to content category."""
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


def calculate_devices(df: DataFrame) -> DataFrame:
    """Count unique devices (Mac) per Contract."""
    return (
        df.select("Contract", "Mac")
          .groupBy("Contract")
          .agg(count("Mac").alias("TotalDevices"))
    )


def clean_base(df: DataFrame) -> DataFrame:
    """Cast TotalDuration and aggregate by Contract + Type."""

    df = df.select("Contract", "Type", "TotalDuration")

    df = df.withColumn(
        "TotalDuration",
        col("TotalDuration").cast("double")
    )

    return (
        df.groupBy("Contract", "Type")
          .agg(fsum("TotalDuration").alias("TotalDuration"))
    )


def pivot_contract_type(df: DataFrame) -> DataFrame:
    """Pivot Type into columns — one row per Contract."""

    return (
        df.groupBy("Contract")
          .pivot("Type", PIVOT_TYPES)
          .agg(fsum("TotalDuration"))
          .na.fill(0)
    )


def add_most_watched(df: DataFrame) -> DataFrame:
    """Add Most_Watched column: category with the highest total duration."""
    return df.withColumn(
        "Most_Watched",
        when(greatest(*[col(t) for t in PIVOT_TYPES]) == col("Truyền Hình"), "Truyền Hình")
        .when(greatest(*[col(t) for t in PIVOT_TYPES]) == col("Phim Truyện"), "Phim Truyện")
        .when(greatest(*[col(t) for t in PIVOT_TYPES]) == col("Giải Trí"), "Giải Trí")
        .when(greatest(*[col(t) for t in PIVOT_TYPES]) == col("Thiếu Nhi"), "Thiếu Nhi")
        .when(greatest(*[col(t) for t in PIVOT_TYPES]) == col("Thể Thao"), "Thể Thao")
        .otherwise("Error")
    )


def add_customer_taste(df: DataFrame) -> DataFrame:
    """Add Customer_Taste: first non-zero category the customer watched."""
    return df.withColumn(
        "Customer_Taste",
        concat_ws(", ",
            when(col("Giải Trí") > 0, "Giải Trí")
            .when(col("Thiếu Nhi") > 0, "Thiếu Nhi")
            .when(col("Thể Thao") > 0, "Thể Thao")
            .when(col("Truyền Hình") > 0, "Truyền Hình")
            .when(col("Phim Truyện") > 0, "Phim Truyện")
            .otherwise("Error")
        )
    )


def transform(df: DataFrame) -> DataFrame:
    """Full transform pipeline: raw JSON DataFrame → pivoted summary."""

    df = select_source(df)
    total_devices = calculate_devices(df)
    df = add_type(df)
    df = clean_base(df)
    df = pivot_contract_type(df)
    df = df.join(total_devices, "Contract", "inner")
    df = add_most_watched(df)
    df = add_customer_taste(df)
    return df