"""
Build Silver and Gold datasets for model training and store them in MinIO.

Scope:
- Input: historical CSV (Data/train_history.csv)
- Output:
  - s3a://lakehouse/silver/train_history/
  - s3a://lakehouse/gold/train_rul/

Notes:
- This job is intentionally independent from Bronze.
- It prepares train-ready datasets for the AI module.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class JobConfig:
    input_csv_path: str
    silver_path: str
    gold_path: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    rul_clip: float
    sequence_length: int

    @staticmethod
    def from_env() -> "JobConfig":
        return JobConfig(
            input_csv_path=os.getenv("TRAIN_INPUT_CSV_PATH", "/app/Data/train_history.csv"),
            silver_path=os.getenv("TRAIN_SILVER_DELTA_PATH", "s3a://lakehouse/silver/train_history/"),
            gold_path=os.getenv("TRAIN_GOLD_DELTA_PATH", "s3a://lakehouse/gold/train_rul/"),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
            rul_clip=float(os.getenv("TRAIN_RUL_CLIP", "125")),
            sequence_length=int(os.getenv("TRAIN_SEQUENCE_LENGTH", "30")),
        )


def create_spark_session(cfg: JobConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName("BuildTrainSilverGold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", cfg.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def _load_input_dataframe(spark: SparkSession, input_path: str):
    # CMAPSS train files are whitespace-delimited text without headers.
    if input_path.lower().endswith(".txt"):
        txt = spark.read.text(input_path)
        parts = F.split(F.trim(F.col("value")), r"\\s+")

        sensor_cols = [f"s_{i}" for i in range(1, 22)]
        projected = [
            parts.getItem(0).alias("unit_nr"),
            parts.getItem(1).alias("time_cycles"),
            parts.getItem(2).alias("setting_1"),
            parts.getItem(3).alias("setting_2"),
            parts.getItem(4).alias("setting_3"),
        ]
        projected.extend(parts.getItem(i + 4).alias(sensor_cols[i - 1]) for i in range(1, 22))

        return txt.select(*projected)

    return spark.read.option("header", "true").csv(input_path)


def _normalize_to_training_schema(df_raw):
    """
    Normalize input to a unified schema required by downstream AI prep:
    unit_nr, time_cycles, setting_1..3, s_1..s_21, RUL

    Supported input contracts:
    - Custom CSV: asset_id, cycle, s1..s21, rul
    - Original CMAPSS-like: unit_nr, time_cycles, setting_*, s_1..s_21 (RUL/rul optional)
    """
    cols = set(df_raw.columns)
    sensor_plain = [f"s{i}" for i in range(1, 22)]
    sensor_underscore = [f"s_{i}" for i in range(1, 22)]

    has_custom = {"asset_id", "cycle"}.issubset(cols) and all(c in cols for c in sensor_plain)
    has_cmapss = {"unit_nr", "time_cycles"}.issubset(cols) and all(c in cols for c in sensor_underscore)

    if has_custom:
        # Adapt custom schema to original CMAPSS naming.
        select_expr = [
            F.col("asset_id").alias("unit_nr"),
            F.col("cycle").alias("time_cycles"),
            F.lit(0.0).alias("setting_1"),
            F.lit(0.0).alias("setting_2"),
            F.lit(0.0).alias("setting_3"),
        ]
        select_expr.extend(F.col(f"s{i}").alias(f"s_{i}") for i in range(1, 22))
        if "rul" in cols:
            select_expr.append(F.col("rul").alias("RUL"))
        df = df_raw.select(*select_expr)
    elif has_cmapss:
        select_expr = [
            F.col("unit_nr"),
            F.col("time_cycles"),
            F.col("setting_1") if "setting_1" in cols else F.lit(0.0).alias("setting_1"),
            F.col("setting_2") if "setting_2" in cols else F.lit(0.0).alias("setting_2"),
            F.col("setting_3") if "setting_3" in cols else F.lit(0.0).alias("setting_3"),
        ]
        select_expr.extend(F.col(f"s_{i}") for i in range(1, 22))
        if "RUL" in cols:
            select_expr.append(F.col("RUL"))
        elif "rul" in cols:
            select_expr.append(F.col("rul").alias("RUL"))
        df = df_raw.select(*select_expr)
    else:
        raise ValueError(
            "Unsupported input schema. Provide either custom schema "
            "(asset_id, cycle, s1..s21, rul) or original CMAPSS schema "
            "(unit_nr, time_cycles, setting_*, s_1..s_21)."
        )

    required = ["unit_nr", "time_cycles", "setting_1", "setting_2", "setting_3", *sensor_underscore]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns after normalization: {missing}")

    df_typed = (
        df.select(*required, *(["RUL"] if "RUL" in df.columns else []))
        .withColumn("unit_nr", F.col("unit_nr").cast("long"))
        .withColumn("time_cycles", F.col("time_cycles").cast("long"))
        .withColumn("setting_1", F.col("setting_1").cast("double"))
        .withColumn("setting_2", F.col("setting_2").cast("double"))
        .withColumn("setting_3", F.col("setting_3").cast("double"))
    )

    for c in sensor_underscore:
        df_typed = df_typed.withColumn(c, F.col(c).cast("double"))

    if "RUL" in df_typed.columns:
        df_typed = df_typed.withColumn("RUL", F.col("RUL").cast("double"))
    else:
        # CMAPSS train files do not include RUL; derive from max cycle per unit.
        max_cycles = df_typed.groupBy("unit_nr").agg(F.max("time_cycles").alias("max_cycle"))
        df_typed = (
            df_typed.join(max_cycles, on="unit_nr", how="left")
            .withColumn("RUL", (F.col("max_cycle") - F.col("time_cycles")).cast("double"))
            .drop("max_cycle")
        )

    return df_typed


def build_silver(spark: SparkSession, cfg: JobConfig):
    sensor_cols = [f"s_{i}" for i in range(1, 22)]
    df_raw = _load_input_dataframe(spark, cfg.input_csv_path)
    df_typed = _normalize_to_training_schema(df_raw)

    non_null_cols = ["unit_nr", "time_cycles", "RUL", "setting_1", "setting_2", "setting_3", *sensor_cols]
    df_clean = df_typed
    for c in non_null_cols:
        df_clean = df_clean.filter(F.col(c).isNotNull())

    df_clean = (
        df_clean.filter(F.col("time_cycles") > 0)
        .filter(F.col("RUL") >= 0)
        .dropDuplicates(["unit_nr", "time_cycles"])
    )

    df_silver = (
        df_clean.withColumn("data_quality_passed", F.lit(True))
        .withColumn("dataset_version", F.current_timestamp())
    )

    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(cfg.silver_path)
    )

    return spark.read.format("delta").load(cfg.silver_path)


def build_gold(df_silver, cfg: JobConfig):
    sensor_cols = [f"s_{i}" for i in range(1, 22)]

    asset_lengths = df_silver.groupBy("unit_nr").agg(F.count(F.lit(1)).alias("cycle_count"))
    eligible_assets = asset_lengths.filter(F.col("cycle_count") >= cfg.sequence_length).select("unit_nr")

    df_gold_base = (
        df_silver.join(eligible_assets, on="unit_nr", how="inner")
        .withColumn("RUL_clipped", F.least(F.col("RUL"), F.lit(cfg.rul_clip)))
        .withColumn("train_ready", F.lit(True))
    )

    select_cols = ["unit_nr", "time_cycles", "setting_1", "setting_2", "setting_3", *sensor_cols, "RUL", "RUL_clipped", "train_ready"]
    df_gold = df_gold_base.select(*select_cols)

    (
        df_gold.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(cfg.gold_path)
    )


def main():
    cfg = JobConfig.from_env()
    spark = create_spark_session(cfg)

    df_silver = build_silver(spark, cfg)
    build_gold(df_silver, cfg)

    silver_count = spark.read.format("delta").load(cfg.silver_path).count()
    gold_df = spark.read.format("delta").load(cfg.gold_path)
    gold_count = gold_df.count()

    print(f"Silver path: {cfg.silver_path}")
    print(f"Input path: {cfg.input_csv_path}")
    print(f"Silver rows: {silver_count}")
    print(f"Gold path: {cfg.gold_path}")
    print(f"Gold rows: {gold_count}")

    spark.stop()


if __name__ == "__main__":
    main()
