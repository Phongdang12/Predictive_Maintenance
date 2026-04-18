"""
Bronze landing pipeline for telemetry stream.

Scope:
- Kafka RAW topic -> Delta bronze_telemetry_raw
- Kafka DLQ topic -> Delta bronze_telemetry_dlq

Principles:
- Preserve raw payload and Kafka metadata for replay/audit.
- Do not apply business validation or anomaly logic in Bronze.
- Keep configuration externalized via environment variables.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


LOGGER = logging.getLogger("stream_bronze_telemetry")


# RAW payload contract from ingest layer.
RAW_PAYLOAD_SCHEMA = StructType(
    [
        StructField("unit_nr", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("time_cycles", StringType(), True),
        StructField("setting_1", StringType(), True),
        StructField("setting_2", StringType(), True),
        StructField("setting_3", StringType(), True),
        StructField("s_1", StringType(), True),
        StructField("s_2", StringType(), True),
        StructField("s_3", StringType(), True),
        StructField("s_4", StringType(), True),
        StructField("s_5", StringType(), True),
        StructField("s_6", StringType(), True),
        StructField("s_7", StringType(), True),
        StructField("s_8", StringType(), True),
        StructField("s_9", StringType(), True),
        StructField("s_10", StringType(), True),
        StructField("s_11", StringType(), True),
        StructField("s_12", StringType(), True),
        StructField("s_13", StringType(), True),
        StructField("s_14", StringType(), True),
        StructField("s_15", StringType(), True),
        StructField("s_16", StringType(), True),
        StructField("s_17", StringType(), True),
        StructField("s_18", StringType(), True),
        StructField("s_19", StringType(), True),
        StructField("s_20", StringType(), True),
        StructField("s_21", StringType(), True),
        StructField("source_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_row_number", StringType(), True),
    ]
)


# DLQ envelope expected from ingest bridge, with backward-compatible fields.
DLQ_ENVELOPE_SCHEMA = StructType(
    [
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("received_at", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("dlq_reason_code", StringType(), True),
        StructField("dlq_reason_detail", StringType(), True),
        StructField("failed_fields", ArrayType(StringType()), True),
        StructField("source_type", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("source_row_number", StringType(), True),
    ]
)


@dataclass(frozen=True)
class JobConfig:
    kafka_bootstrap_servers: str
    raw_topic: str
    dlq_topic: str
    starting_offsets: str
    fail_on_data_loss: str
    max_offsets_per_trigger: str
    trigger_interval: str

    bronze_raw_path: str
    bronze_dlq_path: str
    checkpoint_raw_path: str
    checkpoint_dlq_path: str

    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str

    @staticmethod
    def from_env() -> "JobConfig":
        return JobConfig(
            kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            raw_topic=os.getenv("BRONZE_RAW_TOPIC", "pdm.fd001.raw"),
            dlq_topic=os.getenv("BRONZE_DLQ_TOPIC", "pdm.fd001.raw.dlq"),
            starting_offsets=os.getenv("KAFKA_STARTING_OFFSETS", "earliest"),
            fail_on_data_loss=os.getenv("KAFKA_FAIL_ON_DATA_LOSS", "false"),
            max_offsets_per_trigger=os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000"),
            trigger_interval=os.getenv("BRONZE_TRIGGER_INTERVAL", "10 seconds"),
            bronze_raw_path=os.getenv("BRONZE_RAW_DELTA_PATH", "s3a://lakehouse/bronze/telemetry_raw/"),
            bronze_dlq_path=os.getenv("BRONZE_DLQ_DELTA_PATH", "s3a://lakehouse/bronze/telemetry_dlq/"),
            checkpoint_raw_path=os.getenv(
                "BRONZE_RAW_CHECKPOINT_PATH", "s3a://checkpoints/bronze/telemetry_raw/"
            ),
            checkpoint_dlq_path=os.getenv(
                "BRONZE_DLQ_CHECKPOINT_PATH", "s3a://checkpoints/bronze/telemetry_dlq/"
            ),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        )


def configure_logging() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def create_spark_session(cfg: JobConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName("StreamBronzeTelemetry")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"))
        .config("spark.hadoop.fs.s3a.endpoint", cfg.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def path_exists(spark: SparkSession, path: str) -> bool:
    try:
        jvm = spark.sparkContext._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        return False


def safe_delta_count(spark: SparkSession, path: str) -> int:
    if not path_exists(spark, path):
        return 0
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return 0


def read_kafka_stream(spark: SparkSession, cfg: JobConfig, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", cfg.starting_offsets)
        .option("failOnDataLoss", cfg.fail_on_data_loss)
        .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
        .load()
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("kafka_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").cast("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("spark_ingest_timestamp"),
        )
    )


def build_raw_bronze_df(df_kafka_raw: DataFrame) -> DataFrame:
    payload_col = F.from_json(F.col("kafka_value"), RAW_PAYLOAD_SCHEMA)

    return (
        df_kafka_raw.withColumn("payload", payload_col)
        .withColumn("payload_parse_ok", F.col("payload").isNotNull())
        .withColumn("raw_payload", F.col("kafka_value"))
        .withColumn(
            "source_type",
            F.coalesce(F.col("payload.source_type"), F.get_json_object(F.col("kafka_value"), "$.source_type")),
        )
        .withColumn(
            "source_file",
            F.coalesce(F.col("payload.source_file"), F.get_json_object(F.col("kafka_value"), "$.source_file")),
        )
        .withColumn(
            "source_row_number",
            F.coalesce(
                F.col("payload.source_row_number"),
                F.get_json_object(F.col("kafka_value"), "$.source_row_number"),
                F.get_json_object(F.col("kafka_value"), "$.source_row"),
            ),
        )
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "spark_ingest_timestamp",
            "kafka_key",
            "raw_payload",
            "payload_parse_ok",
            "source_type",
            "source_file",
            "source_row_number",
            F.col("payload.unit_nr").alias("unit_nr"),
            F.col("payload.event_time").alias("event_time"),
            F.col("payload.time_cycles").alias("time_cycles"),
            F.col("payload.setting_1").alias("setting_1"),
            F.col("payload.setting_2").alias("setting_2"),
            F.col("payload.setting_3").alias("setting_3"),
            F.col("payload.s_1").alias("s_1"),
            F.col("payload.s_2").alias("s_2"),
            F.col("payload.s_3").alias("s_3"),
            F.col("payload.s_4").alias("s_4"),
            F.col("payload.s_5").alias("s_5"),
            F.col("payload.s_6").alias("s_6"),
            F.col("payload.s_7").alias("s_7"),
            F.col("payload.s_8").alias("s_8"),
            F.col("payload.s_9").alias("s_9"),
            F.col("payload.s_10").alias("s_10"),
            F.col("payload.s_11").alias("s_11"),
            F.col("payload.s_12").alias("s_12"),
            F.col("payload.s_13").alias("s_13"),
            F.col("payload.s_14").alias("s_14"),
            F.col("payload.s_15").alias("s_15"),
            F.col("payload.s_16").alias("s_16"),
            F.col("payload.s_17").alias("s_17"),
            F.col("payload.s_18").alias("s_18"),
            F.col("payload.s_19").alias("s_19"),
            F.col("payload.s_20").alias("s_20"),
            F.col("payload.s_21").alias("s_21"),
        )
    )


def build_dlq_bronze_df(df_kafka_dlq: DataFrame) -> DataFrame:
    envelope_col = F.from_json(F.col("kafka_value"), DLQ_ENVELOPE_SCHEMA)

    with_envelope = (
        df_kafka_dlq.withColumn("dlq_envelope", envelope_col)
        .withColumn("dlq_envelope_parse_ok", F.col("dlq_envelope").isNotNull())
        .withColumn("raw_payload", F.coalesce(F.col("dlq_envelope.raw_payload"), F.col("kafka_value")))
        .withColumn(
            "dlq_reason_code",
            F.coalesce(
                F.col("dlq_envelope.dlq_reason_code"),
                F.col("dlq_envelope.error_type"),
                F.get_json_object(F.col("kafka_value"), "$.dlq_reason_code"),
                F.get_json_object(F.col("kafka_value"), "$.error_type"),
            ),
        )
        .withColumn(
            "dlq_reason_detail",
            F.coalesce(
                F.col("dlq_envelope.dlq_reason_detail"),
                F.col("dlq_envelope.error_message"),
                F.get_json_object(F.col("kafka_value"), "$.dlq_reason_detail"),
                F.get_json_object(F.col("kafka_value"), "$.error_message"),
            ),
        )
        .withColumn("failed_fields", F.col("dlq_envelope.failed_fields"))
        .withColumn(
            "source_type",
            F.coalesce(
                F.col("dlq_envelope.source_type"),
                F.get_json_object(F.col("raw_payload"), "$.source_type"),
            ),
        )
        .withColumn(
            "source_file",
            F.coalesce(
                F.col("dlq_envelope.source_file"),
                F.get_json_object(F.col("raw_payload"), "$.source_file"),
            ),
        )
        .withColumn(
            "source_row_number",
            F.coalesce(
                F.col("dlq_envelope.source_row_number"),
                F.get_json_object(F.col("raw_payload"), "$.source_row_number"),
                F.get_json_object(F.col("raw_payload"), "$.source_row"),
            ),
        )
        .withColumn("bridge_received_at", F.col("dlq_envelope.received_at"))
    )

    return with_envelope.select(
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "spark_ingest_timestamp",
        "kafka_key",
        F.col("kafka_value").alias("dlq_envelope_raw"),
        "dlq_envelope_parse_ok",
        "raw_payload",
        "dlq_reason_code",
        "dlq_reason_detail",
        "failed_fields",
        "source_type",
        "source_file",
        "source_row_number",
        "bridge_received_at",
    )


def start_delta_sink(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str,
    query_name: str,
):
    LOGGER.info(
        "Starting query=%s output=%s checkpoint=%s",
        query_name,
        output_path,
        checkpoint_path,
    )

    def _write_and_log(batch_df: DataFrame, batch_id: int) -> None:
        if not batch_df.take(1):
            LOGGER.info("[%s] batch=%d new_rows=0 total_rows=%d", query_name, batch_id, safe_delta_count(batch_df.sparkSession, output_path))
            return

        new_rows = batch_df.count()
        batch_df.write.format("delta").mode("append").option("mergeSchema", "true").save(output_path)
        total_rows = safe_delta_count(batch_df.sparkSession, output_path)
        LOGGER.info("[%s] batch=%d new_rows=%d total_rows=%d", query_name, batch_id, new_rows, total_rows)

    return (
        df.writeStream
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .queryName(query_name)
        .foreachBatch(_write_and_log)
        .start()
    )


def main() -> None:
    configure_logging()
    cfg = JobConfig.from_env()

    LOGGER.info("Bronze telemetry config: raw_topic=%s dlq_topic=%s", cfg.raw_topic, cfg.dlq_topic)
    LOGGER.info("Bronze paths: raw=%s dlq=%s", cfg.bronze_raw_path, cfg.bronze_dlq_path)
    LOGGER.info("Checkpoint paths: raw=%s dlq=%s", cfg.checkpoint_raw_path, cfg.checkpoint_dlq_path)

    spark = create_spark_session(cfg)

    raw_kafka_df = read_kafka_stream(spark, cfg, cfg.raw_topic)
    dlq_kafka_df = read_kafka_stream(spark, cfg, cfg.dlq_topic)

    bronze_raw_df = build_raw_bronze_df(raw_kafka_df)
    bronze_dlq_df = build_dlq_bronze_df(dlq_kafka_df)

    raw_query = start_delta_sink(
        bronze_raw_df,
        cfg.bronze_raw_path,
        cfg.checkpoint_raw_path,
        cfg.trigger_interval,
        "bronze_telemetry_raw",
    )

    dlq_query = start_delta_sink(
        bronze_dlq_df,
        cfg.bronze_dlq_path,
        cfg.checkpoint_dlq_path,
        cfg.trigger_interval,
        "bronze_telemetry_dlq",
    )

    LOGGER.info("Both Bronze streams started. Awaiting termination...")
    spark.streams.awaitAnyTermination()

    # Keep explicit references for clarity and easier troubleshooting.
    _ = (raw_query, dlq_query)


if __name__ == "__main__":
    main()
