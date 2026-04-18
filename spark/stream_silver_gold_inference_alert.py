"""
End-to-end operations pipeline: Bronze -> Silver -> Gold -> Inference -> Alert.

Flow
----
1. Read Delta Bronze telemetry_raw as a streaming source.
2. Clean / validate / dedup into Silver stream_clean.
3. For each micro-batch (foreachBatch):
   a. Write batch to Silver Delta.
   b. Read full Silver (cross-batch dedup) for downstream.
   c. Compute alerts in realtime (no Active/Ended machine_state).
   d. Run real LSTM inference -> gold/prediction_history + prediction_current.
   e. Compute sensor symptom scores from 25-cycle window.
   f. Compute risk-based alerts with hysteresis -> gold/alert_history + alert_current.
   g. Log pipeline quality metrics -> gold/pipeline_quality.

Alignment
---------
Constants (DROP_COLS, FEATURE_COLS, SEQUENCE_LENGTH, SMOOTHING_ALPHA, RUL_CLIP)
are derived directly from  NASA-Turbofan-Predictive-Modeling/src/config.py  so that
the streaming inference reproduces the exact same preprocessing used during training.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
LOGGER = logging.getLogger("stream_silver_gold")

# ---------------------------------------------------------------------------
# Constants — kept in sync with  src/config.py
# ---------------------------------------------------------------------------
DROP_COLS = ["setting_3", "s_1", "s_5", "s_10", "s_16", "s_18", "s_19"]

ALL_SENSOR_SETTING_COLS = (
    ["setting_1", "setting_2", "setting_3"]
    + [f"s_{i}" for i in range(1, 22)]
)

FEATURE_COLS: List[str] = [
    c for c in ALL_SENSOR_SETTING_COLS if c not in DROP_COLS
]
# ['setting_1','setting_2','s_2','s_3','s_4','s_6','s_7','s_8','s_9',
#  's_11','s_12','s_13','s_14','s_15','s_17','s_20','s_21']  -> 17 features

SEQUENCE_LENGTH = 25
SMOOTHING_ALPHA = 0.1
RUL_CLIP = 125.0

SYMPTOM_SENSORS = ["s_7", "s_12", "s_15", "s_20", "s_21"]

ALERT_LEVELS = ["Normal", "Watch", "Warning", "Critical"]

HYSTERESIS_UP = 2
HYSTERESIS_DOWN = 3


# ═══════════════════════════════════════════════════════════════════════════
# Configuration
# ═══════════════════════════════════════════════════════════════════════════
@dataclass(frozen=True)
class JobConfig:
    bronze_raw_path: str
    silver_stream_clean_path: str
    checkpoint_silver_path: str
    trigger_interval: str
    train_history_csv: str
    model_path: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str

    gold_prediction_history_path: str
    gold_prediction_current_path: str
    gold_alert_history_path: str
    gold_alert_current_path: str
    gold_pipeline_quality_path: str

    @staticmethod
    def from_env() -> "JobConfig":
        return JobConfig(
            bronze_raw_path=os.getenv(
                "BRONZE_RAW_DELTA_PATH", "s3a://lakehouse/bronze/telemetry_raw/"
            ),
            silver_stream_clean_path=os.getenv(
                "SILVER_STREAM_CLEAN_PATH", "s3a://lakehouse/silver/stream_clean/"
            ),
            checkpoint_silver_path=os.getenv(
                "SILVER_STREAM_CHECKPOINT_PATH",
                "s3a://checkpoints/silver/stream_clean/",
            ),
            trigger_interval=os.getenv(
                "SILVER_STREAM_TRIGGER_INTERVAL", "15 seconds"
            ),
            train_history_csv=os.getenv(
                "TRAIN_HISTORY_CSV_PATH", "/app/Data/train_history.csv"
            ),
            model_path=os.getenv(
                "MODEL_PATH",
                "/app/NASA-Turbofan-Predictive-Modeling/model_GOLD_MINIO.keras",
            ),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),

            gold_prediction_history_path=os.getenv(
                "GOLD_PREDICTION_HISTORY_PATH",
                "s3a://lakehouse/gold/prediction_history/",
            ),
            gold_prediction_current_path=os.getenv(
                "GOLD_PREDICTION_CURRENT_PATH",
                "s3a://lakehouse/gold/prediction_current/",
            ),
            gold_alert_history_path=os.getenv(
                "GOLD_ALERT_HISTORY_PATH",
                "s3a://lakehouse/gold/alert_history/",
            ),
            gold_alert_current_path=os.getenv(
                "GOLD_ALERT_CURRENT_PATH",
                "s3a://lakehouse/gold/alert_current/",
            ),
            gold_pipeline_quality_path=os.getenv(
                "GOLD_PIPELINE_QUALITY_PATH",
                "s3a://lakehouse/gold/pipeline_quality/",
            ),
        )


# ═══════════════════════════════════════════════════════════════════════════
# Spark helpers
# ═══════════════════════════════════════════════════════════════════════════
def create_spark_session(cfg: JobConfig) -> SparkSession:
    spark = (
        SparkSession.builder.appName("StreamSilverGoldInferenceAlert")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "4"),
        )
        .config("spark.hadoop.fs.s3a.endpoint", cfg.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", cfg.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", cfg.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def delta_path_exists(spark: SparkSession, path: str) -> bool:
    try:
        jvm = spark.sparkContext._jvm
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        uri = jvm.java.net.URI(path)
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        return False


def safe_delta_count(spark: SparkSession, path: str) -> int:
    if not delta_path_exists(spark, path):
        return 0
    try:
        return spark.read.format("delta").load(path).count()
    except Exception:
        return 0


# ═══════════════════════════════════════════════════════════════════════════
# Inference Engine — loads the real trained LSTM model
# Computes symptom scores and predicted RUL for units when streaming >= SEQUENCE_LENGTH cycles
# ═══════════════════════════════════════════════════════════════════════════
class InferenceEngine:


    def __init__(self, model_path: str, train_csv_path: str) -> None:
        self.model: Any = None
        self.scaler = MinMaxScaler()
        self.train_baselines: Dict[str, Dict[str, float]] = {}
        self._fit_scaler_and_baselines(train_csv_path)
        self._load_model(model_path)

    # ── scaler + baselines ────────────────────────────────────────────────
    def _fit_scaler_and_baselines(self, csv_path: str) -> None:
        LOGGER.info("Fitting scaler from %s", csv_path)
        df = pd.read_csv(csv_path)
        df = df.sort_values(["unit_nr", "time_cycles"]).reset_index(drop=True)
    
        raw = df[FEATURE_COLS]

        for sensor in SYMPTOM_SENSORS:
            self.train_baselines[sensor] = {
                "mean": float(raw[sensor].mean()),
                "std": max(float(raw[sensor].std()), 1e-8),
            }

        # EWM smoothing must match training preprocessing so the scaler is fit on the same distribution.
        smoothed = raw.copy()
        for col in FEATURE_COLS:
            smoothed[col] = smoothed[col].ewm(alpha=SMOOTHING_ALPHA).mean()

        self.scaler.fit(smoothed.values)
        LOGGER.info(
            "Scaler fitted: %d rows, %d features", len(smoothed), len(FEATURE_COLS)
        )

    # ── model loading ─────────────────────────────────────────────────────
    def _load_model(self, model_path: str) -> None:
        if not os.path.exists(model_path):
            LOGGER.warning("Model not found at %s - heuristic fallback", model_path)
            return

        try:
            os.environ.setdefault("TF_CPP_MIN_LOG_LEVEL", "2")
            import keras

            self.model = keras.models.load_model(model_path, compile=False)
            LOGGER.info("Loaded model directly, input_shape=%s", self.model.input_shape)
        except Exception as exc:
            LOGGER.warning("Cannot load model directly (%s) - heuristic fallback", exc)

    # predict for units when streaming >= SEQUENCE_LENGTH cycles
    def predict_for_units(
        self, silver_valid_pdf: pd.DataFrame
    ) -> pd.DataFrame:
        """Return one prediction row per unit that has >= SEQUENCE_LENGTH cycles."""
        results: List[Dict[str, Any]] = []
        now_ts = pd.Timestamp.utcnow().isoformat()

        for unit_nr, grp in silver_valid_pdf.groupby("unit_nr"):
            grp = grp.sort_values("time_cycles").reset_index(drop=True)
            if len(grp) < SEQUENCE_LENGTH:
                continue

            window_raw = grp.tail(SEQUENCE_LENGTH).copy()
            w_start = int(window_raw["time_cycles"].iloc[0])
            w_end = int(window_raw["time_cycles"].iloc[-1])

            symptom_score, symptom_details = self._compute_symptom(window_raw)
            predicted_rul, model_ver = self._predict_single(grp)

            results.append(
                {
                    "unit_nr": int(unit_nr),
                    "prediction_time": now_ts,
                    "window_start_cycle": w_start,
                    "window_end_cycle": w_end,
                    "predicted_rul": round(predicted_rul, 2),
                    "symptom_score": round(symptom_score, 2),
                    "symptom_details_json": json.dumps(symptom_details),
                    "model_version": model_ver,
                }
            )

        cols = [
            "unit_nr",
            "prediction_time",
            "window_start_cycle",
            "window_end_cycle",
            "predicted_rul",
            "symptom_score",
            "symptom_details_json",
            "model_version",
        ]
        return pd.DataFrame(results, columns=cols) if results else pd.DataFrame(columns=cols)

    # predict for a single unit
    def _predict_single(self, unit_df: pd.DataFrame) -> Tuple[float, str]:
        features = unit_df[FEATURE_COLS].copy()
        for col in FEATURE_COLS:
            features[col] = features[col].ewm(alpha=SMOOTHING_ALPHA).mean()

        scaled = self.scaler.transform(features.values)
        scaled = np.nan_to_num(scaled, nan=0.0, posinf=1.0, neginf=0.0)
        X = scaled[-SEQUENCE_LENGTH:].reshape(1, SEQUENCE_LENGTH, len(FEATURE_COLS))

        if self.model is not None:
            pred = float(self.model.predict(X, verbose=0).flatten()[0])
            return max(0.0, min(pred, RUL_CLIP)), "lstm-v1"

        cycle = float(unit_df["time_cycles"].iloc[-1])
        return max(0.0, RUL_CLIP - cycle * 0.8), "heuristic-v1"

    # ── symptom scoring ───────────────────────────────────────────────────
    def _compute_symptom(
        self, window: pd.DataFrame
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Per SYMPTOM_SENSOR in the 25-cycle window compute:
          - deviation  : distance of window mean from training baseline (σ units)
          - trend      : linear slope over window, normalised by training σ
          - volatility : window σ / training σ
        Aggregate into symptom_score ∈ [0, 100].
        """
        details: Dict[str, Any] = {}
        scores: List[float] = []

        for sensor in SYMPTOM_SENSORS:
            if sensor not in window.columns:
                continue
            vals = window[sensor].values.astype(np.float64)
            bl = self.train_baselines.get(sensor, {"mean": 0.0, "std": 1.0})
            t_mean, t_std = bl["mean"], bl["std"]

            w_mean = float(np.mean(vals))
            w_std = float(np.std(vals))
            deviation = min(abs(w_mean - t_mean) / t_std, 5.0) / 5.0

            try:
                slope = float(np.polyfit(np.arange(len(vals), dtype=float), vals, 1)[0])
            except Exception:
                slope = 0.0
            trend = min(abs(slope * SEQUENCE_LENGTH) / t_std, 5.0) / 5.0
            volatility = min(w_std / t_std, 3.0) / 3.0

            s_score = (0.4 * deviation + 0.4 * trend + 0.2 * volatility) * 100.0
            s_score = min(max(s_score, 0.0), 100.0)

            details[sensor] = {
                "deviation": round(deviation * 100, 2),
                "trend": round(trend * 100, 2),
                "volatility": round(volatility * 100, 2),
                "score": round(s_score, 2),
            }
            scores.append(s_score)

        agg = float(np.mean(scores)) if scores else 0.0
        return round(agg, 2), details


# ═══════════════════════════════════════════════════════════════════════════
# Silver layer
# ═══════════════════════════════════════════════════════════════════════════
def build_silver_stream(bronze_stream_df: DataFrame) -> DataFrame:
    """Bronze -> Silver: cast types, validate, dedup within micro-batch."""
    typed = (
        bronze_stream_df.withColumn("unit_nr_long", F.col("unit_nr").cast("long"))
        .withColumn("time_cycles_long", F.col("time_cycles").cast("long"))
        .withColumn("event_time_ts", F.to_timestamp("event_time"))
    )
    for c in ALL_SENSOR_SETTING_COLS:
        typed = typed.withColumn(c, F.col(c).cast("double"))

    errs = F.array_compact(
        F.array(
            F.when(F.col("payload_parse_ok") != F.lit(True), F.lit("payload_parse_failed")),
            F.when(F.col("unit_nr_long").isNull(), F.lit("invalid_unit_nr")),
            F.when(F.col("time_cycles_long").isNull(), F.lit("invalid_time_cycles")),
            F.when(F.col("time_cycles_long") <= 0, F.lit("non_positive_cycle")),
            F.when(F.col("event_time_ts").isNull(), F.lit("invalid_event_time")),
        )
    )

    silver = (
        typed.withColumn("validation_errors", errs)
        .withColumn("is_valid", F.size("validation_errors") == 0)
        .withColumn("event_date", F.to_date("event_time_ts"))
        .select(
            F.col("unit_nr_long").alias("unit_nr"),
            F.col("time_cycles_long").alias("time_cycles"),
            F.col("event_time_ts").alias("event_time"),
            *[F.col(c) for c in ALL_SENSOR_SETTING_COLS],
            "is_valid",
            "validation_errors",
            "event_date",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "spark_ingest_timestamp",
            "source_type",
            "source_file",
            "source_row_number",
        )
    )

    return silver.dropDuplicates(["unit_nr", "time_cycles"])


def dedup_silver_cross_batch(df: DataFrame) -> DataFrame:

    w = Window.partitionBy("unit_nr", "time_cycles").orderBy(
        F.col("kafka_timestamp").desc(), F.col("kafka_offset").desc()
    )
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


# ═══════════════════════════════════════════════════════════════════════════
# Alert scoring helpers  (pure Python — called on pandas DataFrames)
# ═══════════════════════════════════════════════════════════════════════════
def _rul_score(predicted_rul: float) -> float:
    """Lower RUL -> higher danger score [0, 100]."""
    if predicted_rul >= 50.0:
        return 0.0
    if predicted_rul <= 5.0:
        return 100.0
    return (50.0 - predicted_rul) / 45.0 * 100.0


def _trend_score(rul_values: List[float]) -> float:
    """Steeper RUL decline -> higher score [0, 100]."""
    if len(rul_values) < 2:
        return 0.0
    x = np.arange(len(rul_values), dtype=float)
    try:
        slope = float(np.polyfit(x, rul_values, 1)[0])
    except Exception:
        return 0.0
    return min(max(-slope / 5.0, 0.0), 1.0) * 100.0


def _risk_score(rul_s: float, trend_s: float, symptom_s: float) -> float:
    return 0.55 * rul_s + 0.25 * trend_s + 0.20 * symptom_s


def _level_from_risk(risk: float) -> str:
    if risk >= 75:
        return "Critical"
    if risk >= 50:
        return "Warning"
    if risk >= 25:
        return "Watch"
    return "Normal"


def _apply_hysteresis(
    raw_level: str, prev_level: str, prev_pending: int
) -> Tuple[str, int]:
    if raw_level == prev_level:
        return raw_level, 0
    raw_idx = ALERT_LEVELS.index(raw_level)
    prev_idx = ALERT_LEVELS.index(prev_level)
    threshold = HYSTERESIS_UP if raw_idx > prev_idx else HYSTERESIS_DOWN
    new_pending = prev_pending + 1
    if new_pending >= threshold:
        return raw_level, 0
    return prev_level, new_pending


# ═══════════════════════════════════════════════════════════════════════════
# Pipeline quality
# ═══════════════════════════════════════════════════════════════════════════
def write_pipeline_quality(
    silver: DataFrame, batch_id: int, path: str
) -> None:
    row = (
        silver.agg(
            F.count(F.lit(1)).alias("silver_total_records"),
            F.sum(F.when(F.col("is_valid") == F.lit(True), 1).otherwise(0)).alias(
                "silver_valid_records"
            ),
            F.sum(F.when(F.col("is_valid") == F.lit(False), 1).otherwise(0)).alias(
                "silver_invalid_records"
            ),
            F.countDistinct("unit_nr", "time_cycles").alias(
                "silver_distinct_keys"
            ),
        )
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("quality_time", F.current_timestamp())
    )
    row.write.format("delta").mode("append").save(path)


# ═══════════════════════════════════════════════════════════════════════════
# Main batch processor
# ═══════════════════════════════════════════════════════════════════════════
def process_gold_batch(
    batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    cfg: JobConfig,
    engine: InferenceEngine,
) -> None:
    if not batch_df.take(1):
        return

    bronze_in_batch = batch_df.count()

    # ── 1. Write micro-batch to Silver Delta ──────────────────────────────
    batch_df.write.format("delta").mode("append").save(
        cfg.silver_stream_clean_path
    )

    # ── 2. Read full Silver (cross-batch dedup) ──────────────────────────
    silver_all = spark.read.format("delta").load(cfg.silver_stream_clean_path)
    silver_deduped = dedup_silver_cross_batch(silver_all)
    silver_valid = silver_deduped.filter(F.col("is_valid") == F.lit(True))
    silver_total = silver_deduped.count()
    silver_valid_total = silver_valid.count()

    # ── 3. Collect Silver valid to pandas for inference ───────────────────
    select_cols = ["unit_nr", "time_cycles"] + FEATURE_COLS
    silver_pdf = silver_valid.select(*select_cols).toPandas()
    if silver_pdf.empty:
        write_pipeline_quality(silver_deduped, batch_id, cfg.gold_pipeline_quality_path)
        return

    # ── 4. Run inference (real LSTM or heuristic fallback) ────────────────
    new_preds_pdf = engine.predict_for_units(silver_pdf)
    if new_preds_pdf.empty:
        write_pipeline_quality(silver_deduped, batch_id, cfg.gold_pipeline_quality_path)
        return

    # ── 5. Append to prediction_history (dedup by unit+cycle) ─────────────
    if delta_path_exists(spark, cfg.gold_prediction_history_path):
        existing_keys = (
            spark.read.format("delta")
            .load(cfg.gold_prediction_history_path)
            .select("unit_nr", "window_end_cycle")
            .distinct()
            .toPandas()
        )
        ek_set = set(
            zip(
                existing_keys["unit_nr"].astype(int),
                existing_keys["window_end_cycle"].astype(int),
            )
        )
        truly_new = new_preds_pdf[
            ~new_preds_pdf.apply(
                lambda r: (int(r["unit_nr"]), int(r["window_end_cycle"])) in ek_set,
                axis=1,
            )
        ]
    else:
        truly_new = new_preds_pdf

    if not truly_new.empty:
        spark.createDataFrame(truly_new).write.format("delta").mode(
            "append"
        ).save(cfg.gold_prediction_history_path)

    # ── 6. Build prediction_current ───────────────────────────────────────
    if not delta_path_exists(spark, cfg.gold_prediction_history_path):
        write_pipeline_quality(silver_deduped, batch_id, cfg.gold_pipeline_quality_path)
        return

    pred_history_total = safe_delta_count(spark, cfg.gold_prediction_history_path)
    all_hist_pdf = (
        spark.read.format("delta")
        .load(cfg.gold_prediction_history_path)
        .toPandas()
    )
    pred_cur_rows: List[Dict[str, Any]] = []
    for unit_nr, grp in all_hist_pdf.groupby("unit_nr"):
        grp = grp.sort_values("prediction_time", ascending=False)
        latest = grp.iloc[0]
        recent_ruls = grp.head(5)["predicted_rul"].tolist()
        recent_ruls_asc = list(reversed(recent_ruls))
        t_score = _trend_score(recent_ruls_asc)
        pred_cur_rows.append(
            {
                "unit_nr": int(unit_nr),
                "window_end_cycle": int(latest["window_end_cycle"]),
                "predicted_rul": float(latest["predicted_rul"]),
                "symptom_score": float(latest["symptom_score"]),
                "avg_rul_5": float(np.mean(recent_ruls)),
                "rul_trend_5": round(
                    float(max(recent_ruls) - min(recent_ruls)), 2
                ),
                "trend_score": round(t_score, 2),
                "last_prediction_time": str(latest["prediction_time"]),
                "updated_at": pd.Timestamp.utcnow().isoformat(),
            }
        )
    pred_cur_pdf = pd.DataFrame(pred_cur_rows)
    if not pred_cur_pdf.empty:
        spark.createDataFrame(pred_cur_pdf).write.format("delta").mode(
            "overwrite"
        ).option("overwriteSchema", "true").save(
            cfg.gold_prediction_current_path
        )
    pred_current_total = len(pred_cur_pdf)

    # ── 7. Load previous alert_current for hysteresis ─────────────────────
    # prev_alert_map: unit_nr -> {alert_level, pending_count}
    prev_alert_map: Dict[int, Dict[str, Any]] = {}
    if delta_path_exists(spark, cfg.gold_alert_current_path):
        pa_pdf = (
            spark.read.format("delta")
            .load(cfg.gold_alert_current_path)
            .toPandas()
        )
        for _, r in pa_pdf.iterrows():
            prev_alert_map[int(r["unit_nr"])] = {
                "alert_level": str(r.get("alert_level", "Normal")),
                "pending_count": int(r.get("pending_count", 0)),
            }

    # ── 8. Compute alerts ─────────────────────────────────────────────────
    alert_hist_rows: List[Dict[str, Any]] = []
    alert_cur_rows: List[Dict[str, Any]] = []
    now_iso = pd.Timestamp.utcnow().isoformat()

    for _, pr in pred_cur_pdf.iterrows():
        uid = int(pr["unit_nr"])
        prev = prev_alert_map.get(uid, {"alert_level": "Normal", "pending_count": 0})

        rul_s = _rul_score(float(pr["predicted_rul"]))
        trend_s = float(pr["trend_score"])
        sym_s = float(pr["symptom_score"])
        risk = _risk_score(rul_s, trend_s, sym_s)
        raw_lvl = _level_from_risk(risk)

        confirmed, pending = _apply_hysteresis(
            raw_lvl, prev["alert_level"], prev["pending_count"]
        )

        reason = {
            "predicted_rul": float(pr["predicted_rul"]),
            "avg_rul_5": float(pr["avg_rul_5"]),
            "rul_score": round(rul_s, 2),
            "trend_score": round(trend_s, 2),
            "symptom_score": round(sym_s, 2),
            "risk_score": round(risk, 2),
            "raw_level": raw_lvl,
            "confirmed_level": confirmed,
        }

        alert_hist_rows.append(
            {
                "unit_nr": uid,
                "alert_time": now_iso,
                "alert_level": confirmed,
                "risk_score": round(risk, 2),
                "rul_score": round(rul_s, 2),
                "trend_score": round(trend_s, 2),
                "symptom_score": round(sym_s, 2),
                "reason_json": json.dumps(reason),
            }
        )
        alert_cur_rows.append(
            {
                "unit_nr": uid,
                "alert_level": confirmed,
                "risk_score": round(risk, 2),
                "rul_score": round(rul_s, 2),
                "trend_score": round(trend_s, 2),
                "symptom_score": round(sym_s, 2),
                "pending_count": pending,
                "reason_json": json.dumps(reason),
                "updated_at": now_iso,
            }
        )

    # ── 9. Write alert tables ────────────────────────────────────────────
    if alert_hist_rows:
        spark.createDataFrame(pd.DataFrame(alert_hist_rows)).write.format(
            "delta"
        ).mode("append").save(cfg.gold_alert_history_path)

    if alert_cur_rows:
        spark.createDataFrame(pd.DataFrame(alert_cur_rows)).write.format(
            "delta"
        ).mode("overwrite").option("overwriteSchema", "true").save(
            cfg.gold_alert_current_path
        )
    alert_history_total = safe_delta_count(spark, cfg.gold_alert_history_path)
    alert_current_total = len(alert_cur_rows)
    machines_total = len(pred_cur_pdf)

    # ── 10. Pipeline quality ──────────────────────────────────────────────
    write_pipeline_quality(
        silver_deduped, batch_id, cfg.gold_pipeline_quality_path
    )

    LOGGER.info(
        (
            "batch=%d bronze_in_batch=%d silver_total=%d silver_valid=%d "
            "machines(total=%d) "
            "preds(new=%d,total_hist=%d,current=%d) "
            "alerts(new=%d,total_hist=%d,current=%d)"
        ),
        batch_id,
        bronze_in_batch,
        silver_total,
        silver_valid_total,
        machines_total,
        len(truly_new) if not truly_new.empty else 0,
        pred_history_total,
        pred_current_total,
        len(alert_hist_rows),
        alert_history_total,
        alert_current_total,
    )


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════
def main() -> None:
    cfg = JobConfig.from_env()
    spark = create_spark_session(cfg)

    LOGGER.info("Initialising inference engine (model=%s)", cfg.model_path)
    engine = InferenceEngine(cfg.model_path, cfg.train_history_csv)

    LOGGER.info("Starting Bronze -> Silver streaming query")
    bronze_stream = spark.readStream.format("delta").load(cfg.bronze_raw_path)
    silver_stream = build_silver_stream(bronze_stream)

    query = (
        silver_stream.writeStream.outputMode("append")
        .option("checkpointLocation", cfg.checkpoint_silver_path)
        .trigger(processingTime=cfg.trigger_interval)
        .foreachBatch(
            lambda df, bid: process_gold_batch(
                df, bid, spark, cfg, engine
            )
        )
        .start()
    )

    LOGGER.info("Streaming started — awaiting termination")
    spark.streams.awaitAnyTermination()
    _ = query


if __name__ == "__main__":
    main()
