import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    precision_recall_fscore_support,
    r2_score,
)
from tensorflow.keras.models import load_model

from src import config
from src.data_utils import CMAPSSData


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate trained RUL model on FD001 test set")
    parser.add_argument(
        "--test-path",
        default=r"c:\Users\Admin\OneDrive\Documents\test_FD001.txt",
        help="Path to test_FD001.txt",
    )
    parser.add_argument(
        "--rul-path",
        default=r"c:\Users\Admin\OneDrive\Documents\RUL_FD001.txt",
        help="Path to RUL_FD001.txt",
    )
    parser.add_argument(
        "--model-path",
        default="model_GOLD_MINIO.keras",
        help="Path to trained keras model",
    )
    parser.add_argument(
        "--alert-threshold",
        type=float,
        default=35.0,
        help="Alert threshold on predicted RUL (<= threshold = alert)",
    )
    return parser.parse_args()


def load_fd001_test(test_path: str) -> pd.DataFrame:
    cols = ["unit_nr", "time_cycles", "setting_1", "setting_2", "setting_3"]
    cols += [f"s_{i}" for i in range(1, 22)]
    return pd.read_csv(test_path, sep=r"\s+", header=None, names=cols)


def nasa_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    diff = y_pred - y_true
    pos = np.exp(diff[diff >= 0] / 10.0) - 1.0
    neg = np.exp(-diff[diff < 0] / 13.0) - 1.0
    return float(pos.sum() + neg.sum())


def prepare_feature_pipeline(loader: CMAPSSData) -> list:
    try:
        train_df = loader._load_minio_gold_dataframe().copy()
    except Exception:
        fallback = Path(__file__).resolve().parents[1] / "Data" / "train_history.csv"
        if not fallback.exists():
            raise
        train_df = pd.read_csv(fallback)

    train_df.drop(columns=[loader.target_col, "RUL_clipped", "split"], errors="ignore", inplace=True)
    train_df.drop(columns=config.DROP_COLS, errors="ignore", inplace=True)

    feature_cols = [
        c
        for c in train_df.columns
        if c not in {loader.id_col, loader.cycle_col, loader.target_col}
    ]
    if not feature_cols:
        raise ValueError("No feature columns available after preprocessing")

    for c in feature_cols:
        train_df[c] = pd.to_numeric(train_df[c], errors="coerce")
        train_df[c] = train_df[c].ewm(alpha=config.SMOOTHING_ALPHA).mean()

    train_df = train_df.dropna(subset=feature_cols)
    loader.scaler.fit(train_df[feature_cols])
    return feature_cols


def build_test_windows(loader: CMAPSSData, test_df: pd.DataFrame, feature_cols: list):
    test_df = test_df.copy()
    test_df.drop(columns=config.DROP_COLS, errors="ignore", inplace=True)

    required = [loader.id_col, loader.cycle_col] + feature_cols
    missing = [c for c in required if c not in test_df.columns]
    if missing:
        raise ValueError(f"Missing required columns in test data: {missing}")

    for c in feature_cols:
        test_df[c] = pd.to_numeric(test_df[c], errors="coerce")
        test_df[c] = test_df[c].ewm(alpha=config.SMOOTHING_ALPHA).mean()

    test_df = test_df.dropna(subset=feature_cols)
    test_df[feature_cols] = loader.scaler.transform(test_df[feature_cols])

    seq_len = config.SEQUENCE_LENGTH
    X_test = []
    units = []

    for unit in sorted(test_df[loader.id_col].unique()):
        u_df = test_df[test_df[loader.id_col] == unit].sort_values(loader.cycle_col)
        if len(u_df) < seq_len:
            continue
        X_test.append(u_df[feature_cols].values[-seq_len:])
        units.append(int(unit))

    if not X_test:
        raise ValueError("No test windows were generated. Check sequence length and input data.")

    return np.array(X_test), units


def load_truth(rul_path: str, units: list) -> np.ndarray:
    y = pd.read_csv(rul_path, sep=r"\s+", header=None, names=["RUL"])["RUL"].astype(float).to_numpy()
    if len(units) > len(y):
        raise ValueError("Ground truth rows are fewer than available test units")
    return y[: len(units)]


def print_metrics(y_true: np.ndarray, y_pred: np.ndarray, alert_threshold: float) -> None:
    rmse = math.sqrt(mean_squared_error(y_true, y_pred))
    mae = mean_absolute_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)
    mape = float(np.mean(np.abs((y_true - y_pred) / np.maximum(y_true, 1e-8))) * 100.0)
    ns = nasa_score(y_true, y_pred)

    yt = (y_true <= alert_threshold).astype(int)
    yp = (y_pred <= alert_threshold).astype(int)
    precision, recall, f1, _ = precision_recall_fscore_support(
        yt,
        yp,
        average="binary",
        zero_division=0,
    )

    print("===== FD001 Evaluation =====")
    print(f"RMSE : {rmse:.4f}")
    print(f"MAE  : {mae:.4f}")
    print(f"R2   : {r2:.4f}")
    print(f"MAPE : {mape:.4f}%")
    print(f"NASA Score : {ns:.4f}")
    print(f"Alert threshold (RUL <= {alert_threshold:.1f})")
    print(f"Precision: {precision:.4f}")
    print(f"Recall   : {recall:.4f}")
    print(f"F1-score : {f1:.4f}")


def main() -> None:
    args = parse_args()

    loader = CMAPSSData()
    feature_cols = prepare_feature_pipeline(loader)

    test_df = load_fd001_test(args.test_path)
    X_test, units = build_test_windows(loader, test_df, feature_cols)
    y_true = load_truth(args.rul_path, units)

    model = load_model(args.model_path)
    y_pred = model.predict(X_test, verbose=0).reshape(-1)

    print_metrics(y_true, y_pred, args.alert_threshold)


if __name__ == "__main__":
    main()
