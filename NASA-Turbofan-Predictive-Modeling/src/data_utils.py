# src/data_utils.py
import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from . import config

try:
    from deltalake import DeltaTable
except Exception:
    DeltaTable = None

class CMAPSSData:
    def __init__(self):
        self.scaler = MinMaxScaler()
        self.id_col = 'unit_nr'
        self.cycle_col = 'time_cycles'
        self.target_col = 'RUL'

    def _prepare_train_data(self, train_df):
        train_df = train_df.copy()
        train_df.drop(columns=config.DROP_COLS, inplace=True, errors='ignore')

        feature_cols = [c for c in train_df.columns if c not in [self.id_col, self.cycle_col, self.target_col]]
        if not feature_cols:
            raise ValueError('No feature columns remain after preprocessing.')

        for col in feature_cols:
            train_df[col] = train_df[col].ewm(alpha=config.SMOOTHING_ALPHA).mean()

        train_df[feature_cols] = self.scaler.fit_transform(train_df[feature_cols])
        X_train, y_train = self._gen_train_seq(train_df, config.SEQUENCE_LENGTH, feature_cols)
        return X_train, y_train

    def _load_minio_gold_dataframe(self):
        if DeltaTable is None:
            raise ImportError(
                "Missing dependency 'deltalake'. Install with: pip install deltalake"
            )

        storage_options = {
            'AWS_ACCESS_KEY_ID': config.MINIO_ACCESS_KEY,
            'AWS_SECRET_ACCESS_KEY': config.MINIO_SECRET_KEY,
            'AWS_REGION': config.MINIO_REGION,
            'AWS_ENDPOINT_URL': config.MINIO_ENDPOINT_URL,
            'AWS_ALLOW_HTTP': 'true' if config.MINIO_ALLOW_HTTP else 'false',
        }

        dt = DeltaTable(config.MINIO_GOLD_DELTA_PATH, storage_options=storage_options)
        df = dt.to_pandas()
        if df.empty:
            raise ValueError(f"Gold Delta table is empty: {config.MINIO_GOLD_DELTA_PATH}")

        required = [self.id_col, self.cycle_col, 'setting_1', 'setting_2', 'setting_3'] + [f's_{i}' for i in range(1, 22)]
        if 'RUL_clipped' in df.columns:
            target_col = 'RUL_clipped'
        elif 'RUL' in df.columns:
            target_col = 'RUL'
        else:
            raise ValueError('Missing required target column in MinIO Gold Delta: expected RUL_clipped or RUL')
        required.append(target_col)

        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns in MinIO Gold Delta: {missing}")

        if target_col != self.target_col and self.target_col in df.columns:
            df = df.drop(columns=[self.target_col])
        df = df.rename(columns={target_col: self.target_col})
        keep_cols = [
            self.id_col,
            self.cycle_col,
            'setting_1',
            'setting_2',
            'setting_3',
            self.target_col,
            *[f's_{i}' for i in range(1, 22)],
        ]
        if 'split' in df.columns:
            keep_cols.append('split')
        df = df[keep_cols].copy()

        for c in [col for col in df.columns if col != 'split']:
            df[c] = pd.to_numeric(df[c], errors='coerce')

        df = df.dropna().sort_values([self.id_col, self.cycle_col]).reset_index(drop=True)
        return df

    def _process_minio_gold_data(self):
        train_df = self._load_minio_gold_dataframe()
        if 'split' in train_df.columns:
            train_df = train_df.drop(columns=['split'])

        if train_df.empty:
            raise ValueError("Gold Delta table produced empty train set.")

        return self._prepare_train_data(train_df)

    def process_data(self):
        return self._process_minio_gold_data()

    def _gen_train_seq(self, df, seq_length, cols):
        num_units = df[self.id_col].unique()
        seq_gen = []
        label_gen = []
        
        for unit in num_units:
            u_data = df[df[self.id_col] == unit]
            if len(u_data) < seq_length: continue
            
            u_val = u_data[cols].values
            u_rul = u_data[self.target_col].values
            num_elements = u_val.shape[0]
            
            for start, stop in zip(range(0, num_elements-seq_length), range(seq_length, num_elements)):
                seq_gen.append(u_val[start:stop, :])
                label_gen.append(u_rul[stop])
                
        return np.array(seq_gen), np.array(label_gen)
