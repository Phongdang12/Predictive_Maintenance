# src/config.py

# Keep one clear source-of-truth idea only: train directly from Gold on MinIO.
DATA_SOURCE = 'minio_gold'

# MinIO Gold (Delta) integration for direct AI loader consumption.
# Use s3:// URI for Delta reader (not s3a://).
MINIO_GOLD_DELTA_PATH = 's3://lakehouse/gold/train_rul/'
MINIO_ENDPOINT_URL = 'http://localhost:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin123'
MINIO_REGION = 'us-east-1'
MINIO_ALLOW_HTTP = True

DROP_COLS = ['setting_3', 's_1', 's_5', 's_10', 's_16', 's_18', 's_19']

# Preprocessing Config
RUL_CLIP = 125
SMOOTHING_ALPHA = 0.1  # Exponential Weighted Moving Average
SEQUENCE_LENGTH = 25   # Window size

# Model Config
EPOCHS = 30
BATCH_SIZE = 32