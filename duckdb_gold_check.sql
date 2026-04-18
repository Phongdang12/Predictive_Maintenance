-- DuckDB quick check for Gold Delta tables on MinIO
-- Run in VSCode DuckDB extension (or duckdb CLI)

INSTALL httpfs;
LOAD httpfs;
INSTALL delta;
LOAD delta;

-- MinIO S3-compatible settings
-- Use a named S3 secret so DuckDB does not try AWS metadata credentials.
CREATE OR REPLACE SECRET minio_lakehouse (
  TYPE S3,
  KEY_ID 'minioadmin',
  SECRET 'minioadmin123',
  REGION 'us-east-1',
  ENDPOINT 'localhost:9000',
  USE_SSL false,
  URL_STYLE 'path'
);

-- Keep these as explicit session settings (some clients still consult them).
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin123';
SET s3_use_ssl=false;
SET s3_url_style='path';
SET s3_region='us-east-1';

-- Optional: speed up local analysis
PRAGMA threads=4;



SELECT * FROM delta_scan('s3://lakehouse/gold/alert_current/') LIMIT 200;
SELECT * FROM delta_scan('s3://lakehouse/gold/alert_history/') LIMIT 200;
SELECT * FROM delta_scan('s3://lakehouse/gold/pipeline_quality/') LIMIT 200;
SELECT * FROM delta_scan('s3://lakehouse/gold/prediction_current/') LIMIT 200;
SELECT * FROM delta_scan('s3://lakehouse/gold/prediction_history/') LIMIT 200;
