# mqtt-to-kafka Predictive Maintenance

Runbook ngắn gọn để chạy pipeline cần thiết và train AI.

## Prerequisites

1. Docker Desktop đang chạy.
2. Ở thư mục gốc project: `mqtt-to-kafka`.

## 1) Start core + build Gold data

```powershell
.\run.ps1 -Action up-core
.\run.ps1 -Action build-train-silver-gold
```

## 2) (Optional) Run ingest + bronze stream

```powershell
.\run.ps1 -Action up-ingest
.\run.ps1 -Action replay -CsvPath "C:/Users/Admin/OneDrive/Documents/mqtt-to-kafka/Data/raw_stream.csv" -ReplayMode event-time -ReplaySpeed 20
.\run.ps1 -Action up-bronze
```

## 3) Setup AI environment

```powershell
Set-Location .\NASA-Turbofan-Predictive-Modeling
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## 4) AI smoke test (optional)

```powershell
python -c "from src.data_utils import CMAPSSData; X_train,y_train,X_test,y_test=CMAPSSData().process_data(); print('X_train',X_train.shape,'y_train',y_train.shape,'X_test',X_test.shape,'y_test',y_test.shape)"
python -c "from src import config; config.EPOCHS=1; import train; train.main()"
```

## 5) Train AI full

```powershell
python train.py
```

If model đã tồn tại và bạn muốn train lại từ đầu:

```powershell
Remove-Item .\model_GOLD_MINIO.keras -Force
python train.py
```

## 6) Shutdown

```powershell
Set-Location ..
.\run.ps1 -Action down-train
.\run.ps1 -Action down-bronze
.\run.ps1 -Action down-ingest
.\run.ps1 -Action down-core
```
