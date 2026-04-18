# Predictive Maintenance — NASA CMAPSS FD001

Luồng: **MQTT → Kafka → Bronze / Silver / Gold (Delta trên MinIO) → LSTM inference & cảnh báo → Postgres → Grafana / Superset**.

**Kiến trúc end-to-end** (sơ đồ, lớp dữ liệu, AI, profiles): xem [`ARCHITECTURE.md`](ARCHITECTURE.md).

---

## Chuẩn bị

| File | Vai trò |
|------|---------|
| `Data/train_history.csv` | Fit scaler / baseline cho inference |
| `Data/raw_streaming.csv` | Replay vào pipeline |
| `NASA-Turbofan-Predictive-Modeling/model_GOLD_MINIO.keras` | Model LSTM |

---

## Build image Ops (khi mới clone hoặc đổi `Dockerfile.ops`)

```powershell
docker compose --profile core --profile bronze --profile ops build --no-cache silver-gold-inference-alert
```

---

## Chạy pipeline (thứ tự bắt buộc)

1. **Core + ingest** (EMQX, Kafka, bridge MQTT → Kafka)

   ```powershell
   .\run.ps1 -Action up-ingest
   ```

2. **Bronze** — Spark đọc Kafka → MinIO Bronze Delta

   ```powershell
   .\run.ps1 -Action up-bronze
   ```

3. **Replay** CSV qua MQTT

   ```powershell
   .\run.ps1 -Action replay -CsvPath "Data/raw_streaming.csv" -ReplayMode event-time -ReplaySpeed 0.2
   ```

4. **Ops** — Silver → Gold → inference → alert

   ```powershell
   .\run.ps1 -Action up-ops
   ```

   Theo dõi log:

   ```powershell
   .\run.ps1 -Action logs -Service silver-gold-inference-alert -Follow
   ```

   Khi ổn, Gold trên MinIO gồm các prefix: `prediction_history`, `prediction_current`, `alert_history`, `alert_current`, `pipeline_quality`.

5. **Dashboard** — Postgres, gold-sync, Grafana, Superset (bootstrap charts)

   ```powershell
   .\run.ps1 -Action up-dashboard
   ```

6. **Làm mới metadata Superset** — chạy **sau** khi warehouse đã có dữ liệu (ít nhất một vòng sync từ gold-sync)

   ```powershell
   .\run.ps1 -Action refresh-superset
   ```

**Luồng dữ liệu dashboard:** `MinIO Gold Delta → gold-sync (DuckDB) → Postgres → Grafana / Superset`

---

## URL & đăng nhập

| Công cụ | URL | User | Password |
|---------|-----|------|----------|
| Grafana | http://localhost:3000 | admin | admin |
| Superset | http://localhost:8088 | admin | admin |

---

## Grafana vs Superset

- **Grafana — PDM Gold Overview** (refresh ~5s): KPI fleet, phân bố cảnh báo, xu hướng Warning/Critical, Top RUL/risk. Bảng **Investigate Entry Point**: click `unit_nr` mở Superset **Machine Investigation**, sau đó **chọn máy ở bộ lọc bên trái** trên Superset (không truyền filter qua URL).
- **Superset — Machine Investigation**: KPI theo máy, lịch sử RUL/risk, breakdown symptom. Charts được tạo khi `up-dashboard`.

---

## Không thấy dữ liệu (“No data”)

1. Ops có ghi Gold? → `.\run.ps1 -Action logs -Service silver-gold-inference-alert -Follow`
2. gold-sync đọc MinIO? → `.\run.ps1 -Action logs -Service gold-sync -Follow`
3. Postgres có bảng `gold.*`?

   ```powershell
   docker exec -it predictive_maintenance-dashboard-db-1 psql -U pdm -d pdm_dashboard -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname='gold' ORDER BY tablename;"
   ```

4. SQL Lab (database `Gold Warehouse`, schema `gold`): `SELECT COUNT(*) FROM gold.gold_prediction_current;`
5. Đã chạy `refresh-superset` sau khi có dữ liệu?
6. Grafana cache: `docker compose --profile core --profile bronze --profile dashboard restart grafana`

---

## Cổng (host)

| Port | Dịch vụ |
|------|---------|
| 18831 | EMQX MQTT (trong cluster bridge dùng `emqx:1883`) |
| 18083 | EMQX Dashboard |
| 9092 | Kafka |
| 8080 | Kafka UI |
| 9000 / 9001 | MinIO API / Console |
| 5433 | Postgres dashboard → container 5432 |
| 3000 | Grafana |
| 8088 | Superset |

---

## Tắt dịch vụ

```powershell
.\run.ps1 -Action down-dashboard
.\run.ps1 -Action down-ops
.\run.ps1 -Action down-bronze
.\run.ps1 -Action down-ingest
```

Hoặc một lệnh:

```powershell
.\run.ps1 -Action down-all
```

---

## Phụ lục: reset sạch (xóa volume — mất dữ liệu local)

Chỉ dùng khi cần làm sạch hoàn toàn compose project và volume Docker.

```powershell
docker compose --profile core --profile ingest --profile bronze --profile ops --profile dashboard --profile train down -v --remove-orphans
docker volume ls | Select-String predictive_maintenance
docker volume rm predictive_maintenance_superset_home predictive_maintenance_grafana-data predictive_maintenance_dashboard-pg-data
# Tuỳ chọn: docker image prune -a -f
```

Sau đó chạy lại từ mục **Chạy pipeline** từ bước 1.
