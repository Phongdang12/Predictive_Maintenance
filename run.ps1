param(
    [ValidateSet(
        "install",
        "split-train-stream",
        "up-core", "down-core",
        "up-ingest", "down-ingest",
        "up-bronze", "down-bronze",
        "up-ops", "down-ops",
        "up-dashboard", "down-dashboard", "refresh-superset",
        "build-train-silver-gold", "down-train",
        "up-all", "down-all",
        "replay",
        "consume-raw", "consume-dlq",
        "logs", "status", "health",
        "up", "bridge", "bronze", "down", "all"
    )]
    [string]$Action = "status",

    [string]$CsvPath = "Data/raw_streaming.csv",
    [string]$FullLifecycleCsv = "Data/full_lifecycle_fd001.csv",
    [string]$TrainHistoryCsv = "Data/train_history.csv",
    [string]$RawStreamingCsv = "Data/raw_streaming.csv",
    [double]$TrainRatio = 0.7,
    [int]$SplitSeed = 42,
    [string]$StreamingBaseTime = "2026-01-01 00:00:00",
    [string]$Broker = "localhost",
    [int]$Port = 18831,
    [ValidateSet(0, 1, 2)]
    [int]$Qos = 1,

    [ValidateSet("fixed", "event-time")]
    [string]$ReplayMode = "event-time",
    [double]$FixedIntervalSeconds = 0.1,
    [double]$ReplaySpeed = 20.0,
    [int]$MaxRows = 0,

    [string]$MqttTopic = "factory/pdm/fd001/raw",
    [string]$KafkaBootstrap = "localhost:9092",
    [string]$RawTopic = "pdm.fd001.raw",
    [string]$DlqTopic = "pdm.fd001.raw.dlq",

    [ValidateSet("emqx", "kafka", "kafka-ui", "mqtt-kafka-bridge", "minio", "minio-init", "bronze-telemetry", "silver-gold-inference-alert", "train-silver-gold", "dashboard-db", "gold-sync", "superset", "grafana")]
    [string]$Service = "mqtt-kafka-bridge",
    [switch]$Follow,
    [int]$Tail = 120
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

# docker compose only resolves profiled services when those profiles are enabled (ps/logs/exec).
$script:ComposeProfileCore = @("--profile", "core")
$script:ComposeProfileCoreIngest = @("--profile", "core", "--profile", "ingest")
$script:ComposeProfileAll = @(
    "--profile", "core", "--profile", "ingest", "--profile", "bronze",
    "--profile", "ops", "--profile", "train", "--profile", "dashboard"
)

function Write-Step([string]$Message) {
    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
}

function Ensure-Command([string]$CommandName) {
    if (-not (Get-Command $CommandName -ErrorAction SilentlyContinue)) {
        throw "Missing required command: $CommandName"
    }
}

function Invoke-Checked([scriptblock]$Script, [string]$ErrorMessage) {
    & $Script
    if ($LASTEXITCODE -ne 0) {
        throw $ErrorMessage
    }
}

function Remove-ConflictingContainers([string[]]$Names) {
    # Disabled by request: do not force-remove existing containers.
    # Keep function in place so existing calls remain compatible.
    return
}



function Install-Dependencies {
    Write-Step "Installing Python dependencies"
    Ensure-Command "python"
    Invoke-Checked { python -m pip install -r "simulator/requirements.txt" } "Failed to install Python dependencies"
}

function Compose-UpCore {
    Write-Step "Starting CORE stage (EMQX + Kafka + Kafka UI)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("kafka", "emqx", "kafka-ui")
    Invoke-Checked { docker compose --profile core up -d emqx kafka kafka-ui } "Failed to start core stage"
}

function Compose-DownCore {
    Write-Step "Stopping CORE stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core stop emqx kafka kafka-ui } "Failed to stop core stage"
}

function Wait-KafkaReady([int]$TimeoutSeconds = 90) {
    Write-Step "Waiting for Kafka readiness"
    Ensure-Command "docker"

    $start = Get-Date
    while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSeconds) {
        # Use compose service name (not fixed container_name) so project-prefixed names work
        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Kafka is ready" -ForegroundColor Green
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for Kafka"
}

function Ensure-KafkaTopic([string]$TopicName, [int]$Partitions = 4, [int]$Retries = 8, [int]$DelaySeconds = 2) {
    Ensure-Command "docker"

    for ($attempt = 1; $attempt -le $Retries; $attempt++) {
        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "Kafka metadata not ready (attempt $attempt/$Retries), retrying..." -ForegroundColor Yellow
            Start-Sleep -Seconds $DelaySeconds
            continue
        }

        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --topic $TopicName --describe *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Topic '$TopicName' is ready" -ForegroundColor Green
            return
        }

        docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
            --bootstrap-server kafka:9092 `
            --create --if-not-exists `
            --topic $TopicName `
            --partitions $Partitions `
            --replication-factor 1 *> $null

        if ($LASTEXITCODE -eq 0) {
            Write-Host "Ensured topic '$TopicName'" -ForegroundColor Green
            return
        }

        Write-Host "Topic '$TopicName' not ready yet (attempt $attempt/$Retries), retrying..." -ForegroundColor Yellow
        Start-Sleep -Seconds $DelaySeconds
    }

    throw "Failed to ensure topic '$TopicName' after $Retries attempts"
}

function Wait-EmqxHealthy([int]$TimeoutSeconds = 90) {
    Write-Step "Waiting for EMQX healthy state"
    Ensure-Command "docker"

    $start = Get-Date
    while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSeconds) {
        $id = docker compose @ComposeProfileCore ps -q emqx 2>$null
        if ($id) {
            $status = docker inspect --format "{{.State.Health.Status}}" $id 2>$null
            if ($LASTEXITCODE -eq 0 -and $status -eq "healthy") {
                Write-Host "EMQX is healthy" -ForegroundColor Green
                return
            }
        }
        Start-Sleep -Seconds 2
    }

    throw "Timed out waiting for EMQX"
}

function Ensure-KafkaTopics {
    Write-Step "Ensuring Kafka topics ($RawTopic, $DlqTopic)"
    Ensure-Command "docker"

    Ensure-KafkaTopic -TopicName $RawTopic -Partitions 4
    Ensure-KafkaTopic -TopicName $DlqTopic -Partitions 1
}

function Start-Ingest {
    Write-Step "Starting INGEST stage (MQTT -> Kafka bridge)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("mqtt-kafka-bridge")

    Invoke-Checked { docker compose --profile core --profile ingest up -d mqtt-kafka-bridge } "Failed to start ingest stage"
    Write-Host "Follow logs with: docker compose --profile core --profile ingest logs -f mqtt-kafka-bridge" -ForegroundColor Yellow
}

function Stop-Ingest {
    Write-Step "Stopping INGEST stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile ingest stop mqtt-kafka-bridge } "Failed to stop ingest stage"
}

function Start-Bronze {
    Write-Step "Starting BRONZE stage (MinIO + Spark Structured Streaming)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("minio", "minio-init", "bronze-telemetry")

    Invoke-Checked { docker compose --profile core --profile bronze up -d minio minio-init bronze-telemetry } "Failed to start bronze stage"
    Write-Host "MinIO console: http://localhost:9001 (minioadmin / minioadmin123)" -ForegroundColor Yellow
    Write-Host "Follow Bronze logs with: docker logs -f bronze-telemetry" -ForegroundColor Yellow
}

function Stop-Bronze {
    Write-Step "Stopping BRONZE stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile bronze stop bronze-telemetry minio-init minio } "Failed to stop bronze stage"
}

function Start-Ops {
    Write-Step "Starting OPS stage (Silver -> Gold -> Inference -> Alert)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("silver-gold-inference-alert")
    Invoke-Checked { docker compose --profile core --profile bronze --profile ops up -d --no-deps silver-gold-inference-alert } "Failed to start ops stage"
}

function Stop-Ops {
    Write-Step "Stopping OPS stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile bronze --profile ops stop silver-gold-inference-alert } "Failed to stop ops stage"
}

function Start-Dashboard {
    Write-Step "Starting DASHBOARD stage (Grafana + Superset + Gold sync)"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("dashboard-db", "gold-sync", "superset", "grafana")

    Invoke-Checked { docker compose --profile core --profile bronze --profile dashboard up -d dashboard-db gold-sync superset grafana } "Failed to start dashboard services"
    Invoke-Checked { docker compose --profile core --profile bronze --profile dashboard run --rm superset-init } "Failed to initialize Superset"

    Write-Host "Grafana: http://localhost:3000 (admin/admin)" -ForegroundColor Yellow
    Write-Host "Superset: http://localhost:8088 (admin/admin)" -ForegroundColor Yellow
}

function Stop-Dashboard {
    Write-Step "Stopping DASHBOARD stage"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile dashboard stop grafana superset gold-sync dashboard-db } "Failed to stop dashboard stage"
}

function Refresh-SupersetMeta {
    Write-Step "Refreshing Superset dataset metadata from Gold Warehouse"
    Ensure-Command "docker"
    $container = docker ps --filter "name=superset" --filter "status=running" --format "{{.Names}}" | Select-Object -First 1
    if (-not $container) {
        Write-Host "[warn] Superset container is not running. Start dashboard first with: .\run.ps1 -Action up-dashboard" -ForegroundColor Yellow
        return
    }
    Write-Host "Running metadata refresh in container: $container" -ForegroundColor Cyan
    docker exec $container python3 /app/dashboard/superset/refresh_metadata.py
    Write-Host "Superset metadata refreshed. Open http://localhost:8088 and Ctrl+F5." -ForegroundColor Green
}

function Build-TrainSilverGold {
    Write-Step "Building TRAIN Silver/Gold datasets to MinIO"
    Ensure-Command "docker"
    Remove-ConflictingContainers -Names @("minio", "minio-init", "train-silver-gold")

    Invoke-Checked { docker compose --profile train up -d minio minio-init } "Failed to start MinIO for train datasets"
    Invoke-Checked { docker compose --profile train run --rm train-silver-gold } "Failed to build train silver/gold datasets"
}

function Run-SplitTrainStream {
    Write-Step "Splitting full-life-cycle data into physical train/stream CSV files"
    Ensure-Command "python"

    if (-not (Test-Path $FullLifecycleCsv)) {
        throw "Full lifecycle CSV not found: $FullLifecycleCsv"
    }

    Invoke-Checked {
        python "scripts/split_train_stream_files.py" `
            --input-csv "$FullLifecycleCsv" `
            --train-output-csv "$TrainHistoryCsv" `
            --stream-output-csv "$RawStreamingCsv" `
            --train-ratio $TrainRatio `
            --seed $SplitSeed `
            --stream-base-time "$StreamingBaseTime"
    } "Failed to split full lifecycle dataset"
}

function Stop-TrainStage {
    Write-Step "Stopping TRAIN stage services"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile train stop train-silver-gold minio-init minio } "Failed to stop train stage"
}

function Wait-BridgeReady([int]$TimeoutSeconds = 180) {
    Write-Step "Waiting for bridge MQTT subscription"
    Ensure-Command "docker"

    $start = Get-Date
    while (((Get-Date) - $start).TotalSeconds -lt $TimeoutSeconds) {
        # mqtt-kafka-bridge is profile "ingest" only — ps -q without --profile ingest returns nothing.
        $bridgeId = docker compose @ComposeProfileCoreIngest ps -q mqtt-kafka-bridge 2>$null
        if (-not $bridgeId) {
            Start-Sleep -Seconds 2
            continue
        }
        $running = docker inspect --format "{{.State.Running}}" $bridgeId 2>$null
        if ($LASTEXITCODE -ne 0 -or $running -ne "true") {
            Start-Sleep -Seconds 2
            continue
        }

        $logs = ""
        try {
            # Use docker logs (not compose logs): profiled services may not show logs via
            # `docker compose logs` unless every call repeats --profile flags.
            $logs = docker logs --tail 200 $bridgeId 2>&1 | Out-String
        } catch {
            Start-Sleep -Seconds 2
            continue
        }

        if ($logs -match "ERROR:\s*MQTT connect failed") {
            throw "MQTT bridge failed to connect to EMQX. Last logs:`n$logs"
        }

        if ($logs -match "Connected MQTT and subscribed to topic=") {
            Write-Host "Bridge is subscribed and ready" -ForegroundColor Green
            return
        }

        Start-Sleep -Seconds 2
    }

    $tail = ""
    try {
        $bid = docker compose @ComposeProfileCoreIngest ps -q mqtt-kafka-bridge 2>$null
        if ($bid) { $tail = docker logs --tail 120 $bid 2>&1 | Out-String }
    } catch { }

    throw "Timed out waiting for bridge MQTT subscription. Last logs:`n$tail"
}

function Run-Replay {
    Write-Step "Replaying CSV to MQTT"
    Ensure-Command "python"

    if (-not (Test-Path $CsvPath)) {
        throw "CSV file not found: $CsvPath"
    }

    Invoke-Checked {
        python "simulator/replay_mqtt_from_csv.py" `
            --csv "$CsvPath" `
            --broker "$Broker" `
            --port $Port `
            --qos $Qos `
            --topic "$MqttTopic" `
            --replay-mode $ReplayMode `
            --fixed-interval-seconds $FixedIntervalSeconds `
            --replay-speed $ReplaySpeed `
            --max-rows $MaxRows
    } "Replay failed"
}

function Consume-Raw {
    Write-Step "Consuming Kafka RAW topic ($RawTopic)"
    Ensure-Command "docker"
    Ensure-KafkaTopics

    Invoke-Checked {
        docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
            --topic $RawTopic `
            --from-beginning `
            --property print.key=true `
            --bootstrap-server kafka:9092
    } "Kafka raw consumer failed"
}

function Consume-Dlq {
    Write-Step "Consuming Kafka DLQ topic ($DlqTopic)"
    Ensure-Command "docker"
    Ensure-KafkaTopics

    Invoke-Checked {
        docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh `
            --topic $DlqTopic `
            --from-beginning `
            --bootstrap-server kafka:9092
    } "Kafka DLQ consumer failed"
}

function Compose-Status {
    Write-Step "Docker compose status"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard ps } "Docker compose status failed"
}

function Compose-DownAll {
    Write-Step "Stopping all stages"
    Ensure-Command "docker"
    Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard down } "Docker compose down failed"
}

function Show-Logs {
    Write-Step "Showing service logs ($Service)"
    Ensure-Command "docker"

    if ($Follow) {
        Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard logs -f --tail $Tail $Service } "Failed to follow service logs"
    }
    else {
        Invoke-Checked { docker compose --profile core --profile ingest --profile bronze --profile ops --profile train --profile dashboard logs --tail $Tail $Service } "Failed to show service logs"
    }
}

function Check-Health {
    Write-Step "Health checks"
    Ensure-Command "docker"

    $services = @("emqx", "kafka", "mqtt-kafka-bridge", "minio", "bronze-telemetry", "silver-gold-inference-alert", "dashboard-db", "gold-sync", "superset", "grafana")
    foreach ($svc in $services) {
        $id = docker compose @ComposeProfileAll ps -a -q $svc 2>$null
        if (-not $id) {
            Write-Host "$svc : not created" -ForegroundColor DarkYellow
            continue
        }

        $running = docker inspect --format "{{.State.Running}}" $id 2>$null
        if ($LASTEXITCODE -ne 0) {
            Write-Host "$svc : unknown" -ForegroundColor Yellow
            continue
        }

        if ($running -eq "true") {
            Write-Host "$svc : running" -ForegroundColor Green
        }
        else {
            Write-Host "$svc : stopped" -ForegroundColor DarkYellow
        }
    }
}

function Normalize-LegacyAction([string]$RequestedAction) {
    switch ($RequestedAction) {
        "up" { return "up-core" }
        "bridge" { return "up-ingest" }
        "bronze" { return "up-bronze" }
        "down" { return "down-all" }
        "all" { return "up-all" }
        default { return $RequestedAction }
    }
}

 $NormalizedAction = Normalize-LegacyAction -RequestedAction $Action

if ($Action -ne $NormalizedAction) {
    Write-Host "Legacy action '$Action' mapped to '$NormalizedAction'." -ForegroundColor Yellow
}

switch ($NormalizedAction) {
    "install" {
        Install-Dependencies
    }
    "split-train-stream" {
        Run-SplitTrainStream
    }
    "up-core" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Wait-EmqxHealthy
    }
    "down-core" {
        Compose-DownCore
    }
    "up-ingest" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Wait-EmqxHealthy
        Start-Ingest
        Wait-BridgeReady
    }
    "down-ingest" {
        Stop-Ingest
    }
    "up-bronze" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Start-Bronze
    }
    "down-bronze" {
        Stop-Bronze
    }
    "up-ops" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Start-Ops
    }
    "down-ops" {
        Stop-Ops
    }
    "up-dashboard" {
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Start-Bronze
        Start-Dashboard
    }
    "down-dashboard" {
        Stop-Dashboard
    }
    "refresh-superset" {
        Refresh-SupersetMeta
    }
    "build-train-silver-gold" {
        Build-TrainSilverGold
    }
    "down-train" {
        Stop-TrainStage
    }
    "up-all" {
        Install-Dependencies
        Compose-UpCore
        Wait-KafkaReady
        Ensure-KafkaTopics
        Wait-EmqxHealthy
        Start-Ingest
        Wait-BridgeReady
        Start-Bronze
        Start-Ops
    }
    "down-all" {
        Compose-DownAll
    }
    "replay" {
        Run-Replay
    }
    "consume-raw" {
        Consume-Raw
    }
    "consume-dlq" {
        Consume-Dlq
    }
    "logs" {
        Show-Logs
    }
    "status" {
        Compose-Status
    }
    "health" {
        Check-Health
    }
}
