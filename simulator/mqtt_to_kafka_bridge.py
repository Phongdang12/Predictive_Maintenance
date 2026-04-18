import argparse
import datetime as dt
import json
import signal
import sys
import time
from typing import Any, Dict, Optional, Tuple

import paho.mqtt.client as mqtt
from kafka import KafkaProducer


SETTING_COLUMNS = [f"setting_{i}" for i in range(1, 4)]
SENSOR_COLUMNS = [f"s_{i}" for i in range(1, 22)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bridge raw MQTT messages to Kafka with minimal validation and DLQ routing."
    )
    parser.add_argument("--mqtt-broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--mqtt-port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--mqtt-topic", default="factory/pdm/fd001/raw", help="MQTT source topic")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--raw-topic", default="pdm.fd001.raw", help="Kafka raw topic")
    parser.add_argument("--dlq-topic", default="pdm.fd001.raw.dlq", help="Kafka DLQ topic")
    return parser.parse_args()


def parse_timestamp(value: Any) -> Optional[dt.datetime]:
    if not isinstance(value, str):
        return None

    text = value.strip()
    if not text:
        return None

    normalized = text.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except ValueError:
        pass

    formats = [
        "%m/%d/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
    ]
    for fmt in formats:
        try:
            parsed = dt.datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue

    return None


def is_number(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        try:
            float(value)
            return True
        except ValueError:
            return False
    return False


def validate_payload(obj: Any) -> Tuple[bool, str]:
    if not isinstance(obj, dict):
        return False, "payload must be a JSON object"

    if "unit_nr" not in obj:
        return False, "unit_nr is required"
    unit_nr = obj.get("unit_nr")
    if isinstance(unit_nr, bool):
        return False, "unit_nr must be an integer"
    if isinstance(unit_nr, int):
        pass
    elif isinstance(unit_nr, str):
        try:
            int(unit_nr)
        except ValueError:
            return False, "unit_nr must be an integer"
    else:
        return False, "unit_nr must be an integer"

    if "event_time" not in obj:
        return False, "event_time is required"
    if parse_timestamp(obj.get("event_time")) is None:
        return False, "event_time must be a valid timestamp"

    if "time_cycles" not in obj:
        return False, "time_cycles is required"
    cycle = obj.get("time_cycles")
    if isinstance(cycle, bool):
        return False, "time_cycles must be an integer"
    if isinstance(cycle, int):
        pass
    elif isinstance(cycle, str):
        try:
            int(cycle)
        except ValueError:
            return False, "time_cycles must be an integer"
    else:
        return False, "time_cycles must be an integer"

    for setting_name in SETTING_COLUMNS:
        if setting_name not in obj:
            return False, f"{setting_name} is required"
        if not is_number(obj.get(setting_name)):
            return False, f"{setting_name} must be numeric"

    for sensor_name in SENSOR_COLUMNS:
        if sensor_name not in obj:
            return False, f"{sensor_name} is required"
        if not is_number(obj.get(sensor_name)):
            return False, f"{sensor_name} must be numeric"

    return True, ""


def utc_now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def build_dlq_message(error_type: str, error_message: str, raw_payload: str) -> Dict[str, str]:
    return {
        "error_type": error_type,
        "error_message": error_message,
        "received_at": utc_now_iso(),
        "raw_payload": raw_payload,
    }


def main() -> int:
    args = parse_args()

    print("Bridge: connecting to Kafka (metadata)...", flush=True)
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_bootstrap,
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
        api_version_auto_timeout_ms=10_000,
    )
    print("Bridge: Kafka producer ready", flush=True)

    running = {"value": True}

    def stop_handler(_signum, _frame):
        running["value"] = False

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    client_id = f"pdm-bridge-{dt.datetime.now().strftime('%Y%m%d%H%M%S')}"
    # MQTT 3.1.1 avoids rare broker/client quirks with MQTT v5 + Paho 2.x callbacks.
    mqtt_client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv311,
    )

    def send_raw(unit_nr: str, raw_payload: str) -> None:
        producer.send(
            topic=args.raw_topic,
            key=unit_nr.encode("utf-8"),
            value=raw_payload.encode("utf-8"),
        )

    def send_dlq(error_type: str, error_message: str, raw_payload: str) -> None:
        dlq_payload = build_dlq_message(error_type, error_message, raw_payload)
        producer.send(
            topic=args.dlq_topic,
            key=None,
            value=json.dumps(dlq_payload, ensure_ascii=True).encode("utf-8"),
        )

    def on_connect(_client, _userdata, _flags, reason_code, _properties=None):
        if isinstance(reason_code, int):
            ok = reason_code == 0
        else:
            ok = not reason_code.is_failure
        if not ok:
            print(
                f"ERROR: MQTT connect failed reason_code={reason_code!r}",
                file=sys.stderr,
                flush=True,
            )
            return
        mqtt_client.subscribe(args.mqtt_topic, qos=1)
        print(
            f"Connected MQTT and subscribed to topic={args.mqtt_topic}",
            flush=True,
        )

    def on_message(_client, _userdata, msg):
        raw_payload = msg.payload.decode("utf-8", errors="replace")

        try:
            parsed = json.loads(raw_payload)
        except json.JSONDecodeError as ex:
            send_dlq("json_parse_error", str(ex), raw_payload)
            return

        is_valid, error_message = validate_payload(parsed)
        if not is_valid:
            send_dlq("validation_error", error_message, raw_payload)
            return

        unit_nr = str(int(parsed["unit_nr"]))
        send_raw(unit_nr=unit_nr, raw_payload=raw_payload)

    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    print(
        f"Bridge: connecting MQTT to {args.mqtt_broker}:{args.mqtt_port} ...",
        flush=True,
    )
    mqtt_client.connect(args.mqtt_broker, args.mqtt_port, keepalive=60)
    mqtt_client.loop_start()

    print(
        "Bridge started "
        f"mqtt={args.mqtt_broker}:{args.mqtt_port}/{args.mqtt_topic} "
        f"kafka={args.kafka_bootstrap} raw={args.raw_topic} dlq={args.dlq_topic}",
        flush=True,
    )

    try:
        while running["value"]:
            time.sleep(0.2)
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        producer.flush(timeout=5.0)
        producer.close(timeout=5.0)

    print("Bridge stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
