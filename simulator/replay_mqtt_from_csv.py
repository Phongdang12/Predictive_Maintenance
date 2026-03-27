import argparse
import csv
import datetime as dt
import json
import sys
import time
import uuid
from typing import Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt


SETTING_COLUMNS = [f"setting_{i}" for i in range(1, 4)]
SENSOR_COLUMNS = [f"s_{i}" for i in range(1, 22)]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay predictive maintenance CSV rows to MQTT as raw events."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help=(
            "Path to CSV file with schema "
            "unit_nr,event_time,time_cycles,setting_1..3,s_1..s_21"
        ),
    )
    parser.add_argument("--broker", default="localhost", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--qos", type=int, choices=[0, 1, 2], default=1, help="MQTT QoS")
    parser.add_argument("--topic", default="factory/pdm/fd001/raw", help="MQTT topic")
    parser.add_argument("--replay-mode", choices=["fixed", "event-time"], default="event-time")
    parser.add_argument(
        "--fixed-interval-seconds",
        type=float,
        default=0.1,
        help="Sleep between rows in fixed mode (seconds)",
    )
    parser.add_argument(
        "--replay-speed",
        type=float,
        default=1.0,
        help="Speed factor. 2.0 is 2x faster; applies to fixed and event-time modes.",
    )
    parser.add_argument("--max-rows", type=int, default=0, help="0 means all rows")
    parser.add_argument("--print-every", type=int, default=1000)
    return parser.parse_args()


def parse_event_time(value: str) -> Optional[dt.datetime]:
    text = (value or "").strip()
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


def to_iso_z(value: dt.datetime) -> str:
    utc_value = value.astimezone(dt.timezone.utc)
    return utc_value.isoformat().replace("+00:00", "Z")


def parse_row(row: Dict[str, str]) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
    unit_nr_raw = (row.get("unit_nr") or "").strip()
    if not unit_nr_raw:
        return None, "unit_nr is missing"
    try:
        unit_nr = int(unit_nr_raw)
    except ValueError:
        return None, f"unit_nr is not an integer: {unit_nr_raw!r}"

    event_time_raw = row.get("event_time") or ""
    parsed_event_time = parse_event_time(event_time_raw)
    if parsed_event_time is None:
        return None, f"event_time is invalid: {event_time_raw!r}"

    cycle_raw = (row.get("time_cycles") or "").strip()
    try:
        cycle = int(cycle_raw)
    except ValueError:
        return None, f"time_cycles is not an integer: {cycle_raw!r}"

    payload: Dict[str, object] = {
        "unit_nr": unit_nr,
        "event_time": to_iso_z(parsed_event_time),
        "time_cycles": cycle,
    }

    for setting_name in SETTING_COLUMNS:
        setting_raw = (row.get(setting_name) or "").strip()
        if setting_raw == "":
            return None, f"{setting_name} is missing"
        try:
            payload[setting_name] = float(setting_raw)
        except ValueError:
            return None, f"{setting_name} is not numeric: {setting_raw!r}"

    for sensor_name in SENSOR_COLUMNS:
        sensor_raw = (row.get(sensor_name) or "").strip()
        if sensor_raw == "":
            return None, f"{sensor_name} is missing"
        try:
            payload[sensor_name] = float(sensor_raw)
        except ValueError:
            return None, f"{sensor_name} is not numeric: {sensor_raw!r}"

    return payload, None


def build_raw_row_payload(row: Dict[str, str]) -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for column in ["unit_nr", "event_time", "time_cycles", *SETTING_COLUMNS, *SENSOR_COLUMNS]:
        payload[column] = (row.get(column) or "").strip()
    return payload


def load_and_sort_payloads(csv_path: str) -> Tuple[List[Dict[str, object]], int]:
    rows_to_publish: List[Dict[str, object]] = []
    invalid_count = 0

    with open(csv_path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {"unit_nr", "event_time", "time_cycles", *SETTING_COLUMNS, *SENSOR_COLUMNS}
        missing = required.difference(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {sorted(missing)}")

        for row_number, row in enumerate(reader, start=2):
            payload, error = parse_row(row)
            if error is not None:
                invalid_count += 1
                rows_to_publish.append(
                    {
                        "row_number": row_number,
                        "payload": build_raw_row_payload(row),
                        "parse_error": error,
                    }
                )
                continue
            rows_to_publish.append(
                {
                    "row_number": row_number,
                    "payload": payload,
                    "parse_error": None,
                }
            )

    return rows_to_publish, invalid_count


def sleep_between_rows(
    replay_mode: str,
    replay_speed: float,
    fixed_interval_seconds: float,
    previous_event_time: Optional[dt.datetime],
    current_event_time: dt.datetime,
) -> None:
    if replay_mode == "fixed":
        delay_seconds = max(fixed_interval_seconds / replay_speed, 0.0)
    else:
        if previous_event_time is None:
            return
        delta_seconds = max((current_event_time - previous_event_time).total_seconds(), 0.0)
        delay_seconds = delta_seconds / replay_speed

    if delay_seconds > 0:
        time.sleep(delay_seconds)


def main() -> int:
    args = parse_args()

    if args.replay_speed <= 0:
        raise ValueError("replay-speed must be > 0")

    payload_entries, invalid_count = load_and_sort_payloads(args.csv)
    payloads = payload_entries
    if args.max_rows > 0:
        payloads = payloads[: args.max_rows]

    client_id = f"pdm-csv-replay-{uuid.uuid4().hex[:8]}"
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=client_id,
        protocol=mqtt.MQTTv5,
    )

    connected = {"ok": False}

    def on_connect(_client, _userdata, _flags, reason_code, _properties):
        rc_value = getattr(reason_code, "value", reason_code)
        connected["ok"] = rc_value == 0

    client.on_connect = on_connect
    client.connect(args.broker, args.port, keepalive=60)
    client.loop_start()

    wait_start = time.time()
    while not connected["ok"]:
        if time.time() - wait_start > 10:
            print("ERROR: MQTT connect timeout", file=sys.stderr)
            client.loop_stop()
            return 1
        time.sleep(0.1)

    print(
        f"Starting CSV replay rows={len(payloads)} topic={args.topic} "
        f"broker={args.broker}:{args.port} mode={args.replay_mode} speed={args.replay_speed}"
    )

    previous_event_time: Optional[dt.datetime] = None
    published = 0
    invalid_forwarded = 0

    try:
        for entry in payloads:
            payload = entry["payload"]
            parse_error = entry["parse_error"]
            row_number = entry["row_number"]

            if parse_error is not None:
                invalid_forwarded += 1
                print(f"WARN row={row_number}: {parse_error}", file=sys.stderr)

            current_event_time = parse_event_time(str(payload["event_time"]))
            if current_event_time is not None:
                sleep_between_rows(
                    replay_mode=args.replay_mode,
                    replay_speed=args.replay_speed,
                    fixed_interval_seconds=args.fixed_interval_seconds,
                    previous_event_time=previous_event_time,
                    current_event_time=current_event_time,
                )
                previous_event_time = current_event_time
            elif args.replay_mode == "fixed":
                delay_seconds = max(args.fixed_interval_seconds / args.replay_speed, 0.0)
                if delay_seconds > 0:
                    time.sleep(delay_seconds)

            body = json.dumps(payload, ensure_ascii=True)
            info = client.publish(args.topic, body, qos=args.qos)
            info.wait_for_publish()
            published += 1

            if args.print_every > 0 and published % args.print_every == 0:
                print(f"progress published={published}")
    finally:
        client.loop_stop()
        client.disconnect()

    print(f"done published={published}")
    if invalid_count > 0:
        print(
            f"done invalid_forwarded={invalid_forwarded}/{invalid_count}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
