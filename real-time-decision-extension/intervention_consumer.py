"""Preview real-time intervention decisions from Kafka."""

from __future__ import annotations

import argparse
import json

from kafka import KafkaConsumer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default="broker:9092")
    parser.add_argument("--topic", default="driver_interventions")
    parser.add_argument("--from-beginning", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap_server,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=True,
        group_id="telematics-intervention-preview",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        key_deserializer=lambda key: key.decode("utf-8") if key else None,
    )

    print(f"Listening on topic {args.topic}. Press Ctrl+C to stop.")
    for msg in consumer:
        print(json.dumps({"key": msg.key, "value": msg.value}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
