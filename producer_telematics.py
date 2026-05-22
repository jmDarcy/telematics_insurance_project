"""Kafka producer for synthetic vehicle telematics events.

Run:
    python producer_telematics.py --events-per-second 10 --duration-seconds 120
"""

from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import numpy as np
from kafka import KafkaProducer


SERVER = "broker:9092"
TOPIC = "telematics_raw"

ROAD_LIMITS = {
    "urban": [30, 50, 60],
    "suburban": [50, 70, 90],
    "highway": [100, 120, 140],
}
WEATHER = ["clear", "rain", "fog", "snow"]
PROFILES = ["safe", "average", "aggressive", "night_driver", "urban_driver"]


@dataclass(frozen=True)
class Driver:
    driver_id: str
    vehicle_id: str
    profile: str


def build_drivers(n_drivers: int, rng: random.Random) -> list[Driver]:
    weights = [0.30, 0.35, 0.15, 0.10, 0.10]
    profiles = rng.choices(PROFILES, weights=weights, k=n_drivers)
    return [
        Driver(driver_id=f"D{i:03d}", vehicle_id=f"V{i:03d}", profile=profile)
        for i, profile in enumerate(profiles, start=1)
    ]


def choose_road_type(profile: str, rng: random.Random) -> str:
    if profile == "urban_driver":
        return rng.choices(["urban", "suburban", "highway"], weights=[0.75, 0.20, 0.05])[0]
    if profile == "aggressive":
        return rng.choices(["urban", "suburban", "highway"], weights=[0.35, 0.30, 0.35])[0]
    return rng.choices(["urban", "suburban", "highway"], weights=[0.45, 0.35, 0.20])[0]


def event_for_driver(driver: Driver, event_no: int, rng: random.Random, np_rng: np.random.Generator) -> dict[str, Any]:
    road_type = choose_road_type(driver.profile, rng)
    speed_limit = rng.choice(ROAD_LIMITS[road_type])
    weather = rng.choices(WEATHER, weights=[0.72, 0.18, 0.07, 0.03])[0]

    night_prob = 0.12
    if driver.profile == "night_driver":
        night_prob = 0.55
    elif driver.profile == "safe":
        night_prob = 0.06
    is_night = rng.random() < night_prob

    if driver.profile == "safe":
        speed_delta_mean, speed_delta_sd = -7, 7
        harsh_multiplier = 0.55
        phone_prob = 0.015
    elif driver.profile == "aggressive":
        speed_delta_mean, speed_delta_sd = 8, 14
        harsh_multiplier = 1.75
        phone_prob = 0.08
    elif driver.profile == "night_driver":
        speed_delta_mean, speed_delta_sd = 0, 10
        harsh_multiplier = 1.15
        phone_prob = 0.04
    else:
        speed_delta_mean, speed_delta_sd = 0, 9
        harsh_multiplier = 1.0
        phone_prob = 0.035

    if weather in {"rain", "fog", "snow"}:
        speed_delta_mean -= 4
        harsh_multiplier *= 1.15

    speed = max(0.0, np_rng.normal(speed_limit + speed_delta_mean, speed_delta_sd))
    acceleration = np_rng.normal(0.7, 0.75 * harsh_multiplier)
    braking = -abs(np_rng.normal(1.4, 0.85 * harsh_multiplier))
    cornering = max(0.0, np_rng.normal(0.16, 0.09 * harsh_multiplier))
    distance_delta = max(0.01, speed / 3600.0 + np_rng.normal(0, 0.01))

    return {
        "event_id": f"EV{event_no:06d}-{uuid.uuid4().hex[:6]}",
        "driver_id": driver.driver_id,
        "vehicle_id": driver.vehicle_id,
        "driver_profile": driver.profile,
        "event_time": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "speed_kmh": round(float(speed), 1),
        "speed_limit_kmh": int(speed_limit),
        "acceleration_ms2": round(float(acceleration), 2),
        "braking_ms2": round(float(braking), 2),
        "cornering_g": round(float(cornering), 3),
        "road_type": road_type,
        "weather": weather,
        "is_night": bool(is_night),
        "distance_delta_km": round(float(distance_delta), 4),
        "phone_usage": bool(rng.random() < phone_prob),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-server", default=SERVER)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--drivers", type=int, default=50)
    parser.add_argument("--events-per-second", type=float, default=5.0)
    parser.add_argument("--duration-seconds", type=int, default=120)
    parser.add_argument("--seed", type=int, default=2026)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    np_rng = np.random.default_rng(args.seed)
    drivers = build_drivers(args.drivers, rng)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        linger_ms=20,
    )

    delay = 1.0 / max(args.events_per_second, 0.1)
    deadline = time.time() + args.duration_seconds
    event_no = 1
    try:
        while time.time() < deadline:
            driver = rng.choice(drivers)
            event = event_for_driver(driver, event_no, rng, np_rng)
            producer.send(args.topic, key=event["driver_id"], value=event)
            if event_no % 100 == 0:
                producer.flush()
                print(f"sent {event_no} events; last={event}")
            event_no += 1
            time.sleep(delay)
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        producer.flush()
        producer.close()
        print(f"Finished. Sent {event_no - 1} events to {args.topic}.")


if __name__ == "__main__":
    main()
