"""Offline demo of the real-time intervention policy.

This script is useful for presentations when Kafka/Spark are not available.
It simulates a stream of telematics events and applies the same decision ideas
in small in-memory rolling windows.

Run:
    python real_time_decision_extension/offline_intervention_demo.py
"""

from __future__ import annotations

import argparse
import json
import random
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np


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


def build_demo_drivers(n_drivers: int, rng: random.Random) -> list[Driver]:
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


def demo_event_for_driver(driver: Driver, event_no: int, rng: random.Random, np_rng: np.random.Generator) -> dict[str, Any]:
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
        "event_id": f"OFFLINE{event_no:06d}",
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


def risk_flags(event: dict[str, Any]) -> dict[str, Any]:
    flags = {
        "is_speeding": event["speed_kmh"] > event["speed_limit_kmh"] + 10,
        "is_hard_braking": event["braking_ms2"] < -3.0,
        "is_extreme_braking": event["braking_ms2"] < -5.0,
        "is_harsh_acceleration": event["acceleration_ms2"] > 2.5,
        "is_sharp_cornering": event["cornering_g"] > 0.35,
        "is_night_risk": event["is_night"] and event["speed_kmh"] > event["speed_limit_kmh"],
        "is_bad_weather": event["weather"] in {"rain", "fog", "snow"},
        "is_phone_usage": event["phone_usage"],
    }
    score_keys = [
        "is_speeding",
        "is_hard_braking",
        "is_harsh_acceleration",
        "is_sharp_cornering",
        "is_night_risk",
        "is_bad_weather",
        "is_phone_usage",
    ]
    flags["risk_score_event"] = sum(int(flags[key]) for key in score_keys)
    flags["risk_event"] = flags["risk_score_event"] > 0
    flags["possible_crash_or_incident"] = flags["is_extreme_braking"] or (
        event["braking_ms2"] < -4.2 and event["speed_kmh"] > 60 and flags["is_sharp_cornering"]
    )
    return flags


def active_rules(flags: dict[str, Any]) -> list[str]:
    mapping = {
        "is_speeding": "speeding",
        "is_hard_braking": "hard_braking",
        "is_extreme_braking": "extreme_braking",
        "is_harsh_acceleration": "harsh_acceleration",
        "is_sharp_cornering": "sharp_cornering",
        "is_night_risk": "night_speed_risk",
        "is_bad_weather": "bad_weather",
        "is_phone_usage": "phone_usage",
    }
    return [label for key, label in mapping.items() if flags.get(key)]


def event_decision(event: dict[str, Any], flags: dict[str, Any]) -> dict[str, Any] | None:
    if flags["possible_crash_or_incident"]:
        return {
            "decision_source": "event",
            "decision_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "driver_id": event["driver_id"],
            "event_id": event["event_id"],
            "decision_type": "CHECK_DRIVER_STATUS",
            "priority": "critical",
            "recommended_action": "Send push/SMS asking if the driver is safe and prepare assistance triage.",
            "customer_message": "Wykryto gwaltowne zdarzenie. Czy wszystko w porzadku?",
            "active_rules": active_rules(flags),
            "risk_score_event": flags["risk_score_event"],
        }
    if flags["risk_score_event"] >= 4:
        return {
            "decision_source": "event",
            "decision_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "driver_id": event["driver_id"],
            "event_id": event["event_id"],
            "decision_type": "SEND_SAFETY_NUDGE",
            "priority": "high",
            "recommended_action": "Send an in-app safety nudge during the trip.",
            "customer_message": "W ostatnich minutach jazda wyglada ryzykownie. Zwolnij i zachowaj wiekszy odstep.",
            "active_rules": active_rules(flags),
            "risk_score_event": flags["risk_score_event"],
        }
    return None


def window_decision(driver_id: str, events: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not events:
        return None
    flagged = [(event, risk_flags(event)) for event in events]
    event_count = len(flagged)
    km_driven = sum(event["distance_delta_km"] for event, _ in flagged)
    risk_event_count = sum(int(flags["risk_event"]) for _, flags in flagged)
    risk_score = sum(flags["risk_score_event"] for _, flags in flagged)
    phone_usage_count = sum(int(flags["is_phone_usage"]) for _, flags in flagged)
    risk_intensity = risk_event_count / event_count

    if risk_score >= 12 or phone_usage_count >= 3:
        decision_type = "ESCALATE_HIGH_RISK_REVIEW"
        priority = "high"
        action = "Flag this active trip for operational review and possible outbound contact."
        message = "Widzimy podwyzszone ryzyko tej podrozy. Jedz ostrozniej, zeby utrzymac bonus."
    elif event_count >= 5 and risk_intensity >= 0.45:
        decision_type = "SEND_SAFETY_NUDGE"
        priority = "medium"
        action = "Send a real-time safety nudge before the risky pattern becomes a claim."
        message = "Widzimy podwyzszone ryzyko tej podrozy. Jedz ostrozniej, zeby utrzymac bonus."
    elif event_count >= 10 and risk_event_count == 0 and km_driven >= 0.25:
        decision_type = "GRANT_SAFE_DRIVING_POINTS"
        priority = "low"
        action = "Add safe-driving points to the customer's reward wallet."
        message = "Dobra, spokojna jazda. Dodajemy punkty safe driving do Twojego konta."
    else:
        return None

    return {
        "decision_source": "rolling_window_demo",
        "decision_ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "driver_id": driver_id,
        "decision_type": decision_type,
        "priority": priority,
        "recommended_action": action,
        "customer_message": message,
        "event_count": event_count,
        "km_driven": round(km_driven, 4),
        "risk_event_count": risk_event_count,
        "risk_score_window": risk_score,
        "risk_intensity": round(risk_intensity, 3),
        "phone_usage_count": phone_usage_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--drivers", type=int, default=20)
    parser.add_argument("--events", type=int, default=250)
    parser.add_argument("--window-events", type=int, default=12)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--output", default="data/realtime_decision_extension/offline_interventions.jsonl")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    np_rng = np.random.default_rng(args.seed)
    drivers = build_demo_drivers(args.drivers, rng)
    windows: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=args.window_events))
    decisions: list[dict[str, Any]] = []

    for event_no in range(1, args.events + 1):
        driver = rng.choice(drivers)
        event = demo_event_for_driver(driver, event_no, rng, np_rng)
        flags = risk_flags(event)
        windows[event["driver_id"]].append(event)

        immediate = event_decision(event, flags)
        if immediate:
            decisions.append(immediate)

        if event_no % 10 == 0:
            for driver_id, recent_events in windows.items():
                decision = window_decision(driver_id, list(recent_events))
                if decision:
                    decisions.append(decision)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for decision in decisions:
            handle.write(json.dumps(decision, ensure_ascii=False) + "\n")

    print(f"Generated {len(decisions)} intervention decisions.")
    print(f"Saved: {output}")
    for decision in decisions[:15]:
        print(json.dumps(decision, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
