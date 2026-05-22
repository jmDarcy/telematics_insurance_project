"""Score drivers with the trained GLM and update technical premiums."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from train_glm import FEATURE_COLUMNS, load_features, prepare_features

try:
    import statsmodels.api as sm
except ModuleNotFoundError:
    sm = None


def cap_change(previous: pd.Series, target: pd.Series, cap: float) -> pd.Series:
    lower = previous * (1 - cap)
    upper = previous * (1 + cap)
    return target.clip(lower=lower, upper=upper)


def score(features_path: Path, model_path: Path, premium_history_path: Path, base_premium: float, cap: float) -> pd.DataFrame:
    model_bundle = joblib.load(model_path)
    result = model_bundle["result"]
    model_type = model_bundle.get("model_type", "statsmodels_glm")
    feature_columns = model_bundle.get("feature_columns", FEATURE_COLUMNS)

    df = prepare_features(load_features(features_path))
    latest = (
        df.groupby("driver_id", as_index=False)
        .agg(
            exposure_km=("exposure_km", "sum"),
            speeding_ratio=("speeding_ratio", "mean"),
            hard_braking_count_per_100km=("hard_braking_count_per_100km", "mean"),
            harsh_acceleration_count_per_100km=("harsh_acceleration_count_per_100km", "mean"),
            night_event_ratio=("night_event_ratio", "mean"),
            bad_weather_event_ratio=("bad_weather_event_ratio", "mean"),
            phone_usage_count=("phone_usage_count", "sum"),
        )
    )
    if model_type == "statsmodels_glm":
        if sm is None:
            raise RuntimeError("This model was trained with statsmodels; install statsmodels to score it.")
        x = sm.add_constant(latest[feature_columns], has_constant="add")
        predicted_count = result.predict(x, offset=np.log(latest["exposure_km"].clip(lower=0.001)))
    else:
        predicted_count = result.predict(latest[feature_columns]) * latest["exposure_km"].clip(lower=0.001)
    latest["predicted_claim_count"] = predicted_count
    latest["predicted_frequency"] = latest["predicted_claim_count"] / latest["exposure_km"].clip(lower=0.001)
    avg_frequency = max(float(latest["predicted_frequency"].mean()), 1e-9)
    latest["risk_multiplier"] = latest["predicted_frequency"] / avg_frequency
    latest["target_technical_premium"] = base_premium * latest["risk_multiplier"]

    if premium_history_path.exists():
        history = pd.read_csv(premium_history_path)
        prev = history.sort_values("scored_at").groupby("driver_id").tail(1)[["driver_id", "technical_premium"]]
        latest = latest.merge(prev, on="driver_id", how="left", suffixes=("", "_previous"))
        latest["technical_premium_previous"] = latest["technical_premium"].fillna(base_premium)
    else:
        latest["technical_premium_previous"] = base_premium

    latest["technical_premium"] = cap_change(
        latest["technical_premium_previous"],
        latest["target_technical_premium"],
        cap,
    ).round(2)
    latest["scored_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    latest["base_premium"] = base_premium
    latest["premium_change_cap"] = cap

    premium_history_path.parent.mkdir(parents=True, exist_ok=True)
    if premium_history_path.exists():
        previous = pd.read_csv(premium_history_path)
        output = pd.concat([previous, latest], ignore_index=True)
    else:
        output = latest
    output.to_csv(premium_history_path, index=False)
    return latest


def publish_updates(df: pd.DataFrame, bootstrap_server: str, topic: str) -> None:
    try:
        from kafka import KafkaProducer
    except ModuleNotFoundError as exc:
        raise RuntimeError("Install kafka-python or run without --publish-kafka.") from exc

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    for record in df.to_dict(orient="records"):
        producer.send(topic, key=record["driver_id"], value=record)
    producer.flush()
    producer.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features-path", default="data/historical_features/tumbling_1m")
    parser.add_argument("--model-path", default="models/glm_model.pkl")
    parser.add_argument("--premium-history-path", default="data/premium_history/premium_history.csv")
    parser.add_argument("--base-premium", type=float, default=1000.0)
    parser.add_argument("--premium-change-cap", type=float, default=0.10)
    parser.add_argument("--publish-kafka", action="store_true")
    parser.add_argument("--bootstrap-server", default="broker:9092")
    parser.add_argument("--topic", default="premium_updates")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    scored = score(
        Path(args.features_path),
        Path(args.model_path),
        Path(args.premium_history_path),
        args.base_premium,
        args.premium_change_cap,
    )
    print(scored.sort_values("technical_premium", ascending=False).head(20).to_string(index=False))
    if args.publish_kafka:
        publish_updates(scored, args.bootstrap_server, args.topic)


if __name__ == "__main__":
    main()
