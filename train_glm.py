"""Train a Poisson GLM on telematics driver-window aggregates."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import train_test_split

try:
    import statsmodels.api as sm
except ModuleNotFoundError:
    sm = None

try:
    from sklearn.linear_model import PoissonRegressor
except ModuleNotFoundError:
    PoissonRegressor = None


FEATURE_COLUMNS = [
    "speeding_ratio",
    "hard_braking_count_per_100km",
    "harsh_acceleration_count_per_100km",
    "night_event_ratio",
    "bad_weather_event_ratio",
    "phone_usage_count",
]


def load_features(path: Path) -> pd.DataFrame:
    if path.exists():
        if path.is_dir():
            return pd.read_parquet(path)
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path)
        return pd.read_parquet(path)
    return simulate_historical_features()


def simulate_historical_features(n_drivers: int = 50, n_windows: int = 20, seed: int = 2026) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    profiles = rng.choice(
        ["safe", "average", "aggressive", "night_driver", "urban_driver"],
        size=n_drivers,
        p=[0.30, 0.35, 0.15, 0.10, 0.10],
    )
    rows = []
    for i, profile in enumerate(profiles, start=1):
        for window_id in range(n_windows):
            exposure = float(rng.gamma(8, 0.18) + 0.2)
            if profile == "safe":
                speeding = rng.beta(1.2, 12)
                hard = rng.poisson(0.4)
                harsh = rng.poisson(0.35)
                night = rng.beta(1.2, 12)
                phone = rng.poisson(0.05)
            elif profile == "aggressive":
                speeding = rng.beta(4.0, 5.5)
                hard = rng.poisson(2.3)
                harsh = rng.poisson(2.0)
                night = rng.beta(2.2, 7)
                phone = rng.poisson(0.45)
            elif profile == "night_driver":
                speeding = rng.beta(2.2, 7)
                hard = rng.poisson(1.1)
                harsh = rng.poisson(1.0)
                night = rng.beta(6, 4)
                phone = rng.poisson(0.25)
            else:
                speeding = rng.beta(2.0, 8)
                hard = rng.poisson(0.9)
                harsh = rng.poisson(0.8)
                night = rng.beta(1.7, 9)
                phone = rng.poisson(0.18)
            bad_weather = rng.beta(2, 8)
            rows.append(
                {
                    "driver_id": f"D{i:03d}",
                    "driver_profile": profile,
                    "window_id": window_id,
                    "exposure_km": exposure,
                    "speeding_ratio": float(speeding),
                    "hard_braking_count": int(hard),
                    "harsh_acceleration_count": int(harsh),
                    "night_event_ratio": float(night),
                    "bad_weather_event_ratio": float(bad_weather),
                    "phone_usage_count": int(phone),
                }
            )
    df = pd.DataFrame(rows)
    return add_target(df, seed=seed)


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["exposure_km"] = df["exposure_km"].clip(lower=0.001)
    df["hard_braking_count_per_100km"] = df["hard_braking_count"] / df["exposure_km"] * 100
    df["harsh_acceleration_count_per_100km"] = df["harsh_acceleration_count"] / df["exposure_km"] * 100
    for col in FEATURE_COLUMNS:
        if col not in df:
            df[col] = 0.0
    return df


def add_target(df: pd.DataFrame, seed: int = 2026) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    df = prepare_features(df)
    profile_effect = df.get("driver_profile", pd.Series("average", index=df.index)).map(
        {"safe": -0.45, "average": 0.0, "aggressive": 0.45, "night_driver": 0.25, "urban_driver": 0.10}
    ).fillna(0.0)
    linear = (
        -5.15
        + 1.30 * df["speeding_ratio"]
        + 0.012 * df["hard_braking_count_per_100km"]
        + 0.010 * df["harsh_acceleration_count_per_100km"]
        + 0.55 * df["night_event_ratio"]
        + 0.50 * df["bad_weather_event_ratio"]
        + 0.12 * df["phone_usage_count"]
        + profile_effect
    )
    lam = df["exposure_km"] * np.exp(linear)
    df["claim_count"] = rng.poisson(np.clip(lam, 0.0001, 2.5))
    return df


def train(df: pd.DataFrame, output_dir: Path, model_path: Path) -> None:
    df = prepare_features(df)
    if "claim_count" not in df:
        df = add_target(df)

    train_df, test_df = train_test_split(df, test_size=0.25, random_state=2026)
    y_train = train_df["claim_count"]

    output_dir.mkdir(parents=True, exist_ok=True)
    model_path.parent.mkdir(parents=True, exist_ok=True)

    if sm is not None:
        x_train = sm.add_constant(train_df[FEATURE_COLUMNS], has_constant="add")
        x_test = sm.add_constant(test_df[FEATURE_COLUMNS], has_constant="add")
        offset_train = np.log(train_df["exposure_km"].clip(lower=0.001))
        offset_test = np.log(test_df["exposure_km"].clip(lower=0.001))
        model = sm.GLM(y_train, x_train, family=sm.families.Poisson(), offset=offset_train)
        result = model.fit()
        predictions = result.predict(x_test, offset=offset_test)
        joblib.dump({"model_type": "statsmodels_glm", "result": result, "feature_columns": FEATURE_COLUMNS}, model_path)
        coef = pd.DataFrame({"term": result.params.index, "coef": result.params.values, "exp_coef": np.exp(result.params.values)})
        summary_text = result.summary().as_text()
    else:
        if PoissonRegressor is None:
            raise RuntimeError("Install statsmodels or scikit-learn to train the Poisson model.")
        model = PoissonRegressor(alpha=0.001, max_iter=1000)
        exposure_train = train_df["exposure_km"].clip(lower=0.001)
        exposure_test = test_df["exposure_km"].clip(lower=0.001)
        # This estimates claim frequency and weights observations by exposure.
        model.fit(train_df[FEATURE_COLUMNS], y_train / exposure_train, sample_weight=exposure_train)
        predicted_frequency = model.predict(test_df[FEATURE_COLUMNS])
        predictions = predicted_frequency * exposure_test
        joblib.dump({"model_type": "sklearn_poisson_rate", "result": model, "feature_columns": FEATURE_COLUMNS}, model_path)
        coef = pd.DataFrame(
            {
                "term": ["intercept", *FEATURE_COLUMNS],
                "coef": [model.intercept_, *model.coef_],
                "exp_coef": np.exp([model.intercept_, *model.coef_]),
            }
        )
        summary_text = (
            "statsmodels is not installed; trained sklearn.linear_model.PoissonRegressor "
            "on claim frequency with exposure_km as sample_weight.\n"
            f"intercept={model.intercept_}\n"
            f"coefficients={dict(zip(FEATURE_COLUMNS, model.coef_))}\n"
        )
    comparison = test_df[["driver_id", "exposure_km", "claim_count"]].copy()
    comparison["predicted_claim_count"] = predictions
    comparison["predicted_frequency"] = comparison["predicted_claim_count"] / comparison["exposure_km"].clip(lower=0.001)

    metrics = {
        "mae": float(mean_absolute_error(comparison["claim_count"], comparison["predicted_claim_count"])),
        "rmse": float(math.sqrt(mean_squared_error(comparison["claim_count"], comparison["predicted_claim_count"]))),
        "mean_claim_count": float(comparison["claim_count"].mean()),
        "mean_predicted_claim_count": float(comparison["predicted_claim_count"].mean()),
        "poisson_note": "If residual variance materially exceeds the mean, consider Negative Binomial or Tweedie.",
    }

    df.to_csv(output_dir / "training_dataset.csv", index=False)
    coef.to_csv(output_dir / "glm_coefficients.csv", index=False)
    comparison.to_csv(output_dir / "glm_test_predictions.csv", index=False)
    (output_dir / "glm_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    (output_dir / "glm_summary.txt").write_text(summary_text, encoding="utf-8")

    print(summary_text)
    print(json.dumps(metrics, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--features-path", default="data/historical_features/tumbling_1m")
    parser.add_argument("--output-dir", default="data/model_outputs")
    parser.add_argument("--model-path", default="models/glm_model.pkl")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    df = load_features(Path(args.features_path))
    train(df, Path(args.output_dir), Path(args.model_path))


if __name__ == "__main__":
    main()
