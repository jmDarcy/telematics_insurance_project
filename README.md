# Telematics Insurance Real-Time Analytics Project

This repository contains a course-oriented real-time analytics project for telematics-based motor insurance. It demonstrates how synthetic driving events can be produced, streamed through Kafka, processed with Spark Structured Streaming, converted into driver-level features, scored with an interpretable GLM, and used for both periodic premium scoring and real-time operational decisions.

The project was developed in the context of a Real-Time Analytics course workspace:

```text
C:\Users\jakob\OneDrive\Pulpit\A_MAGISTERKA SGH\Analiza_danych_w_czasie_rzeczywistym\Project
```

## Repository Structure

```text
telematics_insurance_project/
|-- producer_telematics.py
|-- spark_streaming_features.py
|-- train_glm.py
|-- score_premiums.py
|-- premium_api.py
|-- real-time-decision-extension/
|   |-- real_time_decision_engine.py
|   |-- intervention_consumer.py
|   |-- offline_intervention_demo.py
|   |-- create_topics.sh
|   `-- README.md
|-- notebooks/
|   `-- 01_telematics_project.ipynb
|-- data/
|   |-- model_outputs/
|   |-- premium_history/
|   |-- historical_features/
|   `-- checkpoints/
|-- models/
|-- scripts/
|   `-- create_topics.sh
|-- requirements.txt
|-- .gitignore
`-- README.md
```

## Component Overview

| Path | Purpose |
| --- | --- |
| `producer_telematics.py` | Generates synthetic telematics events and publishes them to Kafka topic `telematics_raw`. |
| `spark_streaming_features.py` | Reads raw Kafka events, parses JSON, builds risk flags, emits alerts, and writes windowed driver features. |
| `train_glm.py` | Trains a Poisson GLM on historical driver-window aggregates. |
| `score_premiums.py` | Applies the trained GLM to update technical premium estimates with a capped change rule. |
| `premium_api.py` | Minimal FastAPI service exposing the latest premium for a selected driver. |
| `real-time-decision-extension/` | Real-time operational decision layer for safety nudges, assistance checks, and safe-driving rewards. |
| `notebooks/` | Notebook walkthrough for the end-to-end course project. |
| `data/model_outputs/` | Model summaries, coefficients, metrics, predictions, and training dataset exports. |
| `data/premium_history/` | Stored history of technical premium updates. |
| `models/` | Serialized GLM model artifact. |
| `scripts/create_topics.sh` | Kafka topic creation helper for the main pipeline and decision extension. |

## Business Context

The base pipeline supports usage-based insurance and pay-how-you-drive analysis. It does not mechanically change the insurance premium after a single driving event. Instead, events are aggregated into driver features and used for periodic risk and premium scoring.

The real-time decision extension adds an operational decision layer. It can react while a trip is active by recommending:

- `SEND_SAFETY_NUDGE` - warn the driver when short-window risk is elevated,
- `CHECK_DRIVER_STATUS` - ask if the driver is safe after a potentially severe event,
- `GRANT_SAFE_DRIVING_POINTS` - reward calm driving in a recent time window.

This separates actuarial pricing from real-time service actions. The premium remains a periodic model output, while real-time analytics creates immediate customer-care and risk-prevention decisions.

## Processing Flow

```text
synthetic telematics event
-> Kafka topic telematics_raw
-> Spark Structured Streaming parsing and risk flags
-> Kafka topic telematics_alerts
-> tumbling/sliding window driver features
-> historical feature store
-> batch GLM training
-> periodic premium scoring
```

Optional real-time decision flow:

```text
synthetic telematics event
-> Kafka topic telematics_raw
-> event and sliding-window risk rules
-> Kafka topic driver_interventions
-> safety nudge, assistance check, or safe-driving reward
```

## Kafka Topics

```bash
kafka-topics.sh --create --if-not-exists --topic telematics_raw --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic telematics_alerts --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic driver_features --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic premium_updates --bootstrap-server broker:9092
kafka-topics.sh --create --if-not-exists --topic driver_interventions --bootstrap-server broker:9092
```

You can create all topics with:

```bash
bash scripts/create_topics.sh
```

## Prerequisites

- Python 3.10 or newer,
- Kafka broker available as `broker:9092`,
- Spark / PySpark with the Spark-Kafka connector,
- Python packages from `requirements.txt`,
- optional JupyterLab for notebook execution.

Install Python dependencies:

```bash
pip install -r requirements.txt
```

Spark submit examples:

```bash
# Spark 3.5 / Scala 2.12
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming_features.py

# Spark 4.0 preview / Scala 2.13
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 spark_streaming_features.py
```

## Suggested Run Path

1. Create Kafka topics:

```bash
bash scripts/create_topics.sh
```

2. Start the synthetic event producer:

```bash
python producer_telematics.py --drivers 50 --events-per-second 10 --duration-seconds 300 --seed 2026
```

3. Start the Spark streaming feature pipeline:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming_features.py
```

4. Preview risk alerts:

```bash
kafka-console-consumer.sh --bootstrap-server broker:9092 --topic telematics_alerts --from-beginning
```

5. Train the GLM:

```bash
python train_glm.py
```

If streaming parquet features are not available, the training script generates a synthetic historical training dataset.

6. Score technical premiums:

```bash
python score_premiums.py
```

7. Optionally publish premium updates to Kafka:

```bash
python score_premiums.py --publish-kafka
```

8. Optionally start the premium API:

```bash
uvicorn premium_api:app --host 0.0.0.0 --port 8000
```

9. Optionally run the real-time decision extension:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 real-time-decision-extension/real_time_decision_engine.py --show-console
```

10. Preview real-time intervention decisions:

```bash
python real-time-decision-extension/intervention_consumer.py --from-beginning
```

## Data Schema

The producer publishes JSON events to `telematics_raw`. Example:

```json
{
  "event_id": "EV000001",
  "driver_id": "D001",
  "vehicle_id": "V001",
  "driver_profile": "aggressive",
  "event_time": "2026-05-21T12:31:10.123Z",
  "speed_kmh": 72.4,
  "speed_limit_kmh": 50,
  "acceleration_ms2": 1.8,
  "braking_ms2": -3.5,
  "cornering_g": 0.42,
  "road_type": "urban",
  "weather": "rain",
  "is_night": false,
  "distance_delta_km": 0.12,
  "phone_usage": false
}
```

Synthetic profiles include `safe`, `average`, `aggressive`, `night_driver`, and `urban_driver`. These profiles affect speed, acceleration, braking, night driving, road type, and phone usage distributions.

## GLM Model

The premium scoring model is a Poisson GLM:

```text
claim_count ~ speeding_ratio
            + hard_braking_count_per_100km
            + harsh_acceleration_count_per_100km
            + night_event_ratio
            + bad_weather_event_ratio
            + phone_usage_count
            + offset(log(exposure_km))
```

The offset `log(exposure_km)` means the model estimates claim frequency relative to driven distance rather than treating raw claim counts as directly comparable across different exposure levels.

Model outputs are stored in `data/model_outputs/`:

- `training_dataset.csv`,
- `glm_coefficients.csv`,
- `glm_test_predictions.csv`,
- `glm_metrics.json`,
- `glm_summary.txt`.

## Premium Scoring

The scoring script computes:

```text
risk_multiplier = predicted_frequency / average_predicted_frequency
technical_premium = base_premium * risk_multiplier
```

The default base premium is 1000 PLN. Each update is capped at +/-10% relative to the previous premium estimate to avoid excessive volatility after a small batch of new data.

Premium history is stored in:

```text
data/premium_history/premium_history.csv
```

## Limitations

- The data is synthetic and must not be used for real insurance pricing.
- The GLM is intentionally simple and educational.
- A real insurance premium depends on many non-telematics variables.
- A single event should not mechanically change the premium.
- Poisson GLM may be insufficient under material overdispersion; Negative Binomial or Tweedie models may be more appropriate.
- Telematics data requires explicit consent, privacy controls, retention policies, and access control.
- Kafka and Spark are justified by streaming architecture and scale; in this small repository they are primarily educational.
- The real-time decision extension uses demonstrative rules. A production version would need cooldowns, deduplication, monitoring, auditability, and integration with a mobile app, CRM, or assistance workflow.
