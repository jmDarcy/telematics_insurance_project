# Real-Time Decision Extension

This folder adds a real-time operational decision layer to the telematics insurance project.

The base project keeps the actuarial logic conservative: a single driving event does not change the premium. Events are aggregated first, then the GLM and technical premium are updated periodically. This extension addresses a different business question: what should the insurer do while the customer is actively driving?

## Added Components

| File | Purpose |
| --- | --- |
| `real_time_decision_engine.py` | Spark Structured Streaming job that reads `telematics_raw` and publishes operational decisions to `driver_interventions`. |
| `intervention_consumer.py` | Kafka consumer for previewing intervention decisions. |
| `offline_intervention_demo.py` | Local fallback demo that simulates events without Kafka or Spark. |
| `create_topics.sh` | Creates the extension topic `driver_interventions`. |
| `README.md` | This documentation. |

## Decision Flow

```text
raw telematics event
-> event risk flags
-> 5-minute sliding window
-> operational decision
-> Kafka topic driver_interventions
```

## Decision Types

### CHECK_DRIVER_STATUS

Triggered by events that look like a potential crash or severe incident, for example extreme braking.

Example action:

```text
Send push/SMS: "Are you safe?"
Prepare an assistance triage case.
```

This is not premium pricing. It is an operational care response.

### SEND_SAFETY_NUDGE

Triggered when recent driving behavior shows elevated risk, such as speeding, phone usage, harsh braking, night driving, or poor weather.

Example action:

```text
Send an in-app safety message before the risky pattern becomes a claim.
```

### GRANT_SAFE_DRIVING_POINTS

Triggered when recent driving is calm and does not contain risk events.

Example action:

```text
Add safe-driving points to the customer's reward wallet.
```

## Business Rationale

The competitive value is not that the premium changes after every braking event. That would be unstable and difficult to defend actuarially.

The value is that the insurer can react during the trip:

- warn the customer when risk is rising,
- check whether the customer is safe after a severe event,
- reward safe behavior without waiting for a monthly or renewal cycle,
- reduce expected claim frequency through prevention,
- create a service experience based on care rather than only risk classification.

## Kafka Topic

```text
driver_interventions
```

Create only the extension topic:

```bash
bash real-time-decision-extension/create_topics.sh
```

The main project topic script also creates this topic:

```bash
bash scripts/create_topics.sh
```

## Run with Kafka and Spark

Run all commands from the repository root.

1. Start the synthetic producer:

```bash
python producer_telematics.py --drivers 50 --events-per-second 10 --duration-seconds 300 --seed 2026
```

2. Start the real-time decision engine:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 real-time-decision-extension/real_time_decision_engine.py --show-console
```

For Spark 4.0 preview / Scala 2.13:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0-preview2 real-time-decision-extension/real_time_decision_engine.py --show-console
```

3. Preview decisions:

```bash
kafka-console-consumer.sh --bootstrap-server broker:9092 --topic driver_interventions --from-beginning
```

or:

```bash
python real-time-decision-extension/intervention_consumer.py --from-beginning
```

Expected decision fields include:

```text
decision_type
priority
recommended_action
customer_message
risk_score_event or risk_score_5m
```

## Offline Demo

When Kafka and Spark are not available, run:

```bash
python real-time-decision-extension/offline_intervention_demo.py
```

The script generates synthetic telematics events, applies the same decision ideas in memory, and writes JSONL output to:

```text
data/real-time-decision-extension/offline_interventions.jsonl
```

This fallback is useful for presentations. It does not replace the streaming architecture.

## Limitations

- Rules are demonstrative and not validated on real claims data.
- A production system would need notification cooldowns and deduplication.
- Customer messaging requires consent and regulatory review.
- Intervention thresholds need historical calibration.
- Every decision should be auditable and traceable to input events.
- Integration with a mobile app, assistance platform, CRM, or claims system is outside this repository.
