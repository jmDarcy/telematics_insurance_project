#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-broker:9092}"

kafka-topics.sh --create --if-not-exists --topic driver_interventions --bootstrap-server "$BOOTSTRAP_SERVER"

echo "Extension topics:"
kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP_SERVER" | grep -E "driver_interventions|telematics_raw"

echo "Preview real-time interventions:"
echo "kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic driver_interventions --from-beginning"
