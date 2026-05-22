#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-broker:9092}"

kafka-topics.sh --create --if-not-exists --topic telematics_raw --bootstrap-server "$BOOTSTRAP_SERVER"
kafka-topics.sh --create --if-not-exists --topic telematics_alerts --bootstrap-server "$BOOTSTRAP_SERVER"
kafka-topics.sh --create --if-not-exists --topic driver_features --bootstrap-server "$BOOTSTRAP_SERVER"
kafka-topics.sh --create --if-not-exists --topic premium_updates --bootstrap-server "$BOOTSTRAP_SERVER"

echo "Topics:"
kafka-topics.sh --list --bootstrap-server "$BOOTSTRAP_SERVER"

echo "Preview raw events:"
echo "kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic telematics_raw --from-beginning --max-messages 5"
echo "Preview alerts:"
echo "kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_SERVER --topic telematics_alerts --from-beginning --max-messages 5"
