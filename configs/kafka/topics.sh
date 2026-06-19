#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic transactions.raw --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic transactions.scored --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --create --if-not-exists --topic fraud.alerts --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server "$BOOTSTRAP_SERVER" --list
