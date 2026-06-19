"""Produce ULB credit card transactions to Kafka as JSON events."""

from __future__ import annotations

import argparse
import csv
import itertools
import logging
import time
from pathlib import Path

from kafka import KafkaProducer

from fraud_lakehouse.config import require_dataset
from fraud_lakehouse.kafka_utils import json_bytes, row_to_event
from fraud_lakehouse.schemas import validate_creditcard_columns


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("produce_transactions")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/input/creditcard.csv")
    parser.add_argument("--bootstrap-servers", default="localhost:29092")
    parser.add_argument("--topic", default="transactions.raw")
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--sleep-seconds", type=float, default=0.0)
    parser.add_argument("--max-records", type=int, default=0)
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--replay-mode", choices=["once", "loop"], default="once")
    parser.add_argument("--validate-only", action="store_true")
    return parser.parse_args()


def iter_rows(path: Path, start_offset: int, replay_mode: str):
    while True:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            validate_creditcard_columns(reader.fieldnames or [])
            for index, row in enumerate(reader):
                if index < start_offset:
                    continue
                yield row
        if replay_mode != "loop":
            break


def main() -> None:
    args = parse_args()
    dataset = require_dataset(args.input)
    with dataset.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        validate_creditcard_columns(reader.fieldnames or [])

    if args.validate_only:
        LOGGER.info("Dataset validation succeeded: %s", dataset)
        return

    producer = KafkaProducer(bootstrap_servers=args.bootstrap_servers, value_serializer=json_bytes)
    sent = 0
    rows = iter_rows(dataset, args.start_offset, args.replay_mode)
    if args.max_records > 0:
        rows = itertools.islice(rows, args.max_records)

    batch = []
    for row in rows:
        batch.append(row_to_event(row, source_file=dataset.name))
        if len(batch) >= args.batch_size:
            for event in batch:
                producer.send(args.topic, event, key=event["transaction_id"].encode("utf-8"))
            producer.flush()
            sent += len(batch)
            LOGGER.info("Published %s events to %s", sent, args.topic)
            batch.clear()
            if args.sleep_seconds > 0:
                time.sleep(args.sleep_seconds)

    for event in batch:
        producer.send(args.topic, event, key=event["transaction_id"].encode("utf-8"))
    producer.flush()
    LOGGER.info("Finished publishing %s events", sent + len(batch))


if __name__ == "__main__":
    main()

