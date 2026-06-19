COMPOSE := docker compose
PYTHONPATH := /opt/project/src:/opt/project
SPARK_SUBMIT := $(COMPOSE) exec -e PYTHONPATH=$(PYTHONPATH) spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077

.PHONY: build up down restart logs ps init create-topics validate-dataset ingest-sample bronze-stream silver train score-stream gold quality test test-docker clean

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

restart:
	$(COMPOSE) restart

logs:
	$(COMPOSE) logs -f --tail=200

ps:
	$(COMPOSE) ps

init:
	$(SPARK_SUBMIT) --conf spark.executorEnv.PYTHONPATH=$(PYTHONPATH) --conf spark.driverEnv.PYTHONPATH=$(PYTHONPATH) /opt/project/jobs/init/create_iceberg_tables.py

create-topics:
	$(COMPOSE) run --rm kafka-init

validate-dataset:
	$(COMPOSE) exec spark-master python3 /opt/project/jobs/ingestion/produce_transactions.py --validate-only --input /opt/project/data/input/creditcard.csv

ingest-sample:
	$(COMPOSE) exec spark-master python3 /opt/project/jobs/ingestion/produce_transactions.py --input /opt/project/data/input/creditcard.csv --bootstrap-servers kafka:9092 --topic transactions.raw --batch-size 100 --sleep-seconds 1 --max-records 1000

bronze-stream:
	$(SPARK_SUBMIT) --conf spark.executorEnv.PYTHONPATH=$(PYTHONPATH) --conf spark.driverEnv.PYTHONPATH=$(PYTHONPATH) /opt/project/jobs/streaming/kafka_to_bronze.py

silver:
	$(SPARK_SUBMIT) --conf spark.executorEnv.PYTHONPATH=$(PYTHONPATH) --conf spark.driverEnv.PYTHONPATH=$(PYTHONPATH) /opt/project/jobs/processing/bronze_to_silver.py

train:
	$(SPARK_SUBMIT) --conf spark.executorEnv.PYTHONPATH=$(PYTHONPATH) --conf spark.driverEnv.PYTHONPATH=$(PYTHONPATH) /opt/project/jobs/training/train_fraud_model.py

score-stream:
	$(SPARK_SUBMIT) --conf spark.executorEnv.PYTHONPATH=$(PYTHONPATH) --conf spark.driverEnv.PYTHONPATH=$(PYTHONPATH) /opt/project/jobs/streaming/score_transactions.py

gold:
	$(SPARK_SUBMIT) --conf spark.executorEnv.PYTHONPATH=$(PYTHONPATH) --conf spark.driverEnv.PYTHONPATH=$(PYTHONPATH) /opt/project/jobs/gold/build_gold_aggregations.py

quality:
	$(SPARK_SUBMIT) --conf spark.executorEnv.PYTHONPATH=$(PYTHONPATH) --conf spark.driverEnv.PYTHONPATH=$(PYTHONPATH) /opt/project/jobs/quality/data_quality_report.py

test:
	pytest -q

test-docker:
	$(COMPOSE) exec spark-master bash -lc "cd /opt/project && PYTHONPATH=/opt/project/src:/opt/project pytest -q"

clean:
	$(COMPOSE) down -v
