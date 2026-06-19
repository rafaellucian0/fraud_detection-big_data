FROM apache/spark:3.5.1

USER root

RUN pip install --no-cache-dir \
    PyYAML==6.0.2 \
    kafka-python==2.0.2 \
    numpy==1.24.4 \
    pytest==8.2.2 \
    && mkdir -p /tmp/.ivy2 \
    && chmod -R 777 /tmp/.ivy2

USER spark
