# syntax=docker/dockerfile:1.7
# Local Spark Structured Streaming ingestor: Kafka -> Parquet micro-batches -> UC Volume (see ADR-002).
# Spark 4.0.4 / Scala 2.13 / Java 17 / Python 3.10 (Ubuntu 22.04).
FROM apache/spark:4.0.4-scala2.13-java17-python3-ubuntu

ARG SPARK_VERSION=4.0.4
ARG SCALA_BINARY=2.13
ARG MAVEN=https://repo1.maven.org/maven2
ARG DATABRICKS_SDK_VERSION=0.156.0

USER root

# Kafka source connector + its runtime deps (everything else it needs already ships in $SPARK_HOME/jars:
# lz4, snappy, zstd-jni, slf4j, jsr305, scala-parallel-collections, hadoop-client-runtime, spark-tags).
# Baked in so spark-submit needs no --packages / Maven access at startup. Checksums pinned (sha256).
ADD --chmod=644 --checksum=sha256:b2f7b3a4fbf292b5bf10bd4dd6af301eded9c9985b16239b407401d66dbc8a25 \
    ${MAVEN}/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:ae9817053af94a21992e948269eba1a50d511333cb51462865983abfbeea7f60 \
    ${MAVEN}/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY}-${SPARK_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:7568b998572d256f0b7bc0afdc1b7a2588b8b08415c62ce314c864a6851ae9d9 \
    ${MAVEN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar /opt/spark/jars/
ADD --chmod=644 --checksum=sha256:6d3bd18df8410f3e31b031aca582cc109342358a62a2759ebd0c4cdf30d06f8b \
    ${MAVEN}/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar /opt/spark/jars/

# databricks-sdk pinned to the same version as uv.lock (Files API upload to the UC Volume).
RUN pip install --no-cache-dir --disable-pip-version-check "databricks-sdk==${DATABRICKS_SDK_VERSION}" \
 && mkdir -p /checkpoints /app \
 && chown spark:spark /checkpoints

ENV PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    HOME=/home/spark

RUN mkdir -p /home/spark && chown spark:spark /home/spark

WORKDIR /app
# Non-root (uid 185 from the base image). The token arrives only at runtime via env, never baked in.
USER spark

# Default: print versions. docker-compose overrides with `spark-submit /app/ingest_orders.py`.
CMD ["/opt/spark/bin/spark-submit", "--version"]
