FROM apache/airflow:2.10.5-python3.11

USER root
# Spark needs a JVM; default-jre-headless is OpenJDK 17 on Debian bookworm and also
# provides the arch-independent /usr/lib/jvm/default-java path (amd64 and arm64).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt requirements-dev.txt /tmp/
# Pinning apache-airflow + the official constraints keeps pip from changing the base install.
RUN PY_VERSION="$(python -c 'import sys; print(f"{sys.version_info[0]}.{sys.version_info[1]}")')" \
    && pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements-dev.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_VERSION}.txt"

# Resolve the Delta Lake jars at build time so tasks never depend on Maven at runtime.
# Containers run as the host UID with GID 0 (docker-compose.yml), hence the g+rwX, and the
# cache path is explicit because that UID may not have a home directory the JVM can see.
ENV SPARK_IVY_DIR=/home/airflow/.ivy2
RUN python -c "import os; from delta import configure_spark_with_delta_pip; \
from pyspark.sql import SparkSession; \
b = SparkSession.builder.master('local[1]').config('spark.jars.ivy', os.environ['SPARK_IVY_DIR']); \
configure_spark_with_delta_pip(b).getOrCreate().stop()" \
    && chmod -R g+rwX "${SPARK_IVY_DIR}"
