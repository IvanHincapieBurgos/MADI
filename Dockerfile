# syntax=docker/dockerfile:1
# =====================================================================
#  Airflow image
#  Adds everything the pipeline needs to run END-TO-END in ONE container
#  using the LocalExecutor:
#    * Java (OpenJDK 17)      -> required by PySpark
#    * PySpark + pyarrow      -> Transform stage
#    * apify-client           -> Extract stage
#    * dbt-postgres (in venv) -> Load & Model stage
# =====================================================================
ARG AIRFLOW_VERSION=2.10.3
ARG PYTHON_VERSION=3.11
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        procps \
        curl \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

#  PostgreSQL JDBC
ARG POSTGRES_JDBC_VERSION=42.7.4
RUN mkdir -p /opt/spark_jars \
    && curl -fsSL -o /opt/spark_jars/postgresql.jar \
       "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_JDBC_VERSION}/postgresql-${POSTGRES_JDBC_VERSION}.jar" \
    && chmod -R a+r /opt/spark_jars
ENV POSTGRES_JDBC_JAR=/opt/spark_jars/postgresql.jar

#  Python dependencies for Airflow (installed WITH constraints).
USER airflow
ARG AIRFLOW_VERSION
ARG PYTHON_VERSION
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Test tooling, so the pytest suite is runnable inside the container
COPY requirements-dev.txt /tmp/requirements-dev.txt
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

#  dbt 
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt_venv \
    && /opt/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/dbt_venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt

ENV DBT_BIN=/opt/dbt_venv/bin/dbt

#  Make project code importable as `src.*`, `etl.*` from any task.
ENV PYTHONPATH="/opt/airflow:${PYTHONPATH}"

# pytest config (so `pytest` picks up testpaths/markers inside the container;

COPY pytest.ini /opt/airflow/pytest.ini
