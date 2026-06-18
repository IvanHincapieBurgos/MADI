"""
Centralizes SparkSession creation so every job gets the PostgreSQL JDBC
driver on the classpath the same way:
* In the container the driver JAR is baked into the image and its path is
  exposed via ``POSTGRES_JDBC_JAR`` -> added through ``spark.jars``.
* Elsewhere (e.g. ad-hoc local runs) we fall back to ``spark.jars.packages``
  which pulls the driver from Maven Central at startup.
"""
from __future__ import annotations

import os
from typing import Any

# Used only when no pre-downloaded JAR is available.
_JDBC_MAVEN_COORD = "org.postgresql:postgresql:42.7.4"


def get_spark_session(
    app_name: str = "madi-transform",
    *,
    extra_conf: dict[str, str] | None = None,
):
    '''
    Create (or get) a configured local SparkSession.
    '''
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.session.timeZone", "America/Bogota")
        # Keep the local run lean / deterministic.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )

    jar_path = os.environ.get("POSTGRES_JDBC_JAR")
    if jar_path and os.path.exists(jar_path):
        builder = builder.config("spark.jars", jar_path)
    else:
        builder = builder.config("spark.jars.packages", _JDBC_MAVEN_COORD)

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
