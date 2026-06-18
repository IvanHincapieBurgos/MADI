"""Silver layer
Pipeline
--------
raw JSON (Bronze)
  -> read with an EXPLICIT schema (multiLine array, corrupt-record guard)
  -> clean & cast types, handle nulls
  -> flatten nested ``technicalDetails`` (map) + ``contact`` (struct)
  -> derive ``allows_pets`` from free text
  -> write Parquet (Silver) to data/processed/
  -> append to PostgreSQL ``staging.stg_listings`` via JDBC
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

# --- Make `src`/`etl` importable when run as a plain file -------------
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import (  # noqa: E402
    BooleanType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from src.data.features import NEGATIVE_PET_REGEX, POSITIVE_PET_REGEX  # noqa: E402
from src.utils.config_loader import load_config, resolve_path  # noqa: E402
from src.utils.db import (  # noqa: E402
    get_staging_target,
    get_warehouse_jdbc_properties,
    get_warehouse_jdbc_url,
)
from src.utils.logger import get_logger  # noqa: E402
from src.utils.spark import get_spark_session  # noqa: E402

LATEST_FILENAME = "finca_raiz_latest.json"
_CORRUPT_COL = "_corrupt_record"


class TransformError(Exception):
    """Raised when the raw input is empty/malformed or transform fails."""

def build_input_schema() -> StructType:
    '''
    The input JSON has this shape (example with all fields):
    '''
    contact_struct = StructType(
        [
            StructField("name", StringType()),
            StructField("phone", StringType()),
            StructField("whatsapp", BooleanType()),
        ]
    )
    return StructType(
        [
            StructField("id", LongType()),
            StructField("title", StringType()),
            StructField("url", StringType()),
            StructField("propertyType", StringType()),
            StructField("operationType", StringType()),
            StructField("price", DoubleType()),
            StructField("adminPrice", DoubleType()),
            StructField("currency", StringType()),
            StructField("bedrooms", DoubleType()),
            StructField("bathrooms", DoubleType()),
            StructField("area", DoubleType()),
            StructField("garage", DoubleType()),
            StructField("antiquity", DoubleType()),
            StructField("stratum", DoubleType()),
            StructField("address", StringType()),
            StructField("location", StringType()),
            StructField("latitude", DoubleType()),
            StructField("longitude", DoubleType()),
            StructField("description", StringType()),
            StructField("technicalDetails", MapType(StringType(), StringType())),
            StructField("contact", contact_struct),
            StructField("publishedDate", StringType()),
            StructField("updatedDate", StringType()),
            StructField(_CORRUPT_COL, StringType()),
        ]
    )


def read_raw(spark: SparkSession, path: Path, logger) -> DataFrame:
    '''
    Read the Bronze JSON array with an explicit schema and guard rails.
    '''
    if not path.exists():
        raise TransformError(f"Raw input file does not exist: {path}")
    if path.stat().st_size == 0:
        raise TransformError(f"Raw input file is empty: {path}")

    logger.info("Reading raw JSON: %s", path)
    df = (
        spark.read.schema(build_input_schema())
        .option("multiLine", "true")          # file is a pretty-printed array
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", _CORRUPT_COL)
        .json(str(path))
    )

    total = df.count()
    if total == 0:
        raise TransformError(f"No records parsed from {path} (empty/malformed).")

    corrupt = df.filter(F.col(_CORRUPT_COL).isNotNull()).count()
    if corrupt:
        logger.warning("%d/%d records were malformed and skipped.", corrupt, total)
    df = df.filter(F.col(_CORRUPT_COL).isNull()).drop(_CORRUPT_COL)

    valid = df.count()
    if valid == 0:
        raise TransformError(f"All {total} records in {path} were malformed.")

    logger.info("Parsed %d valid record(s).", valid)
    return df


def clean_and_cast(df: DataFrame, logger) -> DataFrame:
    '''
    Explicit type casts + graceful null handling.
    '''
    logger.info("Casting types and handling nulls...")
    return (
        df.withColumn("listing_id", F.col("id").cast(LongType()))
        .withColumn("title", F.trim(F.col("title")))
        .withColumn("property_type", F.lower(F.trim(F.col("propertyType"))))
        .withColumn("operation_type", F.lower(F.trim(F.col("operationType"))))
        # Prices are whole COP pesos -> Long. Negative/invalid -> null.
        .withColumn("price", F.col("price").cast(LongType()))
        .withColumn("admin_price", F.col("adminPrice").cast(LongType()))
        .withColumn("price", F.when(F.col("price") > 0, F.col("price")))
        .withColumn(
            "admin_price",
            F.when(F.col("admin_price") >= 0, F.col("admin_price")),
        )
        # Counts -> Integer (cast through to drop decimals safely).
        .withColumn("bedrooms", F.col("bedrooms").cast("int"))
        .withColumn("bathrooms", F.col("bathrooms").cast("int"))
        .withColumn("garage", F.coalesce(F.col("garage").cast("int"), F.lit(0)))
        .withColumn("antiquity_years", F.col("antiquity").cast("int"))
        .withColumn("stratum", F.col("stratum").cast("int"))
        .withColumn("area_m2", F.col("area").cast(DoubleType()))
        .withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        # Normalize the currency placeholder "$" -> "COP".
        .withColumn(
            "currency",
            F.when(
                (F.col("currency").isNull()) | (F.trim(F.col("currency")) == "$"),
                F.lit("COP"),
            ).otherwise(F.col("currency")),
        )
        .withColumn("published_date", F.to_date("publishedDate"))
        .withColumn("updated_date", F.to_date("updatedDate"))
    )


def flatten(df: DataFrame, logger) -> DataFrame:
    '''
    Lift nested technicalDetails (map) and contact (struct) to columns.
    '''
    logger.info("Flattening nested structures (technicalDetails, contact)...")
    return (
        df
        # Specific technicalDetails keys -> top-level columns.
        .withColumn("floor", F.col("technicalDetails").getItem("Piso"))
        .withColumn("property_state", F.col("technicalDetails").getItem("Estado"))
        # contact struct -> columns.
        .withColumn("contact_name", F.col("contact").getField("name"))
        .withColumn("contact_phone", F.col("contact").getField("phone"))
        .withColumn(
            "contact_whatsapp",
            F.coalesce(F.col("contact").getField("whatsapp"), F.lit(False)),
        )
        # Split "Neighborhood, City, Department" into parts.
        .withColumn("location_raw", F.col("location"))
        .withColumn("_loc_parts", F.split(F.col("location"), r"\s*,\s*"))
        .withColumn("neighborhood", F.trim(F.col("_loc_parts").getItem(0)))
        .withColumn("city", F.trim(F.col("_loc_parts").getItem(1)))
        .drop("_loc_parts")
    )


def derive_features(df: DataFrame, logger) -> DataFrame:
    '''
    Derive allows_pets from free text + a convenience total price.
    '''
    logger.info("Deriving features (allows_pets, price_total)...")
    # Build a single lowercased search blob: description + all map keys/values.
    pet_text = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("description"), F.lit("")),
            F.coalesce(
                F.array_join(F.map_keys(F.col("technicalDetails")), " "), F.lit("")
            ),
            F.coalesce(
                F.array_join(F.map_values(F.col("technicalDetails")), " "), F.lit("")
            ),
        )
    )
    return (
        df.withColumn("_pet_text", pet_text)
        .withColumn(
            "allows_pets",
            F.when(F.col("_pet_text").rlike(NEGATIVE_PET_REGEX), F.lit(False))
            .when(F.col("_pet_text").rlike(POSITIVE_PET_REGEX), F.lit(True))
            .otherwise(F.lit(False)),
        )
        .withColumn(
            "price_total",
            (F.coalesce(F.col("price"), F.lit(0)) + F.coalesce(F.col("admin_price"), F.lit(0))).cast(LongType()),
        )
        .drop("_pet_text")
    )


# Final, fully tabular column order written to Parquet + Postgres.
FINAL_COLUMNS: tuple[str, ...] = (
    "listing_id",
    "title",
    "url",
    "property_type",
    "operation_type",
    "price",
    "admin_price",
    "price_total",
    "currency",
    "bedrooms",
    "bathrooms",
    "area_m2",
    "garage",
    "antiquity_years",
    "stratum",
    "address",
    "location_raw",
    "neighborhood",
    "city",
    "latitude",
    "longitude",
    "floor",
    "property_state",
    "contact_name",
    "contact_phone",
    "contact_whatsapp",
    "allows_pets",
    "description",
    "published_date",
    "updated_date",
)


def select_final(df: DataFrame, source_file: str) -> DataFrame:
    '''
    Keep final columns + add load metadata.
    '''
    return (
        df.select(*[F.col(c) for c in FINAL_COLUMNS])
        .withColumn("source_file", F.lit(source_file))
        .withColumn("loaded_at", F.current_timestamp())
    )


def write_parquet(df: DataFrame, processed_dir: Path, logger) -> Path:
    processed_dir.mkdir(parents=True, exist_ok=True)
    out = processed_dir / "stg_listings"
    logger.info("Writing Parquet (Silver) -> %s", out)
    df.write.mode("overwrite").parquet(str(out))
    return out


def write_to_postgres(
    df: DataFrame, config: dict[str, Any], mode: str, logger
) -> str:
    target = get_staging_target(config)
    url = get_warehouse_jdbc_url(config)
    props = get_warehouse_jdbc_properties(config)
    logger.info("Writing to Postgres %s (mode=%s) via %s", target, mode, url)
    df.write.jdbc(url=url, table=target, mode=mode, properties=props)
    logger.info("Postgres write complete -> %s", target)
    return target


def _resolve_input(config: dict[str, Any], input_path: str | None) -> Path:
    if input_path:
        return Path(input_path) if Path(input_path).is_absolute() else resolve_path(input_path)
    return resolve_path(config["paths"]["raw_dir"]) / LATEST_FILENAME


def run_transform(
    config: dict[str, Any] | None = None,
    *,
    input_path: str | None = None,
    skip_db: bool = False,
    write_mode: str | None = None,
) -> dict[str, Any]:
    '''
    Execute the full transform. Returns a summary dict (XCom-friendly).
    '''
    cfg = config or load_config()
    log_level = cfg.get("logging", {}).get("level", "INFO")
    logger = get_logger("transform.finca_raiz", level=log_level)

    src_path = _resolve_input(cfg, input_path)
    processed_dir = resolve_path(cfg["paths"]["processed_dir"])
    mode = write_mode or cfg["warehouse"].get("staging_write_mode", "append")

    logger.info("=== Fincaraíz transform started ===")
    spark = get_spark_session(app_name="madi-transform-finca-raiz")
    try:
        raw = read_raw(spark, src_path, logger)
        cleaned = clean_and_cast(raw, logger)
        flat = flatten(cleaned, logger)
        featured = derive_features(flat, logger)
        final = select_final(featured, source_file=src_path.name).cache()

        row_count = final.count()
        pet_count = final.filter(F.col("allows_pets")).count()
        logger.info("Transformed %d rows (allows_pets=True: %d).", row_count, pet_count)

        parquet_path = write_parquet(final, processed_dir, logger)

        target = None
        if skip_db:
            logger.warning("--skip-db set: NOT writing to Postgres.")
        else:
            target = write_to_postgres(final, cfg, mode, logger)

        logger.info("=== Fincaraíz transform completed ===")
        return {
            "input": str(src_path),
            "parquet": str(parquet_path),
            "staging_table": target,
            "rows": row_count,
            "allows_pets_true": pet_count,
        }
    finally:
        spark.stop()


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MADI PySpark transform")
    p.add_argument("--input", help="Path to raw JSON (default: latest in data/raw/)")
    p.add_argument("--skip-db", action="store_true", help="Skip the Postgres write")
    p.add_argument("--write-mode", choices=["append", "overwrite"], help="JDBC write mode")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logger = get_logger("transform.finca_raiz")
    try:
        summary = run_transform(
            input_path=args.input, skip_db=args.skip_db, write_mode=args.write_mode
        )
    except TransformError as exc:
        logger.error("Transform failed: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - top-level guard
        logger.exception("Unexpected error during transform: %s", exc)
        return 2
    logger.info("Summary: %s", summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())