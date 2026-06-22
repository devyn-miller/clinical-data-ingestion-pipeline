import logging
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# Custom exception for pipeline failures.
class PipelineExecutionError(Exception):
    """Raised when a Medallion layer (Bronze/Silver/Gold) fails to complete."""

def _s3_join(*parts: str) -> str:
    """Join S3 path components without os.path.join's platform-specific separator."""
    return "/".join(p.strip("/") for p in parts if p)

def _get_spark_session() -> SparkSession:
    """Retrieve the active SparkSession regardless of context."""
    session = SparkSession.getActiveSession()
    if session is None:
        raise PipelineExecutionError(
            "No active SparkSession found. "
            "In Databricks this means the cluster has not initialized yet. "
            "Locally, bootstrap a session before calling run_pipeline()."
        )
    return session


S3_ROOT: str = os.getenv("S3_ROOT", "s3://choc-rady-clinical-bronze-demo")
BRONZE_INPUT_BASE: str = os.getenv("BRONZE_INPUT_BASE", S3_ROOT)
BRONZE_OUTPUT_BASE: str = os.getenv("BRONZE_OUTPUT_BASE", S3_ROOT)
SILVER_OUTPUT_BASE: str = os.getenv(
    "SILVER_OUTPUT_BASE",
    f"{S3_ROOT.rstrip('/')}/silver",
)
GOLD_OUTPUT_BASE: str = os.getenv(
    "GOLD_OUTPUT_BASE",
    f"{S3_ROOT.rstrip('/')}/gold",
)

METADATA_PATH: str = f"{BRONZE_INPUT_BASE.rstrip('/')}/raw/metadata.csv"
DICOM_PATH: str = f"{BRONZE_INPUT_BASE.rstrip('/')}/raw/dicom_manifest.csv"

PIPELINE_RUN_TS: str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
PIPELINE_RUN_DATE: str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

log.info("Pipeline run timestamp: %s", PIPELINE_RUN_TS)
log.info("Bronze input base: %s", BRONZE_INPUT_BASE)
log.info("Silver output base: %s", SILVER_OUTPUT_BASE)
log.info("Gold output base: %s", GOLD_OUTPUT_BASE)



METADATA_RAW_SCHEMA = StructType(
    [
        StructField("participant_id", StringType(), nullable=False),
        StructField("age", StringType(), nullable=True),
        StructField("site_location", StringType(), nullable=True),
        StructField("ehr_system", StringType(), nullable=True),
        StructField("lesion_status_code", StringType(), nullable=True),
        StructField("enrollment_date", StringType(), nullable=True),
    ]
)

DICOM_RAW_SCHEMA = StructType(
    [
        StructField("participant_id", StringType(), nullable=False),
        StructField("dicom_s3_uri", StringType(), nullable=True),
        StructField("modality", StringType(), nullable=True),
        StructField("body_region", StringType(), nullable=True),
    ]
)



def ingest_bronze(
    input_path: str,
    schema: StructType,
    layer_name: str,
    output_base: str,
) -> DataFrame:
    """
    Read a CSV from the Bronze landing zone, append lineage metadata, persist as Parquet, and return the DataFrame.
    Parameters: input_path, schema, layer_name, output_base.
    """
    log.info("[BRONZE] Reading %s from: %s", layer_name, input_path)

    spark = _get_spark_session()
    raw_df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(input_path)
    )

    row_count = raw_df.count()
    log.info("[BRONZE] %s — %d rows ingested from source.", layer_name, row_count)

    if row_count == 0:
        log.warning(
            "[BRONZE] %s — Zero rows read. Verify the source file exists at '%s'.",
            layer_name,
            input_path,
        )

    bronze_df = raw_df.withColumn(
        "ingested_at", F.lit(PIPELINE_RUN_TS).cast("string")
    ).withColumn(
        "source_file", F.lit(input_path)
    ).withColumn(
        "pipeline_run_date", F.lit(PIPELINE_RUN_DATE)
    )

    bronze_output = _s3_join(output_base, "bronze", layer_name)
    (
        bronze_df.write.mode("overwrite")
        .partitionBy("pipeline_run_date")
        .parquet(bronze_output)
    )
    log.info("[BRONZE] %s — Written to: %s", layer_name, bronze_output)

    return bronze_df



DIRTY_LESION_VALUES = {"-1", "NA", "na", "N/A", "n/a", "none", "None", ""}
POSITIVE_LESION_VALUES = {"1", "positive", "Positive", "POSITIVE"}

LESION_GOVERNANCE_RULE = (
    "lesion_status_code values of '1' or 'Positive' (any case) represent a "
    "confirmed lesion finding ('Lesion Detected'). Values of '-1', 'NA', or "
    "NULL represent a confirmed-clear read or a pending radiologist read, "
    "and are harmonized to 'No Lesion Detected'. Codes outside this known set "
    "are flagged via lesion_code_requires_review."
)

log.info("[SILVER] Governance rule registered: %s", LESION_GOVERNANCE_RULE)


def apply_lesion_harmonization(df: DataFrame, col_name: str = "lesion_status_code") -> DataFrame:
    """Normalize lesion status codes and flag unrecognized values."""
    normalized = F.lower(F.trim(F.col(col_name)))

    is_null_or_empty = F.col(col_name).isNull() | (F.trim(F.col(col_name)) == "")
    is_dirty_value = normalized.isin([v.lower() for v in DIRTY_LESION_VALUES if v != ""])
    is_positive_value = normalized.isin([v.lower() for v in POSITIVE_LESION_VALUES])
    is_recognized = is_null_or_empty | is_dirty_value | is_positive_value

    lesion_label = (
        F.when(is_null_or_empty, F.lit("No Lesion Detected"))
        .when(is_dirty_value, F.lit("No Lesion Detected"))
        .when(is_positive_value, F.lit("Lesion Detected"))
        .otherwise(F.lit("No Lesion Detected"))
    )

    df = df.withColumn("lesion_label", lesion_label)
    df = df.withColumn("lesion_code_requires_review", (~is_recognized).cast("boolean"))
    return df


def clean_metadata(df: DataFrame) -> DataFrame:
    """Clean, standardize, and type-cast metadata records."""
    log.info("[SILVER] Cleaning metadata — %d raw rows", df.count())

    df = df.filter(F.col("participant_id").isNotNull())

    string_cols = [
        "participant_id", "site_location", "ehr_system",
        "lesion_status_code", "enrollment_date",
    ]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.col(col)))

    df = (
        df.withColumn("age", F.col("age").cast(IntegerType()))
        .withColumn(
            "enrollment_date",
            F.to_date(F.col("enrollment_date"), "yyyy-MM-dd"),
        )
        .withColumn("site_location", F.upper(F.col("site_location")))
    )

    df = apply_lesion_harmonization(df, "lesion_status_code")

    df = df.withColumn(
        "has_data_quality_flag",
        (F.col("age").isNull() | F.col("enrollment_date").isNull()).cast("boolean"),
    )

    row_count = df.count()
    lesion_count = df.filter(F.col("lesion_label") == "Lesion Detected").count()
    no_lesion_count = df.filter(F.col("lesion_label") == "No Lesion Detected").count()
    review_count = df.filter(F.col("lesion_code_requires_review") == True).count()

    log.info(
        "[SILVER] Metadata cleaning complete: %d rows | Lesion Detected=%d | "
        "No Lesion Detected=%d | Flagged for review=%d",
        row_count, lesion_count, no_lesion_count, review_count,
    )
    return df


def clean_dicom(df: DataFrame) -> DataFrame:
    """Clean and standardize DICOM manifest records."""
    log.info("[SILVER] Cleaning DICOM manifest — %d raw rows", df.count())

    df = df.filter(
        F.col("participant_id").isNotNull() & F.col("dicom_s3_uri").isNotNull()
    )

    df = df.withColumn("modality", F.upper(F.trim(F.col("modality"))))
    df = df.withColumn("body_region", F.initcap(F.trim(F.col("body_region"))))

    df = df.withColumn(
        "uri_format_valid",
        F.col("dicom_s3_uri").startswith("s3://").cast("boolean"),
    )

    invalid_uri_count = df.filter(~F.col("uri_format_valid")).count()
    if invalid_uri_count > 0:
        log.warning(
            "[SILVER] DICOM manifest: %d records have non-standard S3 URI format.",
            invalid_uri_count,
        )

    log.info("[SILVER] DICOM manifest cleaning complete: %d rows", df.count())
    return df



def mask_participant_id(df: DataFrame, id_col: str = "participant_id") -> DataFrame:
    """Replace participant IDs with deterministic SHA-256 surrogate keys."""
    return df.withColumn(
        "subject_surrogate_id",
        F.substring(F.sha2(F.col(id_col), 256), 1, 16),
    ).drop(id_col)


def reduce_date_precision(df: DataFrame, date_col: str = "enrollment_date") -> DataFrame:
    """Convert enrollment dates to enrollment years."""
    return df.withColumn(
        "enrollment_year", F.year(F.col(date_col))
    ).drop(date_col)


GOLD_COLUMNS_TO_DROP = [
    "ingested_at",
    "source_file",
    "pipeline_run_date",
    "uri_format_valid",
    "has_data_quality_flag",
    "lesion_status_code",
    "ehr_system",
]

GOLD_COLUMN_ORDER = [
    "subject_surrogate_id",
    "age",
    "site_location",
    "enrollment_year",
    "lesion_label",
    "lesion_code_requires_review",
    "imaging_s3_uri",
    "modality",
    "body_region",
]



def run_pipeline() -> dict:
    """Execute the full Bronze → Silver → Gold pipeline and return run metrics."""
    log.info("=" * 70)
    log.info("PIPELINE RUN START — %s", PIPELINE_RUN_TS)
    log.info("=" * 70)

    metrics: dict = {"run_timestamp": PIPELINE_RUN_TS}

    log.info("=" * 70)
    log.info("BRONZE LAYER — BEGIN")
    log.info("=" * 70)
    try:
        bronze_metadata_df = ingest_bronze(
            input_path=METADATA_PATH,
            schema=METADATA_RAW_SCHEMA,
            layer_name="metadata",
            output_base=BRONZE_OUTPUT_BASE,
        )
        bronze_dicom_df = ingest_bronze(
            input_path=DICOM_PATH,
            schema=DICOM_RAW_SCHEMA,
            layer_name="dicom",
            output_base=BRONZE_OUTPUT_BASE,
        )

        metrics["bronze_metadata_rows"] = bronze_metadata_df.count()
        metrics["bronze_dicom_rows"] = bronze_dicom_df.count()
        log.info("BRONZE LAYER — COMPLETE")
    except Exception as exc:
        log.error("[BRONZE] Pipeline failed during Bronze ingestion: %s", exc, exc_info=True)
        raise PipelineExecutionError(f"Bronze layer failed: {exc}") from exc

    log.info("=" * 70)
    log.info("SILVER LAYER — BEGIN")
    log.info("=" * 70)
    try:
        silver_metadata_df = clean_metadata(bronze_metadata_df)
        silver_dicom_df = clean_dicom(bronze_dicom_df)

        pre_join_meta_ids = silver_metadata_df.select("participant_id").distinct().count()
        pre_join_dicom_ids = silver_dicom_df.select("participant_id").distinct().count()

        silver_joined_df = silver_metadata_df.join(
            silver_dicom_df.select(
                "participant_id",
                F.col("dicom_s3_uri").alias("imaging_s3_uri"),
                "modality",
                "body_region",
                "uri_format_valid",
            ),
            on="participant_id",
            how="inner",
        )

        post_join_count = silver_joined_df.count()
        log.info(
            "[SILVER] Join complete: metadata IDs=%d | dicom IDs=%d | joined rows=%d",
            pre_join_meta_ids, pre_join_dicom_ids, post_join_count,
        )

        if post_join_count < min(pre_join_meta_ids, pre_join_dicom_ids):
            unmatched = min(pre_join_meta_ids, pre_join_dicom_ids) - post_join_count
            log.warning(
                "[SILVER] %d participant IDs had no matching counterpart and were excluded. "
                "Investigate Bronze tables for unmatched records.",
                unmatched,
            )

        silver_output = _s3_join("output", "silver", "clinical_imaging_joined")
        (
            silver_joined_df.write.mode("overwrite")
            .partitionBy("site_location")
            .parquet(silver_output)
        )
        log.info("[SILVER] Written to: %s", silver_output)

        metrics["silver_joined_rows"] = post_join_count
        metrics["silver_unmatched_count"] = max(
            0, min(pre_join_meta_ids, pre_join_dicom_ids) - post_join_count
        )
        log.info("SILVER LAYER — COMPLETE")
    except Exception as exc:
        log.error("[SILVER] Pipeline failed during Silver transformation: %s", exc, exc_info=True)
        raise PipelineExecutionError(f"Silver layer failed: {exc}") from exc

    log.info("=" * 70)
    log.info("GOLD LAYER — BEGIN")
    log.info("=" * 70)
    try:
        gold_df = mask_participant_id(silver_joined_df)
        gold_df = reduce_date_precision(gold_df)
        gold_df = gold_df.drop(*[c for c in GOLD_COLUMNS_TO_DROP if c in gold_df.columns])

        existing_cols = [c for c in GOLD_COLUMN_ORDER if c in gold_df.columns]
        remaining_cols = [c for c in gold_df.columns if c not in existing_cols]
        gold_df = gold_df.select(existing_cols + remaining_cols)

        gold_row_count = gold_df.count()
        log.info("[GOLD] Final dataset: %d rows, %d columns", gold_row_count, len(gold_df.columns))

        cohort_summary_df = (
            gold_df.groupBy("site_location", "lesion_label")
            .agg(
                F.count("subject_surrogate_id").alias("subject_count"),
                F.avg("age").alias("mean_age"),
                F.min("age").alias("min_age"),
                F.max("age").alias("max_age"),
                F.sum(F.col("lesion_code_requires_review").cast("int")).alias("flagged_for_review"),
            )
            .orderBy("site_location", "lesion_label")
        )
        log.info("[GOLD] Cohort summary by site and lesion finding:")
        cohort_summary_df.show(truncate=False)

        lesion_dist_df = gold_df.groupBy("lesion_label").agg(F.count("*").alias("n"))
        total_n = gold_row_count
        lesion_dist_df = lesion_dist_df.withColumn(
            "pct", F.round(F.col("n") / F.lit(total_n) * 100, 2)
        )
        log.info("[GOLD] Lesion finding distribution:")
        lesion_dist_df.show()

        age_dist_df = (
            gold_df.groupBy("site_location")
            .agg(
                F.count("*").alias("n"),
                F.avg("age").alias("mean_age"),
                F.stddev("age").alias("stddev_age"),
                F.percentile_approx("age", 0.25).alias("p25_age"),
                F.percentile_approx("age", 0.50).alias("median_age"),
                F.percentile_approx("age", 0.75).alias("p75_age"),
            )
            .orderBy("site_location")
        )
        log.info("[GOLD] Age distribution by site:")
        age_dist_df.show()

        gold_output = _s3_join("output", "gold", "research_dataset")
        gold_summary_output = _s3_join("output", "gold", "cohort_summary")

        gold_df.write.mode("overwrite").parquet(gold_output)
        log.info("[GOLD] Research dataset written to: %s", gold_output)

        cohort_summary_df.write.mode("overwrite").parquet(gold_summary_output)
        log.info("[GOLD] Cohort summary written to: %s", gold_summary_output)

        metrics["gold_rows"] = gold_row_count
        metrics["gold_flagged_for_review"] = gold_df.filter(
            F.col("lesion_code_requires_review") == True
        ).count()
        log.info("GOLD LAYER — COMPLETE")
    except Exception as exc:
        log.error("[GOLD] Pipeline failed during Gold transformation: %s", exc, exc_info=True)
        raise PipelineExecutionError(f"Gold layer failed: {exc}") from exc

    log.info("=" * 70)
    log.info("PIPELINE RUN SUMMARY")
    log.info("=" * 70)
    log.info("Run timestamp          : %s", metrics["run_timestamp"])
    log.info("Bronze metadata rows   : %d", metrics["bronze_metadata_rows"])
    log.info("Bronze DICOM rows      : %d", metrics["bronze_dicom_rows"])
    log.info("Silver joined rows     : %d", metrics["silver_joined_rows"])
    log.info("Silver unmatched count : %d", metrics["silver_unmatched_count"])
    log.info("Gold output rows       : %d", metrics["gold_rows"])
    log.info("Gold flagged for review: %d", metrics["gold_flagged_for_review"])
    log.info("Lesion governance      : Applied (Cerner/Epic cross-EHR code harmonization)")
    log.info("PII masking            : participant_id → SHA-256 surrogate (16-char hex prefix)")
    log.info("Date reduction         : enrollment_date → enrollment_year")
    log.info(
        "Output locations       : %s | %s | %s",
        BRONZE_OUTPUT_BASE,
        SILVER_OUTPUT_BASE,
        GOLD_OUTPUT_BASE,
    )
    log.info("=" * 70)
    log.info("Pipeline run complete.")

    return metrics



if __name__ == "__main__":
    try:
        run_pipeline()
    except PipelineExecutionError as exc:
        log.critical("Pipeline terminated: %s", exc)
        sys.exit(1)
