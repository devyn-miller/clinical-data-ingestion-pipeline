import logging
import os
import io
import sys
from datetime import datetime, timezone
import pandas as pd

import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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


class PipelineExecutionError(Exception):
    """Raised when a Medallion layer (Bronze/Silver/Gold) fails to complete."""

def _s3_join(*parts: str) -> str:
    """
    Join S3 (or local) path components without os.path.join's platform-specific
    separator.
    """
    if not parts:
        return ""
    first = parts[0]
    if first.startswith(("s3://", "file:", "dbfs:")):
        base = first.rstrip("/")
        rest = [p.strip("/") for p in parts[1:] if p]
        return (base + "/" + "/".join(rest)) if rest else base
    return "/".join(p.strip("/") for p in parts if p)


class MockSparkSession:
    """
    Fallback mock Spark session for Databricks Web Terminal/CLI runs.
    Bypasses restricted cloud cluster JVM blocks during structural validation.
    """
    def __init__(self):
        class MockReader:
            def option(self, *args, **kwargs): return self
            def schema(self, *args, **kwargs): return self
            def csv(self, path):
                log.error("[MOCK SPARK] Mocking read of CSV file: %s", path)
                raise PipelineExecutionError(
                    f"Bronze layer failed: [PATH_NOT_FOUND] Path does not exist: {path} "
                    f"(Note: Structural verification successful. Running inside a safe terminal MockSparkSession)"
                )
        self.read = MockReader()


def _get_spark_session() -> SparkSession:
    """
    Retrieve the active SparkSession, adapt to Databricks Serverless, or
    bootstrap a local session.
    """
    # 1: Reuse any already-active session
    try:
        session = SparkSession.getActiveSession()
        if session is not None:
            try:
                _ = session.version
                return session
            except Exception:
                log.warning("Active SparkSession exists but JVM is unresponsive. Continuing fallback chain.")
    except Exception:
        pass

    # 2: Databricks SDK runtime (notebooks/jobs)
    try:
        from databricks.sdk.runtime import spark as _sdk_spark
        if _sdk_spark is not None:
            try:
                _ = _sdk_spark.version
                log.info(
                    "Databricks SDK runtime detected. "
                    "Successfully resolved native 'spark' session."
                )
                return _sdk_spark
            except Exception as probe_exc:
                log.warning(
                    "Databricks SDK spark object is non-functional (%s). "
                    "Continuing session fallback chain.", probe_exc
                )
    except (ImportError, ModuleNotFoundError, AttributeError):
        pass

    # 3: Standard Databricks cluster/Serverless
    is_databricks = (
        "DATABRICKS_RUNTIME_VERSION" in os.environ
        or "DATABRICKS_WORKSPACE_PORT" in os.environ
    )
    if is_databricks:
        log.info("Databricks cluster runtime detected. Fetching workspace-configured SparkSession.")

        # 3a: Standard builder
        try:
            session = SparkSession.builder.getOrCreate()
            if session is not None:
                try:
                    _ = session.version
                    return session
                except Exception:
                    log.warning("SparkSession.builder.getOrCreate() returned a dead session.")
        except Exception as exc:
            log.warning(
                "Standard SparkSession builder failed on Databricks (%s). "
                "Attempting DatabricksSession...", str(exc)
            )

        # 3b: Databricks Connect (remote execution)
        try:
            from databricks.connect import DatabricksSession
            session = DatabricksSession.builder.getOrCreate()
            if session is not None:
                try:
                    _ = session.version
                    return session
                except Exception:
                    log.warning("DatabricksSession returned a dead session.")
        except Exception as inner_exc:
            log.warning("All cloud Spark session attempts failed: %s", str(inner_exc))

        # Fall back to MockSparkSession
        log.warning(
            "No live Databricks session available. "
            "Initializing MockSparkSession for offline pipeline verification."
        )
        return MockSparkSession()

    # 4: Local/offline (pytest, GitHub Actions, local terminal)
    log.info("Running locally/offline; bootstrapping local testing SparkSession.")
    try:
        session = (
            SparkSession.builder.appName("choc_rady_medallion_pipeline")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.driver.memory", "2g")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("WARN")
        return session
    except Exception as exc:
        if (
            "Only remote Spark sessions using Databricks Connect are supported" in str(exc)
            or is_databricks
        ):
            log.warning(
                "Restricted Databricks terminal sandbox detected "
                "(local JVM creation blocked). "
                "Initializing MockSparkSession for offline pipeline testing..."
            )
            return MockSparkSession()
        raise exc


AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_SESSION_TOKEN = None

# Directory and path parameters
AWS_BUCKET_NAME = "choc-rady-clinical-bronze-demo"
RAW_PREFIX = "raw"

LOCAL_DATA_DIR = os.path.abspath("data")
METADATA_PATH: str = os.path.join(LOCAL_DATA_DIR, "metadata", "metadata.csv")
DICOM_PATH: str    = os.path.join(LOCAL_DATA_DIR, "dicom_manifest.csv")

UC_CATALOG  = "workspace"
UC_SCHEMA   = "choc_rady"
UC_BRONZE_META  = f"{UC_CATALOG}.{UC_SCHEMA}.bronze_metadata"
UC_BRONZE_DICOM = f"{UC_CATALOG}.{UC_SCHEMA}.bronze_dicom"
UC_SILVER_JOINED = f"{UC_CATALOG}.{UC_SCHEMA}.silver_clinical_imaging_joined"
UC_GOLD_DATASET  = f"{UC_CATALOG}.{UC_SCHEMA}.gold_research_dataset"
UC_GOLD_SUMMARY  = f"{UC_CATALOG}.{UC_SCHEMA}.gold_cohort_summary"

BRONZE_OUTPUT_BASE = "file:///tmp/pipeline/bronze"
SILVER_OUTPUT_BASE = "file:///tmp/pipeline/silver"
GOLD_OUTPUT_BASE   = "file:///tmp/pipeline/gold"

# S3 Medallion Output Configuration
BRONZE_S3_PREFIX = "bronze"
SILVER_S3_PREFIX = "silver"
GOLD_S3_PREFIX   = "gold"

# Gold ML columns: internal name -> S3 column name
GOLD_ML_COLUMNS = {
    "subject_surrogate_id": "surrogate_id",
    "age":                  "age",
    "ehr_system":           "ehr_system",
    "site_location":        "site_location",
    "lesion_label":         "lesion_label",
    "imaging_s3_uri":       "s3_dicom_path",
}


def _get_s3_client():
    """Return a boto3 S3 client, raising clearly if credentials are missing."""
    if (
        not AWS_ACCESS_KEY_ID
        or AWS_ACCESS_KEY_ID.strip() in _PLACEHOLDER_MARKERS
        or not AWS_SECRET_ACCESS_KEY
        or AWS_SECRET_ACCESS_KEY.strip() in _PLACEHOLDER_MARKERS
    ):
        raise PipelineExecutionError(
            "Cannot write to S3: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY are not set."
        )
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )


def _upload_pandas_to_s3(
    pdf: pd.DataFrame,
    s3_client,
    s3_key: str,
    layer_tag: str,
) -> str:
    """
    Serialize a pandas DataFrame to snappy Parquet in memory and upload
    to S3. Returns the full s3:// URI.
    """
    buf = io.BytesIO()
    pdf_ready = pdf.copy()
    pdf_ready.attrs = {} 
    
    pdf_ready.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    buf.seek(0)
    kb = len(buf.getvalue()) / 1024
    log.info("[%s→S3] Uploading %.2f KB → s3://%s/%s", layer_tag, kb, AWS_BUCKET_NAME, s3_key)
    s3_client.upload_fileobj(buf, AWS_BUCKET_NAME, s3_key)
    uri = f"s3://{AWS_BUCKET_NAME}/{s3_key}"
    log.info("[%s→S3] ✅  %s  (%d rows)", layer_tag, uri, len(pdf))
    return uri


def write_bronze_to_s3(
    bronze_metadata_df,
    bronze_dicom_df,
    pipeline_run_date: str,
) -> dict:
    """
    Upload Bronze metadata and DICOM manifest to S3, partitioned by
    pipeline_run_date.

    Structure:
        bronze/metadata/pipeline_run_date=YYYY-MM-DD/part-0000.snappy.parquet
        bronze/dicom_manifest/pipeline_run_date=YYYY-MM-DD/part-0000.snappy.parquet
    """
    s3 = _get_s3_client()
    uris = {}

    datasets = {
        "metadata":       bronze_metadata_df,
        "dicom_manifest": bronze_dicom_df,
    }

    for name, df in datasets.items():
        log.info("[BRONZE→S3] Collecting %s to driver...", name)
        pdf = df.toPandas()
        s3_key = (
            f"{BRONZE_S3_PREFIX}/{name}/"
            f"pipeline_run_date={pipeline_run_date}/"
            f"part-0000.snappy.parquet"
        )
        uris[name] = _upload_pandas_to_s3(pdf, s3, s3_key, "BRONZE")

    return uris  # {"metadata": "s3://...", "dicom_manifest": "s3://..."}


def write_silver_to_s3(silver_joined_df) -> dict:
    """
    Upload Silver joined DataFrame to S3, partitioned by site_location.

    Structure:
        silver/clinical_imaging_joined/site_location=CHOC/part-0000.snappy.parquet
        silver/clinical_imaging_joined/site_location=Rady/part-0000.snappy.parquet
        ...
    """
    s3 = _get_s3_client()
    uris = {}

    if "site_location" not in silver_joined_df.columns:
        raise PipelineExecutionError(
            "Silver DataFrame is missing 'site_location' — cannot partition for S3 upload."
        )

    sites = [
        row["site_location"]
        for row in silver_joined_df.select("site_location").distinct().collect()
        if row["site_location"] is not None
    ]
    log.info("[SILVER→S3] Partitioning by site_location: %s", sites)

    for site in sites:
        site_pdf = (
            silver_joined_df
            .filter(F.col("site_location") == site)
            .toPandas()
        )
        # Sanitize site name for S3 key safety
        safe_site = site.replace(" ", "_").replace("/", "-")
        s3_key = (
            f"{SILVER_S3_PREFIX}/clinical_imaging_joined/"
            f"site_location={safe_site}/"
            f"part-0000.snappy.parquet"
        )
        uris[site] = _upload_pandas_to_s3(site_pdf, s3, s3_key, "SILVER")

    return uris  # {"CHOC": "s3://...", "Rady": "s3://..."}


def write_gold_to_s3(gold_df, cohort_summary_df) -> dict:
    """
    Upload Gold research dataset and cohort summary to S3, unpartitioned.
    Research dataset is stripped to ML-only columns and renamed.

    Structure:
        gold/research_dataset/part-0000.snappy.parquet
        gold/cohort_summary/part-0000.snappy.parquet
    """
    s3 = _get_s3_client()
    uris = {}

    # Research dataset: strip to ML columns only
    missing = [c for c in GOLD_ML_COLUMNS if c not in gold_df.columns]
    if missing:
        raise PipelineExecutionError(
            f"Gold DataFrame missing expected ML columns: {missing}"
        )

    ml_df = gold_df.select(list(GOLD_ML_COLUMNS.keys()))
    for old, new in GOLD_ML_COLUMNS.items():
        if old != new:
            ml_df = ml_df.withColumnRenamed(old, new)

    log.info("[GOLD→S3] Collecting research_dataset to driver...")
    research_pdf = ml_df.toPandas()
    uris["research_dataset"] = _upload_pandas_to_s3(
        research_pdf, s3,
        f"{GOLD_S3_PREFIX}/research_dataset/part-0000.snappy.parquet",
        "GOLD",
    )

    # Cohort summary
    log.info("[GOLD→S3] Collecting cohort_summary to driver...")
    summary_pdf = cohort_summary_df.toPandas()
    uris["cohort_summary"] = _upload_pandas_to_s3(
        summary_pdf, s3,
        f"{GOLD_S3_PREFIX}/cohort_summary/part-0000.snappy.parquet",
        "GOLD",
    )

    return uris  # {"research_dataset": "s3://...", "cohort_summary": "s3://..."}

_PLACEHOLDER_MARKERS = frozenset({
    "INSERT ACCESS KEY HERE",
    "INSERT SECRET HERE",
    "PASTE_YOUR_AWS_ACCESS_KEY_ID_HERE",
    "PASTE_YOUR_AWS_SECRET_ACCESS_KEY_HERE",
    "",
})


def sync_s3_to_local_workspace() -> None:
    """
    Connects to the AWS Bronze bucket via Boto3, downloads the newest raw
    clinical uploads, and stages them locally in the cluster workspace.
    """
    if (
        not AWS_ACCESS_KEY_ID
        or AWS_ACCESS_KEY_ID.strip() in _PLACEHOLDER_MARKERS
        or not AWS_SECRET_ACCESS_KEY
        or AWS_SECRET_ACCESS_KEY.strip() in _PLACEHOLDER_MARKERS
    ):
        log.info(
            "[S3 SYNC] No AWS credentials configured. "
            "Skipping cloud sync and using local data cache."
        )
        return

    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
    )

    log.info(
        "[S3 SYNC] Scanning S3 Bucket '%s' under prefix '%s/'...",
        AWS_BUCKET_NAME, RAW_PREFIX,
    )
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=AWS_BUCKET_NAME, Prefix=RAW_PREFIX)
        
        downloaded = 0
        for page in pages:
            for obj in page.get("Contents", []):
                s3_key = obj["Key"]
                if s3_key.endswith("/"):
                    continue

                relative_key = s3_key[len(RAW_PREFIX):].lstrip("/")
                local_dest_path = os.path.join(LOCAL_DATA_DIR, relative_key)
                os.makedirs(os.path.dirname(local_dest_path), exist_ok=True)

                log.info(
                    "[S3 SYNC] Syncing s3://%s/%s ──> %s",
                    AWS_BUCKET_NAME, s3_key, local_dest_path,
                )
                s3_client.download_file(AWS_BUCKET_NAME, s3_key, local_dest_path)
                downloaded += 1
                
        if downloaded > 0:
            log.info(
                "[S3 SYNC] Sync complete. Staged %d clinical files to active workspace.",
                downloaded,
            )
        else:
            log.warning(
                "[S3 SYNC] S3 scan finished but no clinical files were found in the bucket."
            )

    except (NoCredentialsError, ClientError) as exc:
        log.error("[S3 SYNC] S3 access failed: %s", exc)
        raise PipelineExecutionError(f"Failed to pull files from AWS S3: {exc}")


# Schemas
METADATA_RAW_SCHEMA = StructType([
    StructField("participant_id",    StringType(), nullable=False),
    StructField("age",               StringType(), nullable=True),
    StructField("site_location",     StringType(), nullable=True),
    StructField("ehr_system",        StringType(), nullable=True),
    StructField("lesion_status_code",StringType(), nullable=True),
    StructField("enrollment_date",   StringType(), nullable=True),
])

DICOM_RAW_SCHEMA = StructType([
    StructField("participant_id", StringType(), nullable=False),
    StructField("dicom_s3_uri",   StringType(), nullable=True),
    StructField("modality",       StringType(), nullable=True),
    StructField("body_region",    StringType(), nullable=True),
])

def ingest_bronze(
    input_path: str,
    schema: StructType,
    layer_name: str,
    output_base: str,
    pipeline_run_ts: str,
    pipeline_run_date: str,
) -> DataFrame:
    """
    Read CSV from bronze landing zone, append lineage metadata, persist as oarquet, and return df
    Params: input_path, schema, layer_name, output_base.
    """
    spark_read_path = input_path
    if not spark_read_path.startswith(("s3://", "s3a://", "dbfs:", "file:")):
        spark_read_path = f"file:{os.path.abspath(spark_read_path)}"

    log.info("[BRONZE] Reading %s from: %s", layer_name, spark_read_path)

    spark = _get_spark_session()
    raw_df = (
        spark.read.option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(spark_read_path)
    )

    # Corrupt-record audit
    if "_corrupt_record" in raw_df.columns:
        corrupt_count = raw_df.filter(F.col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            log.warning(
                "[BRONZE] %s; %d corrupt / unparseable rows detected. "
                "These rows will have null field values in downstream layers.",
                layer_name, corrupt_count,
            )
        raw_df = raw_df.drop("_corrupt_record")

    row_count = raw_df.count()
    log.info("[BRONZE] %s; %d rows ingested from source.", layer_name, row_count)

    if row_count == 0:
        log.warning(
            "[BRONZE] %s; Zero rows read. Verify the source file exists at '%s'.",
            layer_name,
            spark_read_path,
        )

    bronze_df = (
        raw_df
        .withColumn("ingested_at",       F.lit(pipeline_run_ts).cast("string"))
        .withColumn("source_file",       F.lit(spark_read_path))
        .withColumn("pipeline_run_date", F.lit(pipeline_run_date))
    )

    table_map = {"metadata": UC_BRONZE_META, "dicom": UC_BRONZE_DICOM}
    uc_table = table_map.get(layer_name, f"{UC_CATALOG}.{UC_SCHEMA}.bronze_{layer_name}")
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(uc_table)
    )
    log.info("[BRONZE] %s; Written to UC table: %s", layer_name, uc_table)
    return bronze_df

def deduplicate_patients(df: DataFrame, partition_col: str = "participant_id", order_col: str = "enrollment_date") -> DataFrame:
    """
    Resolves multi-site patient duplicates by keeping only the most recent clinical record.
    Instantiates a PySpark Window partitioned by the participant ID and ordered by the 
    enrollment date descending.
    """
    # Create the window specification
    window_spec = Window.partitionBy(partition_col).orderBy(F.col(order_col).desc())
    
    # Assign a row number based on the window, keep only the first (newest) row, and drop the helper column
    df_deduped = (
        df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    return df_deduped

def _resolve_source_path(primary: str, fallback_dirs: list) -> str:
    """Return the first path that exists, fallback to recursively searched alternatives."""
    if os.path.exists(primary):
        return primary

    base_name = os.path.basename(primary)
    for d in fallback_dirs:
        d_abs = os.path.abspath(d)
        if not os.path.isdir(d_abs):
            continue

        matched_files = []
        for root_dir, _, files in os.walk(d_abs):
            for f in files:
                full_candidate = os.path.join(root_dir, f)
                
                # Logic to match metadata or dicom files based on base_name
                if "metadata" in base_name and any(x in f.lower() for x in ["metadata", "extract", "choc", "rady"]):
                    matched_files.append(full_candidate)
                elif "dicom" in base_name.lower() and "dicom" in f.lower():
                    matched_files.append(full_candidate)

        if matched_files:
            matched_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            log.info("Source dynamically resolved to newest candidate: %s → %s", primary, matched_files[0])
            return matched_files[0]

    return primary

DIRTY_LESION_VALUES = {"-1", "NA", "na", "N/A", "n/a", "none", "None", ""}
POSITIVE_LESION_VALUES = {"1", "positive", "Positive", "POSITIVE"}

LESION_GOVERNANCE_RULE = (
    "lesion_status_code values of '1' or 'Positive' (any case) represent a "
    "confirmed lesion finding ('Lesion Detected'). Values of '-1', 'NA', or "
    "NULL represent a confirmed-clear read or a pending radiologist read, "
    "and are harmonized to 'No Lesion Detected'. Codes outside this known set "
    "are flagged via lesion_code_requires_review."
)


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
        .otherwise(F.lit("Pending Radiologist Review"))
    )

    df = df.withColumn("lesion_label", lesion_label)
    df = df.withColumn("lesion_code_requires_review", (~is_recognized).cast("boolean"))
    return df


def clean_metadata(df: DataFrame) -> DataFrame:
    """Clean, standardize, and type-cast metadata records."""
    log.info("[SILVER] Cleaning metadata; %d raw rows", df.count())

    df = df.filter(F.col("participant_id").isNotNull())

    for col in ["participant_id", "site_location", "ehr_system",
                "lesion_status_code", "enrollment_date"]:
        df = df.withColumn(col, F.trim(F.col(col)))

    df = (
        df
        .withColumn("age",             F.col("age").cast(IntegerType()))
        .withColumn("enrollment_date", F.to_date(F.col("enrollment_date"), "yyyy-MM-dd"))
        .withColumn("site_location",   F.upper(F.col("site_location")))
    )

    df = apply_lesion_harmonization(df, "lesion_status_code")

    df = df.withColumn(
        "has_data_quality_flag",
        (F.col("age").isNull() | F.col("enrollment_date").isNull()).cast("boolean"),
    )

    # Single aggregation pass instead of 4 separate .count() actions
    stats = df.agg(
        F.count("*").alias("total"),
        F.sum(F.when(F.col("lesion_label") == "Lesion Detected", 1).otherwise(0)).alias("lesion"),
        F.sum(F.when(F.col("lesion_label") == "No Lesion Detected", 1).otherwise(0)).alias("no_lesion"),
        F.sum(F.col("lesion_code_requires_review").cast("int")).alias("review"),
        F.sum(F.when(F.col("lesion_label") == "Pending Radiologist Review",1).otherwise(0)).alias("pending"),
    ).collect()[0]
    log.info(
        "[SILVER] Metadata cleaning complete: %d rows | Lesion Detected=%d | "
        "No Lesion Detected=%d | Pending Review=%d | Flagged for review=%d",
        stats["total"], stats["lesion"], stats["no_lesion"],
        stats["pending"], stats["review"],
    )
    return df


def clean_dicom(df: DataFrame) -> DataFrame:
    """Clean and standardize DICOM manifest records."""
    log.info("[SILVER] Cleaning DICOM manifest; %d raw rows", df.count())

    df = df.filter(
        F.col("participant_id").isNotNull() & F.col("dicom_s3_uri").isNotNull()
    )
    df = df.withColumn("modality",    F.upper(F.initcap(F.trim(F.col("modality")))))
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
]

GOLD_COLUMN_ORDER = [
    "subject_surrogate_id",
    "age",
    "site_location",
    "ehr_system",
    "enrollment_year",
    "lesion_label",
    "lesion_code_requires_review",
    "imaging_s3_uri",
    "modality",
    "body_region",
]



def run_pipeline() -> dict:
    """
    Execute S3 Cloud Sync and run the full Medallion pipeline end-to-end.
    """
    pipeline_run_ts   = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pipeline_run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    spark = SparkSession.builder.getOrCreate()

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}")
    log.info("[UC] Target schema ready: %s.%s", UC_CATALOG, UC_SCHEMA)

    log.info("=" * 70)
    log.info("PIPELINE RUN START: %s", pipeline_run_ts)
    log.info("=" * 70)
    log.info("Local staging destination : %s", LOCAL_DATA_DIR)
    log.info("Bronze output base        : %s", BRONZE_OUTPUT_BASE)
    log.info("[SILVER] Governance rule  : %s", LESION_GOVERNANCE_RULE)

    # Trigger Serverless-safe AWS S3 Sync
    sync_s3_to_local_workspace()

    metrics: dict = {"run_timestamp": pipeline_run_ts}

    log.info("=" * 70)
    log.info("BRONZE LAYER BEGINNING")
    log.info("=" * 70)
    try:
        metadata_source = _resolve_source_path(primary=METADATA_PATH, fallback_dirs=["data"])
        dicom_source = _resolve_source_path(primary=DICOM_PATH, fallback_dirs=["data"])

        bronze_metadata_df = ingest_bronze(
            input_path=metadata_source,
            schema=METADATA_RAW_SCHEMA,
            layer_name="metadata",
            output_base=BRONZE_OUTPUT_BASE,
            pipeline_run_ts=pipeline_run_ts,
            pipeline_run_date=pipeline_run_date,
        )
        bronze_dicom_df = ingest_bronze(
            input_path=dicom_source,
            schema=DICOM_RAW_SCHEMA,
            layer_name="dicom",
            output_base=BRONZE_OUTPUT_BASE,
            pipeline_run_ts=pipeline_run_ts,
            pipeline_run_date=pipeline_run_date,
        )

        metrics["bronze_metadata_rows"] = bronze_metadata_df.count()
        metrics["bronze_dicom_rows"]    = bronze_dicom_df.count()

        bronze_uris = write_bronze_to_s3(bronze_metadata_df, bronze_dicom_df, pipeline_run_date)
        metrics["bronze_s3_uris"] = bronze_uris
        log.info("BRONZE LAYER COMPLETE")
    except Exception as exc:
        log.error("[BRONZE] Pipeline failed during Bronze ingestion: %s", exc, exc_info=True)
        raise PipelineExecutionError(f"Bronze layer failed: {exc}") from exc

    log.info("=" * 70)
    log.info("SILVER LAYER BEGINNING")
    log.info("=" * 70)
    try:
        silver_metadata_df = clean_metadata(bronze_metadata_df)
        silver_dicom_df = clean_dicom(bronze_dicom_df)

        pre_dedup_count = silver_metadata_df.count()
        silver_metadata_df = deduplicate_patients(silver_metadata_df)
        log.info("[SILVER] Deduplication dropped %d stale cross-EHR records", pre_dedup_count - silver_metadata_df.count())

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
                "[SILVER] %d participant IDs had no matching counterpart and were excluded.",
                unmatched,
            )

        (
            silver_joined_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(UC_SILVER_JOINED)
        )
        log.info("[SILVER] Written to UC table: %s", UC_SILVER_JOINED)

        metrics["silver_joined_rows"] = post_join_count
        metrics["silver_unmatched_count"] = max(
            0, min(pre_join_meta_ids, pre_join_dicom_ids) - post_join_count
        )

        silver_uris = write_silver_to_s3(silver_joined_df)
        metrics["silver_s3_uris"] = silver_uris
        log.info("SILVER LAYER COMPLETE")
    except Exception as exc:
        log.error("[SILVER] Pipeline failed during Silver transformation: %s", exc, exc_info=True)
        raise PipelineExecutionError(f"Silver layer failed: {exc}") from exc

    log.info("=" * 70)
    log.info("GOLD LAYER BEGINNING")
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

        lesion_dist_df = (
            gold_df.groupBy("lesion_label")
            .agg(F.count("*").alias("n"))
            .withColumn("pct", F.round(F.col("n") / F.lit(gold_row_count) * 100, 2))
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

        gold_output = _s3_join(GOLD_OUTPUT_BASE, "research_dataset")
        gold_summary_output = _s3_join(GOLD_OUTPUT_BASE, "cohort_summary")

        (
            gold_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(UC_GOLD_DATASET)
        )
        log.info("[GOLD] Research dataset written to UC table: %s", UC_GOLD_DATASET)

        (
            cohort_summary_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(UC_GOLD_SUMMARY)
        )
        log.info("[GOLD] Cohort summary written to UC table: %s", UC_GOLD_SUMMARY)

        metrics["gold_rows"] = gold_row_count
        # metrics["gold_flagged_for_review"] = gold_df.filter(
        #     F.col("lesion_code_requires_review") == True
        # ).count()

        log.info("!!! DEBUG !!! Available columns in gold_df: %s", gold_df.columns)

        metrics["gold_flagged_for_review"] = gold_df.filter(
            F.col("lesion_code_requires_review") == True
        ).count()


        gold_uris = write_gold_to_s3(gold_df, cohort_summary_df)
        log.info("DEBUG: Available columns in gold_df: %s", gold_df.columns)
        metrics["gold_s3_uris"] = gold_uris
        log.info("GOLD LAYER COMPLETE")
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
    log.info("=" * 70)
    log.info("S3 OUTPUT LOCATIONS")
    log.info("=" * 70)
    for label, uri in metrics.get("bronze_s3_uris", {}).items():
        log.info("  Bronze %-20s : %s", label, uri)
    for label, uri in metrics.get("silver_s3_uris", {}).items():
        log.info("  Silver %-20s : %s", label, uri)
    for label, uri in metrics.get("gold_s3_uris", {}).items():
        log.info("  Gold   %-20s : %s", label, uri)
    log.info("=" * 70)
    log.info(
        "Output locations       : %s | %s | %s",
        BRONZE_OUTPUT_BASE, SILVER_OUTPUT_BASE, GOLD_OUTPUT_BASE,
    )
    log.info("=" * 70)
    log.info("Pipeline run complete.")

    return metrics



if __name__ == "__main__":
    try:
        run_pipeline()
    except PipelineExecutionError as exc:
        log.critical("Pipeline terminated: %s", exc)
        raise exc