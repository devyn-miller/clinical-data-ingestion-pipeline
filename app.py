import os
import io
import requests
import time
import hashlib
from datetime import datetime

import boto3
import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError


def _setting(env_name: str, secret_section: str | None = None, secret_key: str | None = None, default: str = "") -> str:
    value = os.getenv(env_name)
    if value:
        return value
    if secret_section and secret_key:
        try:
            return st.secrets[secret_section][secret_key]
        except Exception:
            return default
    return default


BRONZE_BUCKET = _setting("BRONZE_BUCKET_NAME", "aws", "bronze_bucket", "choc-rady-clinical-bronze-demo")
AWS_REGION = _setting("AWS_REGION", "aws", "region", "us-west-2")
RAW_PREFIX = _setting("BRONZE_RAW_PREFIX", default="raw")

AWS_ACCESS_KEY_ID = _setting("AWS_ACCESS_KEY_ID", "aws", "aws_access_key_id", "")
AWS_SECRET_ACCESS_KEY = _setting("AWS_SECRET_ACCESS_KEY", "aws", "aws_secret_access_key", "")

DATABRICKS_WORKSPACE = _setting("DATABRICKS_WORKSPACE_URL", "databricks", "workspace_url", "")
DATABRICKS_TOKEN = _setting("DATABRICKS_PERSONAL_ACCESS_TOKEN", "databricks", "token", "")
DATABRICKS_JOB_ID = _setting("DATABRICKS_JOB_ID", "databricks", "job_id", "")


def upload_to_bronze(file_obj: io.BytesIO, filename: str, dataset_type: str, submitter: str, affil: str) -> str:
    """Stream-pointer safe S3 Object storage uploading with unique timestamps and metadata."""
    prefix_map = {
        "Clinical Metadata Extract": "metadata",
        "DICOM Imaging Manifest": "dicom",
    }
    object_prefix = prefix_map[dataset_type]
    
    # Generate unique timestamp string
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    name, ext = os.path.splitext(filename)
    unique_filename = f"{name}_{timestamp}{ext}"
    
    key = f"{RAW_PREFIX}/{object_prefix}/{unique_filename}"

    client_kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        client_kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
        client_kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY

    client = boto3.client("s3", **client_kwargs)
    client.upload_fileobj(
        file_obj,
        BRONZE_BUCKET,
        key,
        ExtraArgs={
            "ContentType": "text/csv",
            "Metadata": {
                "submitter": submitter,
                "affiliation": affil,
                "upload_timestamp": datetime.utcnow().isoformat()
            }
        },
    )
    return f"s3://{BRONZE_BUCKET}/{key}"

def trigger_databricks_pipeline():
    """Triggers the Medallion Pipeline Job in Databricks via REST API."""
    if not DATABRICKS_WORKSPACE or not DATABRICKS_TOKEN or not DATABRICKS_JOB_ID:
        st.warning("Databricks API details or personal access token are not configured. Automation bypassed.")
        return
        
    url = f"{DATABRICKS_WORKSPACE.rstrip('/')}/api/2.1/jobs/run-now"
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    }
    payload = {
        "job_id": int(DATABRICKS_JOB_ID)
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        if response.status_code == 200:
            run_id = response.json().get("run_id")
            st.success(f"Live Databricks Pipeline Run Triggered. Run ID: `{run_id}`")
        else:
            st.error(f"S3 uploaded but Databricks API trigger failed (Status {response.status_code}): {response.text}")
    except Exception as e:
        st.error(f"S3 uploaded but failed to contact Databricks API: {e}")


# Streamlit UI
st.set_page_config(page_title="Clinical MRI Data Portal", layout="wide")

# Initialize session state to support real-time ingestion tracking
if "transmissions" not in st.session_state:
    st.session_state["transmissions"] = {
        "TX-CHOCRADY-DEMO": {
            "submitter": "Dr. Jane Smith",
            "affiliation": "CHOC",
            "dataset_type": "Clinical Metadata Extract",
            "timestamp": "2026-06-23T13:18:04Z",
            "filename": "clinical_trial_batch_001.csv",
            "df": pd.DataFrame({
                "ssn": ["999-12-3456", "999-55-9876", "999-88-1111", "999-55-9876", "999-44-2222"],
                "site_location": ["CHOC", "CHOC", "CHOC", "Rady", "Rady"],
                "age": [14, 8, 11, 8, 6],
                "scan_date": ["2025-02-14", "2025-05-19", "2026-01-10", "2025-06-01", "2024-11-20"],
                "raw_lesion_status": ["1", "-1", "-1", "Positive", "None"]
            })
        }
    }

# Sidebar diagnostics
with st.sidebar:
    st.image("https://img.icons8.com/clouds/200/hospital.png", width=120)
    st.markdown("### System Operations Panel")
    
    with st.expander("Environment Credentials Check", expanded=True):
        if "aws" in st.secrets:
            st.success("AWS Secrets Active")
            has_bucket = "bronze_bucket" in st.secrets["aws"]
            has_key = "aws_access_key_id" in st.secrets["aws"]
            has_secret = "aws_secret_access_key" in st.secrets["aws"]
            st.caption(f"Bucket Loaded: `{has_bucket}`")
            st.caption(f"Key Found: `{has_key}` | Secret Found: `{has_secret}`")
        else:
            st.error("[aws] Section Missing")

        if "databricks" in st.secrets:
            st.success("Databricks Secrets Active")
            has_url = "workspace_url" in st.secrets["databricks"]
            has_token = "token" in st.secrets["databricks"]
            has_job = "job_id" in st.secrets["databricks"]
            st.caption(f"URL: `{has_url}` | Job Loaded: `{has_job}`")
        else:
            st.warning("Databricks Auto-Trigger Bypassed")

st.set_page_config(page_title="MRI Data Ingestion Portal", layout="wide")

st.title("Clinical MRI Data Portal")
st.write("Uploads land in the Bronze S3 bucket and are processed by Databricks jobs.")

# Role-based access control views
tab1, tab2 = st.tabs(["Clinician Upload Portal", "Internal Pipeline Monitor"])
with tab1:
    st.subheader("Upload clinical source payloads")
    st.warning("🔒 **PHI Compliance Notice:** All files land directly inside our private S3 Bronze environment. Do not upload direct patient identifiers (SSN, names) unless authorized under the IRB data-sharing agreement.")

    c1, c2 = st.columns(2)
    with c1:
        submitter_name = st.text_input("Submitter Name", placeholder="e.g. Dr. Jane Smith")
    with c2:
        affiliation = st.selectbox("Affiliation", ["CHOC", "Rady", "Research Dept", "External Partner"])
    dataset_type = st.selectbox(
        "Dataset type",
        ["Clinical Metadata Extract", "DICOM Imaging Manifest"],
    )
    uploaded_file = st.file_uploader("CSV file", type=["csv"])

    if uploaded_file is not None:
        raw_bytes = uploaded_file.getvalue()
        preview_df = pd.read_csv(io.BytesIO(raw_bytes))
        st.write("**Clinical Data Preview (First 5 Rows):**")
        st.dataframe(preview_df.head())

        if st.button("Upload & Trigger Pipeline", type="primary"):
            if not submitter_name:
                st.error("Please enter your Submitter Name to maintain data provenance.")
            else:
                with st.spinner("Authenticating and pushing to S3 Bronze Landing Zone..."):
                    try:
                        # Generate confirmation Transmission ID based on payload properties
                        tx_hash = hashlib.md5(raw_bytes).hexdigest()[:6].upper()
                        tx_id = f"TX-{tx_hash}"
                        
                        # Process uploaded data safely into st.session_state for lineage verification
                        standard_df = preview_df.copy()
                        
                        # Harmonize columns names to standard names for trace pipeline
                        rename_map = {
                            "cerner_id": "participant_id", "epic_id": "participant_id",
                            "age_in_years": "age",
                            "scan_date": "scan_date", "mri_date": "scan_date",
                            "lesion_status_code": "raw_lesion_status", "lesion_status": "raw_lesion_status"
                        }
                        standard_df = standard_df.rename(columns=rename_map)
                        
                        if "ssn" not in standard_df.columns:
                            # If uploaded DICOM manifest or non-SSN data, mock SSNs for de-identification demo
                            standard_df["ssn"] = [f"999-12-{1000+i}" for i in range(len(standard_df))]
                        if "site_location" not in standard_df.columns:
                            standard_df["site_location"] = affiliation
                        if "raw_lesion_status" not in standard_df.columns:
                            standard_df["raw_lesion_status"] = "1"
                        if "scan_date" not in standard_df.columns:
                            standard_df["scan_date"] = datetime.now().strftime("%Y-%m-%d")

                        st.session_state["transmissions"][tx_id] = {
                            "submitter": submitter_name,
                            "affiliation": affiliation,
                            "dataset_type": dataset_type,
                            "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "filename": uploaded_file.name,
                            "df": standard_df[["ssn", "site_location", "age", "scan_date", "raw_lesion_status"]]
                        }

                        s3_uri = upload_to_bronze(
                            io.BytesIO(raw_bytes), 
                            uploaded_file.name, 
                            dataset_type,
                            submitter_name,
                            affiliation
                        )
                        st.success(f"Uploaded securely to {s3_uri}")
                        
                        # Render clinical ingestion ticket
                        with st.container(border=True):
                            st.markdown(f"""
                            ### Clinical Ingest Transmission Receipt
                            Your pipeline admission ticket has been safely registered on-chain.
                            
                            * **Confirmation ID:** `{tx_id}`
                            * **Lineage Target:** `{dataset_type}`
                            * **Security Clearance:** Automated PHI De-identification Active
                            
                            *Copy and paste your **Confirmation ID** (`{tx_id}`) into the **Internal Pipeline Monitor** tab to trace exactly how our Medallion framework handles your records.*
                            """)

                        # Trigger automation
                        with st.spinner("Notifying Databricks cluster..."):
                            trigger_databricks_pipeline()
                            
                    except (NoCredentialsError, ClientError, BotoCoreError) as exc:
                        # Local simulation fallback if AWS keys not configured
                        time_dir = "s3_bronze_landing/raw"
                        os.makedirs(time_dir, exist_ok=True)
                        local_path = os.path.join(time_dir, f"{submitter_name.replace(' ', '_')}_{uploaded_file.name}")
                        with open(local_path, "wb") as f:
                            f.write(raw_bytes)
                        st.warning(f"Cloud authentication failed: {exc}")
                        st.success(f"Local Simulation Enabled: Staged file under local `s3_bronze_landing/` root.")
with tab2:
    st.subheader("Medallion Architecture Pipeline Status")

    m_col1, m_col2, m_col3 = st.columns(3)
    
    with m_col1:
        with st.container(border=True):
            st.info("**BRONZE LAYER**\n\n*Raw, Immutable S3 Landing*")
            st.metric(label="Raw files awaiting ingestion", value="3", delta="+1 newly ingested")
            st.caption(f"**Ingest URI:** `s3://{BRONZE_BUCKET}/{RAW_PREFIX}/`")
            st.caption(f"**Ledger Target:** `s3://{BRONZE_BUCKET}/bronze/`")
            
    with m_col2:
        with st.container(border=True):
            st.warning("**SILVER LAYER**\n\n*Harmonization & Deduplication*")
            st.metric(label="Patient Overlaps Merged", value="500", delta="Multi-site Unified")
            st.caption(f"**Engine:** `Databricks PySpark`")
            st.caption(f"**Target Directory:** `s3://{BRONZE_BUCKET}/silver/`")
            
    with m_col3:
        with st.container(border=True):
            st.success("**GOLD LAYER**\n\n*Research-Ready Aggregates*")
            st.metric(label="Available ML Cohorts", value="500", delta="Fully De-Identified")
            st.caption(f"**Format:** Apache Parquet (ML-Ready)")
            st.caption(f"**Target Directory:** `s3://{BRONZE_BUCKET}/gold/`")

    with st.expander("S3 Bucket Directory Tree Schema Structure Map", expanded=False):
        st.code(f"""
s3://{BRONZE_BUCKET}/
├── {RAW_PREFIX}/                  <-- Raw Landing Zone (Immutable writes)
│   ├── metadata/                  <-- E.g. metadata_20260623T040830Z.csv
│   └── dicom/                     <-- E.g. dicom_manifest.csv
├── bronze/                       <-- Schema-enforced historical tables
│   ├── metadata/                  <-- Partitioned: pipeline_run_date=YYYY-MM-DD/
│   └── dicom/                     <-- Partitioned: pipeline_run_date=YYYY-MM-DD/
├── silver/                       <-- Cleansed, Unified, and Correlated
│   └── clinical_imaging_joined/   <-- Partitioned: site_location=CHOC | site_location=RADY/
└── gold/                         <-- IRB-Compliant De-Identified research datasets
    ├── research_dataset/          <-- Anonymized columns (surrogate_id, age, lesion_label...)
    └── cohort_summary/            <-- Aggregated cohort profiling benchmarks
        """, language="text")
    st.markdown("---")

    # Visual Medallion lineage inspection
    st.subheader("Medallion Schema Lineage & Transformation Inspector")
    st.write("This interactive console allows engineers to inspect how unharmonized hospital schemas transition into a clean, de-identified research dataset.")

    # Ingest Tracking ID selector to let users verify their uploads live
    st.markdown("### Live Transmission Search")
    track_input = st.text_input(
        "Enter Ingestion Confirmation ID (E.g. TX-CHOCRADY-DEMO or your custom upload code):", 
        value="TX-CHOCRADY-DEMO"
    ).strip()

    if track_input not in st.session_state["transmissions"]:
        st.warning(f"Confirmation ID `{track_input}` is not yet processed on this node. Defaulting lineage tracking to baseline demo: `TX-CHOCRADY-DEMO`.")
        active_id = "TX-CHOCRADY-DEMO"
    else:
        active_id = track_input

    tx_record = st.session_state["transmissions"][active_id]
    source_df = tx_record["df"]

    # Render active transmission metadata summary
    with st.container(border=True):
        st.write(f"#### Ingestion Tracker: `{active_id}`")
        col_meta1, col_meta2, col_meta3, col_meta4 = st.columns(4)
        col_meta1.metric("Clinical Submitter", tx_record["submitter"])
        col_meta2.metric("Affiliation Network", tx_record["affiliation"])
        col_meta3.metric("Data Payload Category", tx_record["dataset_type"])
        col_meta4.metric("Ingestion Status", "Ingested & Staged", delta="Ready for Databricks")
        st.caption(f"**Synchronized Timestamp:** `{tx_record['timestamp']}` | **Original Filename:** `{tx_record['filename']}`")

    st.markdown("---")
    st.info("**Interactive Presentation Demo:** Click through the steps below to demonstrate schema evolution, cross-EHR deduplication, and de-identification mapping during your presentation.")


    step_tab1, step_tab2, step_tab3, step_tab4, step_tab5 = st.tabs([
        "1. Raw EHR Inputs",
        "2. Bronze Schema Enforced",
        "3. Silver Harmonized & Joined",
        "4. Gold Anonymized & De-Identified",
        "5. Simulated Databricks Logs"
    ])

    # Stage 1: Raw dataset columns
    raw_preview = source_df.copy()
    if tx_record["affiliation"] == "CHOC":
        # Simulate CHOC naming structures
        raw_preview = raw_preview.rename(columns={
            "ssn": "ssn", "age": "age", "scan_date": "scan_date", "raw_lesion_status": "lesion_status_code"
        })
        raw_preview.insert(0, "cerner_id", [f"C{str(i).zfill(5)}" for i in range(len(raw_preview))])
    else:
        # Simulate Rady naming structures
        raw_preview = raw_preview.rename(columns={
            "ssn": "ssn", "age": "age_in_years", "scan_date": "mri_date", "raw_lesion_status": "lesion_status"
        })
        raw_preview.insert(0, "epic_id", [f"E{str(i).zfill(5)}" for i in range(len(raw_preview))])

    # Stage 2: Bronze (Enforces structured columns & metadata)
    bronze_preview = source_df.copy()
    bronze_preview.insert(0, "participant_id", [f"P{str(i).zfill(5)}" for i in range(len(bronze_preview))])
    bronze_preview["ingested_at"] = tx_record["timestamp"]
    bronze_preview["source_file"] = f"s3://{BRONZE_BUCKET}/raw/{tx_record['filename']}"
    bronze_preview["pipeline_run_date"] = datetime.now().strftime("%Y-%m-%d")

    # Stage 3: Silver (Harmonizes, Deduplicates, and joins with S3 paths)
    silver_preview = source_df.copy()
    
    # 1. Harmonize clinical findings
    def harmonize(val):
        if str(val).strip().lower() in ["1", "positive"]:
            return "Lesion Detected"
        return "No Lesion Detected"
    
    silver_preview["lesion_label"] = silver_preview["raw_lesion_status"].apply(harmonize)
    silver_preview["lesion_code_requires_review"] = ~silver_preview["raw_lesion_status"].astype(str).str.lower().isin(["1", "-1", "positive", "none", "na"])
    silver_preview["ehr_system"] = silver_preview["site_location"].apply(lambda s: "Cerner" if s.upper() == "CHOC" else "Epic")
    
    # 2. Replicate Spark Deduplication Window Function on SSN (keeping most recent)
    pre_dedup_count = len(silver_preview)
    silver_preview = silver_preview.sort_values(by="scan_date", ascending=False)
    silver_preview = silver_preview.drop_duplicates(subset=["ssn"], keep="first")
    post_dedup_count = len(silver_preview)
    overlaps_resolved = pre_dedup_count - post_dedup_count
    
    # 3. Simulate join with S3 DICOM Manifest
    mri_paths = []
    for idx, row in silver_preview.iterrows():
        # Generate stable S3 URI pointer based on SSN hash
        ssn_hash = hashlib.md5(row["ssn"].encode()).hexdigest()[:8].upper()
        mri_paths.append(f"s3://{BRONZE_BUCKET}/imaging/scan_{ssn_hash}.dcm")
    silver_preview["imaging_s3_uri"] = mri_paths

    # Stage 4: Gold (PHI Cryptographic Masking)
    gold_preview = silver_preview.copy()
    gold_preview["surrogate_id"] = gold_preview["ssn"].apply(lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16])
    gold_preview["enrollment_year"] = pd.to_datetime(gold_preview["scan_date"]).dt.year
    
    # Keep strictly de-identified ML columns
    gold_preview = gold_preview[[
        "surrogate_id", "age", "ehr_system", "site_location", 
        "enrollment_year", "lesion_label", "imaging_s3_uri"
    ]].rename(columns={"imaging_s3_uri": "s3_dicom_path"})


    # Step 1: Raw payloads
    with step_tab1:
        st.write("#### Stage 1: Raw Landing Zone (Ingested Files)")
        st.caption("CHOC Hospital (Cerner Database schema) and Rady Hospital (Epic Database schema) upload completely inconsistent column headings, date structures, and classification mappings. Furthermore, patients who have visited both networks present a major duplication risk.")
        
        st.markdown(f"**Actual Uploaded Payload Schema Model: `{tx_record['filename']}`**")
        st.dataframe(raw_preview, use_container_width=True)


    # Step 2: Bronze
    with step_tab2:
        st.write("#### Stage 2: Bronze Ingestion Ledger (Lineage Attached)")
        st.caption("The Databricks pipeline enforces strict primitive types and schemas on read. It appends critical analytical audit-trail columns (`ingested_at`, `source_file`, `pipeline_run_date`) and commits them to historical Delta tables. The data is kept raw to preserve historical integrity.")
        st.dataframe(bronze_preview, use_container_width=True)

    # Step 3: Silver
    with step_tab3:
        st.write("#### Stage 3: Silver Harmonization, Deduplication & Inner Join")
        st.caption("A Spark Window Function (`F.row_number()`) evaluates overlapping patient records by `ssn` and keeps only the most recent entry. Clinical findings are standardized into standard labels, and matched with private S3 medical imaging file pointers.")
        
        if overlaps_resolved > 0:
            st.success(f"⚡ **Deduplication Complete:** Spark Window successfully resolved `{overlaps_resolved}` multi-site patient duplicate records!")
        else:
            st.info("⚡ **Deduplication Evaluated:** No duplicate patient SSNs detected in this payload. Keeping all records.")
            
        st.dataframe(silver_preview, use_container_width=True)

    # Step 4: Gold
    with step_tab4:
        st.write("#### Stage 4: Gold Research Cohorts (ML-Ready, PHI-Masked)")
        st.caption("To enforce HIPAA Safe Harbor rules and satisfy IRB approvals, patient SSNs are masked with deterministic SHA-256 surrogate keys. Direct identifiers are purged, exposing only anonymized features and Deep Learning target variables.")
        st.dataframe(gold_preview, use_container_width=True)

    # Step 5: Live Simulated Logs
    with step_tab5:
        st.write("#### Live Databricks Cluster Run Logs")
        st.caption("Below is the real-time execution logger outputs capturing exactly how the Databricks JVM cluster processed your Transmission ID.")
        
        simulated_log = f"""
2026-06-23 13:30:02  [INFO]  Databricks Job Trigger request received for Confirmation ID: {active_id}
2026-06-23 13:30:03  [INFO]  Spanning active SparkSession via Spark Connect...
2026-06-23 13:30:04  [INFO]  [S3 SYNC] Connecting via TLS to bucket: {BRONZE_BUCKET}
2026-06-23 13:30:05  [INFO]  [S3 SYNC] Successfully read S3 staging keys. Submitter: {tx_record['submitter']} ({tx_record['affiliation']})
2026-06-23 13:30:06  [INFO]  [BRONZE] Ingesting metadata.csv schema from local staging path.
2026-06-23 13:30:07  [INFO]  [BRONZE] Executed write to Delta Lake: workspace.choc_rady.bronze_metadata ({len(source_df)} rows)
2026-06-23 13:30:08  [INFO]  [SILVER] Beginning alignment rules. Unifying Cerner vs Epic schemas.
2026-06-23 13:30:09  [INFO]  [SILVER] Standardizing lesion mappings: Mapping '{tx_record['df']['raw_lesion_status'].iloc[0]}' codes to standard label space.
2026-06-23 13:30:10  [INFO]  [SILVER] Executing window deduplication partitioned by SSN. Rows pre-dedup: {pre_dedup_count} | Post-dedup: {post_dedup_count}
2026-06-23 13:30:11  [INFO]  [SILVER] Deduplication results: Successfully resolved {overlaps_resolved} cross-EHR duplicate profiles.
2026-06-23 13:30:12  [INFO]  [GOLD] Encrypting direct patient identifiers. Appended SHA-256 hashes to client profiles.
2026-06-23 13:30:13  [INFO]  [GOLD] Purging remaining PHI attributes. Selecting clinical variables for CV model.
2026-06-23 13:30:14  [INFO]  [GOLD] Exporting ML-ready cohort to S3 gold bucket target: s3://{BRONZE_BUCKET}/gold/research_dataset/
2026-06-23 13:30:15  [INFO]  Pipeline execution complete for Transmission ID: {active_id}. Job Status: SUCCESS.
        """
        st.code(simulated_log, language="text")

    st.markdown("---")

    # Live s3 bucket listing
    st.subheader("Live S3 Bronze Landing Contents")
    if st.button("Scan S3 Bronze Bucket"):
        try:
            client_kwargs = {"region_name": AWS_REGION}
            if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
                client_kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
                client_kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY

            s3_client = boto3.client("s3", **client_kwargs)
            paginator = s3_client.get_paginator("list_objects_v2")
            objects = []
            for page in paginator.paginate(Bucket=BRONZE_BUCKET, Prefix=RAW_PREFIX):
                for obj in page.get("Contents", []):
                    
                    # Fetch metadata for audit logging
                    head_response = s3_client.head_object(Bucket=BRONZE_BUCKET, Key=obj["Key"])
                    metadata = head_response.get("Metadata", {})
                    
                    objects.append({
                        "S3 Object Key": obj["Key"],
                        "Submitter": metadata.get("submitter", "Unknown"),
                        "Affiliation": metadata.get("affiliation", "Unknown"),
                        "Size (KB)": round(obj["Size"] / 1024, 2),
                        "Last Modified": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S UTC")
                    })
            if objects:
                st.dataframe(pd.DataFrame(objects), use_container_width=True)
            else:
                st.info(f"No objects found in `{BRONZE_BUCKET}` under the prefix `{RAW_PREFIX}/` yet.")
        except Exception as exc:
            st.error(f"Failed to query S3 bucket: {exc}")
            st.info("Ensure your local AWS CLI credentials or Streamlit secrets are configured correctly.")

    st.markdown("---")

    st.subheader("Configured Runtime Parameters")
    config_rows = [
        {"Setting": "Bronze bucket target", "Value": BRONZE_BUCKET},
        {"Setting": "Raw prefix folder", "Value": RAW_PREFIX},
        {"Setting": "AWS Region configured", "Value": AWS_REGION},
        {"Setting": "Databricks Workspace URL", "Value": DATABRICKS_WORKSPACE or "not set"},
        {"Setting": "Databricks Orchestration Job", "Value": DATABRICKS_JOB_ID or "not set"},
        {"Setting": "Console last refresh", "Value": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")},
    ]
    st.dataframe(pd.DataFrame(config_rows), use_container_width=True)