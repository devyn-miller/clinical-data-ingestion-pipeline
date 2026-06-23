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
                        s3_uri = upload_to_bronze(
                            io.BytesIO(raw_bytes), 
                            uploaded_file.name, 
                            dataset_type,
                            submitter_name,
                            affiliation
                        )
                        st.success(f"Uploaded securely to {s3_uri}")
                        
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

    st.info("**Interactive Demo:** Below are sample datasets representing the architectural steps taken by our Databricks pipeline. Use this during code reviews to trace schema evolution, label standardization, and de-identification.")

    step_tab1, step_tab2, step_tab3, step_tab4 = st.tabs([
        "1. Raw EHR Inputs",
        "2. Bronze Schema Enforced",
        "3. Silver Harmonized & Joined",
        "4. Gold Anonymized & De-Identified"
    ])


    # Pre-baked transformation datasets that mimic real system outputs
    raw_choc = pd.DataFrame({
        "cerner_id": ["C00001", "C00002", "C00003"],
        "ssn": ["999-12-3456", "999-55-9876", "999-88-1111"],
        "age": [14, 8, 11],
        "site_location": ["CHOC", "CHOC", "CHOC"],
        "scan_date": ["2025-02-14", "2025-05-19", "2026-01-10"],
        "lesion_status_code": ["1", "-1", "-1"]  # (1=Lesion, -1=Clear)
    })

    raw_rady = pd.DataFrame({
        "epic_id": ["E00001", "E00002", "E00003"],
        "ssn": ["999-44-2222", "999-55-9876", "999-77-3333"], # 999-55-9876 visited *both* hospitals
        "age_in_years": [6, 8, 17],
        "site_location": ["Rady", "Rady", "Rady"],
        "mri_date": ["2024-11-20", "2025-06-01", "2025-12-05"],
        "lesion_status": ["Positive", "None", "NA"]  # (Positive=Lesion, None/NA=Clear)
    })

    raw_dicom = pd.DataFrame({
        "patient_ssn": ["999-12-3456", "999-55-9876", "999-88-1111", "999-44-2222", "999-77-3333"],
        "s3_dicom_path": [
            "s3://choc-rady-mri-landing-zone/images/scan_10001.dcm",
            "s3://choc-rady-mri-landing-zone/images/scan_10002.dcm",
            "s3://choc-rady-mri-landing-zone/images/scan_10003.dcm",
            "s3://choc-rady-mri-landing-zone/images/scan_10004.dcm",
            "s3://choc-rady-mri-landing-zone/images/scan_10005.dcm"
        ]
    })

    # Step 1: Raw payloads
    with step_tab1:
        st.write("#### Stage: Raw Landing Zone")
        st.caption("CHOC Hospital (Cerner Database schema) and Rady Hospital (Epic Database schema) upload completely inconsistent column headings, date structures, and classification mappings. Furthermore, patient `999-55-9876` visited both hospital networks, creating a cross-site duplication risk.")
       
        col_choc, col_rady = st.columns(2)
        with col_choc:
            st.markdown("**CHOC Hospital (Cerner Database schema)**")
            st.dataframe(raw_choc)
        with col_rady:
            st.markdown("**Rady Hospital (Epic Database schema)**")
            st.dataframe(raw_rady)
            
        st.markdown("**DICOM Manifest (Raw Imaging Directory)**")
        st.dataframe(raw_dicom)

    # Step 2: Bronze
    with step_tab2:
        st.write("#### Stage: Bronze Ingestion Ledger")
        st.caption("The Databricks pipeline enforces strict schemas on read. It appends critical analytical audit-trail columns (`ingested_at`, `source_file`, `pipeline_run_date`) and commits them to historical Delta tables. The data is kept unmodified to preserve history.")
       
        bronze_preview = pd.DataFrame({
            "participant_id": ["C00001", "C00002", "C00003", "E00001", "E00002", "E00003"],
            "ssn": ["999-12-3456", "999-55-9876", "999-88-1111", "999-44-2222", "999-55-9876", "999-77-3333"],
            "raw_lesion_status": ["1", "-1", "-1", "Positive", "None", "NA"],
            "ingested_at": ["2026-06-23 13:18:04 UTC"] * 6,
            "source_file": [
                "file:/Workspace/data/choc_cerner_extract.csv", "file:/Workspace/data/choc_cerner_extract.csv", "file:/Workspace/data/choc_cerner_extract.csv",
                "file:/Workspace/data/rady_epic_extract.csv", "file:/Workspace/data/rady_epic_extract.csv", "file:/Workspace/data/rady_epic_extract.csv"
            ],
            "pipeline_run_date": ["2026-06-23"] * 6
        })
        st.dataframe(bronze_preview, use_container_width=True)

    # Step 3: Silver
    with step_tab3:
        st.write("#### Stage: Silver Harmonization, Deduplication & Inner Join")
        st.caption("A Spark Window Function (`F.row_number()`) evaluates overlapping patients by `ssn` and keeps only the most recent entry (the newer Rady record is kept for patient `999-55-9876`; the CHOC record is discarded). Lesion codes are standardized to clean boolean findings, and the dataset is inner-joined with S3 DICOM image pointers.")
       
        silver_preview = pd.DataFrame({
            "ssn": ["999-12-3456", "999-88-1111", "999-44-2222", "999-55-9876", "999-77-3333"],
            "age": [14, 11, 6, 8, 17],
            "ehr_system": ["Cerner", "Cerner", "Epic", "Epic", "Epic"],
            "site_location": ["CHOC", "CHOC", "RADY", "RADY", "RADY"],
            "scan_date": ["2025-02-14", "2026-01-10", "2024-11-20", "2025-06-01", "2025-12-05"],
            "lesion_label": ["Lesion Detected", "No Lesion Detected", "Lesion Detected", "No Lesion Detected", "No Lesion Detected"],
            "imaging_s3_uri": [
                "s3://choc-rady-mri-landing-zone/images/scan_10001.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10003.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10004.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10002.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10005.dcm"
            ]
        })
        st.dataframe(silver_preview, use_container_width=True)

    # Step 4: Gold
    with step_tab4:
        st.write("#### Stage: Gold Research Cohorts (ML-Ready, PHI-Masked)")
        st.caption("To enforce HIPAA Safe Harbor rules and satisfy IRB approvals, patient SSNs are masked with deterministic SHA-256 surrogate keys. Columns not needed by the computer vision team are purged, yielding an anonymized, high-performance dataset.")
       
        # Calculate SHA-256 for demo
        ssns = ["999-12-3456", "999-88-1111", "999-44-2222", "999-55-9876", "999-77-3333"]
        hashes = [hashlib.sha256(s.encode()).hexdigest()[:16] for s in ssns]
        
        gold_preview = pd.DataFrame({
            "surrogate_id": hashes,
            "age": [14, 11, 6, 8, 17],
            "ehr_system": ["Cerner", "Cerner", "Epic", "Epic", "Epic"],
            "site_location": ["CHOC", "CHOC", "RADY", "RADY", "RADY"],
            "lesion_label": ["Lesion Detected", "No Lesion Detected", "Lesion Detected", "No Lesion Detected", "No Lesion Detected"],
            "s3_dicom_path": [
                "s3://choc-rady-mri-landing-zone/images/scan_10001.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10003.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10004.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10002.dcm",
                "s3://choc-rady-mri-landing-zone/images/scan_10005.dcm"
            ]
        })
        st.dataframe(gold_preview, use_container_width=True)

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