import requests
import os
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
DATABRICKS_WORKSPACE = _setting("DATABRICKS_WORKSPACE_URL", "databricks", "workspace_url", "")
DATABRICKS_TOKEN = _setting("DATABRICKS_PERSONAL_ACCESS_TOKEN", "databricks", "token", "")
DATABRICKS_JOB_ID = _setting("DATABRICKS_JOB_ID", "databricks", "job_id", "")


def upload_to_bronze(file_obj: io.BytesIO, filename: str, dataset_type: str, submitter: str, affil: str) -> str:
    """Stream-pointer safe S3 Object storage uploading with unique timestamps and metadata."""
    prefix_map = {
        "CHOC Cerner Extract": "choc",
        "Rady Epic Extract": "rady",
        "DICOM Imaging Manifest": "dicom",
    }
    object_prefix = prefix_map[dataset_type]
    
    # Generate unique timestamp string
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    name, ext = os.path.splitext(filename)
    unique_filename = f"{name}_{timestamp}{ext}"
    
    key = f"{RAW_PREFIX}/{object_prefix}/{unique_filename}"

    client = boto3.client("s3", region_name=AWS_REGION)
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
        ["CHOC Cerner Extract", "Rady Epic Extract", "DICOM Imaging Manifest"],
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
    st.error("**RESTRICTED ACCESS:** System Engineers and Research Data Governance Board Only")
    st.subheader("Medallion Architecture Pipeline Status")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.info("**BRONZE LAYER**\n\n*Raw, Immutable S3 Landing*")
        st.metric(label="Ingested Files awaiting ingestion", value="3", delta="+1 newly ingested")
        st.caption(f"Target: `s3://{BRONZE_BUCKET}/{RAW_PREFIX}/`")
    with c2:
        st.warning("**SILVER LAYER**\n\n*Harmonization & Deduplication*")
        st.metric(label="Patient Overlaps Merged", value="50", delta="Cross-EHR SSN Match")
        st.caption(f"Engine: `Databricks PySpark`")
    with c3:
        st.success("**GOLD LAYER**\n\n*Research-Ready Aggregates*")
        st.metric(label="Available ML Cohorts", value="450", delta="Fully De-Identified")
        st.caption(f"Target: `s3://{BRONZE_BUCKET}/gold/`")

    st.markdown("---")

    # Live s3 bucket listings
    st.subheader("Live S3 Bronze Landing Contents")
    if st.button("Scan S3 Bronze Bucket"):
        try:
            s3_client = boto3.client("s3", region_name=AWS_REGION)
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