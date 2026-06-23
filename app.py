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
DATABRICKS_JOB_NAME = _setting("DATABRICKS_JOB_NAME", "databricks", "job_name", "Medallion pipeline")
DATABRICKS_TOKEN = _setting("DATABRICKS_PERSONAL_ACCESS_TOKEN", "databricks", "token", "")
DATABRICKS_JOB_ID = _setting("DATABRICKS_JOB_ID", "databricks", "job_id", "")

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


def upload_to_bronze(uploaded_file, dataset_type: str) -> str:
    prefix_map = {
        "CHOC Cerner Extract": "choc",
        "Rady Epic Extract": "rady",
        "DICOM Imaging Manifest": "dicom",
    }
    object_prefix = prefix_map[dataset_type]
    key = f"{RAW_PREFIX}/{object_prefix}/{uploaded_file.name}"

    uploaded_file.seek(0)
    client = boto3.client("s3", region_name=AWS_REGION)
    client.upload_fileobj(
        uploaded_file,
        BRONZE_BUCKET,
        key,
        ExtraArgs={"ContentType": uploaded_file.type or "text/csv"},
    )
    return f"s3://{BRONZE_BUCKET}/{key}"


st.set_page_config(page_title="MRI Data Ingestion Portal", layout="wide")

st.title("Clinical MRI Data Portal")
st.write("Uploads land in the Bronze S3 bucket and are processed by Databricks jobs.")

tab1, tab2 = st.tabs(["Upload", "Pipeline status"])

with tab1:
    st.subheader("Upload a source file")
    st.warning("Files are stored in S3. Do not upload protected data unless the dataset is approved for this workspace.")

    dataset_type = st.selectbox(
        "Dataset type",
        ["CHOC Cerner Extract", "Rady Epic Extract", "DICOM Imaging Manifest"],
    )
    uploaded_file = st.file_uploader("CSV file", type=["csv"])

    if uploaded_file is not None:
        preview_df = pd.read_csv(uploaded_file)
        st.write("Preview")
        st.dataframe(preview_df.head())

        if st.button("Upload to S3", type="primary"):
            try:
                s3_uri = upload_to_bronze(uploaded_file, dataset_type)
            except (NoCredentialsError, ClientError, BotoCoreError) as exc:
                st.error(f"Upload failed: {exc}")
            else:
                st.success(f"Uploaded to {s3_uri}")
                with st.spinner("Notifying Databricks cluster..."):
                    trigger_databricks_pipeline()

with tab2:
    st.subheader("Configured runtime")
    st.caption("Production monitoring is driven by Databricks job runs and S3 object listings.")

    config_rows = [
        {"Setting": "Bronze bucket", "Value": BRONZE_BUCKET},
        {"Setting": "Raw prefix", "Value": RAW_PREFIX},
        {"Setting": "AWS region", "Value": AWS_REGION},
        {"Setting": "Databricks workspace", "Value": DATABRICKS_WORKSPACE or "not set"},
        {"Setting": "Databricks job", "Value": DATABRICKS_JOB_NAME or "not set"},
        {"Setting": "Last refresh", "Value": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")},
    ]
    st.dataframe(pd.DataFrame(config_rows), use_container_width=True)