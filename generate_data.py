import os
import random
import logging
from datetime import date, timedelta

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

OUTPUT_DIR = "data"
N_PARTICIPANTS = 500
RANDOM_SEED = 42
YEAR = 2026

SITES = ["CHOC", "Rady"]
SITE_WEIGHTS = [0.55, 0.45]

EHR_SYSTEM_MAP = {"CHOC": "Cerner", "Rady": "Epic"}

CERNER_POSITIVE_CODE = "1"
CERNER_NEGATIVE_CODE = "-1"
EPIC_POSITIVE_CODE = "Positive"
EPIC_NEGATIVE_CODE = "NA"

LESION_POSITIVE_RATE = 0.20
MISSING_READ_RATE = 0.05

MODALITIES = ["MRI"]
BODY_REGION = "Brain"

def _random_date(year: int) -> str:
    """Return a random date within the given year."""
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, delta))).isoformat()

def _build_ssns(n: int) -> list[str]:
    """Generate zero-padded participant IDs."""
    return [f"999-{str(random.randint(10, 99))}-{str(i).zfill(4)}" for i in range(1, n + 1)]

def _inject_missing(series: pd.Series, rate: float) -> pd.Series:
    """Randomly replace a fraction of values with NaN."""
    series = series.copy()
    n_missing = int(len(series) * rate)
    missing_indices = np.random.choice(series.index, size=n_missing, replace=False)
    series.loc[missing_indices] = np.nan
    return series

def build_metadata(ssns: list[str]) -> pd.DataFrame:
    """Build synthetic clinical metadata records."""
    n = len(ssns)

    site_location = np.random.choice(SITES, size=n, p=SITE_WEIGHTS)
    ehr_system = np.array([EHR_SYSTEM_MAP[s] for s in site_location])
    is_positive = np.random.random(n) < LESION_POSITIVE_RATE

    lesion_status_code = np.empty(n, dtype=object)
    for i in range(n):
        if site_location[i] == "CHOC":
            lesion_status_code[i] = CERNER_POSITIVE_CODE if is_positive[i] else CERNER_NEGATIVE_CODE
        else:
            lesion_status_code[i] = EPIC_POSITIVE_CODE if is_positive[i] else EPIC_NEGATIVE_CODE

    df = pd.DataFrame(
        {
            "ssn": ssns,
            "age": np.random.randint(5, 19, size=n),
            "site_location": site_location,
            "ehr_system": ehr_system,
            "lesion_status_code": lesion_status_code,
            "scan_date": [_random_date(YEAR) for _ in range(n)],
        }
    )

    df["lesion_status_code"] = _inject_missing(df["lesion_status_code"], MISSING_READ_RATE)

    n_missing = df["lesion_status_code"].isna().sum()
    n_positive = (
        df["lesion_status_code"].isin([CERNER_POSITIVE_CODE, EPIC_POSITIVE_CODE]).sum()
    )
    n_negative = n - n_positive - n_missing

    log.info(
        "metadata.csv — %d rows | site split: CHOC=%d, Rady=%d",
        n,
        (site_location == "CHOC").sum(),
        (site_location == "Rady").sum(),
    )
    log.info(
        "lesion_status_code — positive=%d (%.0f%%) | negative=%d (%.0f%%) | "
        "missing/pending=%d (%.0f%%)",
        n_positive, n_positive / n * 100,
        n_negative, n_negative / n * 100,
        n_missing, n_missing / n * 100,
    )
    log.info(
        "EHR coding convention — Cerner (CHOC): '%s'/'%s'  |  Epic (Rady): '%s'/'%s'",
        CERNER_POSITIVE_CODE, CERNER_NEGATIVE_CODE, EPIC_POSITIVE_CODE, EPIC_NEGATIVE_CODE,
    )

    return df

def build_dicom_manifest(ssns: list[str]) -> pd.DataFrame:
    """Build synthetic DICOM manifest records."""
    n = len(ssns)

    uris = [
        f"s3://choc-rady-clinical-bronze-demo/imaging/{pid}_scan1.dcm"
        for pid in ssns
    ]

    df = pd.DataFrame(
        {
            "ssn": ssns,
            "dicom_s3_uri": uris,
            "modality": np.random.choice(MODALITIES, size=n),
            "body_region": BODY_REGION,
        }
    )

    log.info("dicom_manifest.csv — %d rows | body_region=%s | modality=%s", n, BODY_REGION, MODALITIES[0])
    return df

def main() -> None:
    np.random.seed(RANDOM_SEED)
    random.seed(RANDOM_SEED)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    ssns = _build_ssns(N_PARTICIPANTS)

    metadata_df = build_metadata(ssns)
    dicom_df = build_dicom_manifest(ssns)

    metadata_path = os.path.join(OUTPUT_DIR, "metadata.csv")
    dicom_path = os.path.join(OUTPUT_DIR, "dicom_manifest.csv")

    metadata_df.to_csv(metadata_path, index=False)
    log.info("Written: %s", metadata_path)

    dicom_df.to_csv(dicom_path, index=False)
    log.info("Written: %s", dicom_path)

    log.info("--- metadata.csv sample ---")
    log.info("\n%s", metadata_df.head(10).to_string(index=False))
    log.info("--- dicom_manifest.csv sample ---")
    log.info("\n%s", dicom_df.head(5).to_string(index=False))

    log.info("Data generation complete. Files saved to '%s/'", OUTPUT_DIR)


if __name__ == "__main__":
    main()
