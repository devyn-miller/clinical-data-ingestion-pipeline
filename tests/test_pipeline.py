"""
test_pipeline.py
----------------
Unit tests for the Silver layer data governance and harmonization logic.
Ensures critical clinical logic fails gracefully if mapping rules are broken.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Import the function from our medallion script
# (In a standard python project, this would be `from src.transformations import apply_lesion_harmonization`)
from databricks.medallion_pipeline import apply_lesion_harmonization

@pytest.fixture(scope="session")
def spark():
    """Builds a local Spark session for testing."""
    return SparkSession.builder \
        .appName("pytest-pyspark-testing") \
        .master("local[1]") \
        .getOrCreate()

def test_apply_lesion_harmonization(spark):
    """
    Validates that Cerner (-1) and Epic (NA/None) negative scans 
    are correctly mapped to 'No Lesion Detected', and that '1' maps 
    to 'Lesion Detected'.
    """
    # 1. Setup Mock Data
    schema = StructType([StructField("raw_code", StringType(), True)])
    mock_data = [("-1",), ("NA",), (None,), ("1",)]
    
    df = spark.createDataFrame(mock_data, schema)
    
    # 2. Execute Transformation
    result_df = apply_lesion_harmonization(df, "raw_code")
    results = [row["lesion_label"] for row in result_df.collect()]
    
    # 3. Assert Business Rules
    assert results[0] == "No Lesion Detected"  # Cerner Negative
    assert results[1] == "No Lesion Detected"  # Epic Negative
    assert results[2] == "No Lesion Detected"  # Null handling
    assert results[3] == "Lesion Detected"     # Positive Lesion