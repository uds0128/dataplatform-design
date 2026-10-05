import pytest
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from spark_framework.dq.sql_rule_dq import SQLRuleDQGate

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("pytest").getOrCreate()

def test_sql_rule_dq_routing_and_metadata(spark):
    """
    Hardest Test 2: DQ Gate Routing & Metadata Attachment
    Ensures bad records are accurately captured, rule violations are recorded, 
    and good records are perfectly isolated without data loss.
    """
    config = {
        "rules": {
            "amount_positive": "amount >= 0",
            "name_not_null": "name IS NOT NULL"
        }
    }
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("amount", IntegerType(), True)
    ])
    
    # 1 good record, 1 record with negative amount, 1 record with null name
    data = [
        ("1", "Alice", 100),
        ("2", "Bob", -50),
        ("3", None, 200)
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    
    dq_gate = SQLRuleDQGate(spark, config)
    good_df, bad_df = dq_gate.evaluate(df)
    
    # Assert Good DF Isolation
    good_records = good_df.collect()
    assert len(good_records) == 1, "Design Flaw: Good records dropped or dirty records slipped through."
    assert good_records[0]["id"] == "1"
    
    # Assert Bad DF Routing
    bad_records = bad_df.collect()
    assert len(bad_records) == 2, "Design Flaw: Failed to capture all bad records."
    
    # Extract metadata to check if exact failure reason was captured accurately
    bad_dict = {row["id"]: json.loads(row["dq_metadata"]) for row in bad_records}
    
    assert "amount_positive" in bad_dict["2"]["failed_rules"], "Design Flaw: Incorrect rule failure tagged."
    assert "name_not_null" in bad_dict["3"]["failed_rules"], "Design Flaw: NULL handling failed."
