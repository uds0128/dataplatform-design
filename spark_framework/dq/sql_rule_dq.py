import logging
from typing import Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spark_framework.core.interfaces import DataQualityGate

logger = logging.getLogger(__name__)

class SQLRuleDQGate(DataQualityGate):
    """
    Evaluates a dictionary of SQL rules against the DataFrame.
    Attaches a 'dq_metadata' struct to track which specific rules failed.
    """
    def validate(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        rules = self.config.get("rules", {})
        if not rules:
            logger.info("No DQ rules specified, all records passed.")
            return df, None
            
        logger.info(f"Applying {len(rules)} DQ Rules: {list(rules.keys())}")
        
        # 1. Evaluate every rule and store the boolean results in a metadata struct
        struct_exprs = [F.expr(expr).alias(name) for name, expr in rules.items()]
        df_with_meta = df.withColumn("dq_metadata", F.struct(*struct_exprs))
        
        # 2. A record is good if ALL rules evaluated to True
        combined_condition = " AND ".join([f"(dq_metadata.{name} = true)" for name in rules.keys()])
        
        # 3. Good records drop the metadata before going to the main sink (Iceberg)
        good_df = df_with_meta.filter(combined_condition).drop("dq_metadata")
        
        # 4. Bad records KEEP the metadata so the DLQ knows exactly what failed
        bad_df = df_with_meta.filter(f"NOT ({combined_condition})")
        
        return good_df, bad_df
