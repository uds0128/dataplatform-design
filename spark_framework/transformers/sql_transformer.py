import logging
from pyspark.sql import DataFrame
from spark_framework.core.interfaces import Transformer

logger = logging.getLogger(__name__)

class SQLTransformer(Transformer):
    """
    Transforms a DataFrame using a dictionary of column aliases and SQL expressions.
    Example Config: {"cola": "cola", "colb": "concat('AB', colB)"}
    """
    def transform(self, df: DataFrame) -> DataFrame:
        columns = self.config.get("columns", {})
        if not columns:
            logger.info("No columns specified for SQLTransformer, returning original DataFrame.")
            return df
            
        # Dynamically build the select expressions: "expr AS alias"
        select_exprs = [f"{expr} AS {alias}" for alias, expr in columns.items()]
        
        logger.info(f"Applying SQL Transformation: SELECT {', '.join(select_exprs)}")
        
        # Apply the expressions to the DataFrame
        return df.selectExpr(*select_exprs)
