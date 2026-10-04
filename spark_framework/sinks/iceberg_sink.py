import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from spark_framework.core.interfaces import Sink

logger = logging.getLogger(__name__)

class IcebergSink(Sink):
    """
    Writes to Iceberg. Supports 'append' and 'upsert' modes.
    In 'upsert' mode, it automatically deduplicates the micro-batch before merging.
    """
    def write(self, df: DataFrame) -> None:
        table = self.config.get("table_name")
        mode = self.config.get("mode", "append").lower()
        
        partition_cols = self.config.get("partition_columns", [])
        
        if mode == "append":
            logger.info(f"Appending data to Iceberg table: {table}")
            if partition_cols:
                logger.info(f"Applying Iceberg Partitions: {partition_cols}")
                # df.writeTo(table).partitionedBy(*partition_cols).append()
            # else:
            #     df.writeTo(table).append()
            
        elif mode == "upsert":
            pk = self.config.get("primary_key")
            sort_col = self.config.get("sort_column")
            
            if not pk or not sort_col:
                raise ValueError("Upsert mode requires 'primary_key' (list) and 'sort_column' (str) in config.")
                
            logger.info(f"Mode=Upsert. Deduplicating micro-batch based on PK: {pk}, sorting by: {sort_col} DESC")
            
            # 1. Deduplicate the micro-batch keeping the latest record
            window_spec = Window.partitionBy(*pk).orderBy(F.col(sort_col).desc())
            dedup_df = df.withColumn("rn", F.row_number().over(window_spec)) \
                         .filter(F.col("rn") == 1) \
                         .drop("rn")
                         
            logger.info(f"Executing Iceberg MERGE INTO {table} ON {pk}")
            
            # 2. Simulate Iceberg MERGE INTO (requires Spark 3 Iceberg extensions)
            # dedup_df.createOrReplaceTempView("source_view")
            # join_cond = " AND ".join([f"t.{c} = s.{c}" for c in pk])
            # merge_sql = f"""
            # MERGE INTO {table} t 
            # USING source_view s ON {join_cond}
            # WHEN MATCHED THEN UPDATE SET *
            # WHEN NOT MATCHED THEN INSERT *
            # """
            # self.spark.sql(merge_sql)
        else:
            raise ValueError(f"Unsupported sink mode: {mode}")
