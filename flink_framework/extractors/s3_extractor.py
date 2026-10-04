import logging
from pyflink.table import Table
from flink_framework.core.interfaces import Extractor

logger = logging.getLogger(__name__)

class S3Extractor(Extractor):
    """
    Dynamic S3 Extractor for Flink.
    If 'checkpoint_strategy' is provided, runs as a Cron Batch Job using UNION ALL across exact files.
    If 'checkpoint_strategy' is omitted, runs as a Continuous Stream using Flink native directory monitoring.
    """
    def extract(self) -> Table:
        base_path = self.config.get("path")
        file_format = self.config.get("format", "parquet")
        schema = self.config.get("schema")
        
        if not schema:
            raise ValueError("S3Extractor requires a 'schema' block in config.")
            
        schema_fields = ",\n                ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        
        # Scenario A: Batch Mode (Custom Checkpoint Strategy provided)
        if self.checkpoint_strategy:
            logger.info("Custom CheckpointStrategy detected. Extracting specific files for batch processing.")
            new_files = self.checkpoint_strategy.discover_new_files(base_path)
            
            if not new_files:
                logger.info("No new files found. Returning None.")
                return None 
                
            table_queries = []
            for i, file_path in enumerate(new_files):
                table_name = f"source_s3_file_{i}"
                ddl = f"""
                    CREATE TEMPORARY TABLE {table_name} (
                        {schema_fields}
                    ) WITH (
                        'connector' = 'filesystem',
                        'path' = '{file_path}',
                        'format' = '{file_format}'
                    )
                """
                self.t_env.execute_sql(ddl)
                table_queries.append(f"SELECT * FROM {table_name}")
                
            union_sql = " UNION ALL ".join(table_queries)
            logger.info(f"Generated UNION ALL across {len(new_files)} specific S3 files.")
            return self.t_env.sql_query(union_sql)
            
        # Scenario B: Continuous Streaming Mode (No strategy provided)
        else:
            logger.info("No CheckpointStrategy detected. Using Flink native continuous directory monitoring.")
            ddl = f"""
                CREATE TEMPORARY TABLE source_s3 (
                    {schema_fields}
                ) WITH (
                    'connector' = 'filesystem',
                    'path' = '{base_path}',
                    'format' = '{file_format}',
                    'source.monitor-interval' = '1 m' 
                )
            """
            self.t_env.execute_sql(ddl)
            return self.t_env.from_path("source_s3")
