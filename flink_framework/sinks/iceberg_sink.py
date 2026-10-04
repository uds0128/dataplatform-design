import logging
from pyflink.table import Table, StatementSet
from flink_framework.core.interfaces import Sink

logger = logging.getLogger(__name__)

class IcebergSink(Sink):
    """
    Writes to Iceberg using Flink SQL.
    Unlike Spark, we don't need manual Window deduplication code. 
    Flink's Iceberg connector handles UPSERT natively via 'format-version'='2' and Equality Deletes.
    """
    def add_to_statement_set(self, table: Table, stmt_set: StatementSet) -> None:
        table_name = self.config.get("table_name")
        pk = self.config.get("primary_key", ["id"])
        schema = self.config.get("schema")
        
        if not schema:
            raise ValueError("IcebergSink requires a 'schema' block in config.")
            
        pk_str = ", ".join(pk)
        schema_fields = ",\n                ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        
        ddl = f"""
            CREATE TEMPORARY TABLE sink_iceberg (
                {schema_fields},
                PRIMARY KEY ({pk_str}) NOT ENFORCED
            ) WITH (
                'connector' = 'iceberg',
                'catalog-name' = 'default',
                'catalog-type' = 'hive',
                'uri' = 'thrift://localhost:9083',
                'warehouse' = 's3://warehouse/',
                'format-version' = '2'
            )
        """
        logger.info(f"Registering Flink Iceberg Sink Table {table_name} with Upsert PK: {pk_str}")
        self.t_env.execute_sql(ddl)
        
        stmt_set.add_insert("sink_iceberg", table)
