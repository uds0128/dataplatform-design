import logging
from pyflink.table import Table
from flink_framework.core.interfaces import Extractor

logger = logging.getLogger(__name__)

class KafkaExtractor(Extractor):
    """
    Creates a Flink Kafka Source using SQL DDL.
    Unlike PySpark Micro-batching, we do NOT need a custom DuckDB state manager here.
    Flink's native distributed checkpointing handles Kafka offsets natively!
    """
    def extract(self) -> Table:
        topic = self.config.get("topic")
        servers = self.config.get("bootstrap_servers")
        schema = self.config.get("schema")
        
        if not schema:
            raise ValueError("KafkaExtractor requires a 'schema' block in config.")
            
        # Dynamically build the schema DDL string
        schema_fields = ",\n                ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        
        ddl = f"""
            CREATE TEMPORARY TABLE source_kafka (
                {schema_fields}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{servers}',
                'properties.group.id' = 'flink_ingestion_group',
                'scan.startup.mode' = 'latest-offset',
                'format' = 'json'
            )
        """
        logger.info(f"Registering Flink Kafka Source Table for topic {topic}")
        self.t_env.execute_sql(ddl)
        
        return self.t_env.from_path("source_kafka")
