import logging
from pyflink.table import Table, StatementSet
from flink_framework.core.interfaces import Sink

logger = logging.getLogger(__name__)

class KafkaDLQSink(Sink):
    def add_to_statement_set(self, table: Table, stmt_set: StatementSet) -> None:
        topic = self.config.get("topic")
        servers = self.config.get("bootstrap_servers", "localhost:9092")
        schema = self.config.get("schema")
        
        if not schema:
            raise ValueError("KafkaDLQSink requires a 'schema' block in config.")
            
        schema_fields = ",\n                ".join([f"{col} {dtype}" for col, dtype in schema.items()])
        
        ddl = f"""
            CREATE TEMPORARY TABLE sink_dlq_kafka (
                {schema_fields}
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{servers}',
                'format' = 'json'
            )
        """
        logger.info(f"Registering Flink Kafka DLQ Sink Table for topic {topic}")
        self.t_env.execute_sql(ddl)
        
        stmt_set.add_insert("sink_dlq_kafka", table)
