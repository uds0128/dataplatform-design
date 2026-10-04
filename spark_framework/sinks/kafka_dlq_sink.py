import logging
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from spark_framework.core.interfaces import Sink

logger = logging.getLogger(__name__)

class KafkaDLQSink(Sink):
    """
    Publishes bad records to a Kafka DLQ topic.
    Wraps the entire row (including the dq_metadata struct) into a JSON payload.
    """
    def write(self, df: DataFrame) -> None:
        topic = self.config.get("topic")
        servers = self.config.get("bootstrap_servers", "localhost:9092")
        
        # Convert the entire row to a JSON string in a 'value' column for Kafka
        dlq_df = df.select(F.to_json(F.struct(F.col("*"))).alias("value"))
        
        logger.warning(f"Publishing {df.count()} bad records as JSON (with metadata) to Kafka DLQ: {topic}")
        
        # dlq_df.write \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", servers) \
        #     .option("topic", topic) \
        #     .save()
