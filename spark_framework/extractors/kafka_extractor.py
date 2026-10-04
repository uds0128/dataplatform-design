import logging
import json
from pyspark.sql import DataFrame
from spark_framework.core.interfaces import Extractor

logger = logging.getLogger(__name__)

class KafkaExtractor(Extractor):
    """
    Extracts data from Kafka using starting and ending offsets provided by the strategy.
    Does not rely on Spark Structured Streaming WAL, enabling custom micro-batch control.
    """
    def extract(self) -> DataFrame:
        topic = self.config.get("topic")
        servers = self.config.get("bootstrap_servers")
        
        if not self.checkpoint_strategy:
            raise ValueError("KafkaExtractor requires a checkpoint_strategy to manage offsets!")
            
        # 1. Ask the Strategy for the partition offsets
        starting_offsets, ending_offsets = self.checkpoint_strategy.get_offsets(topic)
        
        if not starting_offsets or not ending_offsets:
            logger.info(f"No new data in Kafka topic {topic}. Returning empty DataFrame.")
            return self.spark.createDataFrame([], schema="value BINARY")
            
        logger.info(f"KafkaExtractor reading {topic} from {starting_offsets} to {ending_offsets}")
        
        # 2. Format offsets for PySpark Kafka Batch API
        start_json = json.dumps({topic: starting_offsets})
        end_json = json.dumps({topic: ending_offsets})
        
        # 3. Actual Spark Read (Commented for demo execution)
        # df = self.spark.read.format("kafka") \
        #     .option("kafka.bootstrap.servers", servers) \
        #     .option("subscribe", topic) \
        #     .option("startingOffsets", start_json) \
        #     .option("endingOffsets", end_json) \
        #     .load()
            
        # Mocking for demo
        df = self.spark.createDataFrame([("Alice", 100), ("Bob", -50)], ["name", "amount"])
        return df
