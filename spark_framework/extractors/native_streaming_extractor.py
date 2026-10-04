import logging
from pyspark.sql import DataFrame
from spark_framework.core.interfaces import Extractor

logger = logging.getLogger(__name__)

class NativeStreamingExtractor(Extractor):
    """
    Uses Spark's native Structured Streaming API.
    Does not require a custom checkpoint_strategy.
    """
    def extract(self) -> DataFrame:
        path = self.config.get("path")
        logger.info(f"NativeStreamingExtractor using spark.readStream for path: {path}")
        
        # Returns a Streaming DataFrame. Spark handles the internal file listing and WAL.
        return self.spark.readStream.format("parquet").load(path)
