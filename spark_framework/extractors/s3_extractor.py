import logging
from pyspark.sql import DataFrame
from spark_framework.core.interfaces import Extractor

logger = logging.getLogger(__name__)

class S3Extractor(Extractor):
    """
    A single, unified Extractor. 
    It doesn't care HOW files are discovered (Lexical or SQS).
    It just asks the strategy for the array of files, and reads them.
    """
    def extract(self) -> DataFrame:
        base_path = self.config.get("path")
        
        if not self.checkpoint_strategy:
            raise ValueError("S3Extractor requires a checkpoint_strategy to discover files!")
            
        # 1. Ask the Strategy to yield an array of un-checkpointed files
        file_paths_array = self.checkpoint_strategy.discover_new_files(base_path)
        
        if not file_paths_array:
            logger.info("No new files discovered. Returning empty DataFrame.")
            return self.spark.createDataFrame([], schema="name STRING, amount DOUBLE")
            
        logger.info(f"S3Extractor received array of {len(file_paths_array)} files. Executing spark.read...")
        
        # 2. Read the files into a DataFrame and hand over to Transform
        df = self.spark.read.parquet(*file_paths_array)
        return df
