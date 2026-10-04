import sys
import os
import logging
import yaml
import boto3
from pyspark.sql import SparkSession

# Ensure the parent directory is in the PYTHONPATH so module imports work inside the zip package
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_framework.core.pipeline import IngestionPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_yaml_config(path: str) -> dict:
    """Reads the YAML configuration file either from local or directly from S3"""
    if path.startswith("s3://"):
        logger.info(f"Fetching config from S3: {path}")
        bucket = path.replace("s3://", "").split("/")[0]
        key = "/".join(path.replace("s3://", "").split("/")[1:])
        
        # s3 = boto3.client('s3')
        # response = s3.get_object(Bucket=bucket, Key=key)
        # return yaml.safe_load(response['Body'])
        
        # Mocking for demo purposes
        return {} 
    else:
        logger.info(f"Loading local config file: {path}")
        with open(path, 'r') as f:
            return yaml.safe_load(f)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit --py-files spark_framework.zip main.py <s3_path_to_yaml_config>")
        sys.exit(1)
        
    config_path = sys.argv[1]
    
    spark = SparkSession.builder.appName("ConfigDrivenPipeline").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Dynamically load the YAML from S3 based on the spark-submit argument
    yaml_config = load_yaml_config(config_path)

    if yaml_config:
        pipeline = IngestionPipeline(spark, yaml_config)
        pipeline.run()
    else:
        logger.error("Failed to load pipeline configuration.")
