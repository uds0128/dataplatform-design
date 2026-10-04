import sys
import os
import logging
import yaml
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from flink_framework.core.pipeline import FlinkIngestionPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <path_to_yaml_config>")
        sys.exit(1)
        
    config_path = sys.argv[1]
    with open(config_path, 'r') as f:
        yaml_config = yaml.safe_load(f)

    # 1. Setup Flink Streaming Environment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Enable Flink Native Checkpointing
    # (This completely replaces the custom DuckDB state machine we needed in Spark!)
    env.enable_checkpointing(60000) # Checkpoint every 60 seconds
    
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 2. Run Pipeline
    pipeline = FlinkIngestionPipeline(t_env, yaml_config)
    pipeline.run()
