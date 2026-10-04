import logging
import boto3
import duckdb
from typing import Tuple, Dict, List
from spark_framework.core.interfaces import CheckpointStrategy

logger = logging.getLogger(__name__)

class KafkaDuckDBCheckpointStrategy(CheckpointStrategy):
    """
    Manages Kafka Offsets in DuckDB to enable custom micro-batching without Spark's WAL.
    Persists the DuckDB state to S3.
    """
    def __init__(self, config):
        super().__init__(config)
        self.checkpoint_location = self.config.get("checkpoint_location")
        self.local_db_path = "/tmp/kafka_state.duckdb"
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.conn = None
        self.pending_offsets = {}  # Format: {topic: {partition: end_offset}}

    def _parse_s3_path(self, path: str):
        bucket = path.replace("s3://", "").split("/")[0]
        key = "/".join(path.replace("s3://", "").split("/")[1:])
        return bucket, key

    def _init_duckdb(self):
        if self.checkpoint_location:
            state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
            try:
                self.s3.download_file(state_bucket, state_key, self.local_db_path)
                logger.info(f"[Kafka-DuckDB] Downloaded offset state from {self.checkpoint_location}")
            except Exception:
                logger.info("[Kafka-DuckDB] No DuckDB state found in S3. Starting fresh.")

        self.conn = duckdb.connect(self.local_db_path)
        # Table schema: topic, partition, committed_offset
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS kafka_offsets (
                topic VARCHAR, 
                partition INT, 
                committed_offset BIGINT,
                PRIMARY KEY (topic, partition)
            )
        """)

    def discover_new_files(self, base_path: str) -> List[str]:
        # Kafka doesn't use files, satisfy the interface
        return []

    def get_offsets(self, topic: str) -> Tuple[Dict[str, int], Dict[str, int]]:
        self._init_duckdb()
        
        # 1. Fetch last committed offsets from DuckDB
        results = self.conn.execute("SELECT partition, committed_offset FROM kafka_offsets WHERE topic = ?", (topic,)).fetchall()
        starting_offsets = {str(row[0]): row[1] for row in results}
        
        if not starting_offsets:
            # Default if completely fresh topic
            starting_offsets = {"0": 0, "1": 0}
            
        # 2. Fetch the latest offsets from Kafka Brokers
        # (In reality, you use kafka-python AdminClient or Consumer.end_offsets() here)
        # Mocking the fetch of high watermarks:
        ending_offsets = {
            p: offset + 100 for p, offset in starting_offsets.items()
        }
        
        logger.info(f"[Kafka-DuckDB] Starting offsets: {starting_offsets}")
        logger.info(f"[Kafka-DuckDB] Ending offsets (latest): {ending_offsets}")
        
        # 3. Store as pending to be committed later
        self.pending_offsets[topic] = ending_offsets
        
        return starting_offsets, ending_offsets

    def commit_checkpoint(self) -> None:
        if not self.conn or not self.pending_offsets:
            return
            
        logger.info("[Kafka-DuckDB] Committing new Kafka offsets to DuckDB.")
        
        for topic, partitions in self.pending_offsets.items():
            for part, offset in partitions.items():
                self.conn.execute("""
                    INSERT INTO kafka_offsets (topic, partition, committed_offset) 
                    VALUES (?, ?, ?) 
                    ON CONFLICT (topic, partition) 
                    DO UPDATE SET committed_offset = excluded.committed_offset
                """, (topic, int(part), offset))
                
        self.pending_offsets = {}
        self.conn.close()
        self.conn = None
        
        # 4. Upload DuckDB back to S3
        if self.checkpoint_location:
            state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
            self.s3.upload_file(self.local_db_path, state_bucket, state_key)
            logger.info(f"[Kafka-DuckDB] Uploaded updated offset state back to S3: {self.checkpoint_location}")
