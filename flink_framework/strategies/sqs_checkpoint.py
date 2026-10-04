import logging
import boto3
import json
import duckdb
from typing import List
from flink_framework.core.interfaces import CheckpointStrategy

logger = logging.getLogger(__name__)

class SQSDuckDBCheckpointStrategy(CheckpointStrategy):
    """
    Identical to the PySpark version. Uses DuckDB as a manual WAL
    to avoid O(N) S3 listing operations.
    """
    def __init__(self, config):
        super().__init__(config)
        self.sqs_url = self.config.get("sqs_url")
        self.checkpoint_location = self.config.get("checkpoint_location")
        self.local_db_path = "/tmp/flink_sqs_state.duckdb"
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        self.conn = None
        self.pending_records = []

    def _parse_s3_path(self, path: str):
        bucket = path.replace("s3://", "").split("/")[0]
        key = "/".join(path.replace("s3://", "").split("/")[1:])
        return bucket, key

    def _init_duckdb(self):
        if self.checkpoint_location:
            state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
            try:
                self.s3.download_file(state_bucket, state_key, self.local_db_path)
            except Exception:
                pass

        self.conn = duckdb.connect(self.local_db_path)
        self.conn.execute("CREATE TABLE IF NOT EXISTS processed_files (filepath VARCHAR PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS pending_files (filepath VARCHAR PRIMARY KEY, receipt VARCHAR)")

    def discover_new_files(self, base_path: str) -> List[str]:
        self._init_duckdb()
        logger.info(f"[Flink SQS-DuckDB] Polling queue: {self.sqs_url}")
        
        try:
            response = self.sqs.receive_message(QueueUrl=self.sqs_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        except Exception:
            response = {} 
            
        if 'Messages' in response:
            for msg in response['Messages']:
                receipt = msg['ReceiptHandle']
                body = json.loads(msg['Body'])
                if 'Records' in body:
                    for record in body['Records']:
                        filepath = f"s3a://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}"
                        is_processed = self.conn.execute("SELECT 1 FROM processed_files WHERE filepath = ?", (filepath,)).fetchone()
                        if is_processed:
                            self.sqs.delete_message(QueueUrl=self.sqs_url, ReceiptHandle=receipt)
                        else:
                            self.conn.execute("INSERT OR IGNORE INTO pending_files VALUES (?, ?)", (filepath, receipt))
                            
        pending_data = self.conn.execute("SELECT filepath, receipt FROM pending_files").fetchall()
        new_files = []
        for filepath, receipt in pending_data:
            new_files.append(filepath)
            self.pending_records.append((filepath, receipt))
            
        logger.info(f"[Flink SQS-DuckDB] Found {len(new_files)} pending files to process.")
        return new_files

    def commit_checkpoint(self) -> None:
        if not self.conn: return
        for filepath, receipt in self.pending_records:
            self.conn.execute("INSERT OR IGNORE INTO processed_files VALUES (?)", (filepath,))
            self.conn.execute("DELETE FROM pending_files WHERE filepath = ?", (filepath,))
            try:
                self.sqs.delete_message(QueueUrl=self.sqs_url, ReceiptHandle=receipt)
            except Exception:
                pass
                
        self.pending_records = []
        self.conn.close()
        self.conn = None
        
        if self.checkpoint_location:
            state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
            self.s3.upload_file(self.local_db_path, state_bucket, state_key)
            logger.info(f"[Flink SQS-DuckDB] Uploaded state back to S3: {self.checkpoint_location}")
