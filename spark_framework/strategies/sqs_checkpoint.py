import logging
import boto3
import json
import duckdb
from typing import List
from spark_framework.core.interfaces import CheckpointStrategy

logger = logging.getLogger(__name__)

class SQSCheckpointStrategy(CheckpointStrategy):
    """
    Advanced SQS Polling with DuckDB for idempotency and WAL state tracking.
    State database is persisted in S3 and downloaded/uploaded on each run.
    """
    def __init__(self, config):
        super().__init__(config)
        self.sqs_url = self.config.get("sqs_url")
        self.checkpoint_location = self.config.get("checkpoint_location")
        self.local_db_path = "/tmp/sqs_state.duckdb"
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.sqs = boto3.client('sqs', region_name='us-east-1')
        self.conn = None
        self.pending_records = []

    def _parse_s3_path(self, path: str):
        bucket = path.replace("s3://", "").split("/")[0]
        key = "/".join(path.replace("s3://", "").split("/")[1:])
        return bucket, key

    def _init_duckdb(self):
        # 1. Download DuckDB WAL state from S3
        if self.checkpoint_location:
            state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
            try:
                self.s3.download_file(state_bucket, state_key, self.local_db_path)
                logger.info(f"[SQS-DuckDB] Downloaded DuckDB state from {self.checkpoint_location}")
            except Exception:
                logger.info("[SQS-DuckDB] No DuckDB state found in S3. Creating new local DB.")

        self.conn = duckdb.connect(self.local_db_path)
        self.conn.execute("CREATE TABLE IF NOT EXISTS processed_files (filepath VARCHAR PRIMARY KEY)")
        self.conn.execute("CREATE TABLE IF NOT EXISTS pending_files (filepath VARCHAR PRIMARY KEY, receipt VARCHAR)")

    def discover_new_files(self, base_path: str) -> List[str]:
        if not self.sqs_url or not self.checkpoint_location:
            raise ValueError("SQS Strategy requires 'sqs_url' and 'checkpoint_location'")

        self._init_duckdb()
        logger.info(f"[SQS-DuckDB] Polling queue: {self.sqs_url}")
        
        try:
            response = self.sqs.receive_message(QueueUrl=self.sqs_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        except Exception:
            response = {} # Mocking for demo
            
        # 2. Insert polled SQS events into DuckDB Pending Table
        if 'Messages' in response:
            for msg in response['Messages']:
                receipt = msg['ReceiptHandle']
                body = json.loads(msg['Body'])
                if 'Records' in body:
                    for record in body['Records']:
                        filepath = f"s3a://{record['s3']['bucket']['name']}/{record['s3']['object']['key']}"
                        
                        # Idempotency check: Did we already process this file?
                        is_processed = self.conn.execute("SELECT 1 FROM processed_files WHERE filepath = ?", (filepath,)).fetchone()
                        if is_processed:
                            logger.info(f"[SQS-DuckDB] File {filepath} already processed. ACKing immediately.")
                            self.sqs.delete_message(QueueUrl=self.sqs_url, ReceiptHandle=receipt)
                        else:
                            self.conn.execute("INSERT OR IGNORE INTO pending_files VALUES (?, ?)", (filepath, receipt))
                            
        # 3. Retrieve all pending files (captures new polls + failed previous runs)
        pending_data = self.conn.execute("SELECT filepath, receipt FROM pending_files").fetchall()
        
        new_files = []
        for filepath, receipt in pending_data:
            new_files.append(filepath)
            self.pending_records.append((filepath, receipt))
            
        logger.info(f"[SQS-DuckDB] Found {len(new_files)} pending files to process.")
        return new_files

    def commit_checkpoint(self) -> None:
        if not self.conn: return
            
        logger.info(f"[SQS-DuckDB] Committing {len(self.pending_records)} successful files to state.")
        for filepath, receipt in self.pending_records:
            # Move from pending to processed
            self.conn.execute("INSERT OR IGNORE INTO processed_files VALUES (?)", (filepath,))
            self.conn.execute("DELETE FROM pending_files WHERE filepath = ?", (filepath,))
            
            # ACK SQS
            try:
                self.sqs.delete_message(QueueUrl=self.sqs_url, ReceiptHandle=receipt)
            except Exception as e:
                logger.error(f"Failed to ACK message for {filepath}: {e}")
                
        self.pending_records = []
        self.conn.close()
        self.conn = None
        
        # 4. Upload updated DuckDB file back to S3
        state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
        self.s3.upload_file(self.local_db_path, state_bucket, state_key)
        logger.info(f"[SQS-DuckDB] Uploaded DuckDB state back to S3: {self.checkpoint_location}")
