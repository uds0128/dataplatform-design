import logging
import boto3
from typing import List
from flink_framework.core.interfaces import CheckpointStrategy

logger = logging.getLogger(__name__)

class LexicalCheckpointStrategy(CheckpointStrategy):
    """
    Downloads state from S3, reads the last file, discovers new files using StartAfter,
    and uploads the new state back to S3 upon commit.
    """
    def __init__(self, config):
        super().__init__(config)
        self.checkpoint_location = self.config.get("checkpoint_location")
        self.local_state_file = "/tmp/flink_lexical_state.txt"
        self.pending_checkpoint = None
        self.s3 = boto3.client('s3', region_name='us-east-1')

    def _parse_s3_path(self, path: str):
        bucket = path.replace("s3://", "").split("/")[0]
        key = "/".join(path.replace("s3://", "").split("/")[1:])
        return bucket, key

    def discover_new_files(self, base_path: str) -> List[str]:
        if not self.checkpoint_location:
            raise ValueError("Lexical Strategy requires 'checkpoint_location' in config")

        state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
        last_file_key = None
        try:
            self.s3.download_file(state_bucket, state_key, self.local_state_file)
            with open(self.local_state_file, 'r') as f:
                last_file_key = f.read().strip()
            logger.info(f"[Flink Lexical Strategy] State loaded from S3: {last_file_key}")
        except Exception:
            logger.info("[Flink Lexical Strategy] No existing state found in S3. Starting fresh.")

        bucket, prefix = self._parse_s3_path(base_path)
        logger.info(f"[Flink Lexical Strategy] Scanning s3://{bucket}/{prefix} starting after {last_file_key}")
        
        list_kwargs = {'Bucket': bucket, 'Prefix': prefix}
        if last_file_key:
            list_kwargs['StartAfter'] = last_file_key
            
        try:
            response = self.s3.list_objects_v2(**list_kwargs)
        except Exception:
            return [] # Mocking for demo
            
        if 'Contents' not in response or len(response['Contents']) == 0:
            return []
            
        new_files = [f"s3a://{bucket}/{obj['Key']}" for obj in response['Contents']]
        self.pending_checkpoint = response['Contents'][-1]['Key']
        
        return new_files

    def commit_checkpoint(self) -> None:
        if self.pending_checkpoint:
            with open(self.local_state_file, 'w') as f:
                f.write(self.pending_checkpoint)
                
            state_bucket, state_key = self._parse_s3_path(self.checkpoint_location)
            self.s3.upload_file(self.local_state_file, state_bucket, state_key)
            logger.info(f"[Flink Lexical Strategy] Committed state {self.pending_checkpoint} to S3: {self.checkpoint_location}")
            
        self.pending_checkpoint = None
