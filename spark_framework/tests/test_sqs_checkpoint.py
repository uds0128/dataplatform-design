import pytest
import os
import duckdb
from unittest.mock import MagicMock, patch
from spark_framework.strategies.sqs_checkpoint import SQSDuckDBCheckpointStrategy

def test_sqs_idempotency_and_crash_recovery():
    """
    Hardest Test 1: SQS Checkpoint State Machine
    Tests that if a pipeline crashes before commit, the files remain pending in DuckDB.
    Tests that duplicate SQS events do not yield duplicate files.
    """
    config = {
        "sqs_url": "mock_queue",
        "checkpoint_location": None # Disable S3 upload for local test
    }
    
    # Setup mock SQS response with a DUPLICATE event
    mock_sqs_response = {
        'Messages': [
            {'ReceiptHandle': 'r1', 'Body': '{"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "file1.parquet"}}}]}'},
            {'ReceiptHandle': 'r2', 'Body': '{"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "file1.parquet"}}}]}'}
        ]
    }
    
    with patch('boto3.client') as mock_boto:
        mock_sqs = MagicMock()
        mock_sqs.receive_message.return_value = mock_sqs_response
        mock_boto.return_value = mock_sqs
        
        strategy = SQSDuckDBCheckpointStrategy(config)
        strategy.local_db_path = "/tmp/test_duckdb.db"
        if os.path.exists(strategy.local_db_path):
            os.remove(strategy.local_db_path)
            
        # 1. Run discovery (Should perfectly deduplicate the 2 SQS events into 1 file)
        new_files = strategy.discover_new_files("s3://b/")
        assert len(new_files) == 1, "Design Flaw: Failed to deduplicate SQS events!"
        assert new_files[0] == "s3a://b/file1.parquet"
        
        # 2. Simulate Pipeline Crash (Do NOT call commit_checkpoint)
        
        # 3. New Pipeline Run (Next cron schedule spin-up)
        strategy2 = SQSDuckDBCheckpointStrategy(config)
        strategy2.local_db_path = "/tmp/test_duckdb.db"
        
        # Mock empty SQS response on 2nd run (the queue is empty)
        mock_sqs.receive_message.return_value = {} 
        
        # Even though SQS is empty, the DuckDB pending table must yield file1 again!
        new_files_run_2 = strategy2.discover_new_files("s3://b/")
        assert len(new_files_run_2) == 1, "Design Flaw: State lost on crash!"
        assert new_files_run_2[0] == "s3a://b/file1.parquet"
        
        # 4. Simulate Pipeline Success
        strategy2.commit_checkpoint()
        
        # 5. Third Run (Should yield nothing, state is committed)
        strategy3 = SQSDuckDBCheckpointStrategy(config)
        strategy3.local_db_path = "/tmp/test_duckdb.db"
        new_files_run_3 = strategy3.discover_new_files("s3://b/")
        assert len(new_files_run_3) == 0, "Design Flaw: Processed files were re-read!"
