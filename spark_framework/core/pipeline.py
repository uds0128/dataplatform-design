import logging
from typing import Dict, Any
from pyspark.sql import SparkSession
from spark_framework.core.factory import ComponentFactory

logger = logging.getLogger(__name__)

class IngestionPipeline:
    """The central engine orchestrating the ETL flow based on YAML config."""
    def __init__(self, spark: SparkSession, yaml_config: Dict[str, Any]):
        self.spark = spark
        self.config = yaml_config
        
        ext_cfg = self.config["extractor"]
        chk_cfg = ext_cfg.get("checkpoint_strategy")
        
        self.checkpoint = ComponentFactory.create(chk_cfg["class"], chk_cfg) if chk_cfg else None
        self.extractor = ComponentFactory.create(ext_cfg["class"], spark, ext_cfg, self.checkpoint)
        
        tx_cfg = self.config.get("transformer")
        self.transformer = ComponentFactory.create(tx_cfg["class"], spark, tx_cfg) if tx_cfg else None
        
        dq_cfg = self.config.get("data_quality")
        self.dq_gate = ComponentFactory.create(dq_cfg["class"], spark, dq_cfg) if dq_cfg else None
        
        self.main_sink = ComponentFactory.create(self.config["sink"]["class"], spark, self.config["sink"])
        
        dlq_cfg = self.config.get("dlq_sink")
        self.dlq_sink = ComponentFactory.create(dlq_cfg["class"], spark, dlq_cfg) if dlq_cfg else None

    def _process_microbatch(self, df):
        """Core logic to transform, split, and sink a batch DataFrame."""
        if self.transformer:
            df = self.transformer.transform(df)
            
        good_df, bad_df = df, None
        if self.dq_gate:
            good_df, bad_df = self.dq_gate.validate(df)
            
        if good_df and not good_df.isEmpty():
            self.main_sink.write(good_df)
            
        if self.dlq_sink and bad_df and not bad_df.isEmpty():
            self.dlq_sink.write(bad_df)

    def run(self):
        logger.info("Starting Ingestion Pipeline...")
        
        # Extractor could return a Batch DF or a Streaming DF
        df = self.extractor.extract()
        
        if df.isStreaming:
            logger.info("Detected Native Streaming DataFrame. Routing to writeStream.foreachBatch...")
            
            def foreach_batch_func(batch_df, batch_id):
                logger.info(f"Processing Micro-Batch ID: {batch_id}")
                batch_df.persist()
                self._process_microbatch(batch_df)
                batch_df.unpersist()
                
            native_chk_loc = self.config.get("native_checkpoint_location", "/tmp/spark_native_chk")
            
            query = df.writeStream \
                .foreachBatch(foreach_batch_func) \
                .option("checkpointLocation", native_chk_loc) \
                .trigger(availableNow=True) \
                .start()
                
            query.awaitTermination()
            
        else:
            logger.info("Detected Batch DataFrame. Executing standard flow...")
            self._process_microbatch(df)
            
            if self.checkpoint:
                self.checkpoint.commit_checkpoint()
                
        logger.info("Pipeline Execution Complete.")
