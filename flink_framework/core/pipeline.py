import logging
from typing import Dict, Any
from pyflink.table import TableEnvironment
from flink_framework.core.factory import ComponentFactory

logger = logging.getLogger(__name__)

class FlinkIngestionPipeline:
    """The central engine orchestrating the Flink Table API flow based on YAML config."""
    def __init__(self, t_env: TableEnvironment, yaml_config: Dict[str, Any]):
        self.t_env = t_env
        self.config = yaml_config
        
        # Instantiate Components Dynamically
        ext_cfg = self.config["extractor"]
        chk_cfg = ext_cfg.get("checkpoint_strategy")
        
        self.checkpoint = ComponentFactory.create(chk_cfg["class"], chk_cfg) if chk_cfg else None
        self.extractor = ComponentFactory.create(ext_cfg["class"], t_env, ext_cfg, self.checkpoint)
        
        tx_cfg = self.config.get("transformer")
        self.transformer = ComponentFactory.create(tx_cfg["class"], t_env, tx_cfg) if tx_cfg else None
        
        dq_cfg = self.config.get("data_quality")
        self.dq_gate = ComponentFactory.create(dq_cfg["class"], t_env, dq_cfg) if dq_cfg else None
        
        self.main_sink = ComponentFactory.create(self.config["sink"]["class"], t_env, self.config["sink"])
        
        dlq_cfg = self.config.get("dlq_sink")
        self.dlq_sink = ComponentFactory.create(dlq_cfg["class"], t_env, dlq_cfg) if dlq_cfg else None

    def run(self):
        logger.info("Building PyFlink Job Graph...")
        
        # E: Extract
        table = self.extractor.extract()
        
        # T: Transform
        if self.transformer:
            table = self.transformer.transform(table)
            
        # DQ: Route Good/Bad Data
        good_table, bad_table = table, None
        if self.dq_gate:
            good_table, bad_table = self.dq_gate.validate(table)
            
        # Flink requires a StatementSet to execute multiple sinks (branching) in a single job
        stmt_set = self.t_env.create_statement_set()
        
        # L: Load Main (Good Data)
        if good_table is not None:
            self.main_sink.add_to_statement_set(good_table, stmt_set)
            
        # DLQ: Load Bad Data
        if self.dlq_sink and bad_table is not None:
            self.dlq_sink.add_to_statement_set(bad_table, stmt_set)
            
        logger.info("Executing PyFlink StatementSet...")
        # Submits the job to the cluster.
        stmt_set.execute()
        
        # If we are managing manual state (e.g. DuckDB WAL), commit it now
        if self.checkpoint:
            self.checkpoint.commit_checkpoint()
