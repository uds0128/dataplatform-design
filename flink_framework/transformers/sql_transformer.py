import logging
from pyflink.table import Table
from pyflink.table.expressions import expr
from flink_framework.core.interfaces import Transformer

logger = logging.getLogger(__name__)

class SQLTransformer(Transformer):
    """
    Transforms a Flink Table using dynamic SQL expressions.
    """
    def transform(self, table: Table) -> Table:
        columns = self.config.get("columns", {})
        if not columns:
            return table
            
        select_exprs = [expr(f"{expression} AS {alias}") for alias, expression in columns.items()]
        logger.info(f"Applying Flink Table Transformation with {len(columns)} columns.")
        
        return table.select(*select_exprs)
