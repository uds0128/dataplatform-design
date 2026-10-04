import logging
from typing import Tuple
from pyflink.table import Table
from pyflink.table.expressions import expr
from flink_framework.core.interfaces import DataQualityGate

logger = logging.getLogger(__name__)

class SQLRuleDQGate(DataQualityGate):
    """
    Evaluates DQ rules using Flink SQL expressions. Splits stream into Good and Bad.
    """
    def validate(self, table: Table) -> Tuple[Table, Table]:
        rules = self.config.get("rules", {})
        if not rules:
            return table, None
            
        logger.info(f"Applying {len(rules)} Flink DQ Rules: {list(rules.keys())}")
        
        # Combine all rule expressions with AND
        rule_expressions = list(rules.values())
        combined_condition = " AND ".join(f"({e})" for e in rule_expressions)
        
        # Good Table
        good_table = table.filter(expr(combined_condition))
        
        # Bad Table (For metadata tracking in Flink, we could add columns via add_columns, 
        # but for simplicity in this demo we just invert the filter)
        bad_table = table.filter(expr(f"NOT ({combined_condition})"))
        
        return good_table, bad_table
