from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple
from pyflink.table import TableEnvironment, Table, StatementSet

class CheckpointStrategy(ABC):
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def discover_new_files(self, base_path: str) -> list:
        pass

    @abstractmethod
    def commit_checkpoint(self) -> None:
        pass

class Extractor(ABC):
    def __init__(self, t_env: TableEnvironment, config: Dict[str, Any], checkpoint_strategy: CheckpointStrategy = None):
        self.t_env = t_env
        self.config = config
        self.checkpoint_strategy = checkpoint_strategy

    @abstractmethod
    def extract(self) -> Table:
        pass

class Transformer(ABC):
    def __init__(self, t_env: TableEnvironment, config: Dict[str, Any]):
        self.t_env = t_env
        self.config = config

    @abstractmethod
    def transform(self, table: Table) -> Table:
        pass

class DataQualityGate(ABC):
    def __init__(self, t_env: TableEnvironment, config: Dict[str, Any]):
        self.t_env = t_env
        self.config = config

    @abstractmethod
    def validate(self, table: Table) -> Tuple[Table, Table]:
        pass

class Sink(ABC):
    def __init__(self, t_env: TableEnvironment, config: Dict[str, Any]):
        self.t_env = t_env
        self.config = config

    @abstractmethod
    def add_to_statement_set(self, table: Table, stmt_set: StatementSet) -> None:
        """In Flink, we append sinks to a StatementSet to execute branching logic in one Job Graph."""
        pass
