from abc import ABC, abstractmethod
from typing import Dict, Any, Tuple, List
from pyspark.sql import DataFrame, SparkSession

class CheckpointStrategy(ABC):
    """Abstract strategy that handles both file discovery and state management."""
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @abstractmethod
    def discover_new_files(self, base_path: str) -> List[str]:
        """Returns an array of file paths that have not been processed yet."""
        pass

    def get_offsets(self, topic: str) -> Tuple[Dict[str, int], Dict[str, int]]:
        """Used specifically by Kafka Extractors to fetch starting and ending offsets."""
        pass

    @abstractmethod
    def commit_checkpoint(self) -> None:
        """Commits the state (e.g., updates last file seen, or ACKs SQS)."""
        pass

class Extractor(ABC):
    def __init__(self, spark: SparkSession, config: Dict[str, Any], checkpoint_strategy: CheckpointStrategy = None):
        self.spark = spark
        self.config = config
        self.checkpoint_strategy = checkpoint_strategy

    @abstractmethod
    def extract(self) -> DataFrame:
        pass

class Transformer(ABC):
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        pass

class DataQualityGate(ABC):
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config

    @abstractmethod
    def validate(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        pass

class Sink(ABC):
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        pass
