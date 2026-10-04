from setuptools import setup, find_packages

setup(
    name="spark_ingestion_framework",
    version="0.1.0",
    description="Config-driven PySpark Lakehouse Ingestion Framework",
    author="Data Platform Team",
    packages=find_packages(include=["core", "extractors", "transformers", "dq", "sinks", "strategies"]),
    install_requires=[
        "pyspark>=3.3.0",
        "boto3>=1.26.0",
        "PyYAML>=6.0"
    ],
)
