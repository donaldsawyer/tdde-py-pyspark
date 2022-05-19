import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
import logging

def quiet_py4j():
    """Suppress spark logging for the test context."""
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.INFO)


@pytest.fixture(scope="session")
def spark_session(request) -> SparkSession:
    """Fixture for creating a spark context."""

    spark = (SparkSession
             .builder
             .master('local[*]')
             .appName("pytest spark_session")
             .enableHiveSupport()
             .getOrCreate())
    request.addfinalizer(lambda: spark.stop())

    quiet_py4j()

    return spark