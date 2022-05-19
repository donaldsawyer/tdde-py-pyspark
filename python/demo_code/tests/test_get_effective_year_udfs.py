import carrier_transforms as ct
import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from cmath import nan
from pytest_fixtures import spark_session


'''
This test demonstrates using a dataframe to test using a data-based test, similar to Spock.

The data and expected results are in the input DataFrame, and the actual results will be appended as a column.
The challenge here is that if there is an assertion error, what is the failed test case?
'''
@pytest.mark.skip()
def test_get_start_year_valid_ranges(spark_session):
    # given
    sut_df: DataFrame = spark_session.createDataFrame(
        [
            (2016, 'Carrier (2016 - 2020)'),
            (2016,'Carrier (2016 - )'),
            (None, 'Carrier ( - 2016)'),
            (2010, 'Carrier (carrier) (2010 - 2016)'),
            (None, 'Carrier ( - )')
            ],
        ["expected", "input_str"]
    )

    # when
    result_df = sut_df.\
        withColumn("actual", ct.get_start_year(col("input_str"))).\
        select('expected', 'actual')

    #then
    failure_df = result_df.where(col("actual") != col("expected"))
    assert(failure_df.rdd.isEmpty())

@pytest.mark.skip()
def test_get_start_year_invalid_formats(spark_session):
    # given
    sut_df: DataFrame = spark_session.createDataFrame(
        [
            (-1, 'Carrier'),
            (-1, 'Carrier (2016 - 2022'),
            (-1, 'Carrier 2016 - 2022)')
            ],
        ["expected", "input_str"])

    # when
    result_df = sut_df.\
        withColumn("actual", ct.get_start_year(col("input_str"))).\
        select('expected', 'actual')

    #then
    failure_df = result_df.where(col("actual") != col("expected"))
    assert(failure_df.rdd.isEmpty())

@pytest.mark.skip()
def test_get_end_year_all_formats(spark_session):
    # given
    sut_df: DataFrame = spark_session.createDataFrame(
        [
            (2020, 'Carrier (2016 - 2020)'),
            (None,'Carrier (2016 - )'),
            (2016, 'Carrier ( - 2016)'),
            (2016, 'Carrier (carrier) (2010 - 2016)'),
            (None, 'Carrier ( - )'),
            (-1, 'Carrier'),
            (-1, 'Carrier (2016 - 2022'),
            (-1, 'Carrier 2016 - 2022)')
            ],
        ["expected", "input_str"])

    # when
    result_df = sut_df.\
        withColumn("actual", ct.get_end_year(col("input_str"))).\
        select('expected', 'actual')

    #then
    failure_df = result_df.where(col("actual") != col("expected"))
    assert(failure_df.rdd.isEmpty())