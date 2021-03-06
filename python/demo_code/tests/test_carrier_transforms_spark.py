from carrier_transforms import processYearRange
from pytest_fixtures import spark_session
import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import DataFrame


@pytest.fixture(scope="module")
def carrier_input_schema() -> StructType:
    return StructType([
        StructField("code", StringType(), False),
        StructField("description", StringType(), False)
    ])


@pytest.fixture(scope="module")
def carrier_output_schema() -> StructType:
    return StructType([
        StructField("code", StringType(), False),
        StructField("description", StringType(), False),
        StructField("effective_start_year", IntegerType(), True),
        StructField("effective_end_year", IntegerType(), True)
    ])


def assert_dataframes_equal(expected_df: DataFrame, actual_df: DataFrame):
    # ensure no columns added or removed unexpectedly
    assert(expected_df.columns == actual_df.columns)

    # assert number of rows match to ensure no rows are extra/missing
    assert(expected_df.count() == actual_df.count())

    # all rows in the expected_df are in the actual (actual_df >= expected_df)
    assert(expected_df.subtract(actual_df).rdd.isEmpty())

    # all rows in the actual_df are in the expected_df (actual_df <= expected_df)
    assert(actual_df.subtract(expected_df).rdd.isEmpty())


# @pytest.mark.skip()
def test_processYearRange_valid_twoYears(spark_session, carrier_input_schema, carrier_output_schema):
    # given
    ''' INPUT DATAFRAME
    |------|---------------------------------|
    | code | description                     |
    |------|---------------------------------|
    | C01  | Carrier (2016 - 2020)           |
    | C04  | Carrier (carrier) (2010 - 2016) |
    |------|---------------------------------|
    '''
    input_data = [
        ('C01', 'Carrier (2016 - 2020)'),
        ('C04', 'Carrier (carrier) (2010 - 2016)')]
    input_df = spark_session.createDataFrame(data=input_data, schema=carrier_input_schema)

    ''' EXPECTED OUTPUT DATAFRAME
    |------|---------------------------------|----------------------|--------------------|
    | code | description                     | effective_start_year | effective_end_year |
    |------|---------------------------------|----------------------|--------------------|
    | C01  | Carrier (2016 - 2020)           |         2016         |        2020        |
    | C04  | Carrier (carrier) (2010 - 2016) |         2010         |        2016        |
    |------|---------------------------------|----------------------|--------------------|
    '''
    expected_df = spark_session.createDataFrame(
        data=[
            ('C01', 'Carrier (2016 - 2020)', 2016, 2020),
            ('C04', 'Carrier (carrier) (2010 - 2016)', 2010, 2016)
        ], 
        schema=carrier_output_schema)

    # when
    actual_df = processYearRange(input_df)

    # then
    assert_dataframes_equal(expected_df, actual_df)


# @pytest.mark.skip()
def test_processYearRange_valid_missingYears(spark_session, carrier_input_schema, carrier_output_schema):
    # given
    ''' INPUT DATAFRAME
    |------|-------------------|
    | code | description       |
    |------|-------------------|
    | C02  | Carrier (2016 - ) |
    | C03  | Carrier ( - 2016) |
    | C05  | Carrier ( - )     |
    |------|-------------------|
    '''
    input_data = [
        ('C02', 'Carrier (2016 - )'),
        ('C03', 'Carrier ( - 2016)'),
        ('C05', 'Carrier ( - )')
    ]
    input_df = spark_session.createDataFrame(data=input_data, schema=carrier_input_schema)

    ''' EXPECTED OUTPUT DATAFRAME
    |------|-------------------|----------------------|--------------------|
    | code | description       | effective_start_year | effective_end_year |
    |------|-------------------|----------------------|--------------------|
    | C02  | Carrier (2016 - ) |         2016         |        9999        |
    | C03  | Carrier ( - 2016) |         1900         |        2016        |
    | C05  | Carrier ( - )     |         1900         |        9999        |
    |------|-------------------|----------------------|--------------------|
    '''
    expected_df = spark_session.createDataFrame(
        data=[
            ('C02', 'Carrier (2016 - )', 2016, None),
            ('C03', 'Carrier ( - 2016)', None, 2016),
            ('C05', 'Carrier ( - )', None, None)], 
        schema=carrier_output_schema)

    # when
    actual_df = processYearRange(input_df)

    # then
    assert_dataframes_equal(expected_df, actual_df)


# @pytest.mark.skip()
def test_processYearRange_invalid_cannotParse(spark_session, carrier_input_schema, carrier_output_schema):
    # given
    ''' INPUT DATAFRAME
    |------|----------------------|
    | code | description          |
    |------|----------------------|
    | C06  | Carrier              |
    | C07  | Carrier (2016 - 2022 |
    | C07  | Carrier 2016 - 2022) |
    |------|----------------------|
    '''
    input_data = [
        ('C06', 'Carrier'),
        ('C07', 'Carrier (2016 - 2022'),
        ('C08', 'Carrier 2016 - 2022)')
    ]
    input_df = spark_session.createDataFrame(data=input_data, schema=carrier_input_schema)

    ''' EXPECTED OUTPUT DATAFRAME
    |------|----------------------|----------------------|--------------------|
    | code | description          | effective_start_year | effective_end_year |
    |------|----------------------|----------------------|--------------------|
    | C06  | Carrier              |          -1          |         -1         |
    | C07  | Carrier (2016 - 2022 |          -1          |         -1         |
    | C07  | Carrier 2016 - 2022) |          -1          |         -1         |
    |------|----------------------|----------------------|--------------------|
    '''
    expected_df = spark_session.createDataFrame(
        data=[
            ('C06', 'Carrier', -1, -1),
            ('C07', 'Carrier (2016 - 2022', -1, -1),
            ('C08', 'Carrier 2016 - 2022)', -1, -1)], 
        schema=carrier_output_schema)

    # when
    actual_df = processYearRange(input_df)

    # then
    assert_dataframes_equal(expected_df, actual_df)

