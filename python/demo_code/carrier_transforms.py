from array import ArrayType
from cmath import nan
import flight_utilities as futil
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType, StructField, StructType, ArrayType
from pyspark.sql import DataFrame

def get_year_range_no_errors(s) -> int:
    try:
        res = futil.get_year_range(s)
    except:
        res = [-1, -1]

    return res

get_start_year = udf(lambda s: get_year_range_no_errors(s)[0], IntegerType())
get_end_year = udf(lambda s: get_year_range_no_errors(s)[1], IntegerType())


def processYearRange(carrier_df: DataFrame):
    df = carrier_df.\
        withColumn('effective_start_year', get_start_year(col('description'))). \
        withColumn('effective_end_year', get_end_year(col('description'))). \
        select('code', 'description', 'effective_start_year', 'effective_end_year')

    return df