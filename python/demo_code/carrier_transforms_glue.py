from awsglue import DynamicFrame
import flight_utilities as futil
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame
from awsglue.transforms import Filter, Map, SelectFields
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext


def add_effective_year_range(record):
    try:
        record['effective_start_year'], record['effective_end_year'] = futil.get_year_range(record['description'])
        # if record['effective_start_year'] == None:
        #     record['effective_start_year'] = 1900
        # if record['effective_end_year'] == None:
        #     record['effective_end_year'] = 9999
    except:
        record['effective_start_year'], record['effective_end_year'] = [-1, -1]
    
    return record


def processYearRange(carrier_df: DataFrame, glue_ctx: GlueContext) -> DataFrame:
    dyf_carrier = DynamicFrame.fromDF(carrier_df, glue_ctx, 'Carriers')
    
    dyf_carrier_with_years = Map.apply(dyf_carrier, f = add_effective_year_range)

    return SelectFields.apply(dyf_carrier_with_years,
        paths=['code', 'description', 'effective_start_year', 'effective_end_year']
        ).toDF()