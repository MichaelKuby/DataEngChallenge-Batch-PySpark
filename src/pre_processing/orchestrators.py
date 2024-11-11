from pyspark.sql.functions import udf

from src.pre_processing.fill_operations import b_fill_and_f_fill_constituent_id_column
from src.pre_processing.parse_dimension import parse_dimensions
from src.pre_processing.process_country_column import process_country_column
from src.utils.schema import get_dimensions_schema


def process_dimension_column(df):
    parse_dimensions_udf = udf(parse_dimensions, get_dimensions_schema())
    df_with_dimensions_parsed = df.withColumn('Dimensions', parse_dimensions_udf(df['Dimensions']))
    result_df = df_with_dimensions_parsed.select('*', 'Dimensions.*')
    return result_df


def pre_processing_transformations(df):
    dim_processed_df = process_dimension_column(df)
    dim_and_country_processed_df = process_country_column(dim_processed_df)
    result_df = b_fill_and_f_fill_constituent_id_column(dim_and_country_processed_df)
    return result_df