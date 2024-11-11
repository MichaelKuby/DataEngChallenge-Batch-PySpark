import os

from src.aggregations.orchestrators import aggregation_transformations
from src.pre_processing.orchestrators import pre_processing_transformations
from src.utils.batch_pipeline_helpers import get_data_dir, write_corrupt_records_for_future_processing
from src.utils.schema import get_met_objects_schema
from src.utils.spark_session import get_spark_session


def main(spark):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = get_data_dir(base_dir=base_dir)
    corrupt_records_dir = os.path.join(data_dir, 'corrupt_records')
    met_objects_filename = os.path.join(data_dir, 'MetObjects.csv')

    # Use permissive mode to handle corrupt records. columnNameOfCorruptRecord is used to store corrupt records.
    met_objects_raw_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("mode", "permissive") \
        .option("columnNameOfCorruptRecord", "corrupt_record") \
        .schema(get_met_objects_schema()) \
        .load(met_objects_filename)

    # Check if there are corrupt records in the dataset. If so, filter them out and write them to a separate file.
    if 'corrupt_record' in met_objects_raw_df.columns:
        met_objects_df = met_objects_raw_df.filter(met_objects_raw_df['corrupt_record'].isNull())
        corrupt_records = met_objects_raw_df.filter(met_objects_raw_df['corrupt_record'].isNotNull())
        write_corrupt_records_for_future_processing(corrupt_records, corrupt_records_dir)
    else:
        met_objects_df = met_objects_raw_df

    met_objects_pre_processed_df = pre_processing_transformations(df=met_objects_df)
    met_objects_aggregated_df = aggregation_transformations(df=met_objects_pre_processed_df)


if __name__ == "__main__":
    main(get_spark_session('DataEng_Challenge_Batch_PySpark'))
