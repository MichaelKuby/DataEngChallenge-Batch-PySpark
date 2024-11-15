from src.aggregations.agg_orchestrator import aggregation_transformations
from src.pre_processing.pre_orchestrator import pre_processing_transformations
from src.utils.batch_pipeline_helpers import (
    write_dataframes_to_file,
    print_each_dataframe,
    read_source_data,
    get_directories,
    check_for_corrupt_records,
)
from src.utils.spark_session import get_spark_session


def main(spark):
    # Get the directories
    data_dir, input_dir, output_dir, corrupt_records_dir, met_objects_filename = (
        get_directories()
    )

    # Read the source data and check for corrupt records
    met_objects_raw_df = read_source_data(
        met_objects_filename=met_objects_filename, spark=spark
    )
    met_objects_df = check_for_corrupt_records(
        corrupt_records_dir=corrupt_records_dir, df=met_objects_raw_df
    )

    # Perform the pre-processing and aggregation transformations
    met_objects_pre_processed_df = pre_processing_transformations(df=met_objects_df)
    aggregated_dfs = aggregation_transformations(
        df=met_objects_pre_processed_df, spark=spark
    )

    # Write the aggregated dataframes to parquet
    aggregated_dfs_names = [
        "individual_artworks_per_country",
        "number_of_artists_per_country",
        "avg_h_w_l_per_country",
        "constituent_ids_per_country",
    ]
    write_dataframes_to_file(
        dataframes=aggregated_dfs, output_dir=output_dir, names=aggregated_dfs_names
    )

    # Print the aggregated dataframes
    print_each_dataframe(
        aggregated_dfs_names=aggregated_dfs_names, output_dir=output_dir, spark=spark
    )

    spark.stop()


if __name__ == "__main__":
    main(get_spark_session("DataEng_Challenge_Batch_PySpark"))
