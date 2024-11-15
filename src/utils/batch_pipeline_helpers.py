import os

from src.utils.schema import get_met_objects_schema


def print_each_dataframe(aggregated_dfs_names, output_dir, spark):
    for name in aggregated_dfs_names:
        df = spark.read.format("parquet").load(f"{output_dir}/{name}.parquet")
        print(f"First 30 rows of {name}:")
        df.show(30)


def write_dataframes_to_file(dataframes, output_dir, names):
    for dataframe, name in zip(dataframes, names):
        output_path = f"{output_dir}/{name}.parquet"
        dataframe.write.format("parquet").mode("overwrite").save(output_path)


def get_data_dir(base_dir):
    return os.path.join(base_dir, "../data")


def write_corrupt_records_for_future_processing(corrupt_records, corrupt_records_dir):
    corrupt_records_path = os.path.join(corrupt_records_dir, "corrupt_records.parquet")
    if not os.path.exists(corrupt_records_dir):
        os.makedirs(corrupt_records_dir)
    corrupt_records.write.format("parquet").mode("overwrite").save(corrupt_records_path)


def check_for_corrupt_records(corrupt_records_dir, df):
    # Check if there are corrupt records in the dataset. If so, filter them out and write them to a separate file.
    if "corrupt_record" in df.columns:
        met_objects_df = df.filter(df["corrupt_record"].isNull())
        corrupt_records = df.filter(df["corrupt_record"].isNotNull())
        write_corrupt_records_for_future_processing(
            corrupt_records, corrupt_records_dir
        )
    else:
        met_objects_df = df
    return met_objects_df


def read_source_data(met_objects_filename, spark):
    # Use permissive mode to handle corrupt records. columnNameOfCorruptRecord is used to store corrupt records.
    met_objects_raw_df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "permissive")
        .option("columnNameOfCorruptRecord", "corrupt_record")
        .schema(get_met_objects_schema())
        .load(met_objects_filename)
    )
    return met_objects_raw_df


def get_directories():
    base_dir = os.path.dirname(os.path.abspath(__file__ + "/.."))
    data_dir = get_data_dir(base_dir=base_dir)
    input_dir = os.path.join(data_dir, "input")
    output_dir = os.path.join(data_dir, "output")
    corrupt_records_dir = os.path.join(data_dir, "corrupt_records")
    met_objects_filename = os.path.join(input_dir, "MetObjects.csv")
    return data_dir, input_dir, output_dir, corrupt_records_dir, met_objects_filename
