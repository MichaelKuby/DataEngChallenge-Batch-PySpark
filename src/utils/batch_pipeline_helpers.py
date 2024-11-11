import os


def get_data_dir(base_dir):
    return os.path.join(base_dir, "../data")


def write_corrupt_records_for_future_processing(corrupt_records, corrupt_records_dir):
    corrupt_records_path = os.path.join(corrupt_records_dir, 'corrupt_records.csv')
    if not os.path.exists(corrupt_records_dir):
        os.makedirs(corrupt_records_dir)
    corrupt_records.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(corrupt_records_path)
