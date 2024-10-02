import os

from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ShortType, LongType

from src.utils.local import get_spark_session


def aggregation_transformations(df):
    pass


def process_constituent_id_column(df):
    pass


def process_country_column(df):
    pass


def process_dimension_column(df):
    # show the first 100 records
    df.select('Dimensions').show(100, truncate=False)


def pre_processing_transformations(df):
    process_dimension_column(df)
    process_country_column(df)
    process_constituent_id_column(df)


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


def get_met_objects_schema():
    schema = StructType([
        StructField(name="Object Number", dataType=StringType(), nullable=True),
        StructField(name="Is Highlight", dataType=BooleanType(), nullable=True),
        StructField(name="Is Timeline Work", dataType=BooleanType(), nullable=True),
        StructField(name="Is Public Domain", dataType=BooleanType(), nullable=True),
        StructField(name="Object ID", dataType=IntegerType(), nullable=True),
        StructField(name="Gallery Number", dataType=IntegerType(), nullable=True),
        StructField(name="Department", dataType=StringType(), nullable=True),
        StructField(name="AccessionYear", dataType=ShortType(), nullable=True),
        StructField(name="Object Name", dataType=StringType(), nullable=True),
        StructField(name="Title", dataType=StringType(), nullable=True),
        StructField(name="Culture", dataType=StringType(), nullable=True),
        StructField(name="Period", dataType=StringType(), nullable=True),
        StructField(name="Dynasty", dataType=StringType(), nullable=True),
        StructField(name="Reign", dataType=StringType(), nullable=True),
        StructField(name="Portfolio", dataType=StringType(), nullable=True),
        StructField(name="Constituent ID", dataType=LongType(), nullable=True),
        StructField(name="Artist Role", dataType=StringType(), nullable=True),
        StructField(name="Artist Prefix", dataType=StringType(), nullable=True),
        StructField(name="Artist Display Name", dataType=StringType(), nullable=True),
        StructField(name="Artist Display Bio", dataType=StringType(), nullable=True),
        StructField(name="Artist Suffix", dataType=StringType(), nullable=True),
        StructField(name="Artist Alpha Sort", dataType=StringType(), nullable=True),
        StructField(name="Artist Nationality", dataType=StringType(), nullable=True),
        StructField(name="Artist Begin Date", dataType=StringType(), nullable=True),  # Might be converted to DateType
        StructField(name="Artist End Date", dataType=StringType(), nullable=True),  # Might be converted to DateType
        StructField(name="Artist Gender", dataType=StringType(), nullable=True),
        StructField(name="Artist ULAN URL", dataType=StringType(), nullable=True),
        StructField(name="Artist Wikidata URL", dataType=StringType(), nullable=True),
        StructField(name="Object Date", dataType=StringType(), nullable=True),
        StructField(name="Object Begin Date", dataType=StringType(), nullable=True),  # Might be converted to DateType
        StructField(name="Object End Date", dataType=StringType(), nullable=True),  # Might be converted to DateType
        StructField(name="Medium", dataType=StringType(), nullable=True),
        StructField(name="Dimensions", dataType=StringType(), nullable=True),
        StructField(name="Credit Line", dataType=StringType(), nullable=True),
        StructField(name="Geography Type", dataType=StringType(), nullable=True),
        StructField(name="City", dataType=StringType(), nullable=True),
        StructField(name="State", dataType=StringType(), nullable=True),
        StructField(name="County", dataType=StringType(), nullable=True),
        StructField(name="Country", dataType=StringType(), nullable=True),
        StructField(name="Region", dataType=StringType(), nullable=True),
        StructField(name="Subregion", dataType=StringType(), nullable=True),
        StructField(name="Locale", dataType=StringType(), nullable=True),
        StructField(name="Locus", dataType=StringType(), nullable=True),
        StructField(name="Excavation", dataType=StringType(), nullable=True),
        StructField(name="River", dataType=StringType(), nullable=True),
        StructField(name="Classification", dataType=StringType(), nullable=True),
        StructField(name="Rights and Reproduction", dataType=StringType(), nullable=True),
        StructField(name="Link Resource", dataType=StringType(), nullable=True),
        StructField(name="Object Wikidata URL", dataType=StringType(), nullable=True),
        StructField(name="Metadata Date", dataType=StringType(), nullable=True),
        StructField(name="Repository", dataType=StringType(), nullable=True),
        StructField(name="Tags", dataType=StringType(), nullable=True),
        StructField(name="Tags AAT URL", dataType=StringType(), nullable=True),
        StructField(name="Tags Wikidata URL", dataType=StringType(), nullable=True)
    ])

    return schema


def main(spark):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = get_data_dir(base_dir=base_dir)
    corrupt_records_dir = os.path.join(data_dir, 'corrupt_records')
    met_objects_filename = os.path.join(data_dir, 'MetObjects.csv')

    met_objects_raw_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("mode", "permissive") \
        .option("columnNameOfCorruptRecord", "corrupt_record") \
        .schema(get_met_objects_schema()) \
        .load(met_objects_filename)

    if 'corrupt_record' in met_objects_raw_df.columns:
        met_objects_df = met_objects_raw_df.filter(met_objects_raw_df['corrupt_record'].isNull())
        corrupt_records = met_objects_raw_df.filter(met_objects_raw_df['corrupt_record'].isNotNull())
        write_corrupt_records_for_future_processing(corrupt_records, corrupt_records_dir)
    else:
        met_objects_df = met_objects_raw_df

    pre_processing_transformations(df=met_objects_df)
    aggregation_transformations(df=met_objects_df)


if __name__ == "__main__":
    main(get_spark_session('DataEng_Challenge_Batch_PySpark'))
