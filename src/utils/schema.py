from pyspark.sql.types import StructType, StructField, StringType, BooleanType, IntegerType, ShortType, LongType, \
    FloatType


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

def get_dimensions_schema():
    return StructType([
        StructField(name="Height", dataType=FloatType(), nullable=True),
        StructField(name="Width", dataType=FloatType(), nullable=True),
        StructField(name="Length", dataType=FloatType(), nullable=True),
        StructField(name="Diameter", dataType=FloatType(), nullable=True),
        StructField(name="Unit", dataType=StringType(), nullable=True)
    ])