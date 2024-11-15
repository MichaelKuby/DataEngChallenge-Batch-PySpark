import pycountry
from pyspark.sql.functions import explode, lower, trim, broadcast, initcap


def filter_for_valid_countries(df, spark):
    df = df.where(df["Country"].isNotNull())

    # explode the array of countries in column 'Country' to get a row for each country
    df = df.withColumn("Country", explode(df["Country"]))
    df = df.withColumn("Country", lower(trim(df["Country"])))

    # Use pycountry to filter out invalid countries in a case-insensitive way
    valid_countries_list = [(country.name.lower(),) for country in pycountry.countries]
    valid_countries_df = spark.createDataFrame(valid_countries_list, ["ValidCountry"])

    df_valid_countries = df.join(
        broadcast(valid_countries_df),
        df["Country"] == valid_countries_df["ValidCountry"],
        "inner",
    )
    df_valid_countries = df_valid_countries.withColumn(
        "Country", initcap("ValidCountry")
    ).drop("ValidCountry")

    return df_valid_countries
