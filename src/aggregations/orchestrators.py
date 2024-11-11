from pyspark.sql.functions import explode


def aggregation_transformations(df):
    # Group by Country and count the number of objects in each country
    df_country_non_null = df.filter(df['Country_split'].isNotNull())

    # Explode the array of countries to get a row for each country
    df_country_exploded = df_country_non_null.withColumn('Country', explode(df_country_non_null['Country_split']))

    country_count_df = df_country_non_null.groupBy('Country').count()

    df_country_and_artist_non_null = df.where(df['Country'].isNotNull() & df['Constituent ID'].isNotNull())

    return None