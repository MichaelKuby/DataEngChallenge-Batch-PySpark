from pyspark.sql import functions as F


def get_artworks_per_country(df):
    country_count_df = df.groupBy("Country").count()
    return country_count_df


def get_number_of_artists_per_country(df):
    df = df.where(F.col("Constituent ID").isNotNull())
    country_artist_count_df = df.groupBy("Country").agg(
        F.countDistinct("Constituent ID").alias("Number Of Artists")
    )
    return country_artist_count_df


def get_avg_h_w_l_per_country(df):
    avg_h_w_l_df = df.groupBy("Country").agg(
        F.avg("Height").alias("avg_height"),
        F.avg("Width").alias("avg_width"),
        F.avg("Length").alias("avg_length"),
    )
    return avg_h_w_l_df


def unique_list_of_constituent_ids_per_country(df):
    unique_constituent_ids_df = df.groupBy("Country").agg(
        F.collect_set("Constituent ID").alias("unique_constituent_ids")
    )
    return unique_constituent_ids_df
