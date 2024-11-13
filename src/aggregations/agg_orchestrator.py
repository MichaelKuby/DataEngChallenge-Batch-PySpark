from .agg_functions import get_artworks_per_country, get_number_of_artists_per_country, \
    get_avg_h_w_l_per_country, unique_list_of_constituent_ids_per_country
from .agg_helpers import filter_for_valid_countries


def aggregation_transformations(df, spark):
    # filter out invalid countries and cache the result
    df_valid_countries = filter_for_valid_countries(df, spark)
    df_valid_countries = df_valid_countries.repartition(8)

    # use the cached dataframe to perform the aggregations
    df_individual_artworks_per_country = get_artworks_per_country(df_valid_countries)
    df_number_of_artists_per_country = get_number_of_artists_per_country(df_valid_countries)
    df_avg_h_w_l_per_country = get_avg_h_w_l_per_country(df_valid_countries)
    df_constituent_ids_per_country = unique_list_of_constituent_ids_per_country(df_valid_countries)

    return [df_individual_artworks_per_country, df_number_of_artists_per_country, df_avg_h_w_l_per_country, df_constituent_ids_per_country]
