from pyspark.sql.functions import regexp_replace, split, array_distinct, array_remove

def process_country_column(df):
    df_processed = (df
                    .withColumn('Country',
                                array_distinct(
                                    array_remove(
                                        split(
                                            regexp_replace('Country', r"(\||\s+or\s+)", ', '),
                                            ', '
                                        ),
                                        ''
                                    )
                                )
                                )
                    )
    return df_processed