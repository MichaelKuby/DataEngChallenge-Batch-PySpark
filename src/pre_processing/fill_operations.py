from pyspark.sql import Window
from pyspark.sql.functions import first, last

def back_fill(df):
    window = Window.partitionBy('Title').rowsBetween(Window.unboundedPreceding, Window.currentRow)
    df_back_filled = df.withColumn('Constituent ID', first(col='Constituent ID', ignorenulls=True).over(window))
    return df_back_filled


def forward_fill(df):
    window = Window.partitionBy('Title').rowsBetween(Window.currentRow, Window.unboundedFollowing)
    df_forward_filled = df.withColumn('Constituent ID', last(col='Constituent ID', ignorenulls=True).over(window))
    return df_forward_filled


def b_fill_and_f_fill_constituent_id_column(df):
    df_back_filled = back_fill(df)
    result_df = forward_fill(df_back_filled)
    return result_df