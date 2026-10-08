from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate_latest(
    df,
    partition_columns,
    order_column
):

    window = (
        Window
        .partitionBy(*partition_columns)
        .orderBy(
            F.col(order_column).desc()
        )
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window)
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )


def remove_exact_duplicates(df):
    """
    Removes completely identical rows.
    """

    return df.dropDuplicates()