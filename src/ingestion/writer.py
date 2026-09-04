from pyspark.sql import DataFrame


def write_bronze(
    df:DataFrame,
    table_name:str
) -> None:

    """
    write data to bronze layer
    """

    (
      df.write

      .format("delta")

      .mode("append")

      .option("mergeSchema","true")

      .saveAsTable(table_name)


    )

