from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


def write_bronze_stream(
    df: DataFrame,
    table_name: str,
    checkpoint_location: str
) -> StreamingQuery:
    """
    Write streaming DataFrame to Delta bronze table using Auto Loader checkpointing.
    Uses availableNow=True trigger to process all available new files and terminate cleanly.
    """
    query = (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_location)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(table_name)
    )

    query.awaitTermination()
    return query

