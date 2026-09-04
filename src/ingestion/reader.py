from pyspark.sql import DataFrame



def read_csv_autoloader(
    spark,
    path: str,
    schema_location: str
) -> DataFrame:
    """
    Read CSV files incrementally using Databricks Auto Loader (cloudFiles).
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("quote", '"')
        .option("escape", '"')
        .load(path)
    )
        
        