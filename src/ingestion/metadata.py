from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_ingestion_metadata(
    df:DataFrame,
    branch:str,
    source_system:str,
    batch_id:str
) -> DataFrame:

    """
    add metadata columns to a dataframe 

    """

    return(
        df
        .withColumn("_source_branch", F.lit(branch))

        .withColumn("_source_system",F.lit(source_system))
        
        .withColumn("_source_file", F.col("_metadata.file_path"))
        
        .withColumn("_batch_id", F.lit(batch_id))
        
        .withColumn("_ingestion_timestamp", F.current_timestamp())


    )