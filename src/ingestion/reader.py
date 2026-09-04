from pyspark.sql import DataFrame



def read_csv(spark, path:str) -> DataFrame:

    """

     read the csv files from the given path

    """

    return(
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferschema","false")
        .option("mode","PERMISSIVE")
        .option("quote",'"')
        .option("escape",'"')
        .load(path)
    )
        
        