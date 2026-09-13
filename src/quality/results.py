from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType,
)

from config.quality_config import DQ_RESULTS_TABLE


DQ_RESULTS_SCHEMA = StructType([
    StructField("dq_run_id", StringType(), False),
    StructField("dq_timestamp", TimestampType(), True),
    StructField("table_name", StringType(), False),
    StructField("source_branch", StringType(), True),
    StructField("source_system", StringType(), True),
    StructField("rule_id", StringType(), False),
    StructField("rule_name", StringType(), False),
    StructField("dq_category", StringType(), False),
    StructField("severity", StringType(), False),
    StructField("column_name", StringType(), True),
    StructField("total_records", LongType(), False),
    StructField("failed_records", LongType(), False),
    StructField("passed_records", LongType(), False),
    StructField("failure_rate", DoubleType(), True),
    StructField("status", StringType(), False),
    StructField("rule_description", StringType(), True),
])


def write_dq_results(
    spark,
    results: list
):

    if not results:
        return

    result_df = spark.createDataFrame(
        results,
        schema=DQ_RESULTS_SCHEMA
    )

    (
        result_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(DQ_RESULTS_TABLE)
    )

