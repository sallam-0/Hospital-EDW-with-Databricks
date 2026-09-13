from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_quarantine_metadata(
    df: DataFrame,
    dq_run_id: str,
    rule_id: str,
    reason: str,
    severity: str
) -> DataFrame:

    return (
        df
        .withColumn(
            "_quarantine_timestamp",
            F.current_timestamp()
        )
        .withColumn(
            "_quarantine_reason",
            F.lit(reason)
        )
        .withColumn(
            "_quarantine_rule_id",
            F.lit(rule_id)
        )
        .withColumn(
            "_quarantine_severity",
            F.lit(severity)
        )
        .withColumn(
            "_dq_run_id",
            F.lit(dq_run_id)
        )
    )

def write_quarantine(
    df: DataFrame,
    table_name: str
):

    target_table = (
        f"hospital_catalog.quarantine.{table_name}"
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(target_table)
    )