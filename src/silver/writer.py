from delta.tables import DeltaTable

from pyspark.sql import functions as F


def _table_exists(spark, table_name):
    """Check if a Delta table exists in the catalog."""
    return spark.catalog.tableExists(table_name)


def _get_watermark(spark, silver_table):
    """
    Get the high-watermark (_ingestion_timestamp) from the silver table.
    Returns None if the table doesn't exist or is empty.
    """

    if not _table_exists(spark, silver_table):
        return None

    row = (
        spark.table(silver_table)
        .select(F.max("_ingestion_timestamp").alias("max_ts"))
        .first()
    )

    return row["max_ts"] if row else None


def _read_incremental_bronze(spark, bronze_table, watermark):
    """
    Read from bronze, filtered to only new rows after the watermark.
    If watermark is None, reads the entire bronze table (initial load).
    """

    df = spark.table(bronze_table)

    if watermark is not None:
        df = df.filter(
            F.col("_ingestion_timestamp") > watermark
        )

    return df


def _delta_merge(spark, silver_table, updates_df, primary_key):
    """
    MERGE (upsert) updates into the silver Delta table using the primary key.

    - Matching rows are updated with the new values.
    - Non-matching rows are inserted.
    """

    silver_delta = DeltaTable.forName(spark, silver_table)

    merge_condition = f"target.{primary_key} = source.{primary_key}"

    (
        silver_delta.alias("target")
        .merge(
            updates_df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_to_silver(
    spark,
    bronze_table,
    silver_table,
    primary_key,
    transform_fn,
    deduplicate_fn,
    dedup_partition_cols,
    dedup_order_col
):
    """
    Incremental silver layer pipeline:

    1. Read the high-watermark from the existing silver table
    2. Read only new rows from bronze (after the watermark)
    3. Apply the transform function (cleaning, standardization, type casting)
    4. Deduplicate (keep latest per primary key)
    5. MERGE (upsert) into the silver Delta table

    Parameters
    ----------
    spark : SparkSession
    bronze_table : str
        Fully qualified bronze table name (e.g. "hospital_catalog.bronze.patients")
    silver_table : str
        Fully qualified silver table name (e.g. "hospital_catalog.silver.patients")
    primary_key : str
        Column name used as the merge key
    transform_fn : callable
        Function that takes a DataFrame and returns a transformed DataFrame
    deduplicate_fn : callable
        Function that takes (df, partition_cols, order_col) and returns deduplicated DataFrame
    dedup_partition_cols : list[str]
        Columns to partition by for deduplication
    dedup_order_col : str
        Column to order by for deduplication (latest wins)

    Returns
    -------
    dict with keys:
        - "status": "initial_load" or "incremental" or "no_new_data"
        - "rows_processed": int
    """

    # 1. Get watermark
    watermark = _get_watermark(spark, silver_table)

    # 2. Read incremental data
    bronze_df = _read_incremental_bronze(spark, bronze_table, watermark)

    # Check if there's new data
    if bronze_df.head(1) == []:
        print(f"No new data found in {bronze_table}. Skipping.")
        return {"status": "no_new_data", "rows_processed": 0}

    # 3. Transform
    transformed_df = transform_fn(bronze_df)

    # 4. Deduplicate
    deduped_df = deduplicate_fn(
        transformed_df,
        dedup_partition_cols,
        dedup_order_col
    )

    row_count = deduped_df.count()

    # 5. Write to silver
    if not _table_exists(spark, silver_table):

        # Initial load — create the table
        (
            deduped_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(silver_table)
        )

        status = "initial_load"
        print(f"Initial load: wrote {row_count} rows to {silver_table}")

    else:

        # Incremental — MERGE into existing table
        _delta_merge(spark, silver_table, deduped_df, primary_key)

        status = "incremental"
        print(f"Incremental merge: processed {row_count} rows into {silver_table}")

    return {"status": status, "rows_processed": row_count}
