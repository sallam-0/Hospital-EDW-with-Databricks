# src/identity/writer.py

"""Generic Delta table utilities for identity resolution."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def table_exists(spark, table_name):
    """Check if a Delta table exists in the catalog."""
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        try:
            spark.table(table_name).limit(1).count()
            return True
        except Exception:
            return False


def upsert_delta(spark, table_name, updates_df, merge_key, update_columns=None):
    """
    MERGE (upsert) into a Delta table.

    If ``update_columns`` is provided only those columns are updated on match;
    otherwise all columns are updated.  Non-matching rows are always inserted.
    """

    if not table_exists(spark, table_name):
        (
            updates_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        return "initial_load"

    delta = DeltaTable.forName(spark, table_name)
    merge_cond = f"target.{merge_key} = source.{merge_key}"

    builder = delta.alias("target").merge(updates_df.alias("source"), merge_cond)

    if update_columns:
        builder = builder.whenMatchedUpdate(
            set={c: F.col(f"source.{c}") for c in update_columns}
        )
    else:
        builder = builder.whenMatchedUpdateAll()

    builder.whenNotMatchedInsertAll().execute()
    return "incremental"


def optimize_zorder(spark, table_name, columns):
    """Run OPTIMIZE ... ZORDER BY on a Delta table (best-effort)."""
    if table_exists(spark, table_name):
        try:
            cols = ", ".join(columns)
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({cols})")
        except Exception as e:
            print(f"Notice: OPTIMIZE on {table_name} skipped: {e}")
