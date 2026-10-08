# src/gold/writer.py
"""
Shared write utilities for the gold layer.
All gold tables are Delta tables managed in Unity Catalog.
"""

try:
    from delta.tables import DeltaTable
except ImportError:
    DeltaTable = None

from pyspark.sql import DataFrame, functions as F


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def table_exists(spark, table_name: str) -> bool:
    """Return True when the fully-qualified table exists in the catalog."""
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        try:
            spark.table(table_name).limit(1).count()
            return True
        except Exception:
            return False


def overwrite_table(spark, df: DataFrame, table_name: str) -> int:
    """
    Full overwrite of a gold dimension / reference table.

    Use for:
    - dim_date           (generated, stable)
    - dim_department     (small SCD-1, re-derived on every run)
    - dim_medication     (same)
    - dim_diagnosis_code (derived, always re-derived)
    - dim_payer          (derived, always re-derived)

    Returns row count written.
    """
    count = df.count()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"  ✓ Overwritten : {table_name}  ({count:,} rows)")
    return count


def upsert_table(
    spark,
    df: DataFrame,
    table_name: str,
    merge_key: str,
) -> int:
    """
    MERGE (upsert) into a gold fact or slowly-changing dimension table.

    On first run (table absent) falls back to overwrite_table.
    If DeltaTable Python client encounters any environment/connect issues,
    automatically falls back to standard Spark SQL MERGE.

    Returns row count in the incoming DataFrame.
    """
    count = df.count()
    if not table_exists(spark, table_name):
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
        print(f"  ✓ Initial load: {table_name}  ({count:,} rows)")
    else:
        merged = False
        if DeltaTable is not None:
            try:
                delta_tbl = DeltaTable.forName(spark, table_name)
                (
                    delta_tbl.alias("t")
                    .merge(df.alias("s"), f"t.{merge_key} = s.{merge_key}")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
                merged = True
            except Exception:
                merged = False

        if not merged:
            # Resilient fallback: pure Spark SQL MERGE (bypasses any Python Delta client issue)
            clean_name = table_name.replace(".", "_").replace("-", "_")
            view_name = f"_upsert_src_{clean_name}"
            df.createOrReplaceTempView(view_name)
            spark.sql(f"""
                MERGE INTO {table_name} AS t
                USING {view_name} AS s
                ON t.{merge_key} = s.{merge_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
            """)

        print(f"  ✓ Upserted    : {table_name}  ({count:,} rows processed)")
    return count


def optimize(spark, table_name: str, zorder_cols: list = None) -> None:
    """Run OPTIMIZE (+ optional ZORDER) on a gold table."""
    sql = f"OPTIMIZE {table_name}"
    if zorder_cols:
        zorder_str = ", ".join(zorder_cols)
        sql += f" ZORDER BY ({zorder_str})"
    spark.sql(sql)
    print(f"  ✓ Optimized   : {table_name}")

