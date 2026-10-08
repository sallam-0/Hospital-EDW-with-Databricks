# src/identity/doctor_resolution.py

"""
Enterprise doctor ID assignment, alias management,
analytical outputs, tombstoning, and run auditing.
"""

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from src.identity.writer import table_exists, upsert_delta, optimize_zorder


# ─────────────────────────────────────────────────────────────────────────────
# Enterprise ID Resolution
# ─────────────────────────────────────────────────────────────────────────────

def resolve_enterprise_ids(spark, clusters, scored_pairs, valid_doctors, tables, run_id):
    """
    Assign stable enterprise doctor IDs to clusters.

    * Existing IDs always win (oldest ``created_at`` is the survivor).
    * New clusters get a freshly minted ``EP-<uuid>`` ID.
    * When evidence merges two existing enterprise IDs the older one
      survives and an alias row is written for the deprecated ID.
    * Multi-hop alias chains are flattened so all aliases point directly
      to the canonical survivor.

    Writes to: registry, aliases.
    """

    registry_tbl = tables["registry"]
    aliases_tbl  = tables["aliases"]

    # ── Load existing registry (if any) ──────────────────────────────────
    if table_exists(spark, registry_tbl):
        existing_reg = spark.table(registry_tbl)
    else:
        # First run: create an empty DataFrame with the correct schema so that
        # all downstream .select() and .join() column references resolve cleanly
        # even though the join will produce zero rows.
        from pyspark.sql import types as T
        existing_reg = spark.createDataFrame(
            [],
            T.StructType([
                T.StructField("_source_doctor_key",    T.StringType(),    True),
                T.StructField("enterprise_doctor_id",  T.StringType(),    True),
                T.StructField("created_at",             T.TimestampType(), True),
            ]),
        )

    # ── Map clusters to existing enterprise IDs ──────────────────────────
    cluster_eps = (
        clusters.select("cluster_id", "_source_doctor_key")
        .join(
            existing_reg.select(
                "_source_doctor_key", "enterprise_doctor_id", "created_at",
            ),
            on="_source_doctor_key", how="inner",
        )
        .distinct()
    )

    survivor_w = Window.partitionBy("cluster_id").orderBy(
        F.col("created_at").asc(), F.col("enterprise_doctor_id").asc(),
    )
    survivors = (
        cluster_eps
        .withColumn("rn", F.row_number().over(survivor_w))
        .filter(F.col("rn") == 1)
        .select("cluster_id",
                F.col("enterprise_doctor_id").alias("survivor_ep_id"))
    )

    # Mint new IDs for clusters without an existing enterprise ID
    new_ids = (
        clusters.select("cluster_id").distinct()
        .join(survivors.select("cluster_id"), on="cluster_id", how="left_anti")
        .withColumn("new_ep_id",
                     F.concat(F.lit("EP-"), F.upper(F.expr("uuid()"))))
    )

    ep_assignments = (
        clusters.select("cluster_id").distinct()
        .join(survivors, on="cluster_id", how="left")
        .join(new_ids,   on="cluster_id", how="left")
        .withColumn("enterprise_doctor_id",
                     F.coalesce("survivor_ep_id", "new_ep_id"))
        .select("cluster_id", "enterprise_doctor_id")
    )

    # ── Alias records for multi-EP merges ────────────────────────────────
    multi_ep = (
        cluster_eps.groupBy("cluster_id")
        .agg(
            F.countDistinct("enterprise_doctor_id").alias("ep_count"),
            F.collect_set("enterprise_doctor_id").alias("all_eps"),
        )
        .filter(F.col("ep_count") > 1)
        .join(survivors, on="cluster_id", how="inner")
    )

    alias_records = (
        multi_ep
        .withColumn("deprecated_enterprise_doctor_id", F.explode("all_eps"))
        .filter(F.col("deprecated_enterprise_doctor_id") != F.col("survivor_ep_id"))
        .select(
            "deprecated_enterprise_doctor_id",
            F.col("survivor_ep_id").alias("survivor_enterprise_doctor_id"),
            F.current_timestamp().alias("merged_at"),
            F.lit("DOCTOR_CLUSTER_MERGE").alias("merge_reason"),
            F.lit(run_id).alias("run_id"),
        )
        .distinct()
    )

    if alias_records.limit(1).count() > 0:
        # Write aliases
        if table_exists(spark, aliases_tbl):
            aliases_delta = DeltaTable.forName(spark, aliases_tbl)
            (
                aliases_delta.alias("t")
                .merge(
                    alias_records.alias("s"),
                    """
                    t.deprecated_enterprise_doctor_id = s.deprecated_enterprise_doctor_id
                    AND t.survivor_enterprise_doctor_id = s.survivor_enterprise_doctor_id
                    """,
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
            # Flatten multi-hop chains
            flatten_src = alias_records.select(
                "deprecated_enterprise_doctor_id",
                "survivor_enterprise_doctor_id",
            ).distinct()
            (
                aliases_delta.alias("t")
                .merge(
                    flatten_src.alias("s"),
                    """
                    t.survivor_enterprise_doctor_id = s.deprecated_enterprise_doctor_id
                    AND t.deprecated_enterprise_doctor_id != s.survivor_enterprise_doctor_id
                    """,
                )
                .whenMatchedUpdate(set={
                    "survivor_enterprise_doctor_id":
                        F.col("s.survivor_enterprise_doctor_id"),
                    "merged_at": F.current_timestamp(),
                })
                .execute()
            )
        else:
            alias_records.write.format("delta").mode("overwrite").saveAsTable(
                aliases_tbl
            )

        # Re-point deprecated IDs in the registry
        if table_exists(spark, registry_tbl):
            reg_delta = DeltaTable.forName(spark, registry_tbl)
            (
                reg_delta.alias("t")
                .merge(
                    alias_records.alias("s"),
                    "t.enterprise_doctor_id = s.deprecated_enterprise_doctor_id",
                )
                .whenMatchedUpdate(set={
                    "enterprise_doctor_id":
                        F.col("s.survivor_enterprise_doctor_id"),
                    "last_updated_at": F.current_timestamp(),
                })
                .execute()
            )

    # ── Upsert registry ──────────────────────────────────────────────────
    registry_updates = (
        clusters.join(ep_assignments, on="cluster_id", how="inner")
        .select(
            "_source_doctor_key",
            F.col("source_branch"),
            F.col("source_doctor_id"),
            "enterprise_doctor_id",
            "cluster_id",
            "source_record_hash",
            F.lit("ACTIVE").alias("identity_status"),
            F.lit("DOCTOR_ID").alias("match_method"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("last_updated_at"),
        )
    )

    if table_exists(spark, registry_tbl):
        reg_delta = DeltaTable.forName(spark, registry_tbl)
        (
            reg_delta.alias("t")
            .merge(
                registry_updates.alias("s"),
                "t._source_doctor_key = s._source_doctor_key",
            )
            .whenMatchedUpdate(set={
                "source_branch":          F.col("s.source_branch"),
                "source_doctor_id":      F.col("s.source_doctor_id"),
                "enterprise_doctor_id":  F.col("s.enterprise_doctor_id"),
                "cluster_id":             F.col("s.cluster_id"),
                "source_record_hash":     F.col("s.source_record_hash"),
                "identity_status":        F.col("s.identity_status"),
                "match_method":           F.col("s.match_method"),
                "last_updated_at":        F.current_timestamp(),
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        registry_updates.write.format("delta").mode("overwrite").saveAsTable(
            registry_tbl
        )

    print(f"Registry upserts : {registry_updates.count()}")
    print(f"Alias records    : {alias_records.count()}")

    optimize_zorder(spark, registry_tbl,
                    ["_source_doctor_key", "enterprise_doctor_id"])
    optimize_zorder(spark, aliases_tbl,
                    ["deprecated_enterprise_doctor_id",
                     "survivor_enterprise_doctor_id"])


# ─────────────────────────────────────────────────────────────────────────────
# Analytical Outputs
# ─────────────────────────────────────────────────────────────────────────────

def build_analytical_outputs(spark, valid_doctors, tables):
    """
    Build the doctor map, golden dimension, and tombstone deleted records.

    Writes to: doctor_map, dim_doctor, registry (tombstone), doctor_map (tombstone).
    """

    registry_tbl  = tables["registry"]
    map_tbl       = tables["doctor_map"]
    dim_tbl       = tables["dim_doctor"]

    # ── Doctor Map ──────────────────────────────────────────────────────
    current_reg = spark.table(registry_tbl)

    map_updates = (
        valid_doctors.join(
            current_reg.select(
                "_source_doctor_key", "enterprise_doctor_id", "identity_status",
            ),
            on="_source_doctor_key",
        )
        .select(
            "_source_doctor_key",
            "enterprise_doctor_id",
            F.col("source_branch"),
            F.col("source_doctor_id"),
            F.lit("DOCTOR_ID").alias("match_method"),
            F.col("identity_status").alias("match_status"),
            F.current_timestamp().alias("identity_updated_timestamp"),
        )
    )

    upsert_delta(spark, map_tbl, map_updates, "_source_doctor_key",
                 update_columns=[
                     "enterprise_doctor_id", "source_branch",
                     "source_doctor_id", "match_method",
                     "match_status", "identity_updated_timestamp",
                 ])
    print(f"Doctor map upserts: {map_updates.count()}")

    # ── Tombstone registry & map ─────────────────────────────────────────
    _tombstone_deleted(spark, valid_doctors, registry_tbl, map_tbl)

    # ── Golden Doctor Dimension ─────────────────────────────────────────
    _build_golden_dimension(spark, valid_doctors, tables)

    optimize_zorder(spark, map_tbl,
                    ["_source_doctor_key", "enterprise_doctor_id"])
    optimize_zorder(spark, dim_tbl, ["enterprise_doctor_id"])


def _tombstone_deleted(spark, valid_doctors, registry_tbl, map_tbl):
    """Mark records no longer present in ``valid_doctors`` as INACTIVE / DELETED."""

    if table_exists(spark, registry_tbl):
        stale_keys = (
            spark.table(registry_tbl)
            .filter(F.col("identity_status") != "INACTIVE")
            .select("_source_doctor_key")
            .join(valid_doctors.select("_source_doctor_key"),
                  on="_source_doctor_key", how="left_anti")
        )
        if stale_keys.limit(1).count() > 0:
            reg_delta = DeltaTable.forName(spark, registry_tbl)
            (
                reg_delta.alias("t")
                .merge(stale_keys.alias("s"),
                       "t._source_doctor_key = s._source_doctor_key")
                .whenMatchedUpdate(set={
                    "identity_status": F.lit("INACTIVE"),
                    "last_updated_at": F.current_timestamp(),
                })
                .execute()
            )
            print(f"Tombstoned registry records: {stale_keys.count()}")

    if table_exists(spark, map_tbl):
        stale_map = (
            spark.table(map_tbl)
            .filter(F.col("match_status") != "DELETED")
            .select("_source_doctor_key")
            .join(valid_doctors.select("_source_doctor_key"),
                  on="_source_doctor_key", how="left_anti")
        )
        if stale_map.limit(1).count() > 0:
            map_delta = DeltaTable.forName(spark, map_tbl)
            (
                map_delta.alias("t")
                .merge(stale_map.alias("s"),
                       "t._source_doctor_key = s._source_doctor_key")
                .whenMatchedUpdate(set={
                    "match_status":               F.lit("DELETED"),
                    "identity_updated_timestamp":  F.current_timestamp(),
                })
                .execute()
            )
            print(f"Tombstoned map records: {stale_map.count()}")


def _build_golden_dimension(spark, valid_doctors, tables):
    """Build survivorship-based golden doctor dimension."""

    registry_tbl = tables["registry"]
    dim_tbl      = tables["dim_doctor"]

    registered = valid_doctors.join(
        spark.table(registry_tbl).select(
            "_source_doctor_key", "enterprise_doctor_id",
        ),
        on="_source_doctor_key", how="inner",
    )

    def _latest(df, src_col, out_col):
        w = Window.partitionBy("enterprise_doctor_id").orderBy(
            F.col("source_timestamp").desc(),
            F.col("_source_doctor_key").asc(),
        )
        return (
            df.filter(F.col(src_col).isNotNull())
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .select("enterprise_doctor_id", F.col(src_col).alias(out_col))
        )

    gender_mode = (
        registered.filter(F.col("gender_clean").isNotNull())
        .groupBy("enterprise_doctor_id", "gender_clean")
        .agg(F.count("*").alias("cnt"),
             F.max("source_timestamp").alias("latest_ts"))
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("enterprise_doctor_id")
            .orderBy(F.desc("cnt"), F.desc("latest_ts"))
        ))
        .filter(F.col("rn") == 1)
        .select("enterprise_doctor_id",
                F.col("gender_clean").alias("gender"))
    )

    dim_base = (
        registered.groupBy("enterprise_doctor_id")
        .agg(
            F.count("*").alias("source_record_count"),
            F.countDistinct("source_branch").alias("source_branch_count"),
            F.sort_array(F.collect_set("source_branch")).alias("source_branches"),
            F.min("source_timestamp").alias("first_seen_timestamp"),
            F.max("source_timestamp").alias("last_seen_timestamp"),
        )
        .join(_latest(registered, "first_name",      "first_name"),
              on="enterprise_doctor_id", how="left")
        .join(_latest(registered, "last_name",       "last_name"),
              on="enterprise_doctor_id", how="left")
        .join(_latest(registered, "specialty_clean", "specialty"),
              on="enterprise_doctor_id", how="left")
        .join(_latest(registered, "office_no_clean", "office_no"),
              on="enterprise_doctor_id", how="left")
        .join(_latest(registered, "dept_id_clean",   "dept_id"),
              on="enterprise_doctor_id", how="left")
        .join(_latest(registered, "contact_no",      "contact_no"),
              on="enterprise_doctor_id", how="left")
        .join(gender_mode, on="enterprise_doctor_id", how="left")
        .withColumn("is_active",       F.lit(True))
        .withColumn("last_updated_at", F.current_timestamp())
    )

    if table_exists(spark, dim_tbl):
        dim_delta = DeltaTable.forName(spark, dim_tbl)
        (
            dim_delta.alias("t")
            .merge(dim_base.alias("s"),
                   "t.enterprise_doctor_id = s.enterprise_doctor_id")
            .whenMatchedUpdate(set={
                "first_name":           F.coalesce("s.first_name",  "t.first_name"),
                "last_name":            F.coalesce("s.last_name",   "t.last_name"),
                "specialty":            F.coalesce("s.specialty",   "t.specialty"),
                "office_no":            F.coalesce("s.office_no",   "t.office_no"),
                "dept_id":              F.coalesce("s.dept_id",     "t.dept_id"),
                "gender":               F.coalesce("s.gender",      "t.gender"),
                "contact_no":           F.coalesce("s.contact_no",  "t.contact_no"),
                "source_record_count":  F.col("s.source_record_count"),
                "source_branch_count":  F.col("s.source_branch_count"),
                "source_branches":      F.col("s.source_branches"),
                "first_seen_timestamp": F.least("t.first_seen_timestamp",
                                                "s.first_seen_timestamp"),
                "last_seen_timestamp":  F.greatest("t.last_seen_timestamp",
                                                   "s.last_seen_timestamp"),
                "is_active":            F.lit(True),
                "last_updated_at":      F.current_timestamp(),
            })
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        dim_base.write.format("delta").mode("overwrite").saveAsTable(dim_tbl)

    # Inactivate golden doctors whose registry entries are all INACTIVE
    if table_exists(spark, dim_tbl) and table_exists(spark, tables["registry"]):
        active_eps = (
            spark.table(tables["registry"])
            .filter(F.col("identity_status") == "ACTIVE")
            .select("enterprise_doctor_id").distinct()
        )
        stale_eps = (
            spark.table(dim_tbl)
            .filter(F.col("is_active"))
            .select("enterprise_doctor_id")
            .join(active_eps, on="enterprise_doctor_id", how="left_anti")
        )
        if stale_eps.limit(1).count() > 0:
            dim_delta = DeltaTable.forName(spark, dim_tbl)
            (
                dim_delta.alias("t")
                .merge(stale_eps.alias("s"),
                       "t.enterprise_doctor_id = s.enterprise_doctor_id")
                .whenMatchedUpdate(set={
                    "is_active":       F.lit(False),
                    "last_updated_at": F.current_timestamp(),
                })
                .execute()
            )

    print(f"Golden dimension rows: {dim_base.count()}")


# ─────────────────────────────────────────────────────────────────────────────
# Validation & Audit
# ─────────────────────────────────────────────────────────────────────────────

def validate_and_audit(spark, valid_doctors, tables, run_id, full_refresh):
    """
    Assert data-quality invariants and write a run-audit record.

    Returns a single-row Spark DataFrame with the run summary.
    """

    from pyspark.sql import types as T

    registry = spark.table(tables["registry"])
    pat_map  = spark.table(tables["doctor_map"])

    dup_reg = (
        registry.groupBy("_source_doctor_key")
        .agg(F.countDistinct("enterprise_doctor_id").alias("ep_cnt"))
        .filter(F.col("ep_cnt") > 1)
    )
    dup_map = (
        pat_map.groupBy("_source_doctor_key")
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") > 1)
    )
    unmapped = (
        valid_doctors.select("_source_doctor_key")
        .join(registry.select("_source_doctor_key"),
              on="_source_doctor_key", how="left_anti")
    )

    assert dup_reg.count()  == 0, "Registry has source keys mapped to multiple enterprise doctor IDs"
    assert dup_map.count()  == 0, "Doctor map has duplicate source mappings"
    assert unmapped.count() == 0, "Some valid doctors have no registry row"

    summary = spark.createDataFrame(
        [(
            run_id,
            full_refresh,
            valid_doctors.count(),
            registry.count(),
            pat_map.count(),
            datetime.now(timezone.utc),
        )],
        schema=T.StructType([
            T.StructField("run_id",                T.StringType(),    False),
            T.StructField("full_refresh",          T.BooleanType(),   False),
            T.StructField("valid_doctor_count",   T.LongType(),     False),
            T.StructField("registry_count",        T.LongType(),     False),
            T.StructField("doctor_map_count",     T.LongType(),     False),
            T.StructField("completed_at",          T.TimestampType(), False),
        ]),
    )

    summary.write.format("delta").mode("append").saveAsTable(tables["run_audit"])
    print("Validation passed ✓  — run audit written")
    return summary
