# src/identity/doctor_matching.py

"""
Probabilistic doctor matching -- pure Spark SQL implementation.

Zero external dependencies. Works on Databricks Serverless (Spark Connect).

Doctor-specific scoring weights (Fellegi-Sunter log2 Bayes factors)
--------------------------------------------------------------------
Doctors do not have a date-of-birth field, so specialty and office_no
carry the discriminating weight that DOB provides for patients.

| Field      | Exact  | Close (Lev threshold) | No match |
| first_name |  +3.0  | +2.0 (Lev <= 1)       | -1.0     |
| last_name  |  +3.0  | +2.0 (Lev <= 1)       | -1.0     |
| specialty  |  +4.0  | +2.0 (Lev <= 3)       | -1.5     |
| office_no  |  +5.0  | n/a (exact only)      | 0.0      |
| phone      |  +4.0  | n/a                   | 0.0      |
| gender     |  +1.0  | n/a                   | -0.5     |

Blocking rules (candidate pair generation)
------------------------------------------
1. same last_name  AND same specialty
2. same phone      (non-empty)
3. same office_no  (non-empty)
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from src.identity.writer import table_exists


# ---------------------------------------------------------------------------
# Helper: Safe Checkpoint
# ---------------------------------------------------------------------------

def _safe_checkpoint(df):
    """Safely truncate lineage -- works on classic Spark and Spark Connect."""
    try:
        return df.localCheckpoint()
    except Exception:
        try:
            return df.checkpoint()
        except Exception:
            return df.cache()


# ---------------------------------------------------------------------------
# Scoring weights
# ---------------------------------------------------------------------------

_W = {
    # field       : (exact_weight, close_weight, no_match_weight, close_lev_threshold)
    "first_name": (3.0,  2.0, -1.0, 1),
    "last_name":  (3.0,  2.0, -1.0, 1),
    "specialty":  (4.0,  2.0, -1.5, 3),
    "office_no":  (5.0,  None, 0.0, None),
    "phone":      (4.0,  None, 0.0, None),
    "gender":     (1.0,  None, -0.5, None),
}


def _comparison(col_l, col_r, w_exact, w_close, w_none, lev_threshold):
    """Return a Spark Column expression with the comparison weight for one field."""
    exact_match = (
        F.col(col_l).isNotNull() &
        F.col(col_r).isNotNull() &
        (F.col(col_l) != "") &
        (F.col(col_r) != "") &
        (F.col(col_l) == F.col(col_r))
    )

    if w_close is not None and lev_threshold is not None:
        close_match = (
            F.col(col_l).isNotNull() &
            F.col(col_r).isNotNull() &
            (F.col(col_l) != "") &
            (F.col(col_r) != "") &
            (F.levenshtein(F.col(col_l), F.col(col_r)) <= lev_threshold)
        )
        return (
            F.when(exact_match, F.lit(w_exact))
            .when(close_match,  F.lit(w_close))
            .otherwise(F.lit(w_none))
        )
    return F.when(exact_match, F.lit(w_exact)).otherwise(F.lit(w_none))


# ---------------------------------------------------------------------------
# score_pairs
# ---------------------------------------------------------------------------

def score_pairs(spark, valid_doctors):
    """
    Generate and score all candidate doctor pairs.

    Returns a Spark DataFrame with columns:
        unique_id_l, unique_id_r, match_probability, match_weight
    """
    cols = [
        "_source_doctor_key",
        "first_name_clean", "last_name_clean",
        "specialty_clean", "office_no_clean",
        "gender_clean", "phone_clean",
    ]
    df = (
        valid_doctors
        .select(*cols)
        .withColumnRenamed("_source_doctor_key", "unique_id")
        .fillna("")
    )

    l = df.alias("l")
    r = df.alias("r")

    non_empty_specialty = (F.col("l.specialty_clean") != "") & F.col("l.specialty_clean").isNotNull()
    non_empty_phone     = (F.col("l.phone_clean")     != "") & F.col("l.phone_clean").isNotNull()
    non_empty_office    = (F.col("l.office_no_clean") != "") & F.col("l.office_no_clean").isNotNull()

    # -- Blocking: generate candidate pairs
    # Rule 1: same last name + same specialty  (strong professional identifier)
    # Rule 2: same non-empty phone
    # Rule 3: same non-empty office number
    pairs = (
        l.join(r, F.col("l.unique_id") < F.col("r.unique_id"))
        .where(
            (
                (F.col("l.last_name_clean") == F.col("r.last_name_clean")) &
                (F.col("l.specialty_clean") == F.col("r.specialty_clean")) &
                non_empty_specialty
            ) | (
                (F.col("l.phone_clean") == F.col("r.phone_clean")) &
                non_empty_phone
            ) | (
                (F.col("l.office_no_clean") == F.col("r.office_no_clean")) &
                non_empty_office
            )
        )
        .select(
            F.col("l.unique_id").alias("unique_id_l"),
            F.col("r.unique_id").alias("unique_id_r"),
            F.col("l.first_name_clean").alias("fn_l"),
            F.col("r.first_name_clean").alias("fn_r"),
            F.col("l.last_name_clean").alias("ln_l"),
            F.col("r.last_name_clean").alias("ln_r"),
            F.col("l.specialty_clean").alias("spec_l"),
            F.col("r.specialty_clean").alias("spec_r"),
            F.col("l.office_no_clean").alias("office_l"),
            F.col("r.office_no_clean").alias("office_r"),
            F.col("l.phone_clean").alias("phone_l"),
            F.col("r.phone_clean").alias("phone_r"),
            F.col("l.gender_clean").alias("gender_l"),
            F.col("r.gender_clean").alias("gender_r"),
        )
    )

    pairs = _safe_checkpoint(pairs)
    pair_count = pairs.count()
    print(f"Candidate pairs after blocking : {pair_count:,}")

    if pair_count == 0:
        print("No candidate pairs found -- all doctors will be singletons.")
        empty_schema = "unique_id_l STRING, unique_id_r STRING, match_probability DOUBLE, match_weight DOUBLE"
        return spark.createDataFrame([], empty_schema)

    # -- Comparison: field-level similarity weights
    scored = (
        pairs
        .withColumn("w_fn",     _comparison("fn_l",     "fn_r",     *_W["first_name"]))
        .withColumn("w_ln",     _comparison("ln_l",     "ln_r",     *_W["last_name"]))
        .withColumn("w_spec",   _comparison("spec_l",   "spec_r",   *_W["specialty"]))
        .withColumn("w_office", _comparison("office_l", "office_r", *_W["office_no"]))
        .withColumn("w_phone",  _comparison("phone_l",  "phone_r",  *_W["phone"]))
        .withColumn("w_gender", _comparison("gender_l", "gender_r", *_W["gender"]))

        .withColumn(
            "match_weight",
            F.col("w_fn") + F.col("w_ln") + F.col("w_spec") +
            F.col("w_office") + F.col("w_phone") + F.col("w_gender"),
        )
        # P = 1 / (1 + 2^(-weight))
        .withColumn(
            "match_probability",
            F.lit(1.0) / (F.lit(1.0) + F.pow(F.lit(2.0), -F.col("match_weight"))),
        )

        .select("unique_id_l", "unique_id_r", "match_probability", "match_weight")
    )

    print(f"Scored pairs : {scored.count():,}")
    return scored


# ---------------------------------------------------------------------------
# Clustering (identical logic, reuses patient implementation)
# ---------------------------------------------------------------------------

def _connected_components(edges, nodes, max_iterations=25):
    """Label-propagation connected components."""

    edges_bidi = _safe_checkpoint(
        edges.union(
            edges.select(F.col("dst").alias("src"), F.col("src").alias("dst"))
        ).distinct()
    )

    groups = _safe_checkpoint(nodes.withColumn("cluster_id", F.col("node")))

    for iteration in range(max_iterations):
        propagated = (
            edges_bidi
            .join(
                groups.select(
                    F.col("node").alias("dst"),
                    F.col("cluster_id").alias("nbr_cid"),
                ),
                on="dst", how="inner",
            )
            .groupBy("src")
            .agg(F.min("nbr_cid").alias("min_nbr_cid"))
        )

        new_groups = (
            groups.alias("g")
            .join(propagated.alias("p"), F.col("g.node") == F.col("p.src"), how="left")
            .withColumn(
                "cluster_id",
                F.when(
                    F.col("min_nbr_cid").isNotNull(),
                    F.least("cluster_id", "min_nbr_cid"),
                ).otherwise(F.col("cluster_id")),
            )
            .select(F.col("g.node").alias("node"), "cluster_id")
        )

        changed = (
            groups.alias("old")
            .join(new_groups.alias("new"), on="node")
            .filter(F.col("old.cluster_id") != F.col("new.cluster_id"))
            .limit(1).count()
        )

        groups = new_groups
        if iteration % 4 == 0:
            groups = _safe_checkpoint(groups)

        if changed == 0:
            print(f"Connected components converged in {iteration + 1} iterations")
            break
    else:
        print(f"Warning: reached {max_iterations} iterations without convergence")

    return groups


def build_clusters(
    spark,
    scored_pairs,
    valid_doctors,
    tables,
    auto_merge_threshold,
    review_min_threshold,
    run_id,
):
    """
    Build doctor clusters from scored pairs.

    Identical logic to patient build_clusters, using _source_doctor_key
    as the entity key.
    """

    pairs = (
        scored_pairs
        .withColumn("doctor_key_left",  F.col("unique_id_l"))
        .withColumn("doctor_key_right", F.col("unique_id_r"))
        .withColumn(
            "pair_id",
            F.sha2(
                F.concat_ws("||",
                    F.least("doctor_key_left", "doctor_key_right"),
                    F.greatest("doctor_key_left", "doctor_key_right"),
                ), 256,
            ),
        )
        .withColumn(
            "decision_band",
            F.when(F.col("match_probability") >= auto_merge_threshold,
                   F.lit("AUTO_MERGE"))
            .when(F.col("match_probability") >= review_min_threshold,
                   F.lit("STEWARD_REVIEW"))
            .otherwise(F.lit("NO_MATCH")),
        )
        .withColumn("is_current",    F.lit(True))
        .withColumn("run_id",        F.lit(run_id))
        .withColumn("evaluated_at",  F.current_timestamp())
        .withColumn("superseded_at", F.lit(None).cast("timestamp"))
    )

    # -- 1. Persist pair evidence
    evidence_cols = [
        "pair_id", "doctor_key_left", "doctor_key_right",
        "match_probability", "match_weight", "decision_band",
        "is_current", "run_id", "evaluated_at", "superseded_at",
    ]
    evidence = pairs.select(*evidence_cols)

    if table_exists(spark, tables["pair_evidence"]):
        from delta.tables import DeltaTable
        pe_delta = DeltaTable.forName(spark, tables["pair_evidence"])
        (
            pe_delta.alias("target")
            .merge(evidence.alias("source"), "target.pair_id = source.pair_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        evidence.write.format("delta").mode("overwrite").saveAsTable(
            tables["pair_evidence"]
        )

    # -- 2. Persist review candidates
    review = (
        pairs.filter(F.col("decision_band") == "STEWARD_REVIEW")
        .join(valid_doctors.alias("l"),
              F.col("doctor_key_left") == F.col("l._source_doctor_key"), "left")
        .join(valid_doctors.alias("r"),
              F.col("doctor_key_right") == F.col("r._source_doctor_key"), "left")
        .select(
            "pair_id", "doctor_key_left", "doctor_key_right",
            F.col("l.source_branch").alias("branch_left"),
            F.col("r.source_branch").alias("branch_right"),
            F.col("l.first_name").alias("first_name_left"),
            F.col("r.first_name").alias("first_name_right"),
            F.col("l.last_name").alias("last_name_left"),
            F.col("r.last_name").alias("last_name_right"),
            F.col("l.specialty").alias("specialty_left"),
            F.col("r.specialty").alias("specialty_right"),
            "match_probability", "match_weight", "decision_band",
            F.lit("PENDING").alias("review_status"),
            F.lit("BORDERLINE_SCORE").alias("review_reason"),
            F.lit(None).cast("string").alias("reviewed_by"),
            F.lit(None).cast("timestamp").alias("reviewed_at"),
            F.lit(run_id).alias("run_id"),
            F.current_timestamp().alias("evaluated_at"),
        )
    )

    if review.limit(1).count() > 0:
        if table_exists(spark, tables["review_queue"]):
            from delta.tables import DeltaTable
            rq_delta = DeltaTable.forName(spark, tables["review_queue"])
            (
                rq_delta.alias("target")
                .merge(review.alias("source"), "target.pair_id = source.pair_id")
                .whenMatchedUpdate(
                    condition="target.review_status = 'PENDING'",
                    set={
                        "match_probability": F.col("source.match_probability"),
                        "match_weight":      F.col("source.match_weight"),
                        "run_id":            F.col("source.run_id"),
                        "evaluated_at":      F.col("source.evaluated_at"),
                    },
                )
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            review.write.format("delta").mode("overwrite").saveAsTable(
                tables["review_queue"]
            )

    print(f"Pair evidence rows : {evidence.count()}")
    print(f"Review candidates  : {review.count()}")

    # -- 3. Build edge graph
    auto_edges = (
        pairs.filter(F.col("decision_band") == "AUTO_MERGE")
        .select(
            F.col("doctor_key_left").alias("src"),
            F.col("doctor_key_right").alias("dst"),
        )
    )

    if table_exists(spark, tables["review_queue"]):
        approved = (
            spark.table(tables["review_queue"])
            .filter(F.col("review_status") == "MANUALLY_APPROVED")
            .select(
                F.col("doctor_key_left").alias("src"),
                F.col("doctor_key_right").alias("dst"),
            )
        )
        rejected_ids = (
            spark.table(tables["review_queue"])
            .filter(F.col("review_status") == "MANUALLY_REJECTED")
            .select("pair_id")
        )
        edges = (
            auto_edges.union(approved).distinct()
            .join(
                pairs.select("pair_id", "doctor_key_left", "doctor_key_right")
                     .join(rejected_ids, on="pair_id", how="inner")
                     .select(
                         F.col("doctor_key_left").alias("src"),
                         F.col("doctor_key_right").alias("dst"),
                     ),
                on=["src", "dst"], how="left_anti",
            )
        )
    else:
        edges = auto_edges.distinct()

    nodes = valid_doctors.select(
        F.col("_source_doctor_key").alias("node")
    ).distinct()

    # -- 4. Connected components
    groups = _connected_components(edges, nodes)

    vp_cols = valid_doctors.select(
        F.col("_source_doctor_key"),
        F.col("source_branch"),
        F.col("source_doctor_id"),
        F.col("source_record_hash"),
    ).alias("vp")

    clusters = (
        groups.alias("g")
        .join(vp_cols, F.col("g.node") == F.col("vp._source_doctor_key"), how="inner")
        .select(
            F.col("g.cluster_id"),
            F.col("vp._source_doctor_key"),
            F.col("vp.source_branch"),
            F.col("vp.source_doctor_id"),
            F.col("vp.source_record_hash"),
        )
    )

    # -- 5. Same-branch conflict detection
    conflict_cids = (
        clusters.groupBy("cluster_id", "source_branch")
        .agg(F.countDistinct("_source_doctor_key").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .select("cluster_id").distinct()
    )
    conflict_count = conflict_cids.count()
    if conflict_count > 0:
        print(f"Same-branch conflict clusters: {conflict_count}")

    clean      = clusters.join(conflict_cids, on="cluster_id", how="left_anti")
    singletons = (
        clusters.join(conflict_cids, on="cluster_id", how="inner")
        .withColumn("cluster_id", F.col("_source_doctor_key"))
    )
    clusters = clean.unionByName(singletons)

    print(f"Final clusters: {clusters.select('cluster_id').distinct().count()}")
    return clusters