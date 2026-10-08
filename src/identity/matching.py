# src/identity/matching.py

"""
Probabilistic patient matching – pure Spark SQL implementation.

Zero external dependencies beyond PySpark.  Runs on Databricks Serverless
(Spark Connect) without any JVM / sparkContext access.

Algorithm
---------
1. **Blocking**   – SQL self-join on 3 blocking rules to generate candidate
                    pairs (first_name+dob, last_name+dob, phone).
2. **Comparison** – Field-level similarity using Spark SQL levenshtein() and
                    exact equality (all native Catalyst functions – no UDFs).
3. **Scoring**    – Simplified Fellegi-Sunter log-odds weighting.  Each field
                    contributes a weight = log2(m_i / u_i) where m_i is the
                    probability of agreement given a true match and u_i is the
                    probability of chance agreement.
4. **Probability** – match_probability = sigmoid2(match_weight)
                    = 1 / (1 + 2 ^ -match_weight)

Weight calibration (approximate clinical data-linkage literature values)
------------------------------------------------------------------------
| Field      | Exact   | Close (≤ edit dist threshold) | No match |
| first_name |  +3.0   | +2.0 (Lev ≤ 1)               | -1.0     |
| last_name  |  +3.0   | +2.0 (Lev ≤ 1)               | -1.0     |
| dob        |  +5.0   | n/a                           | -2.0     |
| gender     |  +1.0   | n/a                           | -0.5     |
| phone      |  +4.0   | n/a                           | 0.0      |
| address    |  +2.0   | +1.0 (Lev ≤ 5)               | 0.0      |
"""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from src.identity.writer import table_exists


# ---------------------------------------------------------------------------
# Helper: Safe Checkpoint
# ---------------------------------------------------------------------------

def _safe_checkpoint(df):
    """Safely truncate lineage – works on classic Spark and Spark Connect."""
    try:
        return df.localCheckpoint()
    except Exception:
        try:
            return df.checkpoint()
        except Exception:
            return df.cache()


# ---------------------------------------------------------------------------
# Scoring weights (Fellegi-Sunter log2 Bayes factors)
# ---------------------------------------------------------------------------

_W = {
    # field          : (exact_weight, close_weight, no_match_weight, close_lev_threshold)
    "first_name":    (3.0,  2.0, -1.0, 1),
    "last_name":     (3.0,  2.0, -1.0, 1),
    "dob":           (5.0,  None, -2.0, None),
    "gender":        (1.0,  None, -0.5, None),
    "phone":         (4.0,  None,  0.0, None),
    "address":       (2.0,  1.0,   0.0, 5),
}


def _comparison(col_l: str, col_r: str,
                w_exact: float, w_close: float | None,
                w_none: float, lev_threshold: int | None) -> "Column":
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
    else:
        return F.when(exact_match, F.lit(w_exact)).otherwise(F.lit(w_none))


# ---------------------------------------------------------------------------
# score_pairs
# ---------------------------------------------------------------------------

def score_pairs(spark, valid_patients: DataFrame) -> DataFrame:
    """
    Generate and score all candidate patient pairs.

    Returns a Spark DataFrame with columns:
        unique_id_l, unique_id_r, match_probability, match_weight
    """
    cols = [
        "_source_patient_key",
        "first_name_clean", "last_name_clean",
        "dob_str", "gender_clean", "phone_clean", "address_clean",
    ]
    df = (
        valid_patients
        .select(*cols)
        .withColumnRenamed("_source_patient_key", "unique_id")
        .fillna("")
    )

    l = df.alias("l")
    r = df.alias("r")

    # ── Blocking: generate candidate pairs ────────────────────────────────────
    # Three blocking rules combined with OR.  Only pairs where l.unique_id < r.unique_id
    # are retained to avoid duplicate (A,B) / (B,A) pairs.
    non_empty_dob   = (F.col("l.dob_str")      != "") & F.col("l.dob_str").isNotNull()
    non_empty_phone = (F.col("l.phone_clean")   != "") & F.col("l.phone_clean").isNotNull()

    pairs = (
        l.join(r, F.col("l.unique_id") < F.col("r.unique_id"))
        .where(
            (
                # Rule 1: same first name AND same date-of-birth
                (F.col("l.first_name_clean") == F.col("r.first_name_clean")) &
                (F.col("l.dob_str")          == F.col("r.dob_str"))          &
                non_empty_dob
            ) | (
                # Rule 2: same last name AND same date-of-birth
                (F.col("l.last_name_clean") == F.col("r.last_name_clean")) &
                (F.col("l.dob_str")         == F.col("r.dob_str"))         &
                non_empty_dob
            ) | (
                # Rule 3: same (non-empty) phone number
                (F.col("l.phone_clean") == F.col("r.phone_clean")) &
                non_empty_phone
            )
        )
        .select(
            F.col("l.unique_id").alias("unique_id_l"),
            F.col("r.unique_id").alias("unique_id_r"),
            F.col("l.first_name_clean").alias("fn_l"),
            F.col("r.first_name_clean").alias("fn_r"),
            F.col("l.last_name_clean").alias("ln_l"),
            F.col("r.last_name_clean").alias("ln_r"),
            F.col("l.dob_str").alias("dob_l"),
            F.col("r.dob_str").alias("dob_r"),
            F.col("l.gender_clean").alias("gender_l"),
            F.col("r.gender_clean").alias("gender_r"),
            F.col("l.phone_clean").alias("phone_l"),
            F.col("r.phone_clean").alias("phone_r"),
            F.col("l.address_clean").alias("addr_l"),
            F.col("r.address_clean").alias("addr_r"),
        )
    )

    pairs = _safe_checkpoint(pairs)
    pair_count = pairs.count()
    print(f"Candidate pairs after blocking : {pair_count:,}")

    if pair_count == 0:
        print("No candidate pairs found — all patients will be singletons.")
        empty_schema = "unique_id_l STRING, unique_id_r STRING, match_probability DOUBLE, match_weight DOUBLE"
        return spark.createDataFrame([], empty_schema)

    # ── Comparison: field-level similarity weights ─────────────────────────────
    scored = (
        pairs
        .withColumn("w_fn",     _comparison("fn_l",     "fn_r",     *_W["first_name"]))
        .withColumn("w_ln",     _comparison("ln_l",     "ln_r",     *_W["last_name"]))
        .withColumn("w_dob",    _comparison("dob_l",    "dob_r",    *_W["dob"]))
        .withColumn("w_gender", _comparison("gender_l", "gender_r", *_W["gender"]))
        .withColumn("w_phone",  _comparison("phone_l",  "phone_r",  *_W["phone"]))
        .withColumn("w_addr",   _comparison("addr_l",   "addr_r",   *_W["address"]))

        # Sum all field weights → match_weight  (log-odds score)
        .withColumn(
            "match_weight",
            F.col("w_fn") + F.col("w_ln") + F.col("w_dob") +
            F.col("w_gender") + F.col("w_phone") + F.col("w_addr"),
        )

        # Convert to probability via the base-2 sigmoid:
        #   P = 1 / (1 + 2^(-weight))
        .withColumn(
            "match_probability",
            F.lit(1.0) / (F.lit(1.0) + F.pow(F.lit(2.0), -F.col("match_weight"))),
        )

        .select("unique_id_l", "unique_id_r", "match_probability", "match_weight")
    )

    print(f"Scored pairs : {scored.count():,}")
    return scored


# ---------------------------------------------------------------------------
# Clustering (unchanged)
# ---------------------------------------------------------------------------

def _connected_components(edges, nodes, max_iterations=25):
    """
    Simple label-propagation connected components with periodic
    safe lineage truncation to prevent Spark DAG explosion.
    """

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
    valid_patients,
    tables,
    auto_merge_threshold,
    review_min_threshold,
    run_id,
):
    """
    Build patient clusters from scored pairs.

    1. Persist pair evidence and borderline review candidates.
    2. Combine auto-merge edges + manually-approved edges.
    3. Run connected components.
    4. Detect same-branch conflicts -> route to steward queue.
    5. Return clean cluster assignments (singletons for conflicted clusters).

    Returns
    -------
    clusters : DataFrame  (_source_patient_key, cluster_id,
                            source_branch, source_patient_id,
                            source_record_hash)
    """

    # -- 1. Classify pairs
    pairs = (
        scored_pairs
        .withColumn("patient_key_left",  F.col("unique_id_l"))
        .withColumn("patient_key_right", F.col("unique_id_r"))
        .withColumn(
            "pair_id",
            F.sha2(
                F.concat_ws("||",
                    F.least("patient_key_left", "patient_key_right"),
                    F.greatest("patient_key_left", "patient_key_right"),
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
        .withColumn("is_current", F.lit(True))
        .withColumn("run_id",      F.lit(run_id))
        .withColumn("evaluated_at", F.current_timestamp())
        .withColumn("superseded_at", F.lit(None).cast("timestamp"))
    )

    # -- 2. Persist pair evidence
    evidence_cols = [
        "pair_id", "patient_key_left", "patient_key_right",
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

    # -- 3. Persist review candidates
    review = (
        pairs.filter(F.col("decision_band") == "STEWARD_REVIEW")
        .join(
            valid_patients.alias("l"),
            F.col("patient_key_left") == F.col("l._source_patient_key"),
            "left",
        )
        .join(
            valid_patients.alias("r"),
            F.col("patient_key_right") == F.col("r._source_patient_key"),
            "left",
        )
        .select(
            "pair_id", "patient_key_left", "patient_key_right",
            F.col("l.source_branch").alias("branch_left"),
            F.col("r.source_branch").alias("branch_right"),
            F.col("l.first_name").alias("first_name_left"),
            F.col("r.first_name").alias("first_name_right"),
            F.col("l.last_name").alias("last_name_left"),
            F.col("r.last_name").alias("last_name_right"),
            F.col("l.dob_str").alias("dob_left"),
            F.col("r.dob_str").alias("dob_right"),
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

    # -- 4. Build edge graph
    auto_edges = (
        pairs.filter(F.col("decision_band") == "AUTO_MERGE")
        .select(
            F.col("patient_key_left").alias("src"),
            F.col("patient_key_right").alias("dst"),
        )
    )

    # Include manually-approved pairs from previous runs
    if table_exists(spark, tables["review_queue"]):
        approved = (
            spark.table(tables["review_queue"])
            .filter(F.col("review_status") == "MANUALLY_APPROVED")
            .select(
                F.col("patient_key_left").alias("src"),
                F.col("patient_key_right").alias("dst"),
            )
        )
        # Exclude manually-rejected pairs
        rejected_ids = (
            spark.table(tables["review_queue"])
            .filter(F.col("review_status") == "MANUALLY_REJECTED")
            .select("pair_id")
        )
        edges = (
            auto_edges.union(approved).distinct()
            .join(
                pairs.select("pair_id", "patient_key_left", "patient_key_right")
                     .join(rejected_ids, on="pair_id", how="inner")
                     .select(
                         F.col("patient_key_left").alias("src"),
                         F.col("patient_key_right").alias("dst"),
                     ),
                on=["src", "dst"], how="left_anti",
            )
        )
    else:
        edges = auto_edges.distinct()

    nodes = valid_patients.select(
        F.col("_source_patient_key").alias("node")
    ).distinct()

    # -- 5. Connected components
    groups = _connected_components(edges, nodes)

    vp_cols = valid_patients.select(
        F.col("_source_patient_key"),
        F.col("source_branch"),
        F.col("source_patient_id"),
        F.col("source_record_hash"),
    ).alias("vp")

    clusters = (
        groups.alias("g")
        .join(vp_cols, F.col("g.node") == F.col("vp._source_patient_key"), how="inner")
        .select(
            F.col("g.cluster_id"),
            F.col("vp._source_patient_key"),
            F.col("vp.source_branch"),
            F.col("vp.source_patient_id"),
            F.col("vp.source_record_hash"),
        )
    )

    # -- 6. Same-branch conflict detection
    conflict_cids = (
        clusters.groupBy("cluster_id", "source_branch")
        .agg(F.countDistinct("_source_patient_key").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .select("cluster_id").distinct()
    )
    conflict_count = conflict_cids.count()

    if conflict_count > 0:
        print(f"Same-branch conflict clusters: {conflict_count}")

    # Break conflicted clusters into singletons
    clean      = clusters.join(conflict_cids, on="cluster_id", how="left_anti")
    singletons = (
        clusters.join(conflict_cids, on="cluster_id", how="inner")
        .withColumn("cluster_id", F.col("_source_patient_key"))
    )
    clusters = clean.unionByName(singletons)

    print(f"Final clusters: {clusters.select('cluster_id').distinct().count()}")
    return clusters
