# src/identity/preparation.py

"""Load, normalise, and validate patient records for identity resolution."""

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window

from src.identity.writer import table_exists


# ─────────────────────────────────────────────────────────────────────────────
# Text helpers (private)
# ─────────────────────────────────────────────────────────────────────────────

def _clean_text(col):
    text = F.lower(F.trim(col.cast("string")))
    text = F.regexp_replace(text, r"[^\p{L}\p{N}\s]", " ")
    text = F.regexp_replace(text, r"\s+", " ")
    return F.when(F.length(text) > 0, text)


def _clean_gender(col):
    text = F.lower(F.trim(col.cast("string")))
    return (
        F.when(text.isin("m", "male", "ذكر"), F.lit("M"))
        .when(text.isin("f", "female", "انثى", "أنثى"), F.lit("F"))
        .otherwise(F.when(F.length(text) > 0, F.upper(text)))
    )


def _clean_phone(col):
    digits = F.regexp_replace(col.cast("string"), r"[^0-9]", "")
    return F.when(F.length(digits).between(7, 15), digits)


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def prepare_patients(spark, tables, run_id):
    """
    Load ``silver.patients``, normalise columns, reject duplicates,
    and return the valid patient DataFrame.

    Parameters
    ----------
    spark : SparkSession
    tables : dict   – ``IDENTITY_TABLES`` from config
    run_id : str

    Returns
    -------
    valid_patients : DataFrame
    """

    raw = spark.table(tables["source"])

    # Filter soft-deleted records if the column exists
    for col_name in ("_is_deleted", "is_deleted", "_deleted"):
        if col_name in raw.columns:
            raw = raw.filter(
                ~F.coalesce(F.col(col_name).cast("boolean"), F.lit(False))
            )
            break

    prepared = (
        raw
        .withColumn("source_branch",
                     F.lower(F.trim(F.col("_source_branch").cast("string"))))
        .withColumn("source_patient_id",
                     F.trim(F.col("patient_id").cast("string")))
        .withColumn("_source_patient_key",
                     F.concat_ws("|", "source_branch", "source_patient_id"))
        .withColumn("first_name_clean",  _clean_text(F.col("first_name")))
        .withColumn("last_name_clean",   _clean_text(F.col("last_name")))
        .withColumn("dob",               F.col("date_of_birth").cast("date"))
        .withColumn("dob_str",           F.date_format(F.col("dob"), "yyyy-MM-dd"))
        .withColumn("gender_clean",      _clean_gender(F.col("gender")))
        .withColumn("phone_clean",       _clean_phone(F.col("contact_no")))
        .withColumn("address_clean",     _clean_text(F.col("address")))
        .withColumn("source_timestamp",
                     F.coalesce(
                         F.col("created_at").cast("timestamp"),
                         F.col("_ingestion_timestamp").cast("timestamp"),
                         F.to_timestamp(F.lit("1900-01-01 00:00:00")),
                     ))
        .withColumn("source_record_hash",
                     F.sha2(
                         F.concat_ws("||",
                             F.coalesce(F.col("source_branch"),      F.lit("")),
                             F.coalesce(F.col("source_patient_id"),  F.lit("")),
                             F.coalesce(F.col("first_name_clean"),   F.lit("")),
                             F.coalesce(F.col("last_name_clean"),    F.lit("")),
                             F.coalesce(F.col("dob_str"),            F.lit("")),
                             F.coalesce(F.col("gender_clean"),       F.lit("")),
                             F.coalesce(F.col("phone_clean"),        F.lit("")),
                             F.coalesce(F.col("address_clean"),      F.lit("")),
                         ), 256,
                     ))
    )

    # ── Reject duplicate / null source keys ──────────────────────────────
    dup_keys = (
        prepared.groupBy("_source_patient_key")
        .agg(F.count("*").alias("dup_count"))
        .filter(F.col("_source_patient_key").isNull() | (F.col("dup_count") > 1))
    )

    exceptions = (
        prepared.join(dup_keys.select("_source_patient_key"),
                      on="_source_patient_key", how="inner")
        .withColumn("exception_type",
                     F.lit("DUPLICATE_OR_NULL_SOURCE_PATIENT_KEY"))
        .withColumn("run_id",      F.lit(run_id))
        .withColumn("detected_at", F.current_timestamp())
    )

    if exceptions.limit(1).count() > 0:
        (
            exceptions.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(tables["exceptions"])
        )

    valid_patients = prepared.join(
        dup_keys.select("_source_patient_key"),
        on="_source_patient_key", how="left_anti",
    )

    valid_count = valid_patients.count()
    exc_count   = exceptions.count()
    print(f"Valid patient records : {valid_count}")
    print(f"Identity exceptions  : {exc_count}")

    return valid_patients
