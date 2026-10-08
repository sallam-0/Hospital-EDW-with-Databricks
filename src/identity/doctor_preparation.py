# src/identity/doctor_preparation.py

"""Load, normalise, and validate doctor records for identity resolution."""

from pyspark.sql import DataFrame, functions as F

from src.identity.writer import table_exists


# ---------------------------------------------------------------------------
# Text helpers (private)
# ---------------------------------------------------------------------------

def _clean_text(col):
    text = F.lower(F.trim(col.cast("string")))
    text = F.regexp_replace(text, r"[^\p{L}\p{N}\s]", " ")
    text = F.regexp_replace(text, r"\s+", " ")
    return F.when(F.length(text) > 0, text)


def _clean_phone(col):
    digits = F.regexp_replace(col.cast("string"), r"[^0-9]", "")
    return F.when(F.length(digits).between(7, 15), digits)


def _clean_gender(col):
    text = F.lower(F.trim(col.cast("string")))
    return (
        F.when(text.isin("m", "male"), F.lit("M"))
        .when(text.isin("f", "female"), F.lit("F"))
        .otherwise(F.when(F.length(text) > 0, F.upper(text)))
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def prepare_doctors(spark, tables, run_id):
    """
    Load silver.doctors, normalise columns, reject duplicate keys,
    and return the valid doctor DataFrame.

    Doctor identity matching uses:
        - first_name_clean / last_name_clean  (text similarity)
        - specialty_clean                     (exact / fuzzy match)
        - phone_clean                         (exact match)
        - office_no_clean                     (exact match -- strong identifier)
        - gender_clean                        (exact match)

    Note: doctors have no date-of-birth field.  specialty and office_no
    take the role that DOB plays in patient matching.

    Parameters
    ----------
    spark   : SparkSession
    tables  : dict  -- DOCTOR_IDENTITY_TABLES from config
    run_id  : str

    Returns
    -------
    valid_doctors : DataFrame
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
        .withColumn("source_doctor_id",
                     F.trim(F.col("doctor_id").cast("string")))
        .withColumn("_source_doctor_key",
                     F.concat_ws("|", "source_branch", "source_doctor_id"))

        # Name normalisation (same logic as patients)
        .withColumn("first_name_clean",  _clean_text(F.col("first_name")))
        .withColumn("last_name_clean",   _clean_text(F.col("last_name")))

        # Professional identifiers
        .withColumn("specialty_clean",   _clean_text(F.col("specialty")))
        .withColumn("office_no_clean",   F.trim(F.col("office_no").cast("string")))
        .withColumn("dept_id_clean",     F.trim(F.col("dept_id").cast("string")))

        # Contact info
        .withColumn("gender_clean",      _clean_gender(F.col("gender")))
        .withColumn("phone_clean",       _clean_phone(F.col("contact_no")))

        # Provenance
        .withColumn("source_timestamp",
                     F.coalesce(
                         F.col("_ingestion_timestamp").cast("timestamp"),
                         F.to_timestamp(F.lit("1900-01-01 00:00:00")),
                     ))
        .withColumn("source_record_hash",
                     F.sha2(
                         F.concat_ws("||",
                             F.coalesce(F.col("source_branch"),    F.lit("")),
                             F.coalesce(F.col("source_doctor_id"), F.lit("")),
                             F.coalesce(F.col("first_name_clean"), F.lit("")),
                             F.coalesce(F.col("last_name_clean"),  F.lit("")),
                             F.coalesce(F.col("specialty_clean"),  F.lit("")),
                             F.coalesce(F.col("office_no_clean"),  F.lit("")),
                             F.coalesce(F.col("gender_clean"),     F.lit("")),
                             F.coalesce(F.col("phone_clean"),      F.lit("")),
                         ), 256,
                     ))
    )

    # -- Reject duplicate / null source keys
    dup_keys = (
        prepared.groupBy("_source_doctor_key")
        .agg(F.count("*").alias("dup_count"))
        .filter(F.col("_source_doctor_key").isNull() | (F.col("dup_count") > 1))
    )

    exceptions = (
        prepared.join(dup_keys.select("_source_doctor_key"),
                      on="_source_doctor_key", how="inner")
        .withColumn("exception_type",
                     F.lit("DUPLICATE_OR_NULL_SOURCE_DOCTOR_KEY"))
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

    valid_doctors = prepared.join(
        dup_keys.select("_source_doctor_key"),
        on="_source_doctor_key", how="left_anti",
    )

    valid_count = valid_doctors.count()
    exc_count   = exceptions.count()
    print(f"Valid doctor records : {valid_count}")
    print(f"Identity exceptions  : {exc_count}")

    return valid_doctors