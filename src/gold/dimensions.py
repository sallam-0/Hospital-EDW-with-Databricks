# src/gold/dimensions.py
"""
Gold layer — reference and conformed dimensions  (Phase 1).

Builds / refreshes:
  1. dim_date           — calendar spine 2015-2035
  2. dim_department     — hospital departments (from silver.departments)
  3. dim_medication     — pharmacy drug reference (from silver.medications)
  4. dim_diagnosis_code — ICD-10 codes (derived from silver.diagnoses)
  5. dim_payer          — insurance payers (derived from silver.insurance_claims)

dim_patient and dim_doctor are built by the identity-resolution notebooks
and are NOT re-built here.
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from src.gold.writer import overwrite_table, upsert_table, optimize


# ─────────────────────────────────────────────────────────────────────────────
# 1. dim_date — Calendar Spine
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_date(
    spark: SparkSession,
    tables: dict,
    start: str = "2015-01-01",
    end:   str = "2035-12-31",
) -> None:
    """
    Generate a fully-attributed date dimension table.

    Columns
    -------
    date_key          int      YYYYMMDD surrogate (FK in fact tables)
    full_date         date
    day_of_week       int      1 = Sunday … 7 = Saturday (Spark convention)
    day_name          string   'Monday', 'Tuesday', …
    day_of_month      int      1-31
    day_of_year       int      1-366
    week_of_year      int
    month_num         int      1-12
    month_name        string   'January', …
    quarter           int      1-4
    quarter_label     string   'Q1', 'Q2', …
    year              int
    is_weekend        boolean  Saturday or Sunday
    fiscal_year       int      Apr–Mar cycle (common MOHP / hospital fiscal year)
    fiscal_quarter    int      1-4 relative to 1-Apr
    """
    dim_tbl = tables["dim_date"]

    date_range = spark.sql(
        f"SELECT sequence(to_date('{start}'), to_date('{end}'), "
        f"interval 1 day) AS d"
    ).select(F.explode(F.col("d")).alias("full_date"))

    dim_date = (
        date_range
        .withColumn("date_key",
                    F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("day_of_week",     F.dayofweek("full_date"))
        .withColumn("day_name",        F.date_format("full_date", "EEEE"))
        .withColumn("day_of_month",    F.dayofmonth("full_date"))
        .withColumn("day_of_year",     F.dayofyear("full_date"))
        .withColumn("week_of_year",    F.weekofyear("full_date"))
        .withColumn("month_num",       F.month("full_date"))
        .withColumn("month_name",      F.date_format("full_date", "MMMM"))
        .withColumn("quarter",         F.quarter("full_date"))
        .withColumn("quarter_label",
                    F.concat(F.lit("Q"), F.quarter("full_date").cast("string")))
        .withColumn("year",            F.year("full_date"))
        .withColumn("is_weekend",      F.col("day_of_week").isin([1, 7]))
        # Fiscal year: Apr-1 to Mar-31
        .withColumn("fiscal_year",
                    F.when(F.month("full_date") >= 4,
                           F.year("full_date") + 1)
                    .otherwise(F.year("full_date")))
        .withColumn("fiscal_quarter",
                    F.when(F.month("full_date").between(4,  6), F.lit(1))
                     .when(F.month("full_date").between(7,  9), F.lit(2))
                     .when(F.month("full_date").between(10, 12), F.lit(3))
                     .otherwise(F.lit(4)))
    )

    overwrite_table(spark, dim_date, dim_tbl)
    optimize(spark, dim_tbl)


# ─────────────────────────────────────────────────────────────────────────────
# 2. dim_department — Hospital Department Lookup
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_department(
    spark: SparkSession, tables: dict, silver_tables: dict
) -> None:
    """
    Build the department dimension from silver.departments.

    Takes the latest name per dept_id (SCD-1) and tracks which hospital
    branches have reported this department.

    Columns
    -------
    dept_id            string  NK (PK in silver)
    dept_name          string  standardised name
    source_branches    array   branches that reported this dept
    source_branch_count int
    last_updated_at    timestamp
    """
    dim_tbl    = tables["dim_department"]
    silver_src = silver_tables["departments"]

    # Latest record per dept_id
    w = Window.partitionBy("dept_id").orderBy(F.col("_ingestion_timestamp").desc())
    latest = (
        spark.table(silver_src)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select("dept_id", "dept_name")
    )

    # Branch coverage
    branch_agg = (
        spark.table(silver_src)
        .groupBy("dept_id")
        .agg(
            F.sort_array(F.collect_set("_source_branch")).alias("source_branches"),
            F.countDistinct("_source_branch").alias("source_branch_count"),
        )
    )

    dim = (
        latest
        .join(branch_agg, on="dept_id", how="left")
        .withColumn("last_updated_at", F.current_timestamp())
        .select(
            "dept_id",
            "dept_name",
            "source_branches",
            "source_branch_count",
            "last_updated_at",
        )
    )

    overwrite_table(spark, dim, dim_tbl)
    optimize(spark, dim_tbl, ["dept_id"])


# ─────────────────────────────────────────────────────────────────────────────
# 3. dim_medication — Pharmacy Drug Reference
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_medication(
    spark: SparkSession, tables: dict, silver_tables: dict
) -> None:
    """
    Build the medication reference dimension from silver.medications.

    Takes the latest unit_price and attributes per medication_id (SCD-1).

    Columns
    -------
    medication_id     string  NK (PK)
    medication_name   string
    category          string  drug category / ATC class
    dosage_form       string  tablet / capsule / syrup / injection / …
    strength          string  e.g. '500mg', '10mg/5ml'
    unit_price        double  latest recorded price
    last_updated_at   timestamp
    """
    dim_tbl    = tables["dim_medication"]
    silver_src = silver_tables["medications"]

    w = Window.partitionBy("medication_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )

    dim = (
        spark.table(silver_src)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .withColumn("last_updated_at", F.current_timestamp())
        .select(
            "medication_id",
            "medication_name",
            "category",
            "dosage_form",
            "strength",
            "unit_price",
            "last_updated_at",
        )
    )

    overwrite_table(spark, dim, dim_tbl)
    optimize(spark, dim_tbl, ["medication_id"])


# ─────────────────────────────────────────────────────────────────────────────
# 4. dim_diagnosis_code — ICD-10 Reference
# ─────────────────────────────────────────────────────────────────────────────

def build_dim_diagnosis_code(
    spark: SparkSession, tables: dict, silver_tables: dict
) -> None:
    """
    Derive an ICD-10 code reference table from silver.diagnoses.

    Uses the most-frequently-observed diagnosis_name per icd10_code as the
    canonical label, since the same code can have slightly different free-text
    labels across branches.

    Columns
    -------
    icd10_code        string  PK (e.g. 'E11.9', 'J06.9')
    diagnosis_name    string  canonical name (most common across branches)
    icd10_chapter     string  first character of code — ICD-10 chapter letter
    usage_count       long    total occurrences in fact data
    last_updated_at   timestamp
    """
    dim_tbl    = tables["dim_diagnosis_code"]
    silver_src = silver_tables["diagnoses"]

    # Count occurrences per code
    code_stats = (
        spark.table(silver_src)
        .filter(F.col("icd10_code").isNotNull() & (F.trim(F.col("icd10_code")) != ""))
        .groupBy("icd10_code")
        .agg(F.count("*").alias("usage_count"))
    )

    # Most-frequent name per code (canonical label)
    freq_w = Window.partitionBy("icd10_code").orderBy(
        F.desc("name_freq"), F.asc("diagnosis_name")
    )
    canonical_name = (
        spark.table(silver_src)
        .filter(F.col("icd10_code").isNotNull() & (F.trim(F.col("icd10_code")) != ""))
        .groupBy("icd10_code", "diagnosis_name")
        .agg(F.count("*").alias("name_freq"))
        .withColumn("_rn", F.row_number().over(freq_w))
        .filter(F.col("_rn") == 1)
        .select("icd10_code", "diagnosis_name")
    )

    dim = (
        canonical_name
        .join(code_stats, on="icd10_code", how="left")
        .withColumn("icd10_chapter", F.upper(F.substring(F.col("icd10_code"), 1, 1)))
        .withColumn("last_updated_at", F.current_timestamp())
        .select(
            "icd10_code",
            "diagnosis_name",
            "icd10_chapter",
            "usage_count",
            "last_updated_at",
        )
    )

    overwrite_table(spark, dim, dim_tbl)
    optimize(spark, dim_tbl, ["icd10_code"])


# ─────────────────────────────────────────────────────────────────────────────
# 5. dim_payer — Insurance Payer Reference
# ─────────────────────────────────────────────────────────────────────────────

SELF_PAY_NAME = "SELF-PAY"
SELF_PAY_KEY  = "0000000000000000000000000000000000000000000000000000000000000000"


def build_dim_payer(
    spark: SparkSession, tables: dict, silver_tables: dict
) -> None:
    """
    Build an insurance payer reference table from silver.insurance_claims.

    Assigns a SHA-256 surrogate key so the FK in fact_billing is stable
    even without a formal payer master file in the source.

    A special sentinel row is always included:
      payer_key = SELF_PAY_KEY  →  payer_name = "SELF-PAY"
    This covers invoices with no insurance claim (cash / self-pay patients)
    so that fact_billing → dim_payer referential integrity holds for all rows.

    Columns
    -------
    payer_key         string  SHA-256 surrogate PK (or SELF_PAY_KEY sentinel)
    payer_name        string  canonical payer name
    total_claims      long    claim records for this payer (0 for SELF-PAY)
    last_updated_at   timestamp
    """
    dim_tbl    = tables["dim_payer"]
    silver_src = silver_tables["insurance_claims"]

    # Rows from real insurance payers
    real_payers = (
        spark.table(silver_src)
        .filter(
            F.col("payer_name").isNotNull() &
            (F.trim(F.col("payer_name")) != "")
        )
        .groupBy("payer_name")
        .agg(F.count("*").alias("total_claims"))
        .withColumn("payer_key", F.sha2(F.lower(F.trim(F.col("payer_name"))), 256))
        .withColumn("last_updated_at", F.current_timestamp())
        .select("payer_key", "payer_name", "total_claims", "last_updated_at")
    )

    # Sentinel row for self-pay / uninsured invoices
    sentinel = spark.createDataFrame(
        [(SELF_PAY_KEY, SELF_PAY_NAME, 0)],
        ["payer_key", "payer_name", "total_claims"],
    ).withColumn("last_updated_at", F.current_timestamp())

    dim = real_payers.unionByName(sentinel)

    overwrite_table(spark, dim, dim_tbl)
    optimize(spark, dim_tbl, ["payer_key"])


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def build_all_dimensions(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    date_start: str = "2015-01-01",
    date_end:   str = "2035-12-31",
) -> None:
    """Build (or refresh) all Phase 1 gold reference dimensions."""
    print("=" * 65)
    print("  GOLD LAYER — Building reference dimensions")
    print("=" * 65)

    print("\n[1/5] dim_date")
    build_dim_date(spark, tables, date_start, date_end)

    print("\n[2/5] dim_department")
    build_dim_department(spark, tables, silver_tables)

    print("\n[3/5] dim_medication")
    build_dim_medication(spark, tables, silver_tables)

    print("\n[4/5] dim_diagnosis_code")
    build_dim_diagnosis_code(spark, tables, silver_tables)

    print("\n[5/5] dim_payer")
    build_dim_payer(spark, tables, silver_tables)

    print("\n✅  All reference dimensions built successfully.")
