# src/gold/facts.py
"""
Gold layer — fact tables (full schema, all source domains).

ERD → Silver → Gold mapping
════════════════════════════════════════════════════════════════════════════════
EMR
  encounters (OUTPATIENT / EMERGENCY / WALK-IN)
                       →  gold.fact_appointments
  encounters (ALL) + diagnoses
                       →  gold.fact_medical_records
  admissions           →  gold.fact_admissions
  procedures           →  gold.fact_surgeries

Laboratory
  lab_results + lab_orders
                       →  gold.fact_lab_results

Pharmacy
  prescriptions        →  gold.fact_prescriptions
  pharmacy_transactions→  gold.fact_pharmacy_dispenses

Billing
  invoices + payments + insurance_claims
                       →  gold.fact_billing
════════════════════════════════════════════════════════════════════════════════

Surrogate Key Strategy
----------------------
All fact tables carry:
  <fact>_sk  BIGINT  — monotonically_increasing_id() generated at full rebuild.
                        Stable within a single pipeline run; reset on DROP+rebuild.

Dimension FK columns
--------------------
  date_key         INT   → dim_date.date_key       (YYYYMMDD)
  dept_sk          INT   → dim_department.dept_sk
  medication_sk    INT   → dim_medication.medication_sk
  diagnosis_sk     INT   → dim_diagnosis_code.diagnosis_sk
  payer_sk         INT   → dim_payer.payer_sk       (0 = SELF-PAY)
  enterprise_patient_id STRING → dim_patient_enterprise
  enterprise_doctor_id  STRING → dim_doctor_enterprise

Natural keys (degenerate dimensions kept inline)
--------------------
  encounter_id, admission_id, procedure_id, etc. — kept as string NKs
  for traceability back to source silver tables.

Bridge map column schemas (from identity-resolution notebooks)
  patients_map: _source_patient_key, enterprise_patient_id,
                source_branch, source_patient_id, match_method, match_status
  doctors_map:  _source_doctor_key, enterprise_doctor_id,
                source_branch, source_doctor_id, match_method, match_status
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window

from src.gold.writer import upsert_table, optimize
from src.gold.dimensions import SELF_PAY_SK, SELF_PAY_NAME


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

def _patient_bridge(spark: SparkSession, bridge_tables: dict) -> DataFrame:
    """
    (source_branch, source_patient_id) → enterprise_patient_id.
    Pre-aliased to avoid column-name collisions on join.
    """
    return (
        spark.table(bridge_tables["patient_map"])
        .filter(F.col("match_status") != "DELETED")
        .select(
            F.col("source_branch").alias("_pb_branch"),
            F.col("source_patient_id").alias("_pb_patient_id"),
            F.col("enterprise_patient_id"),
        )
        .distinct()
    )


def _doctor_bridge(spark: SparkSession, bridge_tables: dict) -> DataFrame:
    """
    (source_branch, source_doctor_id) → enterprise_doctor_id.
    Pre-aliased to avoid column-name collisions on join.
    """
    return (
        spark.table(bridge_tables["doctor_map"])
        .filter(F.col("match_status") != "DELETED")
        .select(
            F.col("source_branch").alias("_db_branch"),
            F.col("source_doctor_id").alias("_db_doctor_id"),
            F.col("enterprise_doctor_id"),
        )
        .distinct()
    )


def _date_key(col_expr) -> "Column":
    """Convert a date or timestamp column to an integer YYYYMMDD surrogate key."""
    return F.date_format(col_expr, "yyyyMMdd").cast("int")


def _resolve_patient(df: DataFrame, pat_bridge: DataFrame) -> DataFrame:
    """Inner-join source data against the patient bridge; drops bridge helper cols."""
    return (
        df.join(
            pat_bridge,
            (F.col("patient_id") == F.col("_pb_patient_id")) &
            (F.col("_source_branch") == F.col("_pb_branch")),
            how="inner",
        )
        .drop("_pb_branch", "_pb_patient_id")
    )


def _resolve_doctor(
    df: DataFrame,
    doc_bridge: DataFrame,
    source_col: str = "doctor_id",
) -> DataFrame:
    """
    Left-join source data against the doctor bridge.
    `source_col` is the name of the doctor-id column in `df`
    (use 'surgeon_id' for fact_surgeries).
    """
    bridge = doc_bridge.withColumnRenamed("_db_doctor_id", "_db_id_joined")
    return (
        df.join(
            bridge,
            (F.col(source_col) == F.col("_db_id_joined")) &
            (F.col("_source_branch") == F.col("_db_branch")),
            how="left",
        )
        .drop("_db_branch", "_db_id_joined")
    )


def _lookup_dept_sk(spark: SparkSession, tables: dict) -> DataFrame:
    """Return a slim (dept_id → dept_sk) lookup from dim_department."""
    return (
        spark.table(tables["dim_department"])
        .select(
            F.col("dept_id").alias("_lkp_dept_id"),
            F.col("dept_sk"),
        )
    )


def _lookup_medication_sk(spark: SparkSession, tables: dict) -> DataFrame:
    """Return a slim (medication_id → medication_sk) lookup from dim_medication."""
    return (
        spark.table(tables["dim_medication"])
        .select(
            F.col("medication_id").alias("_lkp_med_id"),
            F.col("medication_sk"),
        )
    )


def _lookup_diagnosis_sk(spark: SparkSession, tables: dict) -> DataFrame:
    """Return a slim (icd10_code → diagnosis_sk) lookup from dim_diagnosis_code."""
    return (
        spark.table(tables["dim_diagnosis_code"])
        .select(
            F.col("icd10_code").alias("_lkp_icd10"),
            F.col("diagnosis_sk"),
        )
    )


def _lookup_payer_sk(spark: SparkSession, tables: dict) -> DataFrame:
    """Return a slim (payer_name → payer_sk) lookup from dim_payer."""
    return (
        spark.table(tables["dim_payer"])
        .filter(F.col("payer_sk") != SELF_PAY_SK)   # exclude sentinel from lookup
        .select(
            F.lower(F.trim(F.col("payer_name"))).alias("_lkp_payer_name"),
            F.col("payer_sk"),
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. fact_appointments
#    ERD: appointment  →  silver.encounters (outpatient / emergency visits)
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_appointments(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per outpatient / emergency / walk-in appointment.

    Grain: encounter_id

    Columns produced
    ----------------
    appointment_sk        bigint    Surrogate PK
    encounter_id          string    Natural Key (degenerate dim)
    enterprise_patient_id string    FK → dim_patient_enterprise
    enterprise_doctor_id  string    FK → dim_doctor_enterprise
    dept_sk               int       FK → dim_department
    dept_id               string    Natural Key (kept for reference)
    appointment_date_key  int       FK → dim_date
    appointment_date      timestamp raw
    encounter_type        string    OUTPATIENT / EMERGENCY / WALK-IN
    reason                string    chief complaint
    appointment_status    string    COMPLETED / CANCELLED / RESCHEDULED / …
    payment_amount        double    net invoice amount (nullable)
    payment_method        string    latest payment method for this visit (nullable)
    source_branch         string    originating hospital branch
    _loaded_at            timestamp pipeline load timestamp
    """
    fact_tbl   = tables["fact_appointments"]
    enc_src    = silver_tables["encounters"]
    inv_src    = silver_tables["invoices"]
    pay_src    = silver_tables["payments"]

    pat_bridge   = _patient_bridge(spark, bridge_tables)
    doc_bridge   = _doctor_bridge(spark, bridge_tables)
    dept_lkp     = _lookup_dept_sk(spark, tables)

    # ── Base encounters (outpatient / emergency only) ─────────────────────
    enc = (
        spark.table(enc_src)
        .filter(
            F.upper(F.col("encounter_type")).isin(
                ["OUTPATIENT", "EMERGENCY", "WALK-IN", "AMBULATORY"]
            )
        )
    )

    # ── Latest invoice amount per encounter ───────────────────────────────
    w_inv = Window.partitionBy("encounter_id").orderBy(F.desc("invoice_date"))
    inv_lookup = (
        spark.table(inv_src)
        .withColumn("_rn", F.row_number().over(w_inv))
        .filter(F.col("_rn") == 1)
        .select(
            "encounter_id",
            F.col("invoice_id").alias("_inv_id"),
            F.col("net_amount_due").alias("payment_amount"),
        )
    )

    # ── Latest non-cancelled payment method per invoice ───────────────────
    w_pay = Window.partitionBy("invoice_id").orderBy(F.desc("payment_date"))
    pay_lookup = (
        spark.table(pay_src)
        .filter(F.col("payment_status") != "CANCELLED")
        .withColumn("_rn", F.row_number().over(w_pay))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("invoice_id").alias("_pay_inv_id"),
            F.col("payment_method"),
        )
    )

    # ── Resolve IDs and assemble ──────────────────────────────────────────
    with_patient = _resolve_patient(enc, pat_bridge)
    with_doctor  = _resolve_doctor(with_patient, doc_bridge)

    fact = (
        with_doctor
        .join(inv_lookup, on="encounter_id", how="left")
        .join(pay_lookup, F.col("_inv_id") == F.col("_pay_inv_id"), how="left")
        .join(dept_lkp,   F.col("dept_id") == F.col("_lkp_dept_id"), how="left")
        .withColumn("appointment_date_key", _date_key(F.col("encounter_date")))
        .withColumn("appointment_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "appointment_sk",
            F.col("encounter_id"),
            "enterprise_patient_id",
            "enterprise_doctor_id",
            "dept_sk",
            "dept_id",
            "appointment_date_key",
            F.col("encounter_date").alias("appointment_date"),
            "encounter_type",
            "reason",
            F.col("status").alias("appointment_status"),
            "payment_amount",
            "payment_method",
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
        .drop("_inv_id", "_pay_inv_id", "_lkp_dept_id")
    )

    upsert_table(spark, fact, fact_tbl, merge_key="encounter_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "appointment_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 2. fact_admissions
#    ERD: roomrecords / bedrecords  →  silver.admissions
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_admissions(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per inpatient admission.

    Grain: admission_id

    Columns produced
    ----------------
    admission_sk          bigint    Surrogate PK
    admission_id          string    Natural Key
    encounter_id          string    FK → fact_medical_records (degenerate)
    enterprise_patient_id string    FK → dim_patient_enterprise
    dept_sk               int       FK → dim_department
    dept_id               string    Natural Key (kept for reference)
    admission_date_key    int       FK → dim_date
    discharge_date_key    int       FK → dim_date (nullable)
    admission_date        timestamp raw
    discharge_date        timestamp raw (nullable)
    length_of_stay_days   int       derived (discharge − admission)
    admission_type        string    EMERGENCY / ELECTIVE / URGENT
    discharge_disposition string    HOME / TRANSFERRED / DECEASED / …
    ward_name             string    denormalized (no dim_ward yet)
    room_no               string    denormalized
    bed_no                string    denormalized
    payment_method        string    latest payment method from billing (nullable)
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl  = tables["fact_admissions"]
    adm_src   = silver_tables["admissions"]
    inv_src   = silver_tables["invoices"]
    pay_src   = silver_tables["payments"]

    pat_bridge = _patient_bridge(spark, bridge_tables)
    dept_lkp   = _lookup_dept_sk(spark, tables)

    adm = spark.table(adm_src)

    # ── Latest payment method via invoice linked to this admission ────────
    w_inv = Window.partitionBy("admission_id").orderBy(F.desc("invoice_date"))
    inv_for_adm = (
        spark.table(inv_src)
        .filter(F.col("admission_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w_inv))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("admission_id").alias("_adm_inv_adm_id"),
            F.col("invoice_id").alias("_adm_invoice_id"),
        )
    )
    w_pay = Window.partitionBy("invoice_id").orderBy(F.desc("payment_date"))
    pay_method = (
        spark.table(pay_src)
        .filter(F.col("payment_status") != "CANCELLED")
        .withColumn("_rn", F.row_number().over(w_pay))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("invoice_id").alias("_pm_invoice_id"),
            F.col("payment_method"),
        )
    )

    # ── Resolve and assemble ──────────────────────────────────────────────
    with_patient = _resolve_patient(adm, pat_bridge)

    fact = (
        with_patient
        .join(inv_for_adm,
              F.col("admission_id") == F.col("_adm_inv_adm_id"),
              how="left")
        .join(pay_method,
              F.col("_adm_invoice_id") == F.col("_pm_invoice_id"),
              how="left")
        .join(dept_lkp,
              F.col("dept_id") == F.col("_lkp_dept_id"),
              how="left")
        .withColumn("admission_date_key", _date_key(F.col("admission_date")))
        .withColumn("discharge_date_key",
                    F.when(F.col("discharge_date").isNotNull(),
                           _date_key(F.col("discharge_date")))
                     .otherwise(F.lit(None).cast("int")))
        .withColumn("length_of_stay_days",
                    F.when(
                        F.col("discharge_date").isNotNull(),
                        F.datediff(
                            F.col("discharge_date").cast("date"),
                            F.col("admission_date").cast("date"),
                        )
                    ).otherwise(F.lit(None).cast("int")))
        .withColumn("admission_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "admission_sk",
            "admission_id",
            "encounter_id",
            "enterprise_patient_id",
            "dept_sk",
            "dept_id",
            "admission_date_key",
            "discharge_date_key",
            F.col("admission_date"),
            F.col("discharge_date"),
            "length_of_stay_days",
            "admission_type",
            "discharge_disposition",
            "ward_name",
            "room_no",
            "bed_no",
            "payment_method",
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
        .drop("_adm_inv_adm_id", "_adm_invoice_id", "_pm_invoice_id", "_lkp_dept_id")
    )

    upsert_table(spark, fact, fact_tbl, merge_key="admission_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "admission_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 3. fact_medical_records
#    ERD: medicalrecord  →  silver.encounters + silver.diagnoses (all types)
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_medical_records(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per clinical encounter (all types), enriched with the primary
    diagnosis from the diagnoses table.

    Grain: encounter_id

    Columns produced
    ----------------
    medical_record_sk     bigint    Surrogate PK
    encounter_id          string    Natural Key
    enterprise_patient_id string    FK → dim_patient_enterprise
    enterprise_doctor_id  string    FK → dim_doctor_enterprise
    dept_sk               int       FK → dim_department
    dept_id               string    Natural Key (kept for reference)
    visit_date_key        int       FK → dim_date
    visit_date            timestamp raw
    encounter_type        string    OUTPATIENT / INPATIENT / EMERGENCY
    reason                string    chief complaint / treatment reason
    encounter_status      string
    diagnosis_sk          int       FK → dim_diagnosis_code (primary Dx; nullable)
    primary_icd10_code    string    Natural Key (kept for reference; nullable)
    primary_diagnosis     string    denormalized label (nullable)
    diagnosis_count       int       total diagnoses for this encounter
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl  = tables["fact_medical_records"]
    enc_src   = silver_tables["encounters"]
    diag_src  = silver_tables["diagnoses"]

    pat_bridge   = _patient_bridge(spark, bridge_tables)
    doc_bridge   = _doctor_bridge(spark, bridge_tables)
    dept_lkp     = _lookup_dept_sk(spark, tables)
    diag_sk_lkp  = _lookup_diagnosis_sk(spark, tables)

    enc = spark.table(enc_src)

    # ── Primary diagnosis per encounter ───────────────────────────────────
    priority_w = Window.partitionBy("encounter_id").orderBy(
        F.when(F.upper(F.col("diagnosis_type")) == "PRIMARY", F.lit(0))
         .otherwise(F.lit(1)),
        F.col("diagnosis_date").desc(),
    )
    primary_diag = (
        spark.table(diag_src)
        .withColumn("_rn", F.row_number().over(priority_w))
        .filter(F.col("_rn") == 1)
        .select(
            "encounter_id",
            F.col("icd10_code").alias("primary_icd10_code"),
            F.col("diagnosis_name").alias("primary_diagnosis"),
        )
    )

    # ── Diagnosis count per encounter ─────────────────────────────────────
    diag_count = (
        spark.table(diag_src)
        .groupBy("encounter_id")
        .agg(F.count("*").alias("diagnosis_count"))
    )

    # ── Resolve IDs and assemble ──────────────────────────────────────────
    with_patient = _resolve_patient(enc, pat_bridge)
    with_doctor  = _resolve_doctor(with_patient, doc_bridge)

    fact = (
        with_doctor
        .join(primary_diag, on="encounter_id", how="left")
        .join(diag_count,   on="encounter_id", how="left")
        .join(dept_lkp,     F.col("dept_id") == F.col("_lkp_dept_id"), how="left")
        .join(
            diag_sk_lkp,
            F.col("primary_icd10_code") == F.col("_lkp_icd10"),
            how="left",
        )
        .withColumn("visit_date_key", _date_key(F.col("encounter_date")))
        .withColumn("medical_record_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "medical_record_sk",
            "encounter_id",
            "enterprise_patient_id",
            "enterprise_doctor_id",
            "dept_sk",
            "dept_id",
            "visit_date_key",
            F.col("encounter_date").alias("visit_date"),
            "encounter_type",
            "reason",
            F.col("status").alias("encounter_status"),
            "diagnosis_sk",
            "primary_icd10_code",
            "primary_diagnosis",
            F.coalesce(F.col("diagnosis_count"), F.lit(0)).alias("diagnosis_count"),
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
        .drop("_lkp_dept_id", "_lkp_icd10")
    )

    upsert_table(spark, fact, fact_tbl, merge_key="encounter_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "visit_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 4. fact_surgeries
#    ERD: surgeryrecord  →  silver.procedures
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_surgeries(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per surgical procedure.

    Grain: procedure_id

    Note: nurse_id and helper_id are NOT part of this schema.
    The source procedures CSV does not contain these fields.
    They may be added in a future Phase when the source system is extended.

    Columns produced
    ----------------
    surgery_sk              bigint    Surrogate PK
    procedure_id            string    Natural Key
    encounter_id            string    FK → fact_medical_records (degenerate)
    admission_id            string    FK → fact_admissions (nullable, degenerate)
    enterprise_patient_id   string    FK → dim_patient_enterprise
    enterprise_surgeon_id   string    FK → dim_doctor_enterprise
    procedure_name          string    surgery / procedure name
    procedure_date_key      int       FK → dim_date
    procedure_date          date      raw
    start_time              string    HH:mm format
    end_time                string    HH:mm format
    duration_minutes        int       derived from start/end time (nullable)
    room_no                 string    denormalized operating room
    notes                   string
    source_branch           string
    _loaded_at              timestamp
    """
    fact_tbl   = tables["fact_surgeries"]
    proc_src   = silver_tables["procedures"]

    pat_bridge = _patient_bridge(spark, bridge_tables)
    doc_bridge = _doctor_bridge(spark, bridge_tables)

    procs = spark.table(proc_src)

    with_patient = _resolve_patient(procs, pat_bridge)
    with_surgeon = _resolve_doctor(with_patient, doc_bridge, source_col="surgeon_id")
    with_surgeon = with_surgeon.withColumnRenamed(
        "enterprise_doctor_id", "enterprise_surgeon_id"
    )

    fact = (
        with_surgeon
        .withColumn("procedure_date_key", _date_key(F.col("procedure_date")))
        # Duration in minutes from HH:mm strings
        .withColumn("_start_min",
                    F.when(
                        F.col("start_time").isNotNull() & (F.col("start_time") != ""),
                        (F.split(F.col("start_time"), ":")[0].cast("int") * 60 +
                         F.split(F.col("start_time"), ":")[1].cast("int"))
                    ).otherwise(F.lit(None).cast("int")))
        .withColumn("_end_min",
                    F.when(
                        F.col("end_time").isNotNull() & (F.col("end_time") != ""),
                        (F.split(F.col("end_time"), ":")[0].cast("int") * 60 +
                         F.split(F.col("end_time"), ":")[1].cast("int"))
                    ).otherwise(F.lit(None).cast("int")))
        .withColumn("duration_minutes",
                    F.when(
                        F.col("_start_min").isNotNull() & F.col("_end_min").isNotNull(),
                        F.col("_end_min") - F.col("_start_min")
                    ).otherwise(F.lit(None).cast("int")))
        .withColumn("surgery_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "surgery_sk",
            "procedure_id",
            "encounter_id",
            "admission_id",
            "enterprise_patient_id",
            "enterprise_surgeon_id",
            "procedure_name",
            "procedure_date_key",
            F.col("procedure_date"),
            "start_time",
            "end_time",
            "duration_minutes",
            "room_no",
            "notes",
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
        .drop("_start_min", "_end_min")
    )

    upsert_table(spark, fact, fact_tbl, merge_key="procedure_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "procedure_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 5. fact_lab_results
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_lab_results(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per lab result parameter measurement.

    Grain: result_id

    Joins lab_results → lab_orders to bring in encounter_id and the
    ordering doctor.

    Columns produced
    ----------------
    lab_result_sk         bigint    Surrogate PK
    result_id             string    Natural Key
    order_id              string    degenerate dimension
    encounter_id          string    FK → fact_medical_records (degenerate)
    enterprise_patient_id string    FK → dim_patient_enterprise
    enterprise_doctor_id  string    FK → dim_doctor_enterprise (ordering physician)
    test_name             string
    parameter_name        string
    result_value          double
    unit_of_measure       string
    reference_range       string    e.g. '3.5–5.0'
    abnormal_flag         string    NORMAL / HIGH / LOW / CRITICAL
    is_abnormal           boolean   derived flag
    result_date_key       int       FK → dim_date
    result_date           timestamp raw
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl    = tables["fact_lab_results"]
    results_src = silver_tables["lab_results"]
    orders_src  = silver_tables["lab_orders"]

    pat_bridge = _patient_bridge(spark, bridge_tables)

    # Latest version of each order (for encounter + doctor context)
    w_ord = Window.partitionBy("order_id").orderBy(F.col("_ingestion_timestamp").desc())
    orders = (
        spark.table(orders_src)
        .withColumn("_rn", F.row_number().over(w_ord))
        .filter(F.col("_rn") == 1)
        .select(
            "order_id",
            "encounter_id",
            F.col("doctor_id").alias("_ord_doctor_id"),
            F.col("_source_branch").alias("_ord_branch"),
        )
    )

    results = spark.table(results_src)

    # Join results → orders
    with_order = results.join(orders, on="order_id", how="left")

    # Resolve enterprise patient (from results.patient_id + results._source_branch)
    with_patient = _resolve_patient(with_order, pat_bridge)

    # Resolve enterprise doctor from ORDER branch / doctor (not result branch)
    doc_bridge_clean = (
        spark.table(bridge_tables["doctor_map"])
        .filter(F.col("match_status") != "DELETED")
        .select(
            F.col("source_branch").alias("_ord_bridge_branch"),
            F.col("source_doctor_id").alias("_ord_bridge_doc_id"),
            F.col("enterprise_doctor_id"),
        )
        .distinct()
    )

    with_doctor = with_patient.join(
        doc_bridge_clean,
        (F.col("_ord_doctor_id") == F.col("_ord_bridge_doc_id")) &
        (F.col("_ord_branch") == F.col("_ord_bridge_branch")),
        how="left",
    ).drop("_ord_bridge_branch", "_ord_bridge_doc_id", "_ord_doctor_id", "_ord_branch")

    fact = (
        with_doctor
        .withColumn("result_date_key", _date_key(F.col("result_date")))
        .withColumn("is_abnormal",
                    F.col("abnormal_flag").isin(["HIGH", "LOW", "CRITICAL"]))
        .withColumn("lab_result_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "lab_result_sk",
            "result_id",
            "order_id",
            "encounter_id",
            "enterprise_patient_id",
            "enterprise_doctor_id",
            "test_name",
            "parameter_name",
            "result_value",
            "unit_of_measure",
            "reference_range",
            "abnormal_flag",
            "is_abnormal",
            "result_date_key",
            F.col("result_date"),
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
    )

    upsert_table(spark, fact, fact_tbl, merge_key="result_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "result_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 6. fact_prescriptions
#    Pharmacy domain  →  silver.prescriptions
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_prescriptions(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per prescription line item.

    Grain: prescription_id

    Columns produced
    ----------------
    prescription_sk       bigint    Surrogate PK
    prescription_id       string    Natural Key
    encounter_id          string    FK → fact_medical_records (degenerate)
    enterprise_patient_id string    FK → dim_patient_enterprise
    enterprise_doctor_id  string    FK → dim_doctor_enterprise (prescribing physician)
    medication_sk         int       FK → dim_medication
    medication_id         string    Natural Key (kept for reference)
    medication_name       string    denormalized
    dosage                string
    frequency             string
    duration_days         int
    prescribed_date_key   int       FK → dim_date
    prescribed_date       date      raw
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl   = tables["fact_prescriptions"]
    rx_src     = silver_tables["prescriptions"]

    pat_bridge  = _patient_bridge(spark, bridge_tables)
    doc_bridge  = _doctor_bridge(spark, bridge_tables)
    med_lkp     = _lookup_medication_sk(spark, tables)

    rx = spark.table(rx_src)

    with_patient = _resolve_patient(rx, pat_bridge)
    with_doctor  = _resolve_doctor(with_patient, doc_bridge)

    fact = (
        with_doctor
        .join(med_lkp,
              F.col("medication_id") == F.col("_lkp_med_id"),
              how="left")
        .withColumn("prescribed_date_key", _date_key(F.col("prescribed_date")))
        .withColumn("prescription_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "prescription_sk",
            "prescription_id",
            "encounter_id",
            "enterprise_patient_id",
            "enterprise_doctor_id",
            "medication_sk",
            "medication_id",
            "medication_name",
            "dosage",
            "frequency",
            "duration_days",
            "prescribed_date_key",
            F.col("prescribed_date"),
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
        .drop("_lkp_med_id")
    )

    upsert_table(spark, fact, fact_tbl, merge_key="prescription_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "prescribed_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 7. fact_pharmacy_dispenses
#    Pharmacy domain  →  silver.pharmacy_transactions
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_pharmacy_dispenses(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per pharmacy dispense transaction.

    Grain: transaction_id

    Joins pharmacy_transactions → prescriptions to get encounter_id,
    doctor, and medication context.

    Columns produced
    ----------------
    dispense_sk           bigint    Surrogate PK
    transaction_id        string    Natural Key
    prescription_id       string    FK → fact_prescriptions (degenerate)
    encounter_id          string    FK → fact_medical_records (degenerate; via prescription)
    enterprise_patient_id string    FK → dim_patient_enterprise
    enterprise_doctor_id  string    FK → dim_doctor_enterprise (prescribing physician)
    medication_sk         int       FK → dim_medication
    medication_id         string    Natural Key (kept for reference)
    dispense_date_key     int       FK → dim_date
    dispense_date         timestamp raw
    quantity              int       units dispensed
    unit_price            double    price per unit at time of dispense
    total_amount          double    quantity × unit_price
    payment_method        string    CASH / CREDIT CARD / INSURANCE / INSTALLMENT / …
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl   = tables["fact_pharmacy_dispenses"]
    txn_src    = silver_tables["pharmacy_transactions"]
    rx_src     = silver_tables["prescriptions"]

    pat_bridge  = _patient_bridge(spark, bridge_tables)
    med_lkp     = _lookup_medication_sk(spark, tables)

    # Latest version of each prescription (for encounter + doctor + medication context)
    w_rx = Window.partitionBy("prescription_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )
    rx_ctx = (
        spark.table(rx_src)
        .withColumn("_rn", F.row_number().over(w_rx))
        .filter(F.col("_rn") == 1)
        .select(
            "prescription_id",
            F.col("encounter_id").alias("_rx_encounter_id"),
            F.col("medication_id").alias("_rx_medication_id"),
            F.col("doctor_id").alias("_rx_doctor_id"),
            F.col("_source_branch").alias("_rx_branch"),
        )
    )

    txn = spark.table(txn_src)

    # Join transactions → prescription context
    with_rx = txn.join(rx_ctx, on="prescription_id", how="left")

    # Resolve enterprise patient
    with_patient = _resolve_patient(with_rx, pat_bridge)

    # Resolve enterprise doctor from prescription's doctor_id + prescription's branch
    doc_bridge_clean = (
        spark.table(bridge_tables["doctor_map"])
        .filter(F.col("match_status") != "DELETED")
        .select(
            F.col("source_branch").alias("_phm_bridge_branch"),
            F.col("source_doctor_id").alias("_phm_bridge_doc_id"),
            F.col("enterprise_doctor_id"),
        )
        .distinct()
    )

    with_doctor = with_patient.join(
        doc_bridge_clean,
        (F.col("_rx_doctor_id") == F.col("_phm_bridge_doc_id")) &
        (F.col("_rx_branch") == F.col("_phm_bridge_branch")),
        how="left",
    ).drop("_phm_bridge_branch", "_phm_bridge_doc_id", "_rx_doctor_id", "_rx_branch")

    fact = (
        with_doctor
        .join(med_lkp,
              F.col("_rx_medication_id") == F.col("_lkp_med_id"),
              how="left")
        .withColumn("dispense_date_key", _date_key(F.col("dispense_date")))
        .withColumn("dispense_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "dispense_sk",
            "transaction_id",
            "prescription_id",
            F.col("_rx_encounter_id").alias("encounter_id"),
            "enterprise_patient_id",
            "enterprise_doctor_id",
            "medication_sk",
            F.col("_rx_medication_id").alias("medication_id"),
            "dispense_date_key",
            F.col("dispense_date"),
            "quantity",
            "unit_price",
            "total_amount",
            "payment_method",
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
        .drop("_lkp_med_id", "_rx_encounter_id", "_rx_medication_id")
    )

    upsert_table(spark, fact, fact_tbl, merge_key="transaction_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "dispense_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 8. fact_billing
#    Billing domain  →  silver.invoices + payments + insurance_claims
# ─────────────────────────────────────────────────────────────────────────────

def build_fact_billing(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """
    One row per invoice, enriched with payment and insurance-claim roll-ups.

    Grain: invoice_id

    Payer Resolution
    ----------------
    ALL insurance_claims records (regardless of claim_status) are used to
    identify the payer for each invoice.  Only APPROVED and SETTLED claims
    contribute to total_insurance_paid and total_claim_amount.

    Invoices with no matching insurance claim (self-pay / uninsured) receive
    payer_sk = 0  (the SELF-PAY sentinel row in dim_payer).

    Payment method is derived from the latest non-cancelled payment transaction
    linked to each invoice.

    Columns produced
    ----------------
    billing_sk                 bigint     Surrogate PK
    invoice_id                 string     Natural Key
    encounter_id               string     FK → fact_medical_records (degenerate)
    admission_id               string     FK → fact_admissions (degenerate; nullable)
    enterprise_patient_id      string     FK → dim_patient_enterprise
    invoice_date_key           int        FK → dim_date
    invoice_date               date       raw
    gross_amount               double
    discount_amount            double
    insurance_covered_amount   double     from invoice header
    net_amount_due             double
    invoice_status             string     PAID / PARTIAL / OUTSTANDING / CANCELLED
    total_amount_paid          double     sum of non-cancelled payments
    payment_count              long       number of payment transactions
    payment_method             string     most recent payment method (nullable)
    last_payment_date_key      int        FK → dim_date (nullable)
    outstanding_balance        double     net_amount_due − total_amount_paid
    payer_sk                   int        FK → dim_payer  (0 = SELF-PAY)
    payer_name                 string     denormalized payer name
    total_claim_amount         double     sum of approved/settled claim amounts
    total_insurance_paid       double     sum of (claim_amount × coverage_pct / 100)
    claim_count                long       number of approved/settled claims
    source_branch              string
    _loaded_at                 timestamp
    """
    fact_tbl   = tables["fact_billing"]
    inv_src    = silver_tables["invoices"]
    pay_src    = silver_tables["payments"]
    claim_src  = silver_tables["insurance_claims"]

    pat_bridge  = _patient_bridge(spark, bridge_tables)
    payer_lkp   = _lookup_payer_sk(spark, tables)

    invoices = spark.table(inv_src)

    # ── Payment roll-up per invoice ────────────────────────────────────────
    # Captures: total paid, count, last date, and latest payment method
    pay_agg = (
        spark.table(pay_src)
        .filter(F.col("payment_status") != "CANCELLED")
        .groupBy("invoice_id")
        .agg(
            F.sum("amount_paid").alias("total_amount_paid"),
            F.count("*").alias("payment_count"),
            F.max("payment_date").alias("last_payment_date"),
            F.first(F.col("payment_method"), ignorenulls=True).alias("payment_method"),
        )
    )

    # ── Payer attribution — uses ALL claims regardless of status ──────────
    # This ensures we always know WHO the payer is, even for DENIED/PENDING claims.
    payer_attr = (
        spark.table(claim_src)
        .filter(
            F.col("payer_name").isNotNull() &
            (F.trim(F.col("payer_name")) != "")
        )
        .groupBy("invoice_id")
        .agg(
            F.first(F.col("payer_name"), ignorenulls=True).alias("payer_name_raw"),
        )
    )

    # ── Financial claim roll-up — only APPROVED and SETTLED claims ─────────
    financial_claims = (
        spark.table(claim_src)
        .filter(
            F.upper(F.col("claim_status")).isin(["APPROVED", "SETTLED"])
        )
        .withColumn(
            "insurance_paid",
            F.col("claim_amount") * F.col("coverage_percentage") / 100.0,
        )
        .groupBy("invoice_id")
        .agg(
            F.sum("claim_amount").alias("total_claim_amount"),
            F.sum("insurance_paid").alias("total_insurance_paid"),
            F.count("*").alias("claim_count"),
        )
    )

    # ── Combine payer attribution + financial roll-up ─────────────────────
    claim_agg = payer_attr.join(financial_claims, on="invoice_id", how="left")

    # ── Resolve enterprise patient and assemble ───────────────────────────
    with_patient = _resolve_patient(invoices, pat_bridge)

    fact = (
        with_patient
        .join(pay_agg,   on="invoice_id", how="left")
        .join(claim_agg, on="invoice_id", how="left")
        # Look up integer payer_sk from dim_payer via payer_name
        .join(
            payer_lkp,
            F.lower(F.trim(F.col("payer_name_raw"))) == F.col("_lkp_payer_name"),
            how="left",
        )
        .withColumn("invoice_date_key", _date_key(F.col("invoice_date")))
        .withColumn("last_payment_date_key",
                    F.when(F.col("last_payment_date").isNotNull(),
                           _date_key(F.col("last_payment_date")))
                     .otherwise(F.lit(None).cast("int")))
        .withColumn("outstanding_balance",
                    F.col("net_amount_due") -
                    F.coalesce(F.col("total_amount_paid"), F.lit(0.0)))
        .withColumnRenamed("status", "invoice_status")
        .withColumn("billing_sk", F.monotonically_increasing_id())
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "billing_sk",
            "invoice_id",
            "encounter_id",
            "admission_id",
            "enterprise_patient_id",
            "invoice_date_key",
            F.col("invoice_date"),
            "gross_amount",
            "discount_amount",
            "insurance_covered_amount",
            "net_amount_due",
            "invoice_status",
            # Payment roll-up
            F.coalesce(F.col("total_amount_paid"), F.lit(0.0)).alias("total_amount_paid"),
            F.coalesce(F.col("payment_count"),     F.lit(0)).alias("payment_count"),
            "payment_method",
            "last_payment_date_key",
            "outstanding_balance",
            # Payer FK — default to SELF-PAY sentinel (payer_sk = 0) when no claim
            F.coalesce(F.col("payer_sk"),         F.lit(SELF_PAY_SK)).alias("payer_sk"),
            F.coalesce(F.col("payer_name_raw"),   F.lit(SELF_PAY_NAME)).alias("payer_name"),
            # Financial claim roll-up
            F.coalesce(F.col("total_claim_amount"),   F.lit(0.0)).alias("total_claim_amount"),
            F.coalesce(F.col("total_insurance_paid"), F.lit(0.0)).alias("total_insurance_paid"),
            F.coalesce(F.col("claim_count"),          F.lit(0)).alias("claim_count"),
            # Provenance
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
        .drop("_lkp_payer_name", "payer_name_raw")
    )

    upsert_table(spark, fact, fact_tbl, merge_key="invoice_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "invoice_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# Orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def build_all_facts(
    spark: SparkSession,
    tables: dict,
    silver_tables: dict,
    bridge_tables: dict,
) -> None:
    """Build (or incrementally refresh) all gold fact tables."""
    print("=" * 65)
    print("  GOLD LAYER — Building fact tables")
    print("=" * 65)

    print("\n[1/8] fact_appointments")
    build_fact_appointments(spark, tables, silver_tables, bridge_tables)

    print("\n[2/8] fact_admissions")
    build_fact_admissions(spark, tables, silver_tables, bridge_tables)

    print("\n[3/8] fact_medical_records")
    build_fact_medical_records(spark, tables, silver_tables, bridge_tables)

    print("\n[4/8] fact_surgeries")
    build_fact_surgeries(spark, tables, silver_tables, bridge_tables)

    print("\n[5/8] fact_lab_results")
    build_fact_lab_results(spark, tables, silver_tables, bridge_tables)

    print("\n[6/8] fact_prescriptions")
    build_fact_prescriptions(spark, tables, silver_tables, bridge_tables)

    print("\n[7/8] fact_pharmacy_dispenses")
    build_fact_pharmacy_dispenses(spark, tables, silver_tables, bridge_tables)

    print("\n[8/8] fact_billing")
    build_fact_billing(spark, tables, silver_tables, bridge_tables)

    print("\n✅  All fact tables built successfully.")
