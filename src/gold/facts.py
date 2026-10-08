# src/gold/facts.py
"""
Gold layer — fact tables  (Phase 1).

ERD → Silver → Gold mapping
════════════════════════════════════════════════════════════════════════════════
ERD: appointment       →  silver.encounters  (OUTPATIENT / EMERGENCY)
                       →  gold.fact_appointments

ERD: medicalrecord     →  silver.encounters + silver.diagnoses (all types)
                       →  gold.fact_medical_records

ERD: roomrecords /
     bedrecords        →  silver.admissions
                       →  gold.fact_admissions

ERD: surgeryrecord     →  silver.procedures
                       →  gold.fact_surgeries

Lab system             →  silver.lab_results + silver.lab_orders
                       →  gold.fact_lab_results

Billing system         →  silver.invoices + silver.payments + silver.insurance_claims
                       →  gold.fact_billing
════════════════════════════════════════════════════════════════════════════════

Bridge map column schemas (from identity-resolution notebooks)
  patients_map: _source_patient_key, enterprise_patient_id,
                source_branch, source_patient_id, match_method, match_status
  doctors_map:  _source_doctor_key, enterprise_doctor_id,
                source_branch, source_doctor_id, match_method, match_status
"""

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.window import Window

from src.gold.writer import upsert_table, optimize
from src.gold.dimensions import SELF_PAY_KEY, SELF_PAY_NAME


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
    `source_col` is the name of the doctor-id column in `df` (default: doctor_id,
    for surgeries use: surgeon_id).
    Drops bridge helper cols after join.
    """
    # Rename bridge cols to avoid clashes when source_col != doctor_id
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
    One row per outpatient / emergency appointment.

    Grain: encounter_id

    ERD field mapping
    -----------------
    appointment_id    ← encounter_id
    patient_id        ← enterprise_patient_id  (resolved)
    doct_id           ← enterprise_doctor_id   (resolved)
    dept              ← dept_id
    reason            ← reason
    appointment_date  ← encounter_date
    payment_amount    ← invoices.net_amount_due (joined via encounter_id, optional)
    mode_of_payment   ← payments.payment_method (joined via invoice → encounter)
    appointment_status← status

    Columns produced
    ----------------
    encounter_id          string    NK (maps to ERD: appointment_id)
    enterprise_patient_id string    FK → dim_patient
    enterprise_doctor_id  string    FK → dim_doctor
    dept_id               string    FK → dim_department
    appointment_date_key  int       FK → dim_date
    appointment_date      timestamp raw
    encounter_type        string    OUTPATIENT / EMERGENCY
    reason                string
    appointment_status    string
    payment_amount        double    net amount from invoice (nullable)
    payment_method        string    latest payment method (nullable)
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl   = tables["fact_appointments"]
    enc_src    = silver_tables["encounters"]
    inv_src    = silver_tables["invoices"]
    pay_src    = silver_tables["payments"]

    pat_bridge = _patient_bridge(spark, bridge_tables)
    doc_bridge = _doctor_bridge(spark, bridge_tables)

    # ── Base encounters (outpatient/emergency only) ────────────────────────
    enc = (
        spark.table(enc_src)
        .filter(
            F.upper(F.col("encounter_type")).isin(
                ["OUTPATIENT", "EMERGENCY", "WALK-IN", "AMBULATORY"]
            )
        )
    )

    # ── Invoice amount (latest invoice per encounter) ──────────────────────
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

    # ── Latest payment method per invoice ─────────────────────────────────
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

    # ── Resolve IDs and assemble ───────────────────────────────────────────
    with_patient = _resolve_patient(enc, pat_bridge)
    with_doctor  = _resolve_doctor(with_patient, doc_bridge)

    fact = (
        with_doctor
        .join(inv_lookup, on="encounter_id", how="left")
        .join(
            pay_lookup,
            F.col("_inv_id") == F.col("_pay_inv_id"),
            how="left",
        )
        .withColumn("appointment_date_key", _date_key(F.col("encounter_date")))
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            F.col("encounter_id"),
            "enterprise_patient_id",
            "enterprise_doctor_id",
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
    )

    upsert_table(spark, fact, fact_tbl, merge_key="encounter_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "appointment_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 2. fact_medical_records
#    ERD: medicalrecord  →  silver.encounters + silver.diagnoses
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

    ERD field mapping
    -----------------
    record_id          ← encounter_id
    doct_id            ← enterprise_doctor_id
    patient_id         ← enterprise_patient_id
    visit_date         ← encounter_date
    diagnosis          ← diagnoses.diagnosis_name (PRIMARY type, latest)
    treatment          ← reason (closest available proxy)
    next_visit         ← NULL (not captured in current silver schema)
    [vital signs]      ← NULL (not captured in current silver schema —
                          curr_weight, curr_height, curr_blood_pressure,
                          curr_temp need to be added to ENCOUNTER_SCHEMA)

    Columns produced
    ----------------
    encounter_id          string    NK (maps to ERD: record_id)
    enterprise_patient_id string    FK → dim_patient
    enterprise_doctor_id  string    FK → dim_doctor
    dept_id               string    FK → dim_department
    visit_date_key        int       FK → dim_date
    visit_date            timestamp raw
    encounter_type        string    OUTPATIENT / INPATIENT / EMERGENCY
    reason                string    chief complaint / treatment reason
    encounter_status      string
    primary_icd10_code    string    FK → dim_diagnosis_code (nullable)
    primary_diagnosis     string    denormalized label (nullable)
    diagnosis_count       int       total diagnoses for this encounter
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl  = tables["fact_medical_records"]
    enc_src   = silver_tables["encounters"]
    diag_src  = silver_tables["diagnoses"]

    pat_bridge = _patient_bridge(spark, bridge_tables)
    doc_bridge = _doctor_bridge(spark, bridge_tables)

    enc = spark.table(enc_src)

    # ── Primary diagnosis per encounter ───────────────────────────────────
    # Take the most recent PRIMARY diagnosis; fall back to any diagnosis.
    priority_w = Window.partitionBy("encounter_id").orderBy(
        # PRIMARY first, then SECONDARY, then by date desc
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
        .withColumn("visit_date_key", _date_key(F.col("encounter_date")))
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "encounter_id",
            "enterprise_patient_id",
            "enterprise_doctor_id",
            "dept_id",
            "visit_date_key",
            F.col("encounter_date").alias("visit_date"),
            "encounter_type",
            "reason",
            F.col("status").alias("encounter_status"),
            "primary_icd10_code",
            "primary_diagnosis",
            F.coalesce(F.col("diagnosis_count"), F.lit(0)).alias("diagnosis_count"),
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
    )

    upsert_table(spark, fact, fact_tbl, merge_key="encounter_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "visit_date_key"])


# ─────────────────────────────────────────────────────────────────────────────
# 3. fact_admissions
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

    ERD field mapping
    -----------------
    admission_id       ← admission_id
    patient_id         ← enterprise_patient_id
    room_no            ← room_no  (denormalized — no dim_room yet)
    nurse_id           ← NULL     (not in silver — Phase 2)
    helper_id          ← NULL     (not in silver — Phase 2)
    admission_date     ← admission_date
    discharge_date     ← discharge_date
    mode_of_payment    ← payments.payment_method (joined via invoice)

    Columns produced
    ----------------
    admission_id          string    NK
    encounter_id          string    FK → fact_medical_records
    enterprise_patient_id string    FK → dim_patient
    dept_id               string    FK → dim_department
    admission_date_key    int       FK → dim_date
    discharge_date_key    int       FK → dim_date (nullable)
    admission_date        timestamp raw
    discharge_date        timestamp raw (nullable)
    length_of_stay_days   int       derived
    admission_type        string    EMERGENCY / ELECTIVE / URGENT
    discharge_disposition string    HOME / TRANSFERRED / DECEASED / …
    ward_name             string    denormalized (no dim_ward yet)
    room_no               string    denormalized (no dim_room yet)
    bed_no                string    denormalized (no dim_bed yet)
    nurse_id              string    NULL — Phase 2 placeholder
    helper_id             string    NULL — Phase 2 placeholder
    payment_method        string    from billing (nullable)
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl  = tables["fact_admissions"]
    adm_src   = silver_tables["admissions"]
    inv_src   = silver_tables["invoices"]
    pay_src   = silver_tables["payments"]

    pat_bridge = _patient_bridge(spark, bridge_tables)

    adm = spark.table(adm_src)

    # ── Payment method lookup ─────────────────────────────────────────────
    # invoices link to admission_id; take latest payment method
    w_inv = Window.partitionBy("admission_id").orderBy(F.desc("invoice_date"))
    inv_for_adm = (
        spark.table(inv_src)
        .filter(F.col("admission_id").isNotNull())
        .withColumn("_rn", F.row_number().over(w_inv))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("admission_id").alias("_adm_inv_id"),
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
        .join(
            inv_for_adm,
            F.col("admission_id") == F.col("_adm_inv_id"),
            how="left",
        )
        .join(
            pay_method,
            F.col("_adm_invoice_id") == F.col("_pm_invoice_id"),
            how="left",
        )
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
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
            "admission_id",
            "encounter_id",
            "enterprise_patient_id",
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
            F.lit(None).cast("string").alias("nurse_id"),    # Phase 2
            F.lit(None).cast("string").alias("helper_id"),   # Phase 2
            "payment_method",
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
    )

    upsert_table(spark, fact, fact_tbl, merge_key="admission_id")
    optimize(spark, fact_tbl, ["enterprise_patient_id", "admission_date_key"])


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

    ERD field mapping
    -----------------
    surgery_id         ← procedure_id
    patient_id         ← enterprise_patient_id
    surgeon_id         ← enterprise_surgeon_id  (via doctor bridge on surgeon_id)
    surgery_date       ← procedure_date
    start_time         ← start_time
    end_time           ← end_time
    room_no            ← room_no  (denormalized)
    notes              ← notes
    nurse_id           ← NULL     (Phase 2)
    helper_id          ← NULL     (Phase 2)

    Columns produced
    ----------------
    procedure_id            string    NK (maps to ERD: surgery_id)
    encounter_id            string    FK → fact_medical_records
    admission_id            string    FK → fact_admissions (nullable)
    enterprise_patient_id   string    FK → dim_patient
    enterprise_surgeon_id   string    FK → dim_doctor
    procedure_name          string    surgery / procedure name
    procedure_date_key      int       FK → dim_date
    procedure_date          date      raw
    start_time              string
    end_time                string
    duration_minutes        int       derived from start_time / end_time (nullable)
    room_no                 string    denormalized
    notes                   string
    nurse_id                string    NULL — Phase 2
    helper_id               string    NULL — Phase 2
    source_branch           string
    _loaded_at              timestamp
    """
    fact_tbl   = tables["fact_surgeries"]
    proc_src   = silver_tables["procedures"]

    pat_bridge = _patient_bridge(spark, bridge_tables)
    doc_bridge = _doctor_bridge(spark, bridge_tables)

    procs = spark.table(proc_src)

    with_patient = _resolve_patient(procs, pat_bridge)

    # Resolve surgeon via surgeon_id column
    with_surgeon = _resolve_doctor(with_patient, doc_bridge, source_col="surgeon_id")
    # Rename enterprise_doctor_id → enterprise_surgeon_id for clarity
    with_surgeon = with_surgeon.withColumnRenamed(
        "enterprise_doctor_id", "enterprise_surgeon_id"
    )

    # Derive duration in minutes from HH:mm strings (best-effort)
    def _parse_minutes(col_name):
        """Parse 'HH:mm' or 'HH:mm:ss' string to total minutes past midnight."""
        parts = F.split(F.col(col_name), ":")
        return F.col(parts[0].cast("int") * 60 + parts[1].cast("int"))

    fact = (
        with_surgeon
        .withColumn("procedure_date_key", _date_key(F.col("procedure_date")))
        # Duration: only when both start + end present and parseable
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
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
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
            F.lit(None).cast("string").alias("nurse_id"),   # Phase 2
            F.lit(None).cast("string").alias("helper_id"),  # Phase 2
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
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
    result_id             string    NK
    order_id              string    degenerate dimension
    encounter_id          string    FK → fact_medical_records
    enterprise_patient_id string    FK → dim_patient
    enterprise_doctor_id  string    FK → dim_doctor (ordering physician)
    test_name             string
    parameter_name        string
    result_value          double
    unit_of_measure       string
    reference_range       string    e.g. '3.5–5.0'
    abnormal_flag         string    NORMAL / HIGH / LOW / CRITICAL
    is_abnormal           boolean   derived
    result_date_key       int       FK → dim_date
    result_date           timestamp raw
    source_branch         string
    _loaded_at            timestamp
    """
    fact_tbl    = tables["fact_lab_results"]
    results_src = silver_tables["lab_results"]
    orders_src  = silver_tables["lab_orders"]

    pat_bridge = _patient_bridge(spark, bridge_tables)
    doc_bridge = _doctor_bridge(spark, bridge_tables)

    # Latest version of each order (for encounter + doctor context)
    w_ord = Window.partitionBy("order_id").orderBy(
        F.col("_ingestion_timestamp").desc()
    )
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
    doc_bridge_ord = (
        doc_bridge
        .withColumnRenamed("_db_branch",    "_dbo_branch")
        .withColumnRenamed("_db_id_joined", "_dbo_doctor_id")
    )
    # Re-build doc bridge with clean aliases for this specific join
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
    ).drop("_ord_bridge_branch", "_ord_bridge_doc_id",
           "_ord_doctor_id", "_ord_branch")

    fact = (
        with_doctor
        .withColumn("result_date_key", _date_key(F.col("result_date")))
        .withColumn("is_abnormal",
                    F.col("abnormal_flag").isin(["HIGH", "LOW", "CRITICAL"]))
        .withColumn("_loaded_at", F.current_timestamp())
        .select(
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
# 6. fact_billing
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

    Columns produced
    ----------------
    invoice_id                 string     NK
    encounter_id               string     FK → fact_medical_records
    admission_id               string     FK → fact_admissions (nullable)
    enterprise_patient_id      string     FK → dim_patient
    invoice_date_key           int        FK → dim_date
    invoice_date               date       raw
    gross_amount               double
    discount_amount            double
    insurance_covered_amount   double     from invoice header
    net_amount_due             double
    invoice_status             string     PAID / PARTIAL / OUTSTANDING / CANCELLED
    total_amount_paid          double     sum of payments
    payment_count              long       number of payment transactions
    last_payment_date_key      int        FK → dim_date (nullable)
    outstanding_balance        double     net_amount_due − total_amount_paid
    payer_key                  string     FK → dim_payer (main insurer)
    payer_name                 string     denormalized
    total_claim_amount         double     sum of insurance claim amounts
    total_insurance_paid       double     claim_amount × coverage_pct / 100
    claim_count                long
    source_branch              string
    _loaded_at                 timestamp
    """
    fact_tbl   = tables["fact_billing"]
    inv_src    = silver_tables["invoices"]
    pay_src    = silver_tables["payments"]
    claim_src  = silver_tables["insurance_claims"]

    pat_bridge = _patient_bridge(spark, bridge_tables)

    invoices = spark.table(inv_src)

    # ── Payment roll-up per invoice ───────────────────────────────────────
    pay_agg = (
        spark.table(pay_src)
        .filter(F.col("payment_status") != "CANCELLED")
        .groupBy("invoice_id")
        .agg(
            F.sum("amount_paid").alias("total_amount_paid"),
            F.count("*").alias("payment_count"),
            F.max("payment_date").alias("last_payment_date"),
        )
    )

    # ── Insurance claim roll-up per invoice ───────────────────────────────
    claim_agg = (
        spark.table(claim_src)
        .filter(F.col("claim_status").isin(["APPROVED", "SETTLED", "SUBMITTED"]))
        .withColumn(
            "insurance_paid",
            F.col("claim_amount") * F.col("coverage_percentage") / 100.0,
        )
        .groupBy("invoice_id")
        .agg(
            F.sum("claim_amount").alias("total_claim_amount"),
            F.sum("insurance_paid").alias("total_insurance_paid"),
            F.count("*").alias("claim_count"),
            # Most recent payer name → FK to dim_payer
            F.first(F.col("payer_name"), ignorenulls=True).alias("payer_name"),
        )
        .withColumn(
            "payer_key",
            F.sha2(F.lower(F.trim(F.col("payer_name"))), 256),
        )
    )

    # ── Resolve enterprise patient and assemble ───────────────────────────
    with_patient = _resolve_patient(invoices, pat_bridge)

    fact = (
        with_patient
        .join(pay_agg,   on="invoice_id", how="left")
        .join(claim_agg, on="invoice_id", how="left")
        .withColumn("invoice_date_key", _date_key(F.col("invoice_date")))
        .withColumn("last_payment_date_key",
                    F.when(F.col("last_payment_date").isNotNull(),
                           _date_key(F.col("last_payment_date")))
                     .otherwise(F.lit(None).cast("int")))
        .withColumn("outstanding_balance",
                    F.col("net_amount_due") -
                    F.coalesce(F.col("total_amount_paid"), F.lit(0.0)))
        .withColumn("_loaded_at", F.current_timestamp())
        .withColumnRenamed("status", "invoice_status")
        .select(
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
            "last_payment_date_key",
            "outstanding_balance",
            # Claim roll-up — self-pay invoices (no claim) get sentinel FK
            F.coalesce(F.col("payer_key"),  F.lit(SELF_PAY_KEY)).alias("payer_key"),
            F.coalesce(F.col("payer_name"), F.lit(SELF_PAY_NAME)).alias("payer_name"),
            F.coalesce(F.col("total_claim_amount"),   F.lit(0.0)).alias("total_claim_amount"),
            F.coalesce(F.col("total_insurance_paid"), F.lit(0.0)).alias("total_insurance_paid"),
            F.coalesce(F.col("claim_count"),          F.lit(0)).alias("claim_count"),
            # Provenance
            F.col("_source_branch").alias("source_branch"),
            "_loaded_at",
        )
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
    """Build (or incrementally refresh) all Phase 1 gold fact tables."""
    print("=" * 65)
    print("  GOLD LAYER — Building fact tables")
    print("=" * 65)

    print("\n[1/6] fact_appointments")
    build_fact_appointments(spark, tables, silver_tables, bridge_tables)

    print("\n[2/6] fact_medical_records")
    build_fact_medical_records(spark, tables, silver_tables, bridge_tables)

    print("\n[3/6] fact_admissions")
    build_fact_admissions(spark, tables, silver_tables, bridge_tables)

    print("\n[4/6] fact_surgeries")
    build_fact_surgeries(spark, tables, silver_tables, bridge_tables)

    print("\n[5/6] fact_lab_results")
    build_fact_lab_results(spark, tables, silver_tables, bridge_tables)

    print("\n[6/6] fact_billing")
    build_fact_billing(spark, tables, silver_tables, bridge_tables)

    print("\n✅  All fact tables built successfully.")
