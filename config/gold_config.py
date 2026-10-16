# config/gold_config.py
"""
Gold layer table names — Full Schema (all source domains).

All tables live in hospital_catalog.gold.*

Dimensions
----------
dim_date              — calendar spine (date_key = YYYYMMDD int, already the surrogate)
dim_department        — hospital departments          (dept_sk int surrogate)
dim_medication        — pharmacy drug reference        (medication_sk int surrogate)
dim_diagnosis_code    — ICD-10 code reference          (diagnosis_sk int surrogate)
dim_payer             — insurance payers               (payer_sk int surrogate, 0 = SELF-PAY)
dim_patient           — enterprise patient golden rec  (managed by identity notebooks)
dim_doctor            — enterprise doctor golden rec   (managed by identity notebooks)

Fact Tables
-----------
fact_appointments     — outpatient / emergency visits  (grain: encounter_id)
fact_admissions       — inpatient stays                (grain: admission_id)
fact_medical_records  — all clinical encounters + primary Dx (grain: encounter_id)
fact_surgeries        — surgical procedures            (grain: procedure_id)
fact_lab_results      — individual lab result params   (grain: result_id)
fact_prescriptions    — one row per prescription line  (grain: prescription_id)
fact_pharmacy_dispenses — pharmacy dispense txns       (grain: transaction_id)
fact_billing          — invoice-level billing roll-up  (grain: invoice_id)
"""

from config.ingestion_config import CATALOG

GOLD_SCHEMA   = "gold"
SILVER_SCHEMA = "silver"

# ─────────────────────────────────────────────────────────────────────────────
# Gold Tables
# ─────────────────────────────────────────────────────────────────────────────

GOLD_TABLES = {

    # ── Reference / Type-1 Dimensions ─────────────────────────────────────
    "dim_date":           f"{CATALOG}.{GOLD_SCHEMA}.dim_date",
    "dim_department":     f"{CATALOG}.{GOLD_SCHEMA}.dim_department",
    "dim_medication":     f"{CATALOG}.{GOLD_SCHEMA}.dim_medication",
    "dim_diagnosis_code": f"{CATALOG}.{GOLD_SCHEMA}.dim_diagnosis_code",
    "dim_payer":          f"{CATALOG}.{GOLD_SCHEMA}.dim_payer",

    # ── Enterprise Identity Dimensions (written by identity notebooks) ─────
    "dim_patient":        f"{CATALOG}.{GOLD_SCHEMA}.dim_patient_enterprise",
    "dim_doctor":         f"{CATALOG}.{GOLD_SCHEMA}.dim_doctor_enterprise",

    # ── Fact Tables ────────────────────────────────────────────────────────
    "fact_appointments":      f"{CATALOG}.{GOLD_SCHEMA}.fact_appointments",
    "fact_admissions":        f"{CATALOG}.{GOLD_SCHEMA}.fact_admissions",
    "fact_medical_records":   f"{CATALOG}.{GOLD_SCHEMA}.fact_medical_records",
    "fact_surgeries":         f"{CATALOG}.{GOLD_SCHEMA}.fact_surgeries",
    "fact_lab_results":       f"{CATALOG}.{GOLD_SCHEMA}.fact_lab_results",
    "fact_prescriptions":     f"{CATALOG}.{GOLD_SCHEMA}.fact_prescriptions",
    "fact_pharmacy_dispenses":f"{CATALOG}.{GOLD_SCHEMA}.fact_pharmacy_dispenses",
    "fact_billing":           f"{CATALOG}.{GOLD_SCHEMA}.fact_billing",
}


# ─────────────────────────────────────────────────────────────────────────────
# Silver Source Tables  (read-only inputs)
# ─────────────────────────────────────────────────────────────────────────────

SILVER_TABLES = {
    # EMR
    "departments":           f"{CATALOG}.{SILVER_SCHEMA}.departments",
    "encounters":            f"{CATALOG}.{SILVER_SCHEMA}.encounters",
    "diagnoses":             f"{CATALOG}.{SILVER_SCHEMA}.diagnoses",
    "procedures":            f"{CATALOG}.{SILVER_SCHEMA}.procedures",
    "admissions":            f"{CATALOG}.{SILVER_SCHEMA}.admissions",
    # Laboratory
    "lab_orders":            f"{CATALOG}.{SILVER_SCHEMA}.lab_orders",
    "lab_results":           f"{CATALOG}.{SILVER_SCHEMA}.lab_results",
    # Pharmacy
    "medications":           f"{CATALOG}.{SILVER_SCHEMA}.medications",
    "prescriptions":         f"{CATALOG}.{SILVER_SCHEMA}.prescriptions",
    "pharmacy_transactions": f"{CATALOG}.{SILVER_SCHEMA}.pharmacy_transactions",
    # Billing
    "invoices":              f"{CATALOG}.{SILVER_SCHEMA}.invoices",
    "payments":              f"{CATALOG}.{SILVER_SCHEMA}.payments",
    "insurance_claims":      f"{CATALOG}.{SILVER_SCHEMA}.insurance_claims",
}


# ─────────────────────────────────────────────────────────────────────────────
# Identity Bridge Maps  (source_id → enterprise_id)
# ─────────────────────────────────────────────────────────────────────────────

BRIDGE_TABLES = {
    "patient_map": f"{CATALOG}.{SILVER_SCHEMA}.patients_map_probabilistic_enterprise",
    "doctor_map":  f"{CATALOG}.{SILVER_SCHEMA}.doctors_map_probabilistic_enterprise",
}


# ─────────────────────────────────────────────────────────────────────────────
# Date spine parameters
# ─────────────────────────────────────────────────────────────────────────────

DATE_SPINE_START = "2015-01-01"
DATE_SPINE_END   = "2035-12-31"


# ─────────────────────────────────────────────────────────────────────────────
# Rebuild / Teardown SQL  (run in Databricks SQL editor to start clean)
# ─────────────────────────────────────────────────────────────────────────────

TEARDOWN_SQL = """
-- ============================================================
-- DROP all gold tables before a full pipeline rebuild.
-- Do NOT drop dim_patient_enterprise or dim_doctor_enterprise
-- unless you also plan to re-run identity-resolution notebooks.
-- ============================================================

DROP TABLE IF EXISTS hospital_catalog.gold.fact_billing;
DROP TABLE IF EXISTS hospital_catalog.gold.fact_pharmacy_dispenses;
DROP TABLE IF EXISTS hospital_catalog.gold.fact_prescriptions;
DROP TABLE IF EXISTS hospital_catalog.gold.fact_lab_results;
DROP TABLE IF EXISTS hospital_catalog.gold.fact_surgeries;
DROP TABLE IF EXISTS hospital_catalog.gold.fact_medical_records;
DROP TABLE IF EXISTS hospital_catalog.gold.fact_admissions;
DROP TABLE IF EXISTS hospital_catalog.gold.fact_appointments;
DROP TABLE IF EXISTS hospital_catalog.gold.dim_payer;
DROP TABLE IF EXISTS hospital_catalog.gold.dim_diagnosis_code;
DROP TABLE IF EXISTS hospital_catalog.gold.dim_medication;
DROP TABLE IF EXISTS hospital_catalog.gold.dim_department;
DROP TABLE IF EXISTS hospital_catalog.gold.dim_date;
"""
