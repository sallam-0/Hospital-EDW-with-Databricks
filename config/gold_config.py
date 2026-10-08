# config/gold_config.py
"""
Gold layer table names — Phase 1 (built from existing silver layer).

All tables live in hospital_catalog.gold.*

Phase 2 tables (dim_nurse, dim_helper, dim_ward, dim_room, dim_bed,
fact_staff_shifts) are commented out until new silver ingestion pipelines
are added for those entities.
"""

from config.ingestion_config import CATALOG

GOLD_SCHEMA   = "gold"
SILVER_SCHEMA = "silver"

# ─────────────────────────────────────────────────────────────────────────────
# Gold Tables  (Phase 1)
# ─────────────────────────────────────────────────────────────────────────────

GOLD_TABLES = {

    # ── Reference / Type-1 Dimensions ─────────────────────────────────────
    "dim_date":           f"{CATALOG}.{GOLD_SCHEMA}.dim_date",
    "dim_department":     f"{CATALOG}.{GOLD_SCHEMA}.dim_department",
    "dim_medication":     f"{CATALOG}.{GOLD_SCHEMA}.dim_medication",
    "dim_diagnosis_code": f"{CATALOG}.{GOLD_SCHEMA}.dim_diagnosis_code",
    "dim_payer":          f"{CATALOG}.{GOLD_SCHEMA}.dim_payer",

    # ── Enterprise (Golden) Identity Dimensions ────────────────────────────
    # Written by identity-resolution notebooks; registered here read-only.
    "dim_patient":        f"{CATALOG}.{GOLD_SCHEMA}.dim_patient_enterprise",
    "dim_doctor":         f"{CATALOG}.{GOLD_SCHEMA}.dim_doctor_enterprise",

    # ── Fact Tables ────────────────────────────────────────────────────────
    # ERD: appointment  →  silver.encounters (encounter_type = OUTPATIENT/EMERGENCY)
    "fact_appointments":    f"{CATALOG}.{GOLD_SCHEMA}.fact_appointments",

    # ERD: medicalrecord  →  silver.encounters + silver.diagnoses (all types)
    "fact_medical_records": f"{CATALOG}.{GOLD_SCHEMA}.fact_medical_records",

    # ERD: roomrecords / bedrecords  →  silver.admissions
    "fact_admissions":      f"{CATALOG}.{GOLD_SCHEMA}.fact_admissions",

    # ERD: surgeryrecord  →  silver.procedures
    "fact_surgeries":       f"{CATALOG}.{GOLD_SCHEMA}.fact_surgeries",

    # Laboratory system  →  silver.lab_results + silver.lab_orders
    "fact_lab_results":     f"{CATALOG}.{GOLD_SCHEMA}.fact_lab_results",

    # Billing system  →  silver.invoices + silver.payments + silver.insurance_claims
    "fact_billing":         f"{CATALOG}.{GOLD_SCHEMA}.fact_billing",
}


# ─────────────────────────────────────────────────────────────────────────────
# Silver Source Tables  (read-only)
# ─────────────────────────────────────────────────────────────────────────────

SILVER_TABLES = {
    "departments":           f"{CATALOG}.{SILVER_SCHEMA}.departments",
    "medications":           f"{CATALOG}.{SILVER_SCHEMA}.medications",
    "encounters":            f"{CATALOG}.{SILVER_SCHEMA}.encounters",
    "diagnoses":             f"{CATALOG}.{SILVER_SCHEMA}.diagnoses",
    "admissions":            f"{CATALOG}.{SILVER_SCHEMA}.admissions",
    "procedures":            f"{CATALOG}.{SILVER_SCHEMA}.procedures",
    "lab_orders":            f"{CATALOG}.{SILVER_SCHEMA}.lab_orders",
    "lab_results":           f"{CATALOG}.{SILVER_SCHEMA}.lab_results",
    "prescriptions":         f"{CATALOG}.{SILVER_SCHEMA}.prescriptions",
    "pharmacy_transactions": f"{CATALOG}.{SILVER_SCHEMA}.pharmacy_transactions",
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
