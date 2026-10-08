# config/identity_config.py

from config.ingestion_config import CATALOG

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"


# ─────────────────────────────────────────────────────────────────────────────
# Patient Identity Tables
# ─────────────────────────────────────────────────────────────────────────────

IDENTITY_TABLES = {
    "source":         f"{CATALOG}.{SILVER_SCHEMA}.patients",
    "registry":       f"{CATALOG}.{SILVER_SCHEMA}.patient_identity_registry_enterprise",
    "aliases":        f"{CATALOG}.{SILVER_SCHEMA}.patient_identity_aliases_enterprise",
    "pair_evidence":  f"{CATALOG}.{SILVER_SCHEMA}.patient_identity_pair_evidence_enterprise",
    "review_queue":   f"{CATALOG}.{SILVER_SCHEMA}.patient_steward_review_queue_enterprise",
    "exceptions":     f"{CATALOG}.{SILVER_SCHEMA}.patient_identity_exceptions_enterprise",
    "run_audit":      f"{CATALOG}.{SILVER_SCHEMA}.patient_identity_run_audit_enterprise",
    "patient_map":    f"{CATALOG}.{SILVER_SCHEMA}.patients_map_probabilistic_enterprise",
    "dim_patient":    f"{CATALOG}.{GOLD_SCHEMA}.dim_patient_enterprise",
}


# ─────────────────────────────────────────────────────────────────────────────
# Doctor Identity Tables
# ─────────────────────────────────────────────────────────────────────────────

DOCTOR_IDENTITY_TABLES = {
    "source":         f"{CATALOG}.{SILVER_SCHEMA}.doctors",
    "registry":       f"{CATALOG}.{SILVER_SCHEMA}.doctor_identity_registry_enterprise",
    "aliases":        f"{CATALOG}.{SILVER_SCHEMA}.doctor_identity_aliases_enterprise",
    "pair_evidence":  f"{CATALOG}.{SILVER_SCHEMA}.doctor_identity_pair_evidence_enterprise",
    "review_queue":   f"{CATALOG}.{SILVER_SCHEMA}.doctor_steward_review_queue_enterprise",
    "exceptions":     f"{CATALOG}.{SILVER_SCHEMA}.doctor_identity_exceptions_enterprise",
    "run_audit":      f"{CATALOG}.{SILVER_SCHEMA}.doctor_identity_run_audit_enterprise",
    "doctor_map":     f"{CATALOG}.{SILVER_SCHEMA}.doctors_map_probabilistic_enterprise",
    "dim_doctor":     f"{CATALOG}.{GOLD_SCHEMA}.dim_doctor_enterprise",
}


# ─────────────────────────────────────────────────────────────────────────────
# Matching Thresholds
# ─────────────────────────────────────────────────────────────────────────────

AUTO_MERGE_THRESHOLD = 0.95      # probability >= this  -> auto-merge
REVIEW_MIN_THRESHOLD = 0.70      # probability in [review_min, auto_merge) -> steward review
MAX_COMPONENT_ITERATIONS = 25    # max iterations for connected components

# Doctor thresholds (slightly lower auto-merge: no DOB field, specialty compensates)
DOCTOR_AUTO_MERGE_THRESHOLD = 0.92
DOCTOR_REVIEW_MIN_THRESHOLD = 0.65
