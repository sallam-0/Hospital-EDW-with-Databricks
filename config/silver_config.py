# config/silver_config.py

from config.ingestion_config import CATALOG

SILVER_SCHEMA = "silver"
BRONZE_SCHEMA = "bronze"


# ─────────────────────────────────────────────────────────────────────────────
# Per-table silver configuration
#
# Each entry maps a table name to:
#   - primary_key:        merge key for Delta MERGE upsert
#   - partition_cols:     columns to partition by during deduplication
#   - order_col:          column to order by during deduplication (latest wins)
# ─────────────────────────────────────────────────────────────────────────────

SILVER_TABLE_CONFIG = {

    # EMR Domain
    "patients": {
        "primary_key": "patient_id",
        "partition_cols": ["patient_id"],
        "order_col": "_ingestion_timestamp",
    },

    "doctors": {
        "primary_key": "doctor_id",
        "partition_cols": ["doctor_id"],
        "order_col": "_ingestion_timestamp",
    },

    "departments": {
        "primary_key": "dept_id",
        "partition_cols": ["dept_id"],
        "order_col": "_ingestion_timestamp",
    },

    "encounters": {
        "primary_key": "encounter_id",
        "partition_cols": ["encounter_id"],
        "order_col": "_ingestion_timestamp",
    },

    "diagnoses": {
        "primary_key": "diagnosis_id",
        "partition_cols": ["diagnosis_id"],
        "order_col": "_ingestion_timestamp",
    },

    "procedures": {
        "primary_key": "procedure_id",
        "partition_cols": ["procedure_id"],
        "order_col": "_ingestion_timestamp",
    },

    "admissions": {
        "primary_key": "admission_id",
        "partition_cols": ["admission_id"],
        "order_col": "_ingestion_timestamp",
    },

    # Laboratory Domain
    "lab_orders": {
        "primary_key": "order_id",
        "partition_cols": ["order_id"],
        "order_col": "_ingestion_timestamp",
    },

    "lab_results": {
        "primary_key": "result_id",
        "partition_cols": ["result_id"],
        "order_col": "_ingestion_timestamp",
    },

    # Pharmacy Domain
    "medications": {
        "primary_key": "medication_id",
        "partition_cols": ["medication_id"],
        "order_col": "_ingestion_timestamp",
    },

    "prescriptions": {
        "primary_key": "prescription_id",
        "partition_cols": ["prescription_id"],
        "order_col": "_ingestion_timestamp",
    },

    "pharmacy_transactions": {
        "primary_key": "transaction_id",
        "partition_cols": ["transaction_id"],
        "order_col": "_ingestion_timestamp",
    },

    # Billing Domain
    "invoices": {
        "primary_key": "invoice_id",
        "partition_cols": ["invoice_id"],
        "order_col": "_ingestion_timestamp",
    },

    "payments": {
        "primary_key": "payment_id",
        "partition_cols": ["payment_id"],
        "order_col": "_ingestion_timestamp",
    },

    "insurance_claims": {
        "primary_key": "claim_id",
        "partition_cols": ["claim_id"],
        "order_col": "_ingestion_timestamp",
    },
}


def get_bronze_table(table_name):
    """Get the fully qualified bronze table name."""
    return f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"


def get_silver_table(table_name):
    """Get the fully qualified silver table name."""
    return f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"
