# Hospital branches
BRANCHES = [
    "cairo",
    "giza",
    "alexandria"
]

# Source systems
SOURCE_SYSTEMS = [
    "emr",
    "laboratory",
    "pharmacy",
    "billing"
]

# Databricks Unity Catalog
CATALOG = "hospital_catalog"

LANDING_SCHEMA = "landing"
BRONZE_SCHEMA = "bronze"

RAW_VOLUME = "raw_files"

CHECKPOINT_BASE_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{RAW_VOLUME}/_checkpoints"
SCHEMA_BASE_PATH = f"/Volumes/{CATALOG}/{LANDING_SCHEMA}/{RAW_VOLUME}/_schemas"


# Source tables
TABLE_CONFIG = {

    "emr": [
        "patients",
        "doctors",
        "departments",
        "encounters",
        "diagnoses",
        "procedures",
        "admissions"
    ],

    "laboratory": [
        "lab_orders",
        "lab_results"
    ],

    "pharmacy": [
        "medications",
        "prescriptions",
        "pharmacy_transactions"
    ],

    "billing": [
        "invoices",
        "payments",
        "insurance_claims"
    ]
}