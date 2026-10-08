"""Unit tests for config/schemas.py"""

from config.ingestion_config import TABLE_CONFIG
from config.schemas import (
    DATE_FORMATS,
    TIMESTAMP_FORMATS,
    INGESTION_METADATA_SCHEMA,
    TABLE_SCHEMAS,
    PATIENT_SCHEMA,
    DOCTOR_SCHEMA,
    DEPARTMENT_SCHEMA,
    ENCOUNTER_SCHEMA,
    DIAGNOSIS_SCHEMA,
    PROCEDURE_SCHEMA,
    ADMISSION_SCHEMA,
    LAB_ORDER_SCHEMA,
    LAB_RESULT_SCHEMA,
    MEDICATION_SCHEMA,
    PRESCRIPTION_SCHEMA,
    PHARMACY_TRANSACTION_SCHEMA,
    INVOICE_SCHEMA,
    PAYMENT_SCHEMA,
    INSURANCE_CLAIM_SCHEMA,
)


def test_date_and_timestamp_formats():
    assert "yyyy-MM-dd" in DATE_FORMATS
    assert "dd/MM/yyyy" in DATE_FORMATS
    assert "yyyy-MM-dd HH:mm" in TIMESTAMP_FORMATS
    assert "yyyy-MM-dd HH:mm:ss" in TIMESTAMP_FORMATS


def test_all_tables_covered():
    all_tables = [t for tables in TABLE_CONFIG.values() for t in tables]
    assert len(all_tables) == 15
    for table in all_tables:
        assert table in TABLE_SCHEMAS, f"Missing schema for table {table}"
        schema = TABLE_SCHEMAS[table]
        assert isinstance(schema, dict)
        assert len(schema) > 0


def test_valid_types_in_schemas():
    valid_spark_types = {"string", "integer", "double", "date", "timestamp", "boolean"}
    for table_name, schema in TABLE_SCHEMAS.items():
        for col_name, data_type in schema.items():
            assert data_type in valid_spark_types, (
                f"Invalid type '{data_type}' for column '{col_name}' in table '{table_name}'"
            )


def test_metadata_columns():
    assert "_source_branch" in INGESTION_METADATA_SCHEMA
    assert "_source_system" in INGESTION_METADATA_SCHEMA
    assert "_batch_id" in INGESTION_METADATA_SCHEMA
    assert "_ingestion_timestamp" in INGESTION_METADATA_SCHEMA
