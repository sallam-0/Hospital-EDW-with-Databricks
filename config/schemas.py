# config/schemas.py

DATE_FORMATS = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "MM-dd-yyyy",
    "dd-MMM-yyyy",
    "MM/dd/yyyy",
    "dd-MM-yyyy",
    "yyyy/MM/dd",
]

TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd HH:mm",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "dd/MM/yyyy HH:mm:ss",
    "dd/MM/yyyy HH:mm",
    "MM-dd-yyyy HH:mm:ss",
    "MM-dd-yyyy HH:mm",
]


# ─────────────────────────────────────────────────────────────────────────────
# Ingestion Metadata Schema (added during Bronze ingestion)
# ─────────────────────────────────────────────────────────────────────────────

INGESTION_METADATA_SCHEMA = {
    "_source_branch": "string",
    "_source_system": "string",
    "_source_file": "string",
    "_batch_id": "string",
    "_ingestion_timestamp": "timestamp"
}


# ─────────────────────────────────────────────────────────────────────────────
# EMR Domain Schemas
# ─────────────────────────────────────────────────────────────────────────────

PATIENT_SCHEMA = {
    "patient_id": "string",
    "first_name": "string",
    "last_name": "string",
    "gender": "string",
    "date_of_birth": "date",
    "contact_no": "string",
    "address": "string",
    "created_at": "timestamp"
}

DOCTOR_SCHEMA = {
    "doctor_id": "string",
    "dept_id": "string",
    "first_name": "string",
    "last_name": "string",
    "gender": "string",
    "contact_no": "string",
    "specialty": "string",
    "office_no": "string"
}

DEPARTMENT_SCHEMA = {
    "dept_id": "string",
    "dept_name": "string"
}

ENCOUNTER_SCHEMA = {
    "encounter_id": "string",
    "patient_id": "string",
    "doctor_id": "string",
    "dept_id": "string",
    "encounter_date": "timestamp",
    "encounter_type": "string",
    "reason": "string",
    "status": "string"
}

DIAGNOSIS_SCHEMA = {
    "diagnosis_id": "string",
    "encounter_id": "string",
    "patient_id": "string",
    "doctor_id": "string",
    "diagnosis_name": "string",
    "icd10_code": "string",
    "diagnosis_type": "string",
    "diagnosis_date": "date"
}

PROCEDURE_SCHEMA = {
    # Columns exactly as produced by the data generator / source CSV.
    # nurse_id and helper_id are NOT in scope — the source system does not
    # capture these fields. They have been removed from the gold schema.
    "procedure_id": "string",
    "encounter_id": "string",
    "admission_id": "string",
    "patient_id": "string",
    "surgeon_id": "string",
    "procedure_name": "string",
    "procedure_date": "date",
    "start_time": "string",
    "end_time": "string",
    "room_no": "string",
    "notes": "string",
}

ADMISSION_SCHEMA = {
    "admission_id": "string",
    "encounter_id": "string",
    "patient_id": "string",
    "dept_id": "string",
    "ward_name": "string",
    "room_no": "string",
    "bed_no": "string",
    "admission_date": "timestamp",
    "discharge_date": "timestamp",
    "admission_type": "string",
    "discharge_disposition": "string"
}


# ─────────────────────────────────────────────────────────────────────────────
# Laboratory Domain Schemas
# ─────────────────────────────────────────────────────────────────────────────

LAB_ORDER_SCHEMA = {
    "order_id": "string",
    "encounter_id": "string",
    "patient_id": "string",
    "doctor_id": "string",
    "test_name": "string",
    "order_date": "timestamp",
    "order_status": "string"
}

LAB_RESULT_SCHEMA = {
    "result_id": "string",
    "order_id": "string",
    "patient_id": "string",
    "test_name": "string",
    "parameter_name": "string",
    "result_value": "double",
    "unit_of_measure": "string",
    "reference_range": "string",
    "abnormal_flag": "string",
    "result_date": "timestamp"
}


# ─────────────────────────────────────────────────────────────────────────────
# Pharmacy Domain Schemas
# ─────────────────────────────────────────────────────────────────────────────

MEDICATION_SCHEMA = {
    "medication_id": "string",
    "medication_name": "string",
    "category": "string",
    "dosage_form": "string",
    "strength": "string",
    "unit_price": "double"
}

PRESCRIPTION_SCHEMA = {
    "prescription_id": "string",
    "encounter_id": "string",
    "patient_id": "string",
    "doctor_id": "string",
    "medication_id": "string",
    "medication_name": "string",
    "dosage": "string",
    "frequency": "string",
    "duration_days": "integer",
    "prescribed_date": "date"
}

PHARMACY_TRANSACTION_SCHEMA = {
    "transaction_id": "string",
    "prescription_id": "string",
    "patient_id": "string",
    "dispense_date": "timestamp",
    "quantity": "integer",
    "unit_price": "double",
    "total_amount": "double",
    "payment_method": "string"
}


# ─────────────────────────────────────────────────────────────────────────────
# Billing Domain Schemas
# ─────────────────────────────────────────────────────────────────────────────

INVOICE_SCHEMA = {
    "invoice_id": "string",
    "encounter_id": "string",
    "admission_id": "string",
    "patient_id": "string",
    "invoice_date": "date",
    "gross_amount": "double",
    "discount_amount": "double",
    "insurance_covered_amount": "double",
    "net_amount_due": "double",
    "status": "string"
}

PAYMENT_SCHEMA = {
    "payment_id": "string",
    "invoice_id": "string",
    "patient_id": "string",
    "payment_date": "date",
    "amount_paid": "double",
    "payment_method": "string",
    "payment_status": "string"
}

INSURANCE_CLAIM_SCHEMA = {
    "claim_id": "string",
    "invoice_id": "string",
    "patient_id": "string",
    "payer_name": "string",
    "claim_amount": "double",
    "coverage_percentage": "double",
    "claim_status": "string",
    "claim_date": "date",
    "settlement_date": "date"
}


# ─────────────────────────────────────────────────────────────────────────────
# Plural Aliases (matching table names in TABLE_CONFIG)
# ─────────────────────────────────────────────────────────────────────────────

PATIENTS_SCHEMA = PATIENT_SCHEMA
DOCTORS_SCHEMA = DOCTOR_SCHEMA
DEPARTMENTS_SCHEMA = DEPARTMENT_SCHEMA
ENCOUNTERS_SCHEMA = ENCOUNTER_SCHEMA
DIAGNOSES_SCHEMA = DIAGNOSIS_SCHEMA
PROCEDURES_SCHEMA = PROCEDURE_SCHEMA
ADMISSIONS_SCHEMA = ADMISSION_SCHEMA
LAB_ORDERS_SCHEMA = LAB_ORDER_SCHEMA
LAB_RESULTS_SCHEMA = LAB_RESULT_SCHEMA
MEDICATIONS_SCHEMA = MEDICATION_SCHEMA
PRESCRIPTIONS_SCHEMA = PRESCRIPTION_SCHEMA
PHARMACY_TRANSACTIONS_SCHEMA = PHARMACY_TRANSACTION_SCHEMA
INVOICES_SCHEMA = INVOICE_SCHEMA
PAYMENTS_SCHEMA = PAYMENT_SCHEMA
INSURANCE_CLAIMS_SCHEMA = INSURANCE_CLAIM_SCHEMA


# ─────────────────────────────────────────────────────────────────────────────
# Table Name -> Schema Mapping
# ─────────────────────────────────────────────────────────────────────────────

TABLE_SCHEMAS = {
    # EMR
    "patients": PATIENT_SCHEMA,
    "doctors": DOCTOR_SCHEMA,
    "departments": DEPARTMENT_SCHEMA,
    "encounters": ENCOUNTER_SCHEMA,
    "diagnoses": DIAGNOSIS_SCHEMA,
    "procedures": PROCEDURE_SCHEMA,
    "admissions": ADMISSION_SCHEMA,

    # Laboratory
    "lab_orders": LAB_ORDER_SCHEMA,
    "lab_results": LAB_RESULT_SCHEMA,

    # Pharmacy
    "medications": MEDICATION_SCHEMA,
    "prescriptions": PRESCRIPTION_SCHEMA,
    "pharmacy_transactions": PHARMACY_TRANSACTION_SCHEMA,

    # Billing
    "invoices": INVOICE_SCHEMA,
    "payments": PAYMENT_SCHEMA,
    "insurance_claims": INSURANCE_CLAIM_SCHEMA
}
