from pyspark.sql import functions as F

from config.schemas import DATE_FORMATS, TIMESTAMP_FORMATS

from src.silver.standardization import *



def transform_patients(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "first_name",
            standardize_name(F.col("first_name"))
        )

        .withColumn(
            "last_name",
            standardize_name(F.col("last_name"))
        )

        .withColumn(
            "contact_no",
            clean_string(F.col("contact_no"))
        )

        .withColumn(
            "address",
            clean_string(F.col("address"))
        )

        # -----------------------------
        # Date & Timestamp conversion
        # -----------------------------

        .withColumn(
            "date_of_birth",
            parse_date(
                F.col("date_of_birth"),
                DATE_FORMATS
            )
        )

        .withColumn(
            "created_at",
            parse_timestamp(
                F.col("created_at"),
                TIMESTAMP_FORMATS
            )
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "gender",
            standardize_gender(
                F.col("gender")
            )
        )

        .withColumn(
            "contact_no",
            standardize_phone_number(
                F.col("contact_no")
            )
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# EMR Domain
# ─────────────────────────────────────────────────────────────────────────────

def transform_doctors(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "doctor_id",
            clean_string(F.col("doctor_id"))
        )

        .withColumn(
            "dept_id",
            clean_string(F.col("dept_id"))
        )

        .withColumn(
            "first_name",
            standardize_name(F.col("first_name"))
        )

        .withColumn(
            "last_name",
            standardize_name(F.col("last_name"))
        )

        .withColumn(
            "contact_no",
            clean_string(F.col("contact_no"))
        )

        .withColumn(
            "specialty",
            clean_string(F.col("specialty"))
        )

        .withColumn(
            "office_no",
            clean_string(F.col("office_no"))
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "gender",
            standardize_gender(
                F.col("gender")
            )
        )

        .withColumn(
            "contact_no",
            standardize_phone_number(
                F.col("contact_no")
            )
        )
    )


def transform_departments(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "dept_id",
            clean_string(F.col("dept_id"))
        )

        .withColumn(
            "dept_name",
            clean_string(F.col("dept_name"))
        )
    )


def transform_encounters(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "encounter_id",
            clean_string(F.col("encounter_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "doctor_id",
            clean_string(F.col("doctor_id"))
        )

        .withColumn(
            "dept_id",
            clean_string(F.col("dept_id"))
        )

        .withColumn(
            "reason",
            clean_string(F.col("reason"))
        )

        # -----------------------------
        # Timestamp conversion
        # -----------------------------

        .withColumn(
            "encounter_date",
            parse_timestamp(
                F.col("encounter_date"),
                TIMESTAMP_FORMATS
            )
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "encounter_type",
            standardize_encounter_type(
                F.col("encounter_type")
            )
        )

        .withColumn(
            "status",
            standardize_status(
                F.col("status")
            )
        )
    )


def transform_diagnoses(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "diagnosis_id",
            clean_string(F.col("diagnosis_id"))
        )

        .withColumn(
            "encounter_id",
            clean_string(F.col("encounter_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "doctor_id",
            clean_string(F.col("doctor_id"))
        )

        .withColumn(
            "diagnosis_name",
            clean_string(F.col("diagnosis_name"))
        )

        .withColumn(
            "icd10_code",
            F.upper(
                clean_string(F.col("icd10_code"))
            )
        )

        .withColumn(
            "diagnosis_type",
            clean_string(F.col("diagnosis_type"))
        )

        # -----------------------------
        # Date conversion
        # -----------------------------

        .withColumn(
            "diagnosis_date",
            parse_date(
                F.col("diagnosis_date"),
                DATE_FORMATS
            )
        )
    )


def transform_procedures(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "procedure_id",
            clean_string(F.col("procedure_id"))
        )

        .withColumn(
            "encounter_id",
            clean_string(F.col("encounter_id"))
        )

        .withColumn(
            "admission_id",
            clean_string(F.col("admission_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "surgeon_id",
            clean_string(F.col("surgeon_id"))
        )

        .withColumn(
            "procedure_name",
            clean_string(F.col("procedure_name"))
        )

        .withColumn(
            "room_no",
            clean_string(F.col("room_no"))
        )

        .withColumn(
            "notes",
            clean_string(F.col("notes"))
        )

        .withColumn(
            "start_time",
            clean_string(F.col("start_time"))
        )

        .withColumn(
            "end_time",
            clean_string(F.col("end_time"))
        )

        # -----------------------------
        # Date conversion
        # -----------------------------

        .withColumn(
            "procedure_date",
            parse_date(
                F.col("procedure_date"),
                DATE_FORMATS
            )
        )
    )


def transform_admissions(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "admission_id",
            clean_string(F.col("admission_id"))
        )

        .withColumn(
            "encounter_id",
            clean_string(F.col("encounter_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "dept_id",
            clean_string(F.col("dept_id"))
        )

        .withColumn(
            "ward_name",
            clean_string(F.col("ward_name"))
        )

        .withColumn(
            "room_no",
            clean_string(F.col("room_no"))
        )

        .withColumn(
            "bed_no",
            clean_string(F.col("bed_no"))
        )

        # -----------------------------
        # Timestamp conversion
        # -----------------------------

        .withColumn(
            "admission_date",
            parse_timestamp(
                F.col("admission_date"),
                TIMESTAMP_FORMATS
            )
        )

        .withColumn(
            "discharge_date",
            parse_timestamp(
                F.col("discharge_date"),
                TIMESTAMP_FORMATS
            )
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "admission_type",
            standardize_admission_type(
                F.col("admission_type")
            )
        )

        .withColumn(
            "discharge_disposition",
            standardize_status(
                F.col("discharge_disposition")
            )
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Laboratory Domain
# ─────────────────────────────────────────────────────────────────────────────

def transform_lab_orders(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "order_id",
            clean_string(F.col("order_id"))
        )

        .withColumn(
            "encounter_id",
            clean_string(F.col("encounter_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "doctor_id",
            clean_string(F.col("doctor_id"))
        )

        .withColumn(
            "test_name",
            clean_string(F.col("test_name"))
        )

        # -----------------------------
        # Timestamp conversion
        # -----------------------------

        .withColumn(
            "order_date",
            parse_timestamp(
                F.col("order_date"),
                TIMESTAMP_FORMATS
            )
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "order_status",
            standardize_status(
                F.col("order_status")
            )
        )
    )


def transform_lab_results(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "result_id",
            clean_string(F.col("result_id"))
        )

        .withColumn(
            "order_id",
            clean_string(F.col("order_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "test_name",
            clean_string(F.col("test_name"))
        )

        .withColumn(
            "parameter_name",
            clean_string(F.col("parameter_name"))
        )

        .withColumn(
            "unit_of_measure",
            clean_string(F.col("unit_of_measure"))
        )

        .withColumn(
            "reference_range",
            clean_string(F.col("reference_range"))
        )

        # -----------------------------
        # Type casting
        # -----------------------------

        .withColumn(
            "result_value",
            parse_double(F.col("result_value"))
        )

        # -----------------------------
        # Timestamp conversion
        # -----------------------------

        .withColumn(
            "result_date",
            parse_timestamp(
                F.col("result_date"),
                TIMESTAMP_FORMATS
            )
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "abnormal_flag",
            standardize_abnormal_flag(
                F.col("abnormal_flag")
            )
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pharmacy Domain
# ─────────────────────────────────────────────────────────────────────────────

def transform_medications(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "medication_id",
            clean_string(F.col("medication_id"))
        )

        .withColumn(
            "medication_name",
            clean_string(F.col("medication_name"))
        )

        .withColumn(
            "category",
            clean_string(F.col("category"))
        )

        .withColumn(
            "dosage_form",
            clean_string(F.col("dosage_form"))
        )

        .withColumn(
            "strength",
            clean_string(F.col("strength"))
        )

        # -----------------------------
        # Type casting
        # -----------------------------

        .withColumn(
            "unit_price",
            parse_double(F.col("unit_price"))
        )
    )


def transform_prescriptions(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "prescription_id",
            clean_string(F.col("prescription_id"))
        )

        .withColumn(
            "encounter_id",
            clean_string(F.col("encounter_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "doctor_id",
            clean_string(F.col("doctor_id"))
        )

        .withColumn(
            "medication_id",
            clean_string(F.col("medication_id"))
        )

        .withColumn(
            "medication_name",
            clean_string(F.col("medication_name"))
        )

        .withColumn(
            "dosage",
            clean_string(F.col("dosage"))
        )

        .withColumn(
            "frequency",
            clean_string(F.col("frequency"))
        )

        # -----------------------------
        # Type casting
        # -----------------------------

        .withColumn(
            "duration_days",
            parse_integer(F.col("duration_days"))
        )

        # -----------------------------
        # Date conversion
        # -----------------------------

        .withColumn(
            "prescribed_date",
            parse_date(
                F.col("prescribed_date"),
                DATE_FORMATS
            )
        )
    )


def transform_pharmacy_transactions(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "transaction_id",
            clean_string(F.col("transaction_id"))
        )

        .withColumn(
            "prescription_id",
            clean_string(F.col("prescription_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        # -----------------------------
        # Timestamp conversion
        # -----------------------------

        .withColumn(
            "dispense_date",
            parse_timestamp(
                F.col("dispense_date"),
                TIMESTAMP_FORMATS
            )
        )

        # -----------------------------
        # Type casting
        # -----------------------------

        .withColumn(
            "quantity",
            parse_integer(F.col("quantity"))
        )

        .withColumn(
            "unit_price",
            parse_double(F.col("unit_price"))
        )

        .withColumn(
            "total_amount",
            parse_double(F.col("total_amount"))
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "payment_method",
            standardize_payment_method(
                F.col("payment_method")
            )
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Billing Domain
# ─────────────────────────────────────────────────────────────────────────────

def transform_invoices(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "invoice_id",
            clean_string(F.col("invoice_id"))
        )

        .withColumn(
            "encounter_id",
            clean_string(F.col("encounter_id"))
        )

        .withColumn(
            "admission_id",
            clean_string(F.col("admission_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        # -----------------------------
        # Date conversion
        # -----------------------------

        .withColumn(
            "invoice_date",
            parse_date(
                F.col("invoice_date"),
                DATE_FORMATS
            )
        )

        # -----------------------------
        # Type casting
        # -----------------------------

        .withColumn(
            "gross_amount",
            parse_double(F.col("gross_amount"))
        )

        .withColumn(
            "discount_amount",
            parse_double(F.col("discount_amount"))
        )

        .withColumn(
            "insurance_covered_amount",
            parse_double(F.col("insurance_covered_amount"))
        )

        .withColumn(
            "net_amount_due",
            parse_double(F.col("net_amount_due"))
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "status",
            standardize_status(
                F.col("status")
            )
        )
    )


def transform_payments(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "payment_id",
            clean_string(F.col("payment_id"))
        )

        .withColumn(
            "invoice_id",
            clean_string(F.col("invoice_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        # -----------------------------
        # Date conversion
        # -----------------------------

        .withColumn(
            "payment_date",
            parse_date(
                F.col("payment_date"),
                DATE_FORMATS
            )
        )

        # -----------------------------
        # Type casting
        # -----------------------------

        .withColumn(
            "amount_paid",
            parse_double(F.col("amount_paid"))
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "payment_method",
            standardize_payment_method(
                F.col("payment_method")
            )
        )

        .withColumn(
            "payment_status",
            standardize_status(
                F.col("payment_status")
            )
        )
    )


def transform_insurance_claims(df):

    return (
        df

        # -----------------------------
        # String normalization
        # -----------------------------

        .withColumn(
            "claim_id",
            clean_string(F.col("claim_id"))
        )

        .withColumn(
            "invoice_id",
            clean_string(F.col("invoice_id"))
        )

        .withColumn(
            "patient_id",
            clean_string(F.col("patient_id"))
        )

        .withColumn(
            "payer_name",
            clean_string(F.col("payer_name"))
        )

        # -----------------------------
        # Type casting
        # -----------------------------

        .withColumn(
            "claim_amount",
            parse_double(F.col("claim_amount"))
        )

        .withColumn(
            "coverage_percentage",
            parse_double(F.col("coverage_percentage"))
        )

        # -----------------------------
        # Date conversion
        # -----------------------------

        .withColumn(
            "claim_date",
            parse_date(
                F.col("claim_date"),
                DATE_FORMATS
            )
        )

        .withColumn(
            "settlement_date",
            parse_date(
                F.col("settlement_date"),
                DATE_FORMATS
            )
        )

        # -----------------------------
        # Standardization
        # -----------------------------

        .withColumn(
            "claim_status",
            standardize_status(
                F.col("claim_status")
            )
        )
    )


# ─────────────────────────────────────────────────────────────────────────────
# Table Name -> Transform Function Mapping
# ─────────────────────────────────────────────────────────────────────────────

TRANSFORM_FUNCTIONS = {
    # EMR
    "patients": transform_patients,
    "doctors": transform_doctors,
    "departments": transform_departments,
    "encounters": transform_encounters,
    "diagnoses": transform_diagnoses,
    "procedures": transform_procedures,
    "admissions": transform_admissions,
    # Laboratory
    "lab_orders": transform_lab_orders,
    "lab_results": transform_lab_results,
    # Pharmacy
    "medications": transform_medications,
    "prescriptions": transform_prescriptions,
    "pharmacy_transactions": transform_pharmacy_transactions,
    # Billing
    "invoices": transform_invoices,
    "payments": transform_payments,
    "insurance_claims": transform_insurance_claims,
}