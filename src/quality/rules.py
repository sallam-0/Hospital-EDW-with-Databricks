from config.quality_config import (
    CATEGORY_COMPLETENESS,
    CATEGORY_UNIQUENESS,
    CATEGORY_VALIDITY,
    CATEGORY_TYPE_CONFORMITY,
    SEVERITY_CRITICAL,
    SEVERITY_HIGH,
    SEVERITY_MEDIUM,
)


PATIENT_RULES = [
    {
        "rule_id": "PATIENT_001",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",

        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,

        "column": "patient_id",

        "description":
            "Patient ID must not be NULL."
    },

    {
        "rule_id": "PATIENT_002",
        "rule_name": "patient_id_unique",
        "rule_type": "unique",

        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,

        "columns": ["_source_branch", "patient_id"],

        "description":
            "Patient ID must be unique."
    },

    {
        "rule_id": "PATIENT_003",
        "rule_name": "gender_valid",
        "rule_type": "allowed_values",

        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,

        "column": "gender",

        "allowed_values": [
            "M",
            "F",
            "O",
            "male",
            "female"
        ],

        "description":
            "Gender must contain a valid standardized value."
    },

    {
        "rule_id": "PATIENT_004",
        "rule_name": "date_of_birth_not_null",
        "rule_type": "not_null",

        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,

        "column": "date_of_birth",

        "description":
            "Patient date of birth must not be NULL."
    },

    {
        "rule_id": "PATIENT_005",
        "rule_name": "date_of_birth_not_future",
        "rule_type": "not_future",

        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,

        "column": "date_of_birth",

        "description":
            "Patient date of birth cannot be in the future."
    },

    {
        "rule_id": "PATIENT_006",
        "rule_name": "date_of_birth_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "date_of_birth",
        "description":
            "Date of birth must be a parsable date."
    },

    {
        "rule_id": "PATIENT_007",
        "rule_name": "created_at_parsable",
        "rule_type": "timestamp_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_MEDIUM,
        "column": "created_at",
        "description":
            "Created-at must be a parsable timestamp."
    }
]



DOCTOR_RULES = [
    {
        "rule_id": "DOCTOR_001",
        "rule_name": "doctor_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "doctor_id",
        "description": "Doctor ID must not be NULL."
    },

    {
        "rule_id": "DOCTOR_002",
        "rule_name": "doctor_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "doctor_id"],
        "description": "Doctor ID must be unique."
    },

    {
        "rule_id": "DOCTOR_003",
        "rule_name": "doctor_name_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,
        "column": "first_name",
        "description": "Doctor name must not be NULL."
    }
]

ENCOUNTER_RULES = [
    {
        "rule_id": "ENCOUNTER_001",
        "rule_name": "encounter_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "encounter_id",
        "description": "Encounter ID must not be NULL."
    },

    {
        "rule_id": "ENCOUNTER_002",
        "rule_name": "encounter_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "encounter_id"],
        "description": "Encounter ID must be unique."
    },

    {
        "rule_id": "ENCOUNTER_003",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Encounter must have a patient."
    },

   
    {
        "rule_id": "ENCOUNTER_005",
        "rule_name": "encounter_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "encounter_date",
        "description":
            "Encounter date must be a parsable date."
    }
]


DEPARTMENT_RULES = [
    {
        "rule_id": "DEPT_001",
        "rule_name": "dept_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "dept_id",
        "description": "Department ID must not be NULL."
    },

    {
        "rule_id": "DEPT_002",
        "rule_name": "dept_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "dept_id"],
        "description": "Department ID must be unique."
    },

    {
        "rule_id": "DEPT_003",
        "rule_name": "dept_name_not_blank",
        "rule_type": "not_blank",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,
        "column": "dept_name",
        "description": "Department name must not be blank."
    }
]


ADMISSION_RULES = [
    {
        "rule_id": "ADMISSION_001",
        "rule_name": "admission_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "admission_id",
        "description": "Admission ID must not be NULL."
    },

    {
        "rule_id": "ADMISSION_002",
        "rule_name": "admission_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "admission_id"],
        "description": "Admission ID must be unique."
    },

    {
        "rule_id": "ADMISSION_003",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Admission must have a patient."
    },

    {
        "rule_id": "ADMISSION_004",
        "rule_name": "encounter_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "encounter_id",
        "description": "Admission must have an encounter."
    },

    {
        "rule_id": "ADMISSION_005",
        "rule_name": "admission_before_discharge",
        "rule_type": "date_order",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "start_column": "admission_date",
        "end_column": "discharge_date",
        "description":
            "Discharge date cannot be before admission date."
    },

    {
        "rule_id": "ADMISSION_006",
        "rule_name": "admission_type_valid",
        "rule_type": "allowed_values",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "admission_type",
        "allowed_values": [
            "Emergency",
            "Elective",
            "Urgent"
        ],
        "description":
            "Admission type must be a valid value."
    },

    {
        "rule_id": "ADMISSION_007",
        "rule_name": "admission_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "admission_date",
        "description":
            "Admission date must be a parsable date."
    },

    {
        "rule_id": "ADMISSION_008",
        "rule_name": "discharge_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "discharge_date",
        "description":
            "Discharge date must be a parsable date."
    }
]


DIAGNOSIS_RULES = [
    {
        "rule_id": "DIAGNOSIS_001",
        "rule_name": "diagnosis_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "diagnosis_id",
        "description": "Diagnosis ID must not be NULL."
    },

    {
        "rule_id": "DIAGNOSIS_002",
        "rule_name": "diagnosis_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "diagnosis_id"],
        "description": "Diagnosis ID must be unique."
    },

    {
        "rule_id": "DIAGNOSIS_003",
        "rule_name": "encounter_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "encounter_id",
        "description": "Diagnosis must have an encounter."
    },

    {
        "rule_id": "DIAGNOSIS_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Diagnosis must have a patient."
    },

    {
        "rule_id": "DIAGNOSIS_005",
        "rule_name": "diagnosis_type_valid",
        "rule_type": "allowed_values",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "diagnosis_type",
        "allowed_values": [
            "Primary",
            "Secondary",
            "Admitting"
        ],
        "description":
            "Diagnosis type must be a valid value."
    },

    {
        "rule_id": "DIAGNOSIS_006",
        "rule_name": "icd10_code_not_blank",
        "rule_type": "not_blank",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,
        "column": "icd10_code",
        "description": "ICD-10 code must not be blank."
    },

    {
        "rule_id": "DIAGNOSIS_007",
        "rule_name": "diagnosis_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "diagnosis_date",
        "description":
            "Diagnosis date must be a parsable date."
    }
]


PROCEDURE_RULES = [
    {
        "rule_id": "PROCEDURE_001",
        "rule_name": "procedure_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "procedure_id",
        "description": "Procedure ID must not be NULL."
    },

    {
        "rule_id": "PROCEDURE_002",
        "rule_name": "procedure_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "procedure_id"],
        "description": "Procedure ID must be unique."
    },

    {
        "rule_id": "PROCEDURE_003",
        "rule_name": "encounter_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "encounter_id",
        "description": "Procedure must have an encounter."
    },

    {
        "rule_id": "PROCEDURE_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Procedure must have a patient."
    },

    {
        "rule_id": "PROCEDURE_005",
        "rule_name": "procedure_start_before_end",
        "rule_type": "date_order",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "start_column": "start_time",
        "end_column": "end_time",
        "description":
            "Procedure end time cannot be before start time."
    },

    {
        "rule_id": "PROCEDURE_006",
        "rule_name": "procedure_name_not_blank",
        "rule_type": "not_blank",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,
        "column": "procedure_name",
        "description": "Procedure name must not be blank."
    },

    {
        "rule_id": "PROCEDURE_007",
        "rule_name": "procedure_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "procedure_date",
        "description":
            "Procedure date must be a parsable date."
    },

    {
        "rule_id": "PROCEDURE_008",
        "rule_name": "start_time_parsable",
        "rule_type": "timestamp_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "start_time",
        "description":
            "Procedure start time must be a parsable timestamp."
    },

    {
        "rule_id": "PROCEDURE_009",
        "rule_name": "end_time_parsable",
        "rule_type": "timestamp_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "end_time",
        "description":
            "Procedure end time must be a parsable timestamp."
    }
]


LAB_ORDER_RULES = [
    {
        "rule_id": "LAB_ORD_001",
        "rule_name": "order_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "order_id",
        "description": "Lab order ID must not be NULL."
    },

    {
        "rule_id": "LAB_ORD_002",
        "rule_name": "order_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "order_id"],
        "description": "Lab order ID must be unique."
    },

    {
        "rule_id": "LAB_ORD_003",
        "rule_name": "encounter_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "encounter_id",
        "description": "Lab order must have an encounter."
    },

    {
        "rule_id": "LAB_ORD_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Lab order must have a patient."
    },

    {
        "rule_id": "LAB_ORD_005",
        "rule_name": "order_status_valid",
        "rule_type": "allowed_values",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "order_status",
        "allowed_values": [
            "Completed",
            "Pending",
            "Cancelled"
        ],
        "description":
            "Lab order status must be a valid value."
    },

    {
        "rule_id": "LAB_ORD_006",
        "rule_name": "order_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "order_date",
        "description":
            "Lab order date must be a parsable date."
    }
]


LAB_RESULT_RULES = [
    {
        "rule_id": "LAB_RES_001",
        "rule_name": "result_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "result_id",
        "description": "Lab result ID must not be NULL."
    },

    {
        "rule_id": "LAB_RES_002",
        "rule_name": "result_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "result_id"],
        "description": "Lab result ID must be unique."
    },

    {
        "rule_id": "LAB_RES_003",
        "rule_name": "order_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "order_id",
        "description": "Lab result must have an order."
    },

    {
        "rule_id": "LAB_RES_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Lab result must have a patient."
    },

    {
        "rule_id": "LAB_RES_005",
        "rule_name": "result_value_not_blank",
        "rule_type": "not_blank",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,
        "column": "result_value",
        "description": "Lab result value must not be blank."
    },

    {
        "rule_id": "LAB_RES_006",
        "rule_name": "result_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "result_date",
        "description":
            "Lab result date must be a parsable date."
    },

    {
        "rule_id": "LAB_RES_007",
        "rule_name": "result_value_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "result_value",
        "description":
            "Lab result value must be a parsable number."
    }
]


MEDICATION_RULES = [
    {
        "rule_id": "MED_001",
        "rule_name": "medication_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "medication_id",
        "description": "Medication ID must not be NULL."
    },

    {
        "rule_id": "MED_002",
        "rule_name": "medication_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "medication_id"],
        "description": "Medication ID must be unique."
    },

    {
        "rule_id": "MED_003",
        "rule_name": "medication_name_not_blank",
        "rule_type": "not_blank",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,
        "column": "medication_name",
        "description": "Medication name must not be blank."
    },

    {
        "rule_id": "MED_004",
        "rule_name": "unit_price_positive",
        "rule_type": "positive",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "column": "unit_price",
        "description": "Medication unit price must be positive."
    },

    {
        "rule_id": "MED_005",
        "rule_name": "unit_price_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "unit_price",
        "description":
            "Medication unit price must be a parsable number."
    }
]


PRESCRIPTION_RULES = [
    {
        "rule_id": "RX_001",
        "rule_name": "prescription_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "prescription_id",
        "description": "Prescription ID must not be NULL."
    },

    {
        "rule_id": "RX_002",
        "rule_name": "prescription_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "prescription_id"],
        "description": "Prescription ID must be unique."
    },

    {
        "rule_id": "RX_003",
        "rule_name": "encounter_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "encounter_id",
        "description": "Prescription must have an encounter."
    },

    {
        "rule_id": "RX_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Prescription must have a patient."
    },

    {
        "rule_id": "RX_005",
        "rule_name": "medication_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_HIGH,
        "column": "medication_id",
        "description": "Prescription must have a medication."
    },

    {
        "rule_id": "RX_006",
        "rule_name": "duration_days_positive",
        "rule_type": "positive",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "duration_days",
        "description":
            "Prescription duration must be positive."
    },

    {
        "rule_id": "RX_007",
        "rule_name": "prescribed_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "prescribed_date",
        "description":
            "Prescribed date must be a parsable date."
    },

    {
        "rule_id": "RX_008",
        "rule_name": "duration_days_integer",
        "rule_type": "integer_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_MEDIUM,
        "column": "duration_days",
        "description":
            "Duration days must be a parsable integer."
    }
]


PHARMACY_TRANSACTION_RULES = [
    {
        "rule_id": "PHM_001",
        "rule_name": "transaction_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "transaction_id",
        "description":
            "Pharmacy transaction ID must not be NULL."
    },

    {
        "rule_id": "PHM_002",
        "rule_name": "transaction_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "transaction_id"],
        "description":
            "Pharmacy transaction ID must be unique."
    },

    {
        "rule_id": "PHM_003",
        "rule_name": "prescription_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "prescription_id",
        "description":
            "Transaction must have a prescription."
    },

    {
        "rule_id": "PHM_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description":
            "Transaction must have a patient."
    },

    {
        "rule_id": "PHM_005",
        "rule_name": "total_amount_positive",
        "rule_type": "positive",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "column": "total_amount",
        "description":
            "Transaction total amount must be positive."
    },

    {
        "rule_id": "PHM_006",
        "rule_name": "quantity_positive",
        "rule_type": "positive",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "column": "quantity",
        "description":
            "Dispensed quantity must be positive."
    },

    {
        "rule_id": "PHM_007",
        "rule_name": "dispense_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "dispense_date",
        "description":
            "Dispense date must be a parsable date."
    },

    {
        "rule_id": "PHM_008",
        "rule_name": "quantity_integer",
        "rule_type": "integer_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "quantity",
        "description":
            "Dispensed quantity must be a parsable integer."
    },

    {
        "rule_id": "PHM_009",
        "rule_name": "unit_price_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "unit_price",
        "description":
            "Unit price must be a parsable number."
    },

    {
        "rule_id": "PHM_010",
        "rule_name": "total_amount_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "total_amount",
        "description":
            "Total amount must be a parsable number."
    }
]


INVOICE_RULES = [
    {
        "rule_id": "INV_001",
        "rule_name": "invoice_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "invoice_id",
        "description": "Invoice ID must not be NULL."
    },

    {
        "rule_id": "INV_002",
        "rule_name": "invoice_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "invoice_id"],
        "description": "Invoice ID must be unique."
    },

    {
        "rule_id": "INV_003",
        "rule_name": "encounter_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "encounter_id",
        "description": "Invoice must have an encounter."
    },

    {
        "rule_id": "INV_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Invoice must have a patient."
    },

    {
        "rule_id": "INV_005",
        "rule_name": "gross_amount_positive",
        "rule_type": "positive",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "column": "gross_amount",
        "description": "Gross amount must be positive."
    },

    {
        "rule_id": "INV_006",
        "rule_name": "invoice_status_valid",
        "rule_type": "allowed_values",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "status",
        "allowed_values": [
            "Paid",
            "Pending",
            "Partially Paid",
            "Cancelled"
        ],
        "description":
            "Invoice status must be a valid value."
    },

    {
        "rule_id": "INV_007",
        "rule_name": "invoice_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "invoice_date",
        "description":
            "Invoice date must be a parsable date."
    },

    {
        "rule_id": "INV_008",
        "rule_name": "gross_amount_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "gross_amount",
        "description":
            "Gross amount must be a parsable number."
    },

    {
        "rule_id": "INV_009",
        "rule_name": "discount_amount_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_MEDIUM,
        "column": "discount_amount",
        "description":
            "Discount amount must be a parsable number."
    },

    {
        "rule_id": "INV_010",
        "rule_name": "insurance_covered_amount_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_MEDIUM,
        "column": "insurance_covered_amount",
        "description":
            "Insurance covered amount must be a parsable number."
    },

    {
        "rule_id": "INV_011",
        "rule_name": "net_amount_due_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "net_amount_due",
        "description":
            "Net amount due must be a parsable number."
    }
]


PAYMENT_RULES = [
    {
        "rule_id": "PAY_001",
        "rule_name": "payment_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "payment_id",
        "description": "Payment ID must not be NULL."
    },

    {
        "rule_id": "PAY_002",
        "rule_name": "payment_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "payment_id"],
        "description": "Payment ID must be unique."
    },

    {
        "rule_id": "PAY_003",
        "rule_name": "invoice_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "invoice_id",
        "description": "Payment must have an invoice."
    },

    {
        "rule_id": "PAY_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Payment must have a patient."
    },

    {
        "rule_id": "PAY_005",
        "rule_name": "amount_paid_positive",
        "rule_type": "positive",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "column": "amount_paid",
        "description": "Amount paid must be positive."
    },

    {
        "rule_id": "PAY_006",
        "rule_name": "payment_status_valid",
        "rule_type": "allowed_values",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "payment_status",
        "allowed_values": [
            "Success",
            "Failed",
            "Refunded"
        ],
        "description":
            "Payment status must be a valid value."
    },

    {
        "rule_id": "PAY_007",
        "rule_name": "payment_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "payment_date",
        "description":
            "Payment date must be a parsable date."
    },

    {
        "rule_id": "PAY_008",
        "rule_name": "amount_paid_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "amount_paid",
        "description":
            "Amount paid must be a parsable number."
    }
]


INSURANCE_CLAIM_RULES = [
    {
        "rule_id": "CLM_001",
        "rule_name": "claim_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "claim_id",
        "description": "Claim ID must not be NULL."
    },

    {
        "rule_id": "CLM_002",
        "rule_name": "claim_id_unique",
        "rule_type": "unique",
        "category": CATEGORY_UNIQUENESS,
        "severity": SEVERITY_CRITICAL,
        "columns": ["_source_branch", "claim_id"],
        "description": "Claim ID must be unique."
    },

    {
        "rule_id": "CLM_003",
        "rule_name": "invoice_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "invoice_id",
        "description": "Claim must have an invoice."
    },

    {
        "rule_id": "CLM_004",
        "rule_name": "patient_id_not_null",
        "rule_type": "not_null",
        "category": CATEGORY_COMPLETENESS,
        "severity": SEVERITY_CRITICAL,
        "column": "patient_id",
        "description": "Claim must have a patient."
    },

    {
        "rule_id": "CLM_005",
        "rule_name": "claim_amount_positive",
        "rule_type": "positive",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_HIGH,
        "column": "claim_amount",
        "description": "Claim amount must be positive."
    },

    {
        "rule_id": "CLM_006",
        "rule_name": "coverage_percentage_range",
        "rule_type": "range",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "coverage_percentage",
        "min_value": 0,
        "max_value": 100,
        "description":
            "Coverage percentage must be between 0 and 100."
    },

    {
        "rule_id": "CLM_007",
        "rule_name": "claim_status_valid",
        "rule_type": "allowed_values",
        "category": CATEGORY_VALIDITY,
        "severity": SEVERITY_MEDIUM,
        "column": "claim_status",
        "allowed_values": [
            "Pending",
            "Approved",
            "Denied",
            "Settled"
        ],
        "description":
            "Claim status must be a valid value."
    },

    {
        "rule_id": "CLM_008",
        "rule_name": "claim_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "claim_date",
        "description":
            "Claim date must be a parsable date."
    },

    {
        "rule_id": "CLM_009",
        "rule_name": "settlement_date_parsable",
        "rule_type": "date_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_MEDIUM,
        "column": "settlement_date",
        "description":
            "Settlement date must be a parsable date."
    },

    {
        "rule_id": "CLM_010",
        "rule_name": "claim_amount_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_HIGH,
        "column": "claim_amount",
        "description":
            "Claim amount must be a parsable number."
    },

    {
        "rule_id": "CLM_011",
        "rule_name": "coverage_percentage_decimal",
        "rule_type": "decimal_parsable",
        "category": CATEGORY_TYPE_CONFORMITY,
        "severity": SEVERITY_MEDIUM,
        "column": "coverage_percentage",
        "description":
            "Coverage percentage must be a parsable number."
    }
]


# ══════════════════════════════════════════════════════════════════════════════
# GOLD LAYER — Dimension DQ Rules
# ══════════════════════════════════════════════════════════════════════════════

DIM_DATE_RULES = [
    {"rule_id": "GDIM_DATE_001", "rule_name": "date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "date_key", "description": "date_key (surrogate PK) must not be NULL."},
    {"rule_id": "GDIM_DATE_002", "rule_name": "date_key_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["date_key"], "description": "date_key must be unique."},
    {"rule_id": "GDIM_DATE_003", "rule_name": "date_key_range", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "date_key", "min_value": 20150101, "max_value": 20351231,
     "description": "date_key must fall within the configured spine (20150101–20351231)."},
    {"rule_id": "GDIM_DATE_004", "rule_name": "full_date_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "full_date", "description": "full_date must not be NULL."},
    {"rule_id": "GDIM_DATE_005", "rule_name": "month_num_range", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "month_num", "min_value": 1, "max_value": 12,
     "description": "month_num must be between 1 and 12."},
    {"rule_id": "GDIM_DATE_006", "rule_name": "quarter_range", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "quarter", "min_value": 1, "max_value": 4,
     "description": "quarter must be between 1 and 4."},
]

DIM_DEPARTMENT_RULES = [
    {"rule_id": "GDIM_DEPT_001", "rule_name": "dept_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "dept_sk", "description": "dept_sk (integer surrogate PK) must not be NULL."},
    {"rule_id": "GDIM_DEPT_002", "rule_name": "dept_sk_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["dept_sk"], "description": "dept_sk must be unique."},
    {"rule_id": "GDIM_DEPT_003", "rule_name": "dept_sk_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "dept_sk", "description": "dept_sk must be a positive integer (> 0)."},
    {"rule_id": "GDIM_DEPT_004", "rule_name": "dept_id_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "dept_id", "description": "dept_id (natural key) must not be blank."},
    {"rule_id": "GDIM_DEPT_005", "rule_name": "dept_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["dept_id"], "description": "dept_id must be unique in the dimension."},
    {"rule_id": "GDIM_DEPT_006", "rule_name": "dept_name_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "dept_name", "description": "dept_name must not be blank."},
]

DIM_MEDICATION_RULES = [
    {"rule_id": "GDIM_MED_001", "rule_name": "medication_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "medication_sk", "description": "medication_sk (integer surrogate PK) must not be NULL."},
    {"rule_id": "GDIM_MED_002", "rule_name": "medication_sk_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["medication_sk"], "description": "medication_sk must be unique."},
    {"rule_id": "GDIM_MED_003", "rule_name": "medication_id_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "medication_id", "description": "medication_id (natural key) must not be blank."},
    {"rule_id": "GDIM_MED_004", "rule_name": "medication_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["medication_id"], "description": "medication_id must be unique in the dimension."},
    {"rule_id": "GDIM_MED_005", "rule_name": "medication_name_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "medication_name", "description": "medication_name must not be blank."},
    {"rule_id": "GDIM_MED_006", "rule_name": "unit_price_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "unit_price", "description": "unit_price must be positive."},
]

DIM_DIAGNOSIS_CODE_RULES = [
    {"rule_id": "GDIM_DX_001", "rule_name": "diagnosis_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "diagnosis_sk", "description": "diagnosis_sk (integer surrogate PK) must not be NULL."},
    {"rule_id": "GDIM_DX_002", "rule_name": "diagnosis_sk_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["diagnosis_sk"], "description": "diagnosis_sk must be unique."},
    {"rule_id": "GDIM_DX_003", "rule_name": "icd10_code_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "icd10_code", "description": "icd10_code (natural key) must not be blank."},
    {"rule_id": "GDIM_DX_004", "rule_name": "icd10_code_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["icd10_code"], "description": "icd10_code must be unique in the dimension."},
    {"rule_id": "GDIM_DX_005", "rule_name": "icd10_code_format", "rule_type": "regex",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "icd10_code", "pattern": r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$",
     "description": "icd10_code must match the ICD-10 format (e.g. E11.9, J06, Z79.4)."},
    {"rule_id": "GDIM_DX_006", "rule_name": "diagnosis_name_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "diagnosis_name", "description": "diagnosis_name must not be blank."},
    {"rule_id": "GDIM_DX_007", "rule_name": "usage_count_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "usage_count", "description": "usage_count must be > 0 (every code must appear in fact data)."},
]

DIM_PAYER_RULES = [
    {"rule_id": "GDIM_PAY_001", "rule_name": "payer_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "payer_sk", "description": "payer_sk (integer surrogate PK) must not be NULL."},
    {"rule_id": "GDIM_PAY_002", "rule_name": "payer_sk_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["payer_sk"], "description": "payer_sk must be unique."},
    {"rule_id": "GDIM_PAY_003", "rule_name": "payer_sk_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "payer_sk", "min_value": 0,
     "description": "payer_sk must be >= 0 (0 = SELF-PAY sentinel)."},
    {"rule_id": "GDIM_PAY_004", "rule_name": "payer_name_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "payer_name", "description": "payer_name must not be blank."},
    {"rule_id": "GDIM_PAY_005", "rule_name": "payer_name_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_HIGH,
     "columns": ["payer_name"], "description": "payer_name must be unique (no duplicate insurer names)."},
    {"rule_id": "GDIM_PAY_006", "rule_name": "total_claims_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "total_claims", "min_value": 0,
     "description": "total_claims must be >= 0."},
]


# ══════════════════════════════════════════════════════════════════════════════
# GOLD LAYER — Fact DQ Rules
# ══════════════════════════════════════════════════════════════════════════════

FACT_APPOINTMENTS_RULES = [
    {"rule_id": "GFACT_APT_001", "rule_name": "encounter_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "encounter_id", "description": "encounter_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_APT_002", "rule_name": "encounter_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["encounter_id"], "description": "encounter_id must be unique in fact_appointments."},
    {"rule_id": "GFACT_APT_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_APT_004", "rule_name": "appointment_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "appointment_date_key", "description": "appointment_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_APT_005", "rule_name": "appointment_date_key_range", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "appointment_date_key", "min_value": 20150101, "max_value": 20351231,
     "description": "appointment_date_key must be within the date spine range."},
    {"rule_id": "GFACT_APT_006", "rule_name": "dept_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "dept_sk", "description": "dept_sk (FK → dim_department) must not be NULL."},
    {"rule_id": "GFACT_APT_007", "rule_name": "payment_amount_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "payment_amount", "min_value": 0, "description": "payment_amount must be >= 0 when present."},
]

FACT_ADMISSIONS_RULES = [
    {"rule_id": "GFACT_ADM_001", "rule_name": "admission_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "admission_id", "description": "admission_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_ADM_002", "rule_name": "admission_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["admission_id"], "description": "admission_id must be unique in fact_admissions."},
    {"rule_id": "GFACT_ADM_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_ADM_004", "rule_name": "admission_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "admission_date_key", "description": "admission_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_ADM_005", "rule_name": "discharge_after_admission", "rule_type": "date_order",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "start_column": "admission_date", "end_column": "discharge_date",
     "description": "discharge_date cannot be before admission_date."},
    {"rule_id": "GFACT_ADM_006", "rule_name": "length_of_stay_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "length_of_stay_days", "min_value": 0, "description": "length_of_stay_days must be >= 0."},
    {"rule_id": "GFACT_ADM_007", "rule_name": "dept_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "dept_sk", "description": "dept_sk (FK → dim_department) must not be NULL."},
]

FACT_MEDICAL_RECORDS_RULES = [
    {"rule_id": "GFACT_MR_001", "rule_name": "encounter_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "encounter_id", "description": "encounter_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_MR_002", "rule_name": "encounter_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["encounter_id"], "description": "encounter_id must be unique in fact_medical_records."},
    {"rule_id": "GFACT_MR_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_MR_004", "rule_name": "visit_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "visit_date_key", "description": "visit_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_MR_005", "rule_name": "dept_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "dept_sk", "description": "dept_sk (FK → dim_department) must not be NULL."},
    {"rule_id": "GFACT_MR_006", "rule_name": "diagnosis_count_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "diagnosis_count", "min_value": 0, "description": "diagnosis_count must be >= 0."},
]

FACT_SURGERIES_RULES = [
    {"rule_id": "GFACT_SRG_001", "rule_name": "procedure_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "procedure_id", "description": "procedure_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_SRG_002", "rule_name": "procedure_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["procedure_id"], "description": "procedure_id must be unique in fact_surgeries."},
    {"rule_id": "GFACT_SRG_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_SRG_004", "rule_name": "procedure_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "procedure_date_key", "description": "procedure_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_SRG_005", "rule_name": "duration_minutes_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "duration_minutes", "description": "duration_minutes must be positive when present."},
    {"rule_id": "GFACT_SRG_006", "rule_name": "procedure_name_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "procedure_name", "description": "procedure_name must not be blank."},
]

FACT_LAB_RESULTS_RULES = [
    {"rule_id": "GFACT_LAB_001", "rule_name": "result_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "result_id", "description": "result_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_LAB_002", "rule_name": "result_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["result_id"], "description": "result_id must be unique in fact_lab_results."},
    {"rule_id": "GFACT_LAB_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_LAB_004", "rule_name": "result_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "result_date_key", "description": "result_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_LAB_005", "rule_name": "test_name_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "test_name", "description": "test_name must not be blank."},
    {"rule_id": "GFACT_LAB_006", "rule_name": "abnormal_flag_valid", "rule_type": "allowed_values",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "abnormal_flag",
     "allowed_values": ["NORMAL", "HIGH", "LOW", "CRITICAL", "N", "H", "L", "C"],
     "description": "abnormal_flag must be one of: NORMAL, HIGH, LOW, CRITICAL (or abbreviations)."},
]

FACT_PRESCRIPTIONS_RULES = [
    {"rule_id": "GFACT_RX_001", "rule_name": "prescription_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "prescription_id", "description": "prescription_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_RX_002", "rule_name": "prescription_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["prescription_id"], "description": "prescription_id must be unique in fact_prescriptions."},
    {"rule_id": "GFACT_RX_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_RX_004", "rule_name": "prescribed_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "prescribed_date_key", "description": "prescribed_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_RX_005", "rule_name": "medication_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "medication_sk", "description": "medication_sk (FK → dim_medication) must not be NULL."},
    {"rule_id": "GFACT_RX_006", "rule_name": "duration_days_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "duration_days", "description": "duration_days must be positive."},
]

FACT_PHARMACY_DISPENSES_RULES = [
    {"rule_id": "GFACT_PHM_001", "rule_name": "transaction_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "transaction_id", "description": "transaction_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_PHM_002", "rule_name": "transaction_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["transaction_id"], "description": "transaction_id must be unique in fact_pharmacy_dispenses."},
    {"rule_id": "GFACT_PHM_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_PHM_004", "rule_name": "dispense_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "dispense_date_key", "description": "dispense_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_PHM_005", "rule_name": "medication_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "medication_sk", "description": "medication_sk (FK → dim_medication) must not be NULL."},
    {"rule_id": "GFACT_PHM_006", "rule_name": "quantity_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "quantity", "description": "quantity must be positive."},
    {"rule_id": "GFACT_PHM_007", "rule_name": "total_amount_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "total_amount", "description": "total_amount must be positive."},
    {"rule_id": "GFACT_PHM_008", "rule_name": "unit_price_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "unit_price", "description": "unit_price must be positive."},
]

FACT_BILLING_RULES = [
    {"rule_id": "GFACT_BIL_001", "rule_name": "invoice_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "invoice_id", "description": "invoice_id (grain NK) must not be NULL."},
    {"rule_id": "GFACT_BIL_002", "rule_name": "invoice_id_unique", "rule_type": "unique",
     "category": CATEGORY_UNIQUENESS, "severity": SEVERITY_CRITICAL,
     "columns": ["invoice_id"], "description": "invoice_id must be unique in fact_billing."},
    {"rule_id": "GFACT_BIL_003", "rule_name": "enterprise_patient_id_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "enterprise_patient_id", "description": "enterprise_patient_id (FK) must not be NULL."},
    {"rule_id": "GFACT_BIL_004", "rule_name": "invoice_date_key_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "invoice_date_key", "description": "invoice_date_key (FK → dim_date) must not be NULL."},
    {"rule_id": "GFACT_BIL_005", "rule_name": "payer_sk_not_null", "rule_type": "not_null",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_CRITICAL,
     "column": "payer_sk", "description": "payer_sk (FK → dim_payer) must not be NULL. 0 = SELF-PAY is valid."},
    {"rule_id": "GFACT_BIL_006", "rule_name": "payer_sk_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "payer_sk", "min_value": 0, "description": "payer_sk must be >= 0 (0 = SELF-PAY sentinel)."},
    {"rule_id": "GFACT_BIL_007", "rule_name": "gross_amount_positive", "rule_type": "positive",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "gross_amount", "description": "gross_amount must be positive."},
    {"rule_id": "GFACT_BIL_008", "rule_name": "net_amount_due_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "net_amount_due", "min_value": 0, "description": "net_amount_due must be >= 0."},
    {"rule_id": "GFACT_BIL_009", "rule_name": "total_amount_paid_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_HIGH,
     "column": "total_amount_paid", "min_value": 0, "description": "total_amount_paid must be >= 0."},
    {"rule_id": "GFACT_BIL_010", "rule_name": "total_insurance_paid_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "total_insurance_paid", "min_value": 0, "description": "total_insurance_paid must be >= 0."},
    {"rule_id": "GFACT_BIL_011", "rule_name": "payment_count_non_negative", "rule_type": "range",
     "category": CATEGORY_VALIDITY, "severity": SEVERITY_MEDIUM,
     "column": "payment_count", "min_value": 0, "description": "payment_count must be >= 0."},
    {"rule_id": "GFACT_BIL_012", "rule_name": "payer_name_not_blank", "rule_type": "not_blank",
     "category": CATEGORY_COMPLETENESS, "severity": SEVERITY_HIGH,
     "column": "payer_name", "description": "payer_name must not be blank (SELF-PAY is valid default)."},
]