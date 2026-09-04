#!/usr/bin/env python3
"""
Multi-Branch Hospital Data Warehouse — Synthetic Source Data Generator
========================================================================

Generates realistic, INTENTIONALLY OVERLAPPING OLTP-style source data for
three hospital branches — Cairo, Giza and Alexandria — structured into
standard healthcare domains:

    cairo/ (and giza/, alexandria/)
    │
    ├── emr/
    │   ├── patients/
    │   ├── doctors/
    │   ├── departments/
    │   ├── encounters/
    │   ├── diagnoses/
    │   ├── procedures/
    │   └── admissions/
    │
    ├── laboratory/
    │   ├── lab_orders/
    │   └── lab_results/
    │
    ├── pharmacy/
    │   ├── medications/
    │   ├── prescriptions/
    │   └── pharmacy_transactions/
    │
    └── billing/
        ├── invoices/
        ├── payments/
        └── insurance_claims/

All output CSV files include the extract/batch date in their filename, e.g.:
    cairo/emr/patients/patients_2026-09-03.csv

Usage:
------
    python generate_hospital_data.py --patients 3000 --seed 42 --outdir output --batch-date 2026-09-03

(No third-party dependencies — pure Python 3 standard library.)
"""

import argparse
import csv
import json
import os
import random
from datetime import datetime, timedelta, date

# ---------------------------------------------------------------------------
# 0. CONFIG
# ---------------------------------------------------------------------------

DEFAULT_START_DATE = date(2019, 1, 1)
DEFAULT_END_DATE = date(2026, 8, 29)  # simulated "today"
DEFAULT_BATCH_DATE = date(2026, 9, 3)

BRANCH_COUNT_WEIGHTS = {1: 0.70, 2: 0.22, 3: 0.08}

ACTIVITY_TIERS = {
    "low":        (0.65, 1, 2),
    "medium":     (0.25, 3, 6),
    "high":       (0.08, 7, 15),
    "very_high":  (0.02, 16, 40),
}

CHRONIC_PATIENT_RATE = 0.28
DUPLICATE_DIRTY_RATE = 0.55
DOCTOR_CROSS_BRANCH_RATE = 0.10

ENCOUNTER_STATUS_WEIGHTS = {
    "Completed": 0.80, "Cancelled": 0.07, "No-show": 0.06, "Rescheduled": 0.07,
}
PAYMENT_METHODS = ["Cash", "Insurance", "Credit Card", "Installment"]
ENCOUNTER_TYPES = ["Outpatient", "Emergency", "Follow-up", "Inpatient"]
INSURANCE_PAYERS = ["Allianz Egypt", "AXA Egypt", "Bupa Egypt", "MetLife Egypt", "Misr Insurance"]

LAB_ORDER_CHANCE = 0.65
PHARMACY_PRESCRIPTION_CHANCE = 0.75
ADMISSION_BASE_CHANCE = 0.05
ADMISSION_EMERGENCY_CHANCE = 0.20
SURGERY_PROCEDURE_CHANCE = 0.75

# ---------------------------------------------------------------------------
# 1. REFERENCE DATA
# ---------------------------------------------------------------------------

EGY_MALE_FIRST = [
    "Ahmed", "Mohamed", "Mahmoud", "Youssef", "Omar", "Khaled", "Karim", "Amr",
    "Hassan", "Hussein", "Tarek", "Sherif", "Sameh", "Wael", "Ayman", "Ibrahim",
    "Mostafa", "Ali", "Adel", "Fady", "Bassem", "Ashraf", "Ehab", "Hany",
    "Nabil", "Ramy", "Sayed", "Sami", "Waleed", "Yasser", "Ziad", "Osama",
]
EGY_FEMALE_FIRST = [
    "Mona", "Heba", "Nour", "Sara", "Yasmin", "Dina", "Rania", "Aya",
    "Mariam", "Nada", "Salma", "Hala", "Amira", "Reem", "Nesma", "Ghada",
    "Lobna", "Marwa", "Shaimaa", "Doaa", "Eman", "Fatma", "Hanan", "Iman",
    "Manal", "Nashwa", "Ola", "Rasha", "Sahar", "Wafaa", "Yara", "Zeinab",
]
EGY_LAST = [
    "Abdelrahman", "Abdallah", "El-Sayed", "Ibrahim", "Hassan", "Mahmoud",
    "Mansour", "Farouk", "Kamal", "Fathy", "Salem", "Youssef", "Ali",
    "Zaki", "Nasser", "Rashad", "Hamdy", "Fawzy", "Gaber", "Metwally",
    "Shaker", "Abdelaziz", "Ezzat", "Soliman", "Tawfik", "Gomaa", "Ragab",
    "Sabry", "Hegazy", "Amer",
]

BRANCH_DISTRICTS = {
    "CAI": ["Nasr City", "Heliopolis", "Maadi", "Downtown Cairo", "Dokki",
            "Mohandessin", "Shubra", "Zamalek", "New Cairo"],
    "GIZ": ["Dokki", "Mohandessin", "Haram", "6th of October", "Faisal",
            "Sheikh Zayed", "Imbaba"],
    "ALX": ["Sidi Gaber", "Smouha", "Stanley", "Miami", "Louran",
            "Roushdy", "Sporting", "Gleem"],
}

BRANCHES = {
    "CAI": {"name": "Cairo Main", "city": "cairo", "size_mult": 1.4, "excluded_depts": []},
    "GIZ": {"name": "Giza", "city": "giza", "size_mult": 0.85, "excluded_depts": ["Neurology", "Urology"]},
    "ALX": {"name": "Alexandria", "city": "alexandria", "size_mult": 1.15, "excluded_depts": [], "emergency_boost": 1.6},
}
BRANCH_VISIT_WEIGHTS = {"CAI": 0.45, "GIZ": 0.24, "ALX": 0.31}

DEPARTMENTS = [
    "Cardiology", "Orthopedics", "Emergency", "Internal Medicine",
    "General Surgery", "Pediatrics", "Obstetrics & Gynecology", "Neurology",
    "Dermatology", "ENT", "Radiology", "Laboratory", "Ophthalmology", "Urology",
]

SURGICAL_DEPTS = {
    "General Surgery", "Orthopedics", "Cardiology", "Obstetrics & Gynecology",
    "Ophthalmology", "Urology", "ENT",
}

WARD_DEPTS = {
    "Internal Medicine", "Cardiology", "General Surgery", "Orthopedics",
    "Pediatrics", "Obstetrics & Gynecology", "Emergency",
}

# (diagnosis, icd10, [medication_names], is_chronic, needs_procedure, [lab_tests])
DIAGNOSES_DATA = {
    "Cardiology": [
        ("Essential Hypertension", "I10", ["Amlodipine 5mg", "Losartan 50mg"], True, False, ["Lipid Profile", "Serum Electrolytes"]),
        ("Coronary Artery Disease", "I25.10", ["Aspirin 75mg", "Atorvastatin 20mg"], True, True, ["Lipid Profile", "Cardiac Troponin I", "ECG"]),
        ("Cardiac Arrhythmia", "I49.9", ["Atenolol 50mg", "Aspirin 75mg"], True, False, ["ECG", "Serum Electrolytes"]),
        ("Congestive Heart Failure", "I50.9", ["Hydrochlorothiazide 25mg", "Losartan 50mg"], True, True, ["Basic Metabolic Panel", "ECG"]),
    ],
    "Orthopedics": [
        ("Closed Fracture of Bone", "S82.90", ["Ibuprofen 400mg", "Paracetamol 500mg"], False, True, ["Complete Blood Count"]),
        ("Osteoarthritis", "M19.90", ["Ibuprofen 400mg", "Paracetamol 500mg"], True, False, ["Complete Blood Count", "CRP"]),
        ("Intervertebral Disc Disorder", "M51.2", ["Ibuprofen 400mg"], True, False, []),
        ("Ligament Sprain / Tear", "S83.2", ["Ibuprofen 400mg", "Paracetamol 500mg"], False, True, ["Complete Blood Count"]),
    ],
    "Emergency": [
        ("Chest Pain, Unspecified", "R07.9", ["Aspirin 75mg"], False, False, ["Cardiac Troponin I", "ECG", "Complete Blood Count"]),
        ("Traffic Accident Multiple Trauma", "V89.2", ["Paracetamol 500mg"], False, True, ["Complete Blood Count", "Basic Metabolic Panel"]),
        ("Acute Gastroenteritis", "A09", ["Oral Rehydration Salts", "Ondansetron 4mg"], False, False, ["Basic Metabolic Panel", "Stool Analysis"]),
        ("Acute Asthma Exacerbation", "J45.901", ["Salbutamol Inhaler 100mcg"], False, False, ["Complete Blood Count", "Arterial Blood Gas"]),
    ],
    "Internal Medicine": [
        ("Type 2 Diabetes Mellitus", "E11.9", ["Metformin 500mg", "Insulin Glargine 100 IU/mL"], True, False, ["HbA1c & Fasting Glucose", "Lipid Profile"]),
        ("Acute Upper Respiratory Infection", "J06.9", ["Paracetamol 500mg", "Amoxicillin 500mg"], False, False, ["Complete Blood Count"]),
        ("Hyperlipidemia", "E78.5", ["Atorvastatin 20mg"], True, False, ["Lipid Profile", "Liver Function Test"]),
        ("Iron Deficiency Anemia", "D64.9", ["Ferrous Sulfate 200mg"], False, False, ["Complete Blood Count", "Serum Ferritin"]),
    ],
    "General Surgery": [
        ("Acute Appendicitis", "K35.80", ["Amoxicillin 500mg", "Paracetamol 500mg"], False, True, ["Complete Blood Count", "C-Reactive Protein"]),
        ("Calculus of Gallbladder (Gallstones)", "K80.20", ["Ibuprofen 400mg"], False, True, ["Liver Function Test", "Complete Blood Count"]),
        ("Inguinal Hernia", "K40.90", ["Paracetamol 500mg"], False, True, ["Complete Blood Count"]),
    ],
    "Pediatrics": [
        ("Common Cold / Acute Rhinopharyngitis", "J00", ["Paracetamol 500mg"], False, False, []),
        ("Childhood Asthma", "J45.20", ["Salbutamol Inhaler 100mcg"], True, False, ["Complete Blood Count"]),
        ("Viral Fever Syndrome", "A99", ["Paracetamol 500mg"], False, False, ["Complete Blood Count"]),
    ],
    "Obstetrics & Gynecology": [
        ("Normal Antepartum Pregnancy Supervision", "Z34.00", ["Prenatal Multivitamin", "Folic Acid 5mg"], False, False, ["Complete Blood Count", "Urinalysis", "Blood Typing & Rh"]),
        ("Polycystic Ovarian Syndrome (PCOS)", "E28.2", ["Metformin 500mg"], True, False, ["Hormone Panel", "HbA1c & Fasting Glucose"]),
        ("Single Spontaneous Delivery", "O80", ["Paracetamol 500mg"], False, True, ["Complete Blood Count", "Blood Typing & Rh"]),
    ],
    "Neurology": [
        ("Migraine Headache", "G43.909", ["Ibuprofen 400mg", "Paracetamol 500mg"], True, False, []),
        ("Epilepsy, Unspecified", "G40.909", ["Levetiracetam 500mg"], True, False, ["Basic Metabolic Panel", "Complete Blood Count"]),
    ],
    "Dermatology": [
        ("Atopic Dermatitis (Eczema)", "L30.9", ["Hydrocortisone Cream 1%", "Cetirizine 10mg"], True, False, []),
        ("Acne Vulgaris", "L70.0", ["Clindamycin Topical Gel", "Doxycycline 100mg"], False, False, []),
    ],
    "ENT": [
        ("Acute Sinusitis", "J01.90", ["Amoxicillin 500mg", "Cetirizine 10mg"], False, False, []),
        ("Chronic Tonsillitis", "J35.01", ["Amoxicillin 500mg"], True, True, ["Complete Blood Count"]),
    ],
    "Radiology": [
        ("Encounter for Diagnostic Imaging", "Z01.89", [], False, False, []),
    ],
    "Laboratory": [
        ("General Routine Medical Examination", "Z00.00", [], False, False, ["Complete Blood Count", "Lipid Profile", "HbA1c & Fasting Glucose"]),
    ],
    "Ophthalmology": [
        ("Refractive Error", "H52.7", [], False, False, []),
        ("Age-related Cataract", "H26.9", ["Moxifloxacin Eye Drops"], True, True, ["Complete Blood Count", "HbA1c & Fasting Glucose"]),
    ],
    "Urology": [
        ("Urinary Tract Infection", "N39.0", ["Ciprofloxacin 500mg"], False, False, ["Urinalysis", "Urine Culture"]),
        ("Calculus of Kidney (Kidney Stones)", "N20.0", ["Ibuprofen 400mg"], False, True, ["Urinalysis", "Serum Creatinine", "Complete Blood Count"]),
    ],
}

PROCEDURE_NAMES_BY_DIAGNOSIS = {
    "Coronary Artery Disease": "Coronary Angioplasty with Stent",
    "Congestive Heart Failure": "Permanent Pacemaker Implantation",
    "Closed Fracture of Bone": "Open Reduction and Internal Fixation (ORIF)",
    "Ligament Sprain / Tear": "Arthroscopic Ligament Reconstruction",
    "Traffic Accident Multiple Trauma": "Exploratory Trauma Laparotomy",
    "Acute Appendicitis": "Laparoscopic Appendectomy",
    "Calculus of Gallbladder (Gallstones)": "Laparoscopic Cholecystectomy",
    "Inguinal Hernia": "Inguinal Hernia Repair with Mesh",
    "Single Spontaneous Delivery": "Cesarean Section Delivery",
    "Chronic Tonsillitis": "Total Tonsillectomy",
    "Age-related Cataract": "Phacoemulsification with Intraocular Lens",
    "Calculus of Kidney (Kidney Stones)": "Extracorporeal Shock Wave Lithotripsy (ESWL)",
}

MEDICATION_CATALOG = [
    {"medication_id": "MED-001", "medication_name": "Amlodipine 5mg", "category": "Cardiovascular", "dosage_form": "Tablet", "strength": "5mg", "unit_price": 45.0},
    {"medication_id": "MED-002", "medication_name": "Atorvastatin 20mg", "category": "Lipid-lowering", "dosage_form": "Tablet", "strength": "20mg", "unit_price": 85.0},
    {"medication_id": "MED-003", "medication_name": "Metformin 500mg", "category": "Antidiabetic", "dosage_form": "Tablet", "strength": "500mg", "unit_price": 35.0},
    {"medication_id": "MED-004", "medication_name": "Aspirin 75mg", "category": "Antiplatelet", "dosage_form": "Tablet", "strength": "75mg", "unit_price": 20.0},
    {"medication_id": "MED-005", "medication_name": "Amoxicillin 500mg", "category": "Antibiotic", "dosage_form": "Capsule", "strength": "500mg", "unit_price": 60.0},
    {"medication_id": "MED-006", "medication_name": "Paracetamol 500mg", "category": "Analgesic", "dosage_form": "Tablet", "strength": "500mg", "unit_price": 25.0},
    {"medication_id": "MED-007", "medication_name": "Ibuprofen 400mg", "category": "NSAID", "dosage_form": "Tablet", "strength": "400mg", "unit_price": 30.0},
    {"medication_id": "MED-008", "medication_name": "Omeprazole 20mg", "category": "Gastrointestinal", "dosage_form": "Capsule", "strength": "20mg", "unit_price": 50.0},
    {"medication_id": "MED-009", "medication_name": "Salbutamol Inhaler 100mcg", "category": "Respiratory", "dosage_form": "Inhaler", "strength": "100mcg", "unit_price": 120.0},
    {"medication_id": "MED-010", "medication_name": "Ciprofloxacin 500mg", "category": "Antibiotic", "dosage_form": "Tablet", "strength": "500mg", "unit_price": 75.0},
    {"medication_id": "MED-011", "medication_name": "Insulin Glargine 100 IU/mL", "category": "Antidiabetic", "dosage_form": "Injection", "strength": "100 IU/mL", "unit_price": 280.0},
    {"medication_id": "MED-012", "medication_name": "Hydrochlorothiazide 25mg", "category": "Diuretic", "dosage_form": "Tablet", "strength": "25mg", "unit_price": 30.0},
    {"medication_id": "MED-013", "medication_name": "Cetirizine 10mg", "category": "Antihistamine", "dosage_form": "Tablet", "strength": "10mg", "unit_price": 28.0},
    {"medication_id": "MED-014", "medication_name": "Losartan 50mg", "category": "Antihypertensive", "dosage_form": "Tablet", "strength": "50mg", "unit_price": 55.0},
    {"medication_id": "MED-015", "medication_name": "Oral Rehydration Salts", "category": "Electrolytes", "dosage_form": "Sachet", "strength": "20.5g", "unit_price": 15.0},
    {"medication_id": "MED-016", "medication_name": "Ondansetron 4mg", "category": "Antiemetic", "dosage_form": "Tablet", "strength": "4mg", "unit_price": 40.0},
    {"medication_id": "MED-017", "medication_name": "Ferrous Sulfate 200mg", "category": "Hematinic", "dosage_form": "Tablet", "strength": "200mg", "unit_price": 32.0},
    {"medication_id": "MED-018", "medication_name": "Prenatal Multivitamin", "category": "Supplement", "dosage_form": "Tablet", "strength": "1 Tab", "unit_price": 65.0},
    {"medication_id": "MED-019", "medication_name": "Folic Acid 5mg", "category": "Supplement", "dosage_form": "Tablet", "strength": "5mg", "unit_price": 20.0},
    {"medication_id": "MED-020", "medication_name": "Levetiracetam 500mg", "category": "Anticonvulsant", "dosage_form": "Tablet", "strength": "500mg", "unit_price": 140.0},
    {"medication_id": "MED-021", "medication_name": "Hydrocortisone Cream 1%", "category": "Topical Corticosteroid", "dosage_form": "Cream", "strength": "15g", "unit_price": 38.0},
    {"medication_id": "MED-022", "medication_name": "Clindamycin Topical Gel", "category": "Topical Antibiotic", "dosage_form": "Gel", "strength": "30g", "unit_price": 48.0},
    {"medication_id": "MED-023", "medication_name": "Doxycycline 100mg", "category": "Antibiotic", "dosage_form": "Capsule", "strength": "100mg", "unit_price": 52.0},
    {"medication_id": "MED-024", "medication_name": "Moxifloxacin Eye Drops", "category": "Ophthalmic Antibiotic", "dosage_form": "Drops", "strength": "5mL", "unit_price": 70.0},
    {"medication_id": "MED-025", "medication_name": "Atenolol 50mg", "category": "Beta Blocker", "dosage_form": "Tablet", "strength": "50mg", "unit_price": 35.0},
]
MED_BY_NAME = {m["medication_name"]: m for m in MEDICATION_CATALOG}

LAB_TEST_DEFINITIONS = {
    "Complete Blood Count": [
        ("Hemoglobin", 12.0, 17.5, "g/dL"),
        ("White Blood Cell Count", 4.5, 11.0, "x10^3/uL"),
        ("Platelet Count", 150, 450, "x10^3/uL"),
    ],
    "Lipid Profile": [
        ("Total Cholesterol", 130, 200, "mg/dL"),
        ("HDL Cholesterol", 40, 60, "mg/dL"),
        ("LDL Cholesterol", 70, 130, "mg/dL"),
        ("Triglycerides", 50, 150, "mg/dL"),
    ],
    "HbA1c & Fasting Glucose": [
        ("Fasting Blood Glucose", 70, 100, "mg/dL"),
        ("Hemoglobin A1c", 4.0, 5.6, "%"),
    ],
    "Basic Metabolic Panel": [
        ("Serum Sodium", 135, 145, "mmol/L"),
        ("Serum Potassium", 3.5, 5.0, "mmol/L"),
        ("Blood Urea Nitrogen", 7, 20, "mg/dL"),
        ("Serum Creatinine", 0.6, 1.2, "mg/dL"),
    ],
    "Cardiac Troponin I": [
        ("Troponin I", 0.0, 0.04, "ng/mL"),
    ],
    "ECG": [
        ("Heart Rate", 60, 100, "bpm"),
        ("PR Interval", 120, 200, "ms"),
        ("QRS Duration", 80, 120, "ms"),
    ],
    "Liver Function Test": [
        ("Alanine Aminotransferase (ALT)", 7, 56, "U/L"),
        ("Aspartate Aminotransferase (AST)", 10, 40, "U/L"),
        ("Total Bilirubin", 0.1, 1.2, "mg/dL"),
    ],
    "Urinalysis": [
        ("Urine Specific Gravity", 1.005, 1.030, ""),
        ("Urine pH", 4.6, 8.0, "pH"),
    ],
    "Urine Culture": [
        ("Bacterial Colony Count", 0, 10000, "CFU/mL"),
    ],
    "C-Reactive Protein": [
        ("CRP", 0.0, 5.0, "mg/L"),
    ],
    "Serum Ferritin": [
        ("Ferritin", 20, 250, "ng/mL"),
    ],
    "Blood Typing & Rh": [
        ("Rh Factor", 0, 1, ""),
    ],
    "Stool Analysis": [
        ("Fecal Occult Blood", 0, 0, "Neg/Pos"),
    ],
    "Arterial Blood Gas": [
        ("Arterial pH", 7.35, 7.45, "pH"),
        ("PaO2", 80, 100, "mmHg"),
    ],
    "Hormone Panel": [
        ("Total Testosterone", 15, 70, "ng/dL"),
        ("Luteinizing Hormone (LH)", 1.9, 12.5, "mIU/mL"),
    ],
    "Serum Creatinine": [
        ("Serum Creatinine", 0.6, 1.2, "mg/dL"),
    ],
}

# ---------------------------------------------------------------------------
# 2. OUTPUT FOLDER STRUCTURE & SCHEMA SPECIFICATIONS
# ---------------------------------------------------------------------------

DOMAIN_MAP = {
    # Table Name -> (domain, folder_name, columns)
    "patients": (
        "emr", "patients",
        ["patient_id", "first_name", "last_name", "gender", "date_of_birth", "contact_no", "address", "created_at"]
    ),
    "doctors": (
        "emr", "doctors",
        ["doctor_id", "dept_id", "first_name", "last_name", "gender", "contact_no", "specialty", "office_no"]
    ),
    "departments": (
        "emr", "departments",
        ["dept_id", "dept_name"]
    ),
    "encounters": (
        "emr", "encounters",
        ["encounter_id", "patient_id", "doctor_id", "dept_id", "encounter_date", "encounter_type", "reason", "status"]
    ),
    "diagnoses": (
        "emr", "diagnoses",
        ["diagnosis_id", "encounter_id", "patient_id", "doctor_id", "diagnosis_name", "icd10_code", "diagnosis_type", "diagnosis_date"]
    ),
    "procedures": (
        "emr", "procedures",
        ["procedure_id", "encounter_id", "admission_id", "patient_id", "surgeon_id", "procedure_name", "procedure_date", "start_time", "end_time", "room_no", "notes"]
    ),
    "admissions": (
        "emr", "admissions",
        ["admission_id", "encounter_id", "patient_id", "dept_id", "ward_name", "room_no", "bed_no", "admission_date", "discharge_date", "admission_type", "discharge_disposition"]
    ),
    "lab_orders": (
        "laboratory", "lab_orders",
        ["order_id", "encounter_id", "patient_id", "doctor_id", "test_name", "order_date", "order_status"]
    ),
    "lab_results": (
        "laboratory", "lab_results",
        ["result_id", "order_id", "patient_id", "test_name", "parameter_name", "result_value", "unit_of_measure", "reference_range", "abnormal_flag", "result_date"]
    ),
    "medications": (
        "pharmacy", "medications",
        ["medication_id", "medication_name", "category", "dosage_form", "strength", "unit_price"]
    ),
    "prescriptions": (
        "pharmacy", "prescriptions",
        ["prescription_id", "encounter_id", "patient_id", "doctor_id", "medication_id", "medication_name", "dosage", "frequency", "duration_days", "prescribed_date"]
    ),
    "pharmacy_transactions": (
        "pharmacy", "pharmacy_transactions",
        ["transaction_id", "prescription_id", "patient_id", "dispense_date", "quantity", "unit_price", "total_amount", "payment_method"]
    ),
    "invoices": (
        "billing", "invoices",
        ["invoice_id", "encounter_id", "admission_id", "patient_id", "invoice_date", "gross_amount", "discount_amount", "insurance_covered_amount", "net_amount_due", "status"]
    ),
    "payments": (
        "billing", "payments",
        ["payment_id", "invoice_id", "patient_id", "payment_date", "amount_paid", "payment_method", "payment_status"]
    ),
    "insurance_claims": (
        "billing", "insurance_claims",
        ["claim_id", "invoice_id", "patient_id", "payer_name", "claim_amount", "coverage_percentage", "claim_status", "claim_date", "settlement_date"]
    ),
}

# ---------------------------------------------------------------------------
# 3. HELPERS
# ---------------------------------------------------------------------------

class IdGen:
    def __init__(self, branch_code):
        self.branch_code = branch_code
        self._counters = {}

    def next(self, abbr, width=6):
        self._counters[abbr] = self._counters.get(abbr, 0) + 1
        return f"{self.branch_code}-{abbr}-{self._counters[abbr]:0{width}d}"


def rand_date_between(rng, start: date, end: date) -> date:
    days = (end - start).days
    return start + timedelta(days=rng.randint(0, max(days, 0)))


def egyptian_phone(rng):
    prefix = rng.choice(["010", "011", "012", "015"])
    return prefix + "".join(str(rng.randint(0, 9)) for _ in range(8))


def messy_phone(rng, clean_phone):
    variants = [
        clean_phone,
        "+2" + clean_phone,
        "2" + clean_phone,
        clean_phone[:4] + "-" + clean_phone[4:7] + "-" + clean_phone[7:],
        clean_phone[:3] + " " + clean_phone[3:],
    ]
    return rng.choice(variants)


def messy_name_case(rng, name):
    roll = rng.random()
    if roll < 0.4:
        return name.upper()
    if roll < 0.6:
        return name.lower()
    if roll < 0.75:
        return "  " + name + " "
    return name


def format_date_messy(rng, d: date, dirty=False):
    if not dirty:
        return d.strftime("%Y-%m-%d")
    fmt = rng.choice(["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%d-%b-%Y"])
    return d.strftime(fmt)


def messy_gender(rng, g):
    if g == "M":
        return rng.choice(["M", "Male", "male", "M"])
    return rng.choice(["F", "Female", "female", "F"])


def weighted_choice(rng, weight_map):
    keys = list(weight_map.keys())
    weights = list(weight_map.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def pick_age(rng):
    roll = rng.random()
    if roll < 0.15:
        return rng.randint(0, 17)
    if roll < 0.50:
        return rng.randint(18, 40)
    if roll < 0.80:
        return rng.randint(41, 60)
    return rng.randint(61, 90)

# ---------------------------------------------------------------------------
# 4. BRANCH DATA CONTAINER
# ---------------------------------------------------------------------------

class BranchData:
    def __init__(self, code, cfg):
        self.code = code
        self.cfg = cfg
        self.idgen = IdGen(code)
        self.tables = {name: [] for name in DOMAIN_MAP}
        self.dept_ids_by_name = {}
        self.doctors_by_dept = {}

    def add(self, table_name, row):
        self.tables[table_name].append(row)
        return row

# ---------------------------------------------------------------------------
# 5. DATA BUILDERS
# ---------------------------------------------------------------------------

def build_departments(bd: BranchData, rng):
    excluded = set(BRANCHES[bd.code].get("excluded_depts", []))
    for dept_name in DEPARTMENTS:
        if dept_name in excluded:
            continue
        dept_id = bd.idgen.next("DEP", width=3)
        bd.add("departments", {"dept_id": dept_id, "dept_name": dept_name})
        bd.dept_ids_by_name[dept_name] = dept_id
    return bd.dept_ids_by_name


def build_doctors(bd: BranchData, rng):
    doctor_registry = []
    for dept_name, dept_id in bd.dept_ids_by_name.items():
        base = rng.randint(2, 5)
        mult = BRANCHES[bd.code]["size_mult"]
        if dept_name == "Emergency":
            mult *= BRANCHES[bd.code].get("emergency_boost", 1.0)
        n_doctors = max(1, round(base * mult))
        bd.doctors_by_dept[dept_name] = []

        for _ in range(n_doctors):
            doct_id = bd.idgen.next("DOC", width=4)
            g = rng.choice(["M", "F"])
            fname = rng.choice(EGY_MALE_FIRST if g == "M" else EGY_FEMALE_FIRST)
            lname = rng.choice(EGY_LAST)
            row = {
                "doctor_id": doct_id,
                "dept_id": dept_id,
                "first_name": fname,
                "last_name": lname,
                "gender": g,
                "contact_no": egyptian_phone(rng),
                "specialty": dept_name,
                "office_no": f"{bd.code}-{rng.randint(100, 599)}",
            }
            bd.add("doctors", row)
            bd.doctors_by_dept[dept_name].append(doct_id)
            doctor_registry.append((dept_name, row))
    return doctor_registry


def build_master_patients(rng, n, end_date):
    masters = []
    for i in range(n):
        age = pick_age(rng)
        dob = end_date.replace(year=end_date.year - age)
        dob = dob - timedelta(days=rng.randint(0, 364))
        gender = rng.choice(["M", "F"])
        fname = rng.choice(EGY_MALE_FIRST if gender == "M" else EGY_FEMALE_FIRST)
        lname = rng.choice(EGY_LAST)
        home_branch = weighted_choice(rng, BRANCH_VISIT_WEIGHTS)

        n_branches = weighted_choice(rng, BRANCH_COUNT_WEIGHTS)
        others = [b for b in BRANCHES if b != home_branch]
        rng.shuffle(others)
        branch_list = [home_branch] + others[: n_branches - 1]

        tier = weighted_choice(rng, {k: v[0] for k, v in ACTIVITY_TIERS.items()})
        lo, hi = ACTIVITY_TIERS[tier][1], ACTIVITY_TIERS[tier][2]
        total_journeys = rng.randint(lo, hi)

        chronic = age >= 25 and rng.random() < CHRONIC_PATIENT_RATE
        chronic_dept, chronic_entry = None, None
        if chronic:
            chronic_dept = rng.choice([d for d in DIAGNOSES_DATA if any(e[3] for e in DIAGNOSES_DATA[d])])
            chronic_entry = rng.choice([e for e in DIAGNOSES_DATA[chronic_dept] if e[3]])

        masters.append({
            "master_key": f"MPI-{i+1:06d}",
            "gender": gender, "fname": fname, "lname": lname, "dob": dob,
            "phone": egyptian_phone(rng),
            "home_branch": home_branch, "branch_list": branch_list,
            "tier": tier, "total_journeys": total_journeys,
            "chronic": chronic, "chronic_dept": chronic_dept, "chronic_entry": chronic_entry,
        })
    return masters


def make_patient_row(rng, bd: BranchData, master, dirty=False):
    patient_id = bd.idgen.next("PAT", width=6)
    fname, lname, phone = master["fname"], master["lname"], master["phone"]
    gender = master["gender"]
    if dirty:
        fname = messy_name_case(rng, fname)
        lname = messy_name_case(rng, lname)
        phone = messy_phone(rng, phone)
    district = rng.choice(BRANCH_DISTRICTS[bd.code])
    city_name = BRANCHES[bd.code]["city"].title()
    address = f"{rng.randint(1, 180)} {district} St., {city_name}"
    row = {
        "patient_id": patient_id,
        "first_name": fname,
        "last_name": lname,
        "gender": messy_gender(rng, gender) if dirty and rng.random() < 0.3 else gender,
        "date_of_birth": format_date_messy(rng, master["dob"], dirty=dirty and rng.random() < 0.5),
        "contact_no": phone,
        "address": address,
        "created_at": (master["dob"] + timedelta(days=365 * 18)).strftime("%Y-%m-%d"),
    }
    bd.add("patients", row)
    return patient_id


def split_journeys_across_branches(rng, master):
    branches = master["branch_list"]
    total = master["total_journeys"]
    if len(branches) == 1:
        return {branches[0]: total}
    shares = {1: [1.0], 2: [0.72, 0.28], 3: [0.55, 0.30, 0.15]}[len(branches)]
    counts = {}
    remaining = total
    for idx, b in enumerate(branches):
        if idx == len(branches) - 1:
            counts[b] = max(1, remaining)
        else:
            c = max(1, round(total * shares[idx]))
            c = min(c, remaining - (len(branches) - idx - 1))
            counts[b] = max(1, c)
            remaining -= counts[b]
    return counts


def gen_visit_dates(rng, start_date, end_date, count, chronic):
    if not chronic:
        dates = [rand_date_between(rng, start_date, end_date) for _ in range(count)]
        return sorted(dates)
    span_days = (end_date - start_date).days
    first_start = start_date if span_days <= 180 else rand_date_between(
        rng, start_date, min(end_date, start_date + timedelta(days=180)))
    dates, cursor = [], first_start
    for _ in range(count):
        if cursor > end_date:
            break
        dates.append(cursor)
        cursor = cursor + timedelta(days=rng.randint(60, 210))
    while len(dates) < count:
        dates.append(rand_date_between(rng, start_date, end_date))
    return sorted(dates[:count])

# ---------------------------------------------------------------------------
# 6. JOURNEY SIMULATOR (EMR + Laboratory + Pharmacy + Billing)
# ---------------------------------------------------------------------------

def simulate_journey(rng, bd: BranchData, master, patient_id, visit_date, visit_idx, end_date):
    is_chronic_visit = master["chronic"] and rng.random() < 0.85
    if is_chronic_visit and master["chronic_dept"] in bd.dept_ids_by_name:
        dept_name = master["chronic_dept"]
        diag_entry = master["chronic_entry"]
    else:
        dept_name = rng.choice(list(bd.dept_ids_by_name.keys()))
        diag_entry = rng.choice(DIAGNOSES_DATA[dept_name])

    diag_name, icd10, med_names, is_chronic_dx, needs_proc, lab_tests = diag_entry
    dept_id = bd.dept_ids_by_name[dept_name]

    if dept_name not in bd.doctors_by_dept or not bd.doctors_by_dept[dept_name]:
        return
    doctor_id = rng.choice(bd.doctors_by_dept[dept_name])

    encounter_id = bd.idgen.next("ENC", width=6)
    dept_hours = (0, 24) if dept_name == "Emergency" else (8, 20)
    visit_dt = datetime.combine(visit_date, datetime.min.time()) + timedelta(
        hours=rng.randint(*dept_hours), minutes=rng.choice([0, 15, 30, 45]))
    
    enc_status = weighted_choice(rng, ENCOUNTER_STATUS_WEIGHTS)
    enc_type = "Emergency" if dept_name == "Emergency" else ("Follow-up" if (is_chronic_visit and visit_idx > 0) else "Outpatient")
    reason = f"Follow-up: {diag_name}" if (is_chronic_visit and visit_idx > 0) else diag_name

    # 1. Encounter
    bd.add("encounters", {
        "encounter_id": encounter_id,
        "patient_id": patient_id,
        "doctor_id": doctor_id,
        "dept_id": dept_id,
        "encounter_date": visit_dt.strftime("%Y-%m-%d %H:%M"),
        "encounter_type": enc_type,
        "reason": reason,
        "status": enc_status,
    })

    if enc_status != "Completed":
        return

    # 2. Diagnosis
    diag_id = bd.idgen.next("DX", width=6)
    bd.add("diagnoses", {
        "diagnosis_id": diag_id,
        "encounter_id": encounter_id,
        "patient_id": patient_id,
        "doctor_id": doctor_id,
        "diagnosis_name": diag_name,
        "icd10_code": icd10,
        "diagnosis_type": "Primary",
        "diagnosis_date": visit_dt.strftime("%Y-%m-%d"),
    })

    # 3. Admission (if required or emergency)
    admission_id = ""
    admission_cost = 0.0
    admit_chance = ADMISSION_EMERGENCY_CHANCE if dept_name == "Emergency" else (
        0.90 if (needs_proc and dept_name in SURGICAL_DEPTS) else ADMISSION_BASE_CHANCE
    )
    if rng.random() < admit_chance:
        admission_id = bd.idgen.next("ADM", width=6)
        los = rng.randint(1, 3) if dept_name == "Emergency" else rng.randint(2, 8)
        discharge_dt = min(visit_dt + timedelta(days=los, hours=rng.randint(1, 10)),
                           datetime.combine(end_date, datetime.max.time().replace(microsecond=0)))
        ward_name = f"{dept_name} Ward {rng.randint(1, 3)}"
        room_no = f"{bd.code}-R{rng.randint(101, 399)}"
        bed_no = f"{bd.code}-B{rng.randint(1, 20)}"
        bd.add("admissions", {
            "admission_id": admission_id,
            "encounter_id": encounter_id,
            "patient_id": patient_id,
            "dept_id": dept_id,
            "ward_name": ward_name,
            "room_no": room_no,
            "bed_no": bed_no,
            "admission_date": visit_dt.strftime("%Y-%m-%d %H:%M"),
            "discharge_date": discharge_dt.strftime("%Y-%m-%d %H:%M"),
            "admission_type": "Emergency" if dept_name == "Emergency" else "Elective",
            "discharge_disposition": "Routine Home" if rng.random() < 0.94 else "Transferred",
        })
        daily_rate = rng.choice([1000.0, 1500.0, 2200.0, 3000.0])
        admission_cost = daily_rate * los

    # 4. Procedure (if surgical/procedure required)
    procedure_cost = 0.0
    if needs_proc and dept_name in SURGICAL_DEPTS and rng.random() < SURGERY_PROCEDURE_CHANCE:
        proc_id = bd.idgen.next("PRC", width=6)
        proc_name = PROCEDURE_NAMES_BY_DIAGNOSIS.get(diag_name, f"{dept_name} Surgical Intervention")
        proc_date = visit_dt.date() + timedelta(days=rng.randint(0, 3))
        proc_date = min(proc_date, end_date)
        start_h = rng.randint(8, 16)
        dur_h = rng.randint(1, 4)
        bd.add("procedures", {
            "procedure_id": proc_id,
            "encounter_id": encounter_id,
            "admission_id": admission_id,
            "patient_id": patient_id,
            "surgeon_id": doctor_id,
            "procedure_name": proc_name,
            "procedure_date": proc_date.strftime("%Y-%m-%d"),
            "start_time": f"{start_h:02d}:00",
            "end_time": f"{(start_h + dur_h) % 24:02d}:00",
            "room_no": f"{bd.code}-OT{rng.randint(1, 5)}",
            "notes": f"Successful {proc_name} indicated by {diag_name}.",
        })
        procedure_cost = rng.choice([4500.0, 7500.0, 12000.0, 18500.0])

    # 5. Laboratory Orders & Results
    lab_cost = 0.0
    tests_to_order = list(lab_tests) if lab_tests else []
    if not tests_to_order and rng.random() < LAB_ORDER_CHANCE:
        tests_to_order = [rng.choice(list(LAB_TEST_DEFINITIONS.keys()))]

    for test_name in tests_to_order:
        order_id = bd.idgen.next("LAB", width=6)
        order_dt = visit_dt + timedelta(minutes=rng.randint(15, 60))
        bd.add("lab_orders", {
            "order_id": order_id,
            "encounter_id": encounter_id,
            "patient_id": patient_id,
            "doctor_id": doctor_id,
            "test_name": test_name,
            "order_date": order_dt.strftime("%Y-%m-%d %H:%M"),
            "order_status": "Completed",
        })
        lab_cost += rng.choice([150.0, 220.0, 350.0, 480.0])

        if test_name in LAB_TEST_DEFINITIONS:
            for param_name, min_v, max_v, uom in LAB_TEST_DEFINITIONS[test_name]:
                res_id = bd.idgen.next("RES", width=6)
                is_abnormal = rng.random() < 0.22
                if min_v == max_v:
                    val = 0.0 if not is_abnormal else 1.0
                elif is_abnormal:
                    val = round(min_v * rng.uniform(0.7, 0.95), 2) if rng.random() < 0.5 else round(max_v * rng.uniform(1.05, 1.4), 2)
                else:
                    val = round(rng.uniform(min_v, max_v), 2)
                
                res_dt = order_dt + timedelta(hours=rng.randint(1, 6))
                res_dt = min(res_dt, datetime.combine(end_date, datetime.max.time().replace(microsecond=0)))
                bd.add("lab_results", {
                    "result_id": res_id,
                    "order_id": order_id,
                    "patient_id": patient_id,
                    "test_name": test_name,
                    "parameter_name": param_name,
                    "result_value": str(val),
                    "unit_of_measure": uom,
                    "reference_range": f"{min_v} - {max_v}" if min_v != max_v else "Negative",
                    "abnormal_flag": "Y" if is_abnormal else "N",
                    "result_date": res_dt.strftime("%Y-%m-%d %H:%M"),
                })

    # 6. Pharmacy Prescriptions & Transactions
    pharmacy_cost = 0.0
    meds_prescribed = [m for m in med_names if m in MED_BY_NAME]
    if not meds_prescribed and rng.random() < PHARMACY_PRESCRIPTION_CHANCE:
        meds_prescribed = [rng.choice(MEDICATION_CATALOG)["medication_name"]]

    for med_name in meds_prescribed:
        med_info = MED_BY_NAME.get(med_name, MEDICATION_CATALOG[0])
        rx_id = bd.idgen.next("RX", width=6)
        dur_days = rng.choice([7, 14, 30, 60, 90]) if is_chronic_dx else rng.choice([5, 7, 10])
        freq = rng.choice(["Once daily", "Twice daily", "Three times daily", "As needed"])
        dosage = med_info["strength"]
        rx_dt = visit_dt + timedelta(minutes=rng.randint(20, 90))
        bd.add("prescriptions", {
            "prescription_id": rx_id,
            "encounter_id": encounter_id,
            "patient_id": patient_id,
            "doctor_id": doctor_id,
            "medication_id": med_info["medication_id"],
            "medication_name": med_info["medication_name"],
            "dosage": dosage,
            "frequency": freq,
            "duration_days": dur_days,
            "prescribed_date": rx_dt.strftime("%Y-%m-%d"),
        })

        if rng.random() < 0.90:  # patient fills the prescription
            txn_id = bd.idgen.next("PHM", width=6)
            qty = max(1, dur_days // 10)
            total_rx_amt = round(qty * med_info["unit_price"], 2)
            pharmacy_cost += total_rx_amt
            dispense_dt = rx_dt + timedelta(hours=rng.randint(1, 24))
            dispense_dt = min(dispense_dt, datetime.combine(end_date, datetime.max.time().replace(microsecond=0)))
            bd.add("pharmacy_transactions", {
                "transaction_id": txn_id,
                "prescription_id": rx_id,
                "patient_id": patient_id,
                "dispense_date": dispense_dt.strftime("%Y-%m-%d %H:%M"),
                "quantity": qty,
                "unit_price": med_info["unit_price"],
                "total_amount": total_rx_amt,
                "payment_method": rng.choice(PAYMENT_METHODS),
            })

    # 7. Billing: Invoices, Payments & Insurance Claims
    consult_fee = rng.choice([250.0, 350.0, 450.0, 600.0])
    gross_amount = round(consult_fee + admission_cost + procedure_cost + lab_cost, 2)
    discount = round(gross_amount * rng.choice([0.0, 0.05, 0.10]), 2)
    
    pay_mode = rng.choice(PAYMENT_METHODS)
    is_insured = pay_mode == "Insurance"
    insurance_covered = round((gross_amount - discount) * rng.uniform(0.70, 0.90), 2) if is_insured else 0.0
    net_due = round(gross_amount - discount - insurance_covered, 2)

    invoice_id = bd.idgen.next("INV", width=6)
    inv_date = visit_dt.date() + timedelta(days=rng.randint(0, 1))
    inv_date = min(inv_date, end_date)
    bd.add("invoices", {
        "invoice_id": invoice_id,
        "encounter_id": encounter_id,
        "admission_id": admission_id,
        "patient_id": patient_id,
        "invoice_date": inv_date.strftime("%Y-%m-%d"),
        "gross_amount": gross_amount,
        "discount_amount": discount,
        "insurance_covered_amount": insurance_covered,
        "net_amount_due": net_due,
        "status": "Paid" if rng.random() < 0.95 else "Pending",
    })

    # Payments
    pay_id = bd.idgen.next("PAY", width=6)
    bd.add("payments", {
        "payment_id": pay_id,
        "invoice_id": invoice_id,
        "patient_id": patient_id,
        "payment_date": inv_date.strftime("%Y-%m-%d"),
        "amount_paid": net_due,
        "payment_method": pay_mode if not is_insured else "Credit Card",
        "payment_status": "Success",
    })

    # Insurance claims
    if is_insured and insurance_covered > 0:
        claim_id = bd.idgen.next("CLM", width=6)
        claim_status = rng.choice(["Approved", "Approved", "Approved", "Settled", "Pending"])
        settle_dt = (inv_date + timedelta(days=rng.randint(7, 30))).strftime("%Y-%m-%d") if claim_status in ["Approved", "Settled"] else ""
        bd.add("insurance_claims", {
            "claim_id": claim_id,
            "invoice_id": invoice_id,
            "patient_id": patient_id,
            "payer_name": rng.choice(INSURANCE_PAYERS),
            "claim_amount": insurance_covered,
            "coverage_percentage": round((insurance_covered / max(1.0, gross_amount - discount)) * 100, 1),
            "claim_status": claim_status,
            "claim_date": inv_date.strftime("%Y-%m-%d"),
            "settlement_date": settle_dt,
        })

# ---------------------------------------------------------------------------
# 7. EXPORT CSVs IN SPECIFIED HIERARCHICAL FOLDERS
# ---------------------------------------------------------------------------

def write_hierarchical_csvs(bd: BranchData, outdir, batch_date_str):
    branch_city = BRANCHES[bd.code]["city"].lower()
    for table_name, (domain, folder, cols) in DOMAIN_MAP.items():
        folder_path = os.path.join(outdir, branch_city, domain, folder)
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, f"{folder}_{batch_date_str}.csv")
        
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=cols)
            writer.writeheader()
            for row in bd.tables[table_name]:
                writer.writerow(row)


def write_mapping_csv(path, rows, cols):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

# ---------------------------------------------------------------------------
# 8. MASTER RUN ORCHESTRATION
# ---------------------------------------------------------------------------

def apply_doctor_cross_branch_overlap(rng, branch_data, staff_map_rows):
    all_source_entries = []
    for code, bd in branch_data.items():
        for dept_name, ids in bd.doctors_by_dept.items():
            for doct_id in ids:
                all_source_entries.append((code, dept_name, doct_id))

    n_overlap = int(len(all_source_entries) * DOCTOR_CROSS_BRANCH_RATE)
    chosen = rng.sample(all_source_entries, k=min(n_overlap, len(all_source_entries)))
    master_counter = 0

    for src_code, dept_name, src_doct_id in chosen:
        candidates = [c for c in BRANCHES if c != src_code and dept_name in branch_data[c].dept_ids_by_name]
        if not candidates:
            continue
        tgt_code = rng.choice(candidates)
        tgt_bd = branch_data[tgt_code]
        src_row = next(r for r in branch_data[src_code].tables["doctors"] if r["doctor_id"] == src_doct_id)

        new_doct_id = tgt_bd.idgen.next("DOC", width=4)
        dirty = rng.random() < DUPLICATE_DIRTY_RATE
        new_row = {
            "doctor_id": new_doct_id,
            "dept_id": tgt_bd.dept_ids_by_name[dept_name],
            "first_name": messy_name_case(rng, src_row["first_name"]) if dirty else src_row["first_name"],
            "last_name": messy_name_case(rng, src_row["last_name"]) if dirty else src_row["last_name"],
            "gender": src_row["gender"],
            "contact_no": messy_phone(rng, src_row["contact_no"]) if dirty else src_row["contact_no"],
            "specialty": src_row["specialty"],
            "office_no": f"{tgt_code}-{rng.randint(100, 599)}",
        }
        tgt_bd.add("doctors", new_row)
        tgt_bd.doctors_by_dept.setdefault(dept_name, []).append(new_doct_id)

        master_counter += 1
        master_key = f"MSTAFF-{master_counter:05d}"
        staff_map_rows.append({"master_staff_key": master_key, "branch": BRANCHES[src_code]["city"],
                               "doctor_id": src_doct_id, "is_primary_branch": True})
        staff_map_rows.append({"master_staff_key": master_key, "branch": BRANCHES[tgt_code]["city"],
                               "doctor_id": new_doct_id, "is_primary_branch": False})


def generate(n_patients, seed, outdir, start_date, end_date, batch_date):
    rng = random.Random(seed)
    batch_date_str = batch_date.strftime("%Y-%m-%d")

    branch_data = {code: BranchData(code, BRANCHES[code]) for code in BRANCHES}

    # 1. Static resources per branch
    for code, bd in branch_data.items():
        build_departments(bd, rng)
        build_doctors(bd, rng)
        # Add master medications catalog
        for med in MEDICATION_CATALOG:
            bd.add("medications", med)

    staff_map_rows = []
    apply_doctor_cross_branch_overlap(rng, branch_data, staff_map_rows)

    # 2. Master patient pool and journey simulation
    masters = build_master_patients(rng, n_patients, end_date)
    patient_map_rows = []

    for master in masters:
        master["patient_id_by_branch"] = {}
        for branch in master["branch_list"]:
            dirty = (branch != master["home_branch"]) and rng.random() < DUPLICATE_DIRTY_RATE
            pid = make_patient_row(rng, branch_data[branch], master, dirty=dirty)
            master["patient_id_by_branch"][branch] = pid
            patient_map_rows.append({
                "master_patient_key": master["master_key"],
                "branch": BRANCHES[branch]["city"],
                "patient_id": pid,
                "is_primary_branch": branch == master["home_branch"],
                "record_is_dirty_duplicate": dirty,
            })

        journey_counts = split_journeys_across_branches(rng, master)
        for branch, count in journey_counts.items():
            dates = gen_visit_dates(rng, start_date, end_date, count, master["chronic"])
            for idx, d in enumerate(dates):
                simulate_journey(rng, branch_data[branch], master, master["patient_id_by_branch"][branch],
                                 d, idx, end_date)

    # 3. Export data files into hierarchical domain directories with date in filename
    os.makedirs(outdir, exist_ok=True)
    for bd in branch_data.values():
        write_hierarchical_csvs(bd, outdir, batch_date_str)

    write_mapping_csv(
        os.path.join(outdir, f"enterprise_patient_map_{batch_date_str}.csv"),
        patient_map_rows,
        ["master_patient_key", "branch", "patient_id", "is_primary_branch", "record_is_dirty_duplicate"]
    )
    write_mapping_csv(
        os.path.join(outdir, f"enterprise_staff_map_{batch_date_str}.csv"),
        staff_map_rows,
        ["master_staff_key", "branch", "doctor_id", "is_primary_branch"]
    )

    summary = {
        "config": {
            "master_patients": n_patients,
            "seed": seed,
            "batch_date": batch_date_str,
            "date_range": [start_date.isoformat(), end_date.isoformat()],
            "branch_count_weights": BRANCH_COUNT_WEIGHTS,
            "activity_tiers": {k: v for k, v in ACTIVITY_TIERS.items()},
            "chronic_patient_rate": CHRONIC_PATIENT_RATE,
            "duplicate_dirty_rate": DUPLICATE_DIRTY_RATE,
            "doctor_cross_branch_rate": DOCTOR_CROSS_BRANCH_RATE,
        },
        "row_counts": {
            BRANCHES[code]["city"]: {table: len(bd.tables[table]) for table in DOMAIN_MAP}
            for code, bd in branch_data.items()
        },
        "cross_branch_patients": sum(1 for m in masters if len(m["branch_list"]) > 1),
        "cross_branch_doctors": len(staff_map_rows) // 2,
        "total_master_patients": len(masters),
    }

    with open(os.path.join(outdir, "generation_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic multi-branch hospital source data for EDW.")
    parser.add_argument("--patients", type=int, default=3000,
                        help="Number of distinct master patients to generate (default: 3000)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed (default: 42)")
    parser.add_argument("--outdir", type=str, default="output", help="Output directory (default: ./output)")
    parser.add_argument("--start-date", type=str, default=DEFAULT_START_DATE.isoformat(),
                        help="Earliest event date, YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=DEFAULT_END_DATE.isoformat(),
                        help="Latest event date, YYYY-MM-DD (dataset's simulated 'today')")
    parser.add_argument("--batch-date", type=str, default=datetime.now().strftime("%Y-%m-%d"),
                        help="Batch/Extract date to append to CSV filenames (default: current date)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    batch_date = datetime.strptime(args.batch_date, "%Y-%m-%d").date()

    print(f"Generating multi-branch hospital EDW data for Cairo, Giza & Alexandria "
          f"({args.patients} master patients, seed={args.seed}, batch_date={args.batch_date})...")
    
    summary = generate(args.patients, args.seed, args.outdir, start_date, end_date, batch_date)

    print(f"\nDone. Output written to: {os.path.abspath(args.outdir)}\n")
    print(f"Master patients: {summary['total_master_patients']} "
          f"({summary['cross_branch_patients']} visit more than one branch)")
    print(f"Cross-branch doctors: {summary['cross_branch_doctors']}")
    for branch, counts in summary["row_counts"].items():
        total_rows = sum(counts.values())
        print(f"  {branch:12s} -> {total_rows:,} rows across {len(counts)} datasets "
              f"(Patients={counts['patients']:,}, Encounters={counts['encounters']:,}, "
              f"Diagnoses={counts['diagnoses']:,}, LabResults={counts['lab_results']:,}, "
              f"Invoices={counts['invoices']:,})")


if __name__ == "__main__":
    main()
