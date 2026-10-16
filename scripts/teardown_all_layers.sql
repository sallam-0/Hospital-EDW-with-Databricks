-- ============================================================
-- Hospital EDW — FULL TEARDOWN SCRIPT
-- Drops ALL tables in ALL layers so the pipeline can be
-- re-run from scratch.
--
-- ⚠️  DESTRUCTIVE — run only on a clean-rebuild.
--
-- Order matters: drop GOLD first (depends on SILVER),
-- then SILVER, then BRONZE, then LANDING metadata.
-- DQ / quarantine tables are reset last.
--
-- Execute each block independently in Databricks SQL editor,
-- or paste the whole file and run all statements.
-- ============================================================


-- ──────────────────────────────────────────────────────────────
-- 1.  GOLD LAYER  (facts first, then dims)
--     Do NOT drop dim_patient_enterprise or dim_doctor_enterprise
--     unless you also plan to re-run the identity-resolution
--     notebooks (01 & 02 under notebooks/identity/).
-- ──────────────────────────────────────────────────────────────

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

-- Uncomment the two lines below ONLY if you are also re-running
-- the identity-resolution notebooks from scratch:
-- DROP TABLE IF EXISTS hospital_catalog.gold.dim_patient_enterprise;
-- DROP TABLE IF EXISTS hospital_catalog.gold.dim_doctor_enterprise;


-- ──────────────────────────────────────────────────────────────
-- 2.  SILVER LAYER — bridge maps + all domain tables
-- ──────────────────────────────────────────────────────────────

-- Identity bridge maps (produced by identity-resolution notebooks)
DROP TABLE IF EXISTS hospital_catalog.silver.patients_map_probabilistic_enterprise;
DROP TABLE IF EXISTS hospital_catalog.silver.doctors_map_probabilistic_enterprise;

-- EMR
DROP TABLE IF EXISTS hospital_catalog.silver.patients;
DROP TABLE IF EXISTS hospital_catalog.silver.doctors;
DROP TABLE IF EXISTS hospital_catalog.silver.departments;
DROP TABLE IF EXISTS hospital_catalog.silver.encounters;
DROP TABLE IF EXISTS hospital_catalog.silver.diagnoses;
DROP TABLE IF EXISTS hospital_catalog.silver.procedures;
DROP TABLE IF EXISTS hospital_catalog.silver.admissions;

-- Laboratory
DROP TABLE IF EXISTS hospital_catalog.silver.lab_orders;
DROP TABLE IF EXISTS hospital_catalog.silver.lab_results;

-- Pharmacy
DROP TABLE IF EXISTS hospital_catalog.silver.medications;
DROP TABLE IF EXISTS hospital_catalog.silver.prescriptions;
DROP TABLE IF EXISTS hospital_catalog.silver.pharmacy_transactions;

-- Billing
DROP TABLE IF EXISTS hospital_catalog.silver.invoices;
DROP TABLE IF EXISTS hospital_catalog.silver.payments;
DROP TABLE IF EXISTS hospital_catalog.silver.insurance_claims;


-- ──────────────────────────────────────────────────────────────
-- 3.  BRONZE LAYER — all ingested raw tables
-- ──────────────────────────────────────────────────────────────

-- EMR
DROP TABLE IF EXISTS hospital_catalog.bronze.patients;
DROP TABLE IF EXISTS hospital_catalog.bronze.doctors;
DROP TABLE IF EXISTS hospital_catalog.bronze.departments;
DROP TABLE IF EXISTS hospital_catalog.bronze.encounters;
DROP TABLE IF EXISTS hospital_catalog.bronze.diagnoses;
DROP TABLE IF EXISTS hospital_catalog.bronze.procedures;
DROP TABLE IF EXISTS hospital_catalog.bronze.admissions;

-- Laboratory
DROP TABLE IF EXISTS hospital_catalog.bronze.lab_orders;
DROP TABLE IF EXISTS hospital_catalog.bronze.lab_results;

-- Pharmacy
DROP TABLE IF EXISTS hospital_catalog.bronze.medications;
DROP TABLE IF EXISTS hospital_catalog.bronze.prescriptions;
DROP TABLE IF EXISTS hospital_catalog.bronze.pharmacy_transactions;

-- Billing
DROP TABLE IF EXISTS hospital_catalog.bronze.invoices;
DROP TABLE IF EXISTS hospital_catalog.bronze.payments;
DROP TABLE IF EXISTS hospital_catalog.bronze.insurance_claims;


-- ──────────────────────────────────────────────────────────────
-- 4.  QUALITY & QUARANTINE SCHEMAS
--     Reset DQ audit trail for a clean run.
-- ──────────────────────────────────────────────────────────────

DROP TABLE IF EXISTS hospital_catalog.quality.dq_results;
DROP TABLE IF EXISTS hospital_catalog.quality.dq_runs;

-- Quarantine tables — one per bronze source
DROP TABLE IF EXISTS hospital_catalog.quarantine.patients;
DROP TABLE IF EXISTS hospital_catalog.quarantine.doctors;
DROP TABLE IF EXISTS hospital_catalog.quarantine.departments;
DROP TABLE IF EXISTS hospital_catalog.quarantine.encounters;
DROP TABLE IF EXISTS hospital_catalog.quarantine.diagnoses;
DROP TABLE IF EXISTS hospital_catalog.quarantine.procedures;
DROP TABLE IF EXISTS hospital_catalog.quarantine.admissions;
DROP TABLE IF EXISTS hospital_catalog.quarantine.lab_orders;
DROP TABLE IF EXISTS hospital_catalog.quarantine.lab_results;
DROP TABLE IF EXISTS hospital_catalog.quarantine.medications;
DROP TABLE IF EXISTS hospital_catalog.quarantine.prescriptions;
DROP TABLE IF EXISTS hospital_catalog.quarantine.pharmacy_transactions;
DROP TABLE IF EXISTS hospital_catalog.quarantine.invoices;
DROP TABLE IF EXISTS hospital_catalog.quarantine.payments;
DROP TABLE IF EXISTS hospital_catalog.quarantine.insurance_claims;


-- ──────────────────────────────────────────────────────────────
-- 5.  STREAMING CHECKPOINTS
--     Must be deleted from the Volume manually in the
--     Databricks UI or via dbutils — SQL cannot touch Volumes.
--
--     In a Databricks notebook, run:
--
--     dbutils.fs.rm(
--         "/Volumes/hospital_catalog/landing/raw_files/_checkpoints",
--         recurse=True
--     )
--     dbutils.fs.rm(
--         "/Volumes/hospital_catalog/landing/raw_files/_schemas",
--         recurse=True
--     )
-- ──────────────────────────────────────────────────────────────


-- ──────────────────────────────────────────────────────────────
-- 6.  (OPTIONAL) DROP SCHEMAS  — only if doing a total reset
--     Leave commented unless you intend a full catalog reset.
-- ──────────────────────────────────────────────────────────────

-- DROP SCHEMA IF EXISTS hospital_catalog.gold       CASCADE;
-- DROP SCHEMA IF EXISTS hospital_catalog.silver     CASCADE;
-- DROP SCHEMA IF EXISTS hospital_catalog.bronze     CASCADE;
-- DROP SCHEMA IF EXISTS hospital_catalog.quality    CASCADE;
-- DROP SCHEMA IF EXISTS hospital_catalog.quarantine CASCADE;
