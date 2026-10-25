# Hospital Operations Dashboard

A Power BI dashboard that monitors hospital performance across admissions, clinical activity, appointments, billing, pharmacy, laboratory and surgery. It is built on a star-schema semantic model fed by the Databricks `hospital_catalog.gold` layer, with data covering **2019 – August 2026**.

## Data Model

| Layer | Tables |
|---|---|
| **Dimensions (7)** | `DimDate`, `DimDepartment`, `DimDoctor`, `DimPatient`, `DimPayer`, `DimDiagnosisCode`, `DimMedication` |
| **Facts (8)** | `FactAdmission`, `FactMedicalRecords`, `FactAppointments`, `FactBilling`, `FactPharmacy`, `FactPrescriptions`, `FactLabResults`, `FactSurgeries` |

**72 measures** in two display folders:

- **Key Measures (24)**: the base business metrics listed per page below.
- **KPI Cards (48)**: a `KPI … YoY` text measure (▲/▼ % vs. prior year) and a paired `… YoY Color` measure (green/red conditional formatting) for each key metric. Colors are inverted where an increase is unfavorable (length of stay, emergency admissions, no-show, cancellation and reschedule rates, outstanding balance, surgery duration).

> YoY cards compare the latest year with data against the year before. 2026 is a partial year (through August), so filter to a full year for like-for-like comparison.

---

## Pages

### 1. Billing & Revenue
**Purpose:** financial performance, payer mix and cash collection.

| Measure | Definition |
|---|---|
| Total Revenue | `SUM(net_amount_due)` |
| Gross Billed | `SUM(gross_amount)` |
| Insurance Covered | `SUM(insurance_covered_amount)` |
| Outstanding Balance | `SUM(outstanding_balance)` |
| Collection Rate | `SUM(total_amount_paid) ÷ SUM(gross_amount)` |

**Insights**
- **$52.6M billed → $39.7M net revenue.** Insurers cover **$10.2M (19.4%)** and discounts account for **$2.65M (5.0%)**.
- **Self-pay patients generate 93.5% of net revenue ($37.2M).** The five insurers pay far less: AXA ($2.32M), MetLife ($2.18M), Allianz ($1.99M), Bupa ($1.89M) and Misr Insurance ($1.82M).
- **Revenue fell from $9.29M (2019) to $3.81M (2024), a 59% drop**, then rebounded **+10.3% to $4.20M in 2025**. Insurance-covered amounts fell faster (−61% from 2019 to 2025).
- **Collection Rate is 75.6%**, ranging from 72.5% (2022) to a high of **78.1% (2025)**. Because it is patient payments divided by gross billing, insurer-covered amounts and discounts lower it; it does not measure unpaid debt.
- **Payment channels (share of net revenue):** credit card 37.7%, cash 31.6%, installment 30.6%.
- **8,953 invoices:** 94.9% paid, **5.1% pending (453 invoices, $2.14M net)**.

### 2. Appointments
**Purpose:** scheduling efficiency and patient attendance.

| Measure | Definition |
|---|---|
| Total Appointments | `COUNTROWS(FactAppointments)` |
| Completed / Rescheduled Appointments | Row counts filtered by `appointment_status` |
| No-Show Rate | No-shows ÷ total appointments |
| Cancellation Rate | Cancelled ÷ total appointments |

**Insights**
- **10,079 appointments:** **79.7% completed**, 7.5% cancelled, 7.0% rescheduled and **5.8% no-shows**, so about **1 in 5 scheduled visits does not go ahead as planned**.
- Volume dropped from **2,108 (2019)** to about **1,200 per year since 2022**.
- **No-show rate is highest in Pediatrics (7.5%), Urology (7.2%) and Radiology (7.1%)** and lowest in ENT (4.0%) and Obstetrics & Gynecology (4.1%).
- **Dermatology (9.5%) and Obstetrics & Gynecology (9.1%) have the highest cancellation rates.**
- Outpatient visits (5.9% no-show) are missed more often than emergency visits (4.8%).
- Pediatrics, Obstetrics & Gynecology, Dermatology, ENT and Cardiology are the busiest clinics (764–824 appointments each).

### 3. Admissions
**Purpose:** inpatient volume, acuity and length of stay.

| Measure | Definition |
|---|---|
| Total Admissions | `COUNTROWS(FactAdmission)` |
| Emergency Admissions | Admissions where `admission_type = "Emergency"` |
| Avg Length of Stay | `AVERAGE(length_of_stay_days)` |
| Unique Patients | `DISTINCTCOUNT(enterprise_patient_id)` |

**Insights**
- **2,676 admissions** from **1,490 unique patients**, with an average stay of **5.2 days**.
- Admissions fell from **600 (2019)** to about **290 (2024–2025)**, a ~50% decline that stabilized after 2021.
- Care is overwhelmingly planned: **96% elective** vs. **3.9% emergency** (104 cases). Emergency stays are much shorter (**2.3 vs. 5.3 days**).
- Emergency admissions peaked at **25 in 2025**, up from 14 in 2024.
- **General Surgery (488), ENT (437) and Ophthalmology (419)** are the busiest admitting departments, followed by Cardiology (360) and Orthopedics (281).
- **94%** of patients are discharged routine-home; **6%** are transferred.

### 4. Clinical Encounters
**Purpose:** visit activity and diagnosis profile.

| Measure | Definition |
|---|---|
| Total Encounters | `COUNTROWS(FactMedicalRecords)` |
| Avg Diagnoses per Encounter | `AVERAGE(diagnosis_count)` |

**Insights**
- **11,250 encounters:** **83% outpatient**, 10% follow-up, 6% emergency.
- Volume dropped from **2,532 (2019)** to about **1,200 per year since 2022**.
- Routine medical examinations, diagnostic imaging, eczema, chronic tonsillitis and age-related cataract are among the most frequent primary diagnoses; **respiratory (ICD-10 chapter J)** and **health-status factors (Z)** lead by chapter.
- Average of **0.8 recorded diagnoses per encounter**, and about **20% of encounters have no primary diagnosis coded**, a documentation gap.

### 5. Pharmacy & Prescriptions
**Purpose:** prescribing behavior, dispensing and pharmacy revenue.

| Measure | Definition |
|---|---|
| Total Prescriptions | `COUNTROWS(FactPrescriptions)` |
| Avg Rx Duration | `AVERAGE(duration_days)` |
| Total Dispenses | `COUNTROWS(FactPharmacy)` |
| Total Items Dispensed | `SUM(quantity)` |
| Pharmacy Revenue | `SUM(total_amount)` |

**Insights**
- **$1.54M pharmacy revenue** from **10,731 dispenses** (26,757 units) against **11,930 prescriptions**, so about **90% of prescriptions are dispensed**.
- **Antidiabetic drugs earn the most ($321K, 21%)**, and **Insulin Glargine alone contributes $247K (16%)** from only 234 dispenses.
- **Antibiotics are the most dispensed category** (1,437 dispenses) but earn about half the revenue of antidiabetics.
- **Paracetamol** is the most prescribed drug (2,001, average 15 days); **Metformin** has the longest average course (**37 days**), reflecting chronic therapy. The overall average prescription length is **23.6 days**.
- Revenue fell from **$460K (2019)** to about **$137K (2024–2025)**, tracking the decline in patient volume.

### 6. Laboratory
**Purpose:** diagnostic test demand.

| Measure | Definition |
|---|---|
| Total Lab Tests | `COUNTROWS(FactLabResults)` |

**Insights**
- **35,312 lab results**, about 3 per encounter.
- **Complete Blood Count is 40% of all tests** (14,172), followed by Lipid Profile (5,684), Basic Metabolic Panel (3,348), HbA1c & Glucose (3,298) and ECG (2,238).
- Metabolic and cardiac screening is a major share of demand, consistent with the antidiabetic and lipid-lowering prescribing pattern.

### 7. Surgeries
**Purpose:** operating-theatre throughput and procedure efficiency.

| Measure | Definition |
|---|---|
| Total Surgeries | `COUNTROWS(FactSurgeries)` |
| Avg Surgery Duration | `AVERAGE(duration_minutes)` (minutes) |

**Insights**
- **1,835 surgeries**, averaging **148 minutes**; annual averages stay within a narrow **143–151 minute** band, indicating stable theatre efficiency.
- **Cataract surgery (Phacoemulsification, 328)** and **Total Tonsillectomy (321)** are the most common procedures, consistent with Ophthalmology and ENT being top admitting departments.
- Volume fell from **428 (2019)** to about **190 per year since 2022**.

---

## Key Takeaways

1. **Activity is well below 2019 in every domain** (admissions, encounters, appointments, prescriptions, labs, surgeries, revenue). The step-down occurred in 2020–2021 and volumes have been flat since 2022; revenue showed its first rebound in 2025.
2. **Revenue depends on self-pay patients** (93.5% of net revenue), so insurer contracts contribute a small and shrinking share.
3. **Care is predominantly elective and outpatient**; emergency cases are few and short, and rose slightly in 2025.
4. **Surgical specialties (General Surgery, ENT, Ophthalmology) drive inpatient demand.**
5. **Chronic-disease medication (diabetes, respiratory, lipid) dominates pharmacy revenue**, while acute drugs dominate volume.
6. **About 1 in 5 scheduled visits is cancelled, rescheduled or missed**, and about 1 in 5 encounters lacks a primary diagnosis.

## Data Quality Notes

- **Outstanding Balance returns $0** even though 453 invoices ($2.14M net) are marked *Pending*. The source records those invoices as fully paid with a zero balance, so receivables risk is understated until the balance logic is corrected upstream.
- **Collection Rate** divides patient payments by gross billing, so insurer coverage and discounts are counted as uncollected. It reads as 75.6% even though no balance is outstanding.
- Fact tables are filtered by Databricks column masks and row filters. Power BI shows blanks or empty tables for any user whose group is not granted access, so the report account must belong to the permitted groups.
