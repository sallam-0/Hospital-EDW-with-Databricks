# Multi-Branch Hospital Data Generator (Cairo · Giza · Alexandria)

A single-file, dependency-free Python script that generates realistic,
**intentionally overlapping** source data for your Hospital Management
System ERD, across three branches.

## Quick start

```bash
python3 generate_hospital_data.py --patients 3000 --seed 42 --outdir output
```

No pip installs needed — pure Python 3 standard library.

Useful flags:

| Flag | Default | Meaning |
|---|---|---|
| `--patients` | 3000 | Number of distinct **people** (master patients) to generate. Actual `Patients` row count is higher because cross-branch patients get a row per branch. |
| `--seed` | 42 | Random seed — same seed + same flags = same dataset, every time. |
| `--outdir` | `output` | Where the CSVs go. |
| `--start-date` / `--end-date` | 2019-01-01 / 2026-08-29 | Simulated event date range. |

Runtime is fast — 3,000 patients (~57k total rows) generates in well under a
second, so bumping `--patients` to 20,000–50,000 for a bigger dataset is
fine.

## What you get

```
output/
  Cairo/          <- 14 CSVs, one per ERD table
  Giza/           <- same 14 CSVs, independent ID sequence
  Alexandria/     <- same 14 CSVs, independent ID sequence
  enterprise_patient_map.csv   <- ground-truth patient identity links (MPI)
  enterprise_staff_map.csv     <- ground-truth cross-branch doctor links
  generation_summary.json      <- config used + row counts per branch
```

Each branch folder has exactly the 14 tables from your ERD: `Department`,
`Doctor`, `Patients`, `MedicalRecord`, `SurgeryRecord`, `Room`, `Ward`,
`Bed`, `Helpers`, `Nurse`, `StaffShift`, `Appointment`, `RoomRecords`,
`BedRecords` — with the **exact column names from your diagram**, including
its own quirks (`Nurse.conatct_No`, `RoomRecords.admisson_ID` vs.
`BedRecords.admission_Id`) so it drops straight into whatever DDL you
already built from the ERD.

## Why the three branches aren't just three copies of each other

The ERD has no `branch` column, so each branch is generated as its own
independent source system — its own ID sequence (`CAI-…`, `GIZ-…`,
`ALX-…`), its own CSV folder — exactly like three separate hospital
databases in real life. "Overlap" is then layered on top **deliberately**:

- **Cross-branch patients.** ~30% of patients visit more than one branch
  (70% one branch / 22% two / 8% all three). Every branch stores its own
  local `Patients` row (its own `patient_Id`) for that person, and about
  half of those secondary-branch rows are intentionally "dirty" — different
  name casing/spacing, different phone formatting (`01…` vs `+201…`),
  different date format (`2026-08-20` vs `20/08/2026`) — the classic
  multi-source Master Patient Index problem. `enterprise_patient_map.csv`
  gives you the ground truth linking, so you can build (and grade) an
  identity-resolution step, or just delete the file and pretend you don't
  know the answer.

- **Cross-branch doctors.** ~10% of doctors also hold a second `doct_Id`
  at a different branch in the same department — tracked in
  `enterprise_staff_map.csv`.

- **Real patient journeys, not random rows.** Every `Appointment` can fan
  out into an `MedicalRecord` encounter and, depending on department and
  diagnosis, a `SurgeryRecord`, a same-day `RoomRecords` stay, and/or a
  ward `BedRecords` admission — with chronologically consistent
  timestamps (surgery a few days after diagnosis, discharge after
  admission, etc). Cancelled / no-show appointments correctly produce
  **no** downstream clinical records — verified with an automated FK/
  logic check while building this.

- **Skewed, chronic activity.** ~65/25/8/2% of patients are
  low/medium/high/very-high activity (1–2 visits vs. 16–40). ~28% of adult
  patients carry a chronic condition (hypertension, diabetes, asthma,
  etc.) that recurs across follow-up visits spaced ~2–7 months apart,
  instead of every visit being an unrelated random diagnosis.

- **Branch personality.** Cairo is the largest branch with every
  specialty; Alexandria has an oversized Emergency department; Giza is a
  mid-size branch missing Neurology and Urology — so a question like "why
  is Alexandria's ED so busy?" has a real, discoverable answer in the
  data.

- **Controlled data-quality noise.** Name casing, phone formats, DOB
  formats and gender values (`M`/`Male`/`male`) are messed up at a low
  rate on duplicate patient rows — enough to justify a real cleaning
  step without making the dataset unusable.

## Things intentionally left out (for a v2)

This generator focuses on the ERD's 14 tables and the overlap/journey
story above. It does **not** currently include: separate `event_time` vs
`ingestion_time` columns for late-arriving-data simulation, row-level
UPDATE/DELETE change logs for CDC, or heterogeneous source-system schemas
(different ID field names per branch). All of that is straightforward to
layer on top of `run_journey()` / the per-branch generation loop in
`generate_hospital_data.py` if your next step is an incremental/CDC
pipeline — the code is organized by table and by "journey step" precisely
so you can extend it there.

## Reproducibility

Everything is driven by a single `random.Random(seed)` instance, so
`--seed 42` always produces byte-identical CSVs. Change the seed for a
different dataset of the same shape, or change `--patients` for a bigger
one.
