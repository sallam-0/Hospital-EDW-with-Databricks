# Hospital Data Quality Framework

## Purpose

The Data Quality layer validates Bronze data before it enters
the Silver layer.

## DQ Categories

### Completeness

Checks whether required fields contain NULL or blank values.

### Uniqueness

Checks duplicate business keys.

### Validity

Checks whether values conform to allowed values,
formats, ranges, and valid dates.

### Referential Integrity

Checks whether foreign keys reference valid parent records.

### Business Rules

Checks domain-specific relationships and constraints.

## Severity

### CRITICAL

The record cannot safely continue to Silver.

### HIGH

The record normally requires quarantine.

### MEDIUM

The record may be standardized or corrected in Silver.

### LOW

The issue does not normally block processing.

## DQ Results

All rule-level metrics are stored in:

hospital_catalog.quality.dq_results

## Quarantine

Actual invalid records are stored in:

hospital_catalog.quarantine.<table>

Each quarantine record contains:

- original Bronze columns
- quarantine timestamp
- quarantine reason
- rule ID
- severity
- DQ run ID

## Data Flow

Bronze
→ Data Quality
→ Quarantine / Silver
→ Identity Resolution
→ Gold