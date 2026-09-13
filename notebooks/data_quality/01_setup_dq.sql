CREATE SCHEMA IF NOT EXISTS hospital_catalog.quality;
CREATE SCHEMA IF NOT EXISTS hospital_catalog.quarantine;
CREATE SCHEMA IF NOT EXISTS hospital_catalog.silver;


CREATE TABLE IF NOT EXISTS hospital_catalog.quality.dq_results
(
    dq_run_id STRING NOT NULL,
    dq_timestamp TIMESTAMP NOT NULL,

    table_name STRING NOT NULL,

    source_branch STRING,
    source_system STRING,

    rule_id STRING NOT NULL,
    rule_name STRING NOT NULL,

    dq_category STRING NOT NULL,
    severity STRING NOT NULL,

    column_name STRING,

    total_records BIGINT NOT NULL,
    failed_records BIGINT NOT NULL,
    passed_records BIGINT NOT NULL,

    failure_rate DOUBLE,

    status STRING NOT NULL,

    rule_description STRING
)
USING DELTA;


CREATE TABLE IF NOT EXISTS hospital_catalog.quality.dq_runs
(
    dq_run_id STRING NOT NULL,
    start_timestamp TIMESTAMP NOT NULL,
    end_timestamp TIMESTAMP,

    run_status STRING,

    total_tables BIGINT,
    total_rules BIGINT,

    failed_rules BIGINT,
    critical_failures BIGINT,

    total_records BIGINT,
    failed_records BIGINT,

    created_at TIMESTAMP
)
USING DELTA;