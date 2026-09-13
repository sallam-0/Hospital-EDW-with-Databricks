SELECT
    table_name,
    dq_category,
    severity,

    COUNT(*) AS rules_checked,

    SUM(
        CASE
            WHEN status = 'PASSED'
            THEN 1
            ELSE 0
        END
    ) AS passed_rules,

    SUM(
        CASE
            WHEN status = 'FAILED'
            THEN 1
            ELSE 0
        END
    ) AS failed_rules

FROM hospital_catalog.quality.dq_results

GROUP BY
    table_name,
    dq_category,
    severity

ORDER BY
    table_name,
    severity;




SELECT
    table_name,
    rule_id,
    rule_name,
    source_branch,
    total_records,
    failed_records,
    failure_rate,
    dq_timestamp

FROM hospital_catalog.quality.dq_results

WHERE
    severity = 'CRITICAL'
    AND status = 'FAILED'

ORDER BY
    dq_timestamp DESC;


SELECT
    table_name,
    rule_id,
    rule_name,
    dq_category,
    severity,

    SUM(failed_records) AS total_failed_records,

    AVG(failure_rate) AS avg_failure_rate

FROM hospital_catalog.quality.dq_results

GROUP BY
    table_name,
    rule_id,
    rule_name,
    dq_category,
    severity

ORDER BY
    total_failed_records DESC;



SELECT *
FROM hospital_catalog.quarantine.patients
ORDER BY _quarantine_timestamp DESC;



SELECT
    _quarantine_rule_id,
    _quarantine_severity,
    COUNT(*) AS records

FROM hospital_catalog.quarantine.patients

GROUP BY
    _quarantine_rule_id,
    _quarantine_severity

ORDER BY
    records DESC;