import uuid

from src.quality.checks import (
    check_not_null,
    check_not_blank,
    check_allowed_values,
    check_range,
    check_positive,
    check_duplicates,
    check_date_order,
    check_not_future,
    check_regex,
    check_foreign_key,
    check_date_parsable,
    check_timestamp_parsable,
    check_decimal_parsable,
    check_integer_parsable,
    check_boolean_parsable,
)

from src.quality.quarantine import (
    add_quarantine_metadata,
    write_quarantine
)

from src.quality.results import (
    write_dq_results
)


def execute_rule(df, rule):

    rule_type = rule["rule_type"]

    if rule_type == "not_null":

        return check_not_null(
            df,
            rule["column"]
        )

    elif rule_type == "not_blank":

        return check_not_blank(
            df,
            rule["column"]
        )

    elif rule_type == "allowed_values":

        return check_allowed_values(
            df,
            rule["column"],
            rule["allowed_values"]
        )

    elif rule_type == "range":

        return check_range(
            df,
            rule["column"],
            rule.get("min_value"),
            rule.get("max_value")
        )

    elif rule_type == "positive":

        return check_positive(
            df,
            rule["column"]
        )

    elif rule_type == "unique":

        return check_duplicates(
            df,
            rule["columns"]
        )

    elif rule_type == "date_order":

        return check_date_order(
            df,
            rule["start_column"],
            rule["end_column"],
            rule.get("date_formats")
        )

    elif rule_type == "not_future":

        return check_not_future(
            df,
            rule["column"],
            rule.get("date_formats")
        )

    elif rule_type == "regex":

        return check_regex(
            df,
            rule["column"],
            rule["pattern"]
        )

    # ── Type-conformity checks ──────────────────

    elif rule_type == "date_parsable":

        return check_date_parsable(
            df,
            rule["column"],
            rule.get("formats")
        )

    elif rule_type == "timestamp_parsable":

        return check_timestamp_parsable(
            df,
            rule["column"],
            rule.get("formats")
        )

    elif rule_type == "decimal_parsable":

        return check_decimal_parsable(
            df,
            rule["column"]
        )

    elif rule_type == "integer_parsable":

        return check_integer_parsable(
            df,
            rule["column"]
        )

    elif rule_type == "boolean_parsable":

        return check_boolean_parsable(
            df,
            rule["column"],
            rule.get("true_values"),
            rule.get("false_values")
        )

    else:

        raise ValueError(
            f"Unsupported DQ rule type: {rule_type}"
        )


def run_dq(
    spark,
    df,
    table_name,
    rules,
    source_branch=None,
    source_system=None
):

    dq_run_id = str(uuid.uuid4())

    total_records = df.count()

    results = []

    for rule in rules:

        failed_df = execute_rule(
            df,
            rule
        )

        failed_records = failed_df.count()

        passed_records = (
            total_records - failed_records
        )

        failure_rate = (
            failed_records / total_records
            if total_records > 0
            else 0.0
        )

        status = (
            "PASSED"
            if failed_records == 0
            else "FAILED"
        )

        results.append({

            "dq_run_id":
                dq_run_id,

            "dq_timestamp":
                None,

            "table_name":
                table_name,

            "source_branch":
                source_branch,

            "source_system":
                source_system,

            "rule_id":
                rule["rule_id"],

            "rule_name":
                rule["rule_name"],

            "dq_category":
                rule["category"],

            "severity":
                rule["severity"],

            "column_name":
                rule.get("column") or (", ".join(rule.get("columns", [])) if rule.get("columns") else None),

            "total_records":
                total_records,

            "failed_records":
                failed_records,

            "passed_records":
                passed_records,

            "failure_rate":
                failure_rate,

            "status":
                status,

            "rule_description":
                rule["description"]
        })

        if failed_records > 0:

            quarantine_df = (
                add_quarantine_metadata(
                    failed_df,
                    dq_run_id,
                    rule["rule_id"],
                    rule["description"],
                    rule["severity"]
                )
            )

            write_quarantine(
                quarantine_df,
                table_name
            )

    # Add timestamp after results are generated
    current_ts = (
        spark.sql(
            "SELECT current_timestamp() AS ts"
        )
        .collect()[0]["ts"]
    )
    for result in results:
        result["dq_timestamp"] = current_ts

    write_dq_results(
        spark,
        results
    )

    return {

        "dq_run_id":
            dq_run_id,

        "table_name":
            table_name,

        "total_records":
            total_records,

        "rules_executed":
            len(rules),

        "failed_rules":
            sum(
                1
                for r in results
                if r["status"] == "FAILED"
            )
    }