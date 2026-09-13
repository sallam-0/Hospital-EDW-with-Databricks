from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ─────────────────────────────────────────────────────
# Default format lists for multi-format date/timestamp
# parsing.  Spark-style format tokens (Java
# SimpleDateFormat).
# ─────────────────────────────────────────────────────

DEFAULT_DATE_FORMATS = [
    "yyyy-MM-dd",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
    "dd-MM-yyyy",
    "MM-dd-yyyy",
    "dd-MMM-yyyy",
    "d-MMM-yyyy",
    "yyyy/MM/dd",
    "dd.MM.yyyy",
    "yyyyMMdd",
]

DEFAULT_TIMESTAMP_FORMATS = [
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "dd/MM/yyyy HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss",
    "MM-dd-yyyy HH:mm:ss",
    "dd-MMM-yyyy HH:mm:ss",
    "d-MMM-yyyy HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss",
    "dd.MM.yyyy HH:mm:ss",
]


# ─────────────────────────────────────────────────────
# Helpers: ANSI-safe casting and date/timestamp parsing.
# Standard `to_date`, `to_timestamp`, and `cast` throw
# DateTimeException (CANNOT_PARSE_TIMESTAMP) or
# SparkNumberFormatException when ANSI SQL mode is on
# (Databricks default). The `try_*` variants return NULL
# on mismatch instead of failing the job.
# ─────────────────────────────────────────────────────

def _try_cast(column_name: str, target_type: str):
    col = F.col(column_name)
    if hasattr(col, "try_cast"):
        return col.try_cast(target_type)
    return F.expr(f"try_cast(`{column_name}` AS {target_type})")


def _try_to_date(column_name: str, fmt: str = None):
    trimmed = F.trim(F.col(column_name).cast("string"))
    if fmt is None:
        if hasattr(trimmed, "try_cast"):
            return trimmed.try_cast("date")
        return F.expr(
            f"try_cast(trim(cast(`{column_name}` AS string)) AS date)"
        )

    if hasattr(F, "try_to_date"):
        return F.try_to_date(trimmed, fmt)
    return F.expr(
        f"try_to_date(trim(cast(`{column_name}` AS string)), '{fmt}')"
    )


def _try_to_timestamp(column_name: str, fmt: str = None):
    trimmed = F.trim(F.col(column_name).cast("string"))
    if fmt is None:
        if hasattr(trimmed, "try_cast"):
            return trimmed.try_cast("timestamp")
        return F.expr(
            f"try_cast(trim(cast(`{column_name}` AS string)) AS timestamp)"
        )

    if hasattr(F, "try_to_timestamp"):
        return F.try_to_timestamp(trimmed, F.lit(fmt))
    return F.expr(
        f"try_to_timestamp(trim(cast(`{column_name}` AS string)), '{fmt}')"
    )


def _coalesce_to_date(column_name: str, formats=None):
    fmts = formats or DEFAULT_DATE_FORMATS

    candidates = [_try_to_date(column_name)] + [
        _try_to_date(column_name, fmt)
        for fmt in fmts
    ]

    return F.coalesce(*candidates)


def _coalesce_to_timestamp(column_name: str, formats=None):
    fmts = formats or DEFAULT_TIMESTAMP_FORMATS

    candidates = [_try_to_timestamp(column_name)] + [
        _try_to_timestamp(column_name, fmt)
        for fmt in fmts
    ]

    return F.coalesce(*candidates)


# ─────────────────────────────────────────────────────
# Completeness checks
# ─────────────────────────────────────────────────────


def check_not_null(
    df: DataFrame,
    column_name: str
) -> DataFrame:

    return df.filter(
        F.col(column_name).isNull()
    )



def check_not_blank(
    df: DataFrame,
    column_name: str
) -> DataFrame:

    return df.filter(
        F.col(column_name).isNull()
        |
        (F.trim(F.col(column_name)) == "")
    )


# ─────────────────────────────────────────────────────
# Validity checks  (allowed values, regex)
# ─────────────────────────────────────────────────────


def check_allowed_values(
    df: DataFrame,
    column_name: str,
    allowed_values: list
) -> DataFrame:

    normalized_values = [
        v.strip().lower() if isinstance(v, str) else str(v).strip().lower()
        for v in allowed_values
    ]

    cleaned_col = F.lower(
        F.trim(F.col(column_name).cast("string"))
    )

    return df.filter(
        F.col(column_name).isNull()
        |
        (~cleaned_col.isin(normalized_values))
    )


def check_regex(
    df: DataFrame,
    column_name: str,
    pattern: str
) -> DataFrame:

    return df.filter(
        F.col(column_name).isNull()
        |
        ~F.col(column_name).rlike(pattern)
    )


# ─────────────────────────────────────────────────────
# Numeric checks  (type-safe — cast from string first)
# ─────────────────────────────────────────────────────


def check_range(
    df: DataFrame,
    column_name: str,
    min_value=None,
    max_value=None
) -> DataFrame:

    numeric_col = _try_cast(column_name, "double")

    condition = F.lit(False)

    # Non-null value that cannot be cast → failure
    condition = condition | (
        F.col(column_name).isNotNull()
        & numeric_col.isNull()
    )

    if min_value is not None:
        condition = condition | (
            numeric_col < min_value
        )

    if max_value is not None:
        condition = condition | (
            numeric_col > max_value
        )

    return df.filter(condition)


def check_positive(
    df: DataFrame,
    column_name: str,
) -> DataFrame:

    numeric_col = _try_cast(column_name, "double")

    return df.filter(
        F.col(column_name).isNull()
        |
        (
            F.col(column_name).isNotNull()
            & numeric_col.isNull()
        )
        |
        (numeric_col <= 0)
    )


# ─────────────────────────────────────────────────────
# Uniqueness checks
# ─────────────────────────────────────────────────────


def check_duplicates(
    df: DataFrame,
    columns: list
) -> DataFrame:

    duplicate_keys = (
        df
        .groupBy(*columns)
        .count()
        .filter(F.col("count") > 1)
        .drop("count")
    )

    return df.join(
        duplicate_keys,
        on=columns,
        how="inner"
    )


# ─────────────────────────────────────────────────────
# Date / timestamp checks  (type-safe — multi-format)
# ─────────────────────────────────────────────────────


def check_date_order(
    df: DataFrame,
    start_column: str,
    end_column: str,
    date_formats=None
) -> DataFrame:

    parsed_start = _coalesce_to_date(
        start_column, date_formats
    )
    parsed_end = _coalesce_to_date(
        end_column, date_formats
    )

    return df.filter(
        parsed_start.isNotNull()
        &
        parsed_end.isNotNull()
        &
        (parsed_end < parsed_start)
    )


def check_not_future(
    df: DataFrame,
    column_name: str,
    date_formats=None
) -> DataFrame:

    parsed = _coalesce_to_date(
        column_name, date_formats
    )

    return df.filter(
        parsed.isNotNull()
        &
        (parsed > F.current_date())
    )


def check_foreign_key(
    child_df: DataFrame,
    child_column: str,
    parent_df: DataFrame,
    parent_column: str
) -> DataFrame:

    parent_keys = (
        parent_df
        .select(parent_column)
        .where(F.col(parent_column).isNotNull())
        .distinct()
    )

    return (
        child_df.alias("child")
        .join(
            parent_keys.alias("parent"),
            F.col(f"child.{child_column}")
            == F.col(f"parent.{parent_column}"),
            "left_anti"
        )
        .filter(
            F.col(f"child.{child_column}").isNotNull()
        )
    )


# ─────────────────────────────────────────────────────
# Type-conformity / parsability checks
# ─────────────────────────────────────────────────────


def check_date_parsable(
    df: DataFrame,
    column_name: str,
    formats=None
) -> DataFrame:
    """Return rows where column is non-null but
    cannot be parsed as a date by any of the given
    formats."""

    parsed = _coalesce_to_date(
        column_name, formats
    )

    return df.filter(
        F.col(column_name).isNotNull()
        &
        (F.trim(F.col(column_name)) != "")
        &
        parsed.isNull()
    )


def check_timestamp_parsable(
    df: DataFrame,
    column_name: str,
    formats=None
) -> DataFrame:
    """Return rows where column is non-null but
    cannot be parsed as a timestamp by any of the
    given formats."""

    parsed = _coalesce_to_timestamp(
        column_name, formats
    )

    return df.filter(
        F.col(column_name).isNotNull()
        &
        (F.trim(F.col(column_name)) != "")
        &
        parsed.isNull()
    )


def check_decimal_parsable(
    df: DataFrame,
    column_name: str
) -> DataFrame:
    """Return rows where column is non-null but
    cannot be cast to a double (decimal) value."""

    numeric_col = _try_cast(column_name, "double")

    return df.filter(
        F.col(column_name).isNotNull()
        &
        (F.trim(F.col(column_name).cast("string")) != "")
        &
        numeric_col.isNull()
    )


def check_integer_parsable(
    df: DataFrame,
    column_name: str
) -> DataFrame:
    """Return rows where column is non-null but
    cannot be parsed as a whole integer.  Values like
    '3.5' are rejected (they have a fractional
    part)."""

    long_col = _try_cast(column_name, "long")

    # Also reject strings with a decimal point that
    # would silently truncate (e.g. "3.5" casts to 3)
    has_fraction = (
        F.col(column_name).cast("string").rlike(r"\.")
    )

    return df.filter(
        F.col(column_name).isNotNull()
        &
        (F.trim(F.col(column_name).cast("string")) != "")
        &
        (
            long_col.isNull()
            | has_fraction
        )
    )


DEFAULT_TRUE_VALUES = [
    "true", "1", "yes", "y", "t"
]

DEFAULT_FALSE_VALUES = [
    "false", "0", "no", "n", "f"
]


def check_boolean_parsable(
    df: DataFrame,
    column_name: str,
    true_values=None,
    false_values=None
) -> DataFrame:
    """Return rows where column is non-null but
    not a recognised boolean-like value
    (case-insensitive)."""

    tv = true_values or DEFAULT_TRUE_VALUES
    fv = false_values or DEFAULT_FALSE_VALUES

    all_valid = [v.lower() for v in tv + fv]

    lowered = F.lower(
        F.trim(F.col(column_name))
    )

    return df.filter(
        F.col(column_name).isNotNull()
        &
        (F.trim(F.col(column_name)) != "")
        &
        (~lowered.isin(all_valid))
    )
