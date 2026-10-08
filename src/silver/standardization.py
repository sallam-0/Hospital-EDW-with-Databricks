from pyspark.sql import functions as F 


def clean_string(column):

    """
    remove whitespaces from the start and end of a string column

    if the result is empty string, return None
    """

    cleaned =  F.trim(column)

    return F.when(
        cleaned == "", 
        None
    ).otherwise(cleaned)


def clean_id(column):

    """
    Clean an ID column: trim whitespace and uppercase.
    Returns None if empty.
    """

    cleaned = F.upper(F.trim(column))

    return F.when(
        cleaned == "",
        None
    ).otherwise(cleaned)


def standardize_name(column):

    """
    Standardize person names:
    - Trim whitespace
    - Capitalize first letter and lowercase the rest (Title Case)
    - Returns None if empty
    """

    cleaned = F.trim(column)

    return (
        F.when(cleaned == "", None)
        .otherwise(F.initcap(cleaned))
    )


clean_name = standardize_name


def parse_date(column, formats):

    """
    parse the date with multiple accepted formats.

    eg. "2020-10-11" , "11/10/2020"

    both become "2020-10-11" date format.
    Uses SQL try_to_date via call_function and F.lit so that mismatched
    formats return NULL (tolerating ANSI mode) and coalesce evaluates the next
    candidate format without throwing errors or resolving format strings as column names.
    """

    cleaned = F.trim(column)

    return F.coalesce(
        *[
            F.call_function("try_to_date", cleaned, F.lit(date_format))
            for date_format in formats
        ]
        + [F.call_function("try_to_date", cleaned)]
    )


def parse_timestamp(column, formats):

    """
    parse the timestamp with multiple accepted formats.

    eg. "2020-10-11 10:00:00" , "11/10/2020 10:00"

    both become "2020-10-11 10:00:00" timestamp format.
    Uses SQL try_to_timestamp via call_function and F.lit so that mismatched
    formats return NULL (tolerating ANSI mode) and coalesce evaluates the next
    candidate format without throwing errors or resolving format strings as column names.
    """

    cleaned = F.trim(column)

    return F.coalesce(
        *[
            F.call_function("try_to_timestamp", cleaned, F.lit(timestamp_format))
            for timestamp_format in formats
        ]
        + [F.call_function("try_to_timestamp", cleaned)]
    )


def parse_integer(column):
    cleaned = F.trim(column)
    return F.when(cleaned == "", None).otherwise(cleaned).cast("integer")

def parse_long(column):
    cleaned = F.trim(column)
    return F.when(cleaned == "", None).otherwise(cleaned).cast("long")

def parse_double(column):
    cleaned = F.trim(column)
    return F.when(cleaned == "", None).otherwise(cleaned).cast("double")

def parse_decimal(column, precision=18, scale=2):
    cleaned = F.trim(column)
    return F.when(cleaned == "", None).otherwise(cleaned).cast(
        f"decimal({precision}, {scale})"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Domain-Specific Standardization
# ─────────────────────────────────────────────────────────────────────────────

def standardize_gender(column):

    value = F.upper(F.trim(column))

    return (
        F.when(value.isin("M", "MALE"), "M")
        .when(value.isin("F", "FEMALE"), "F")
        .otherwise(None)
    )


def standardize_phone_number(column):

    """
    Standardize phone numbers to 11 digits (e.g. '01005844258'):
    - removes whitespace, dashes, dots, spaces, slashes, and parentheses
    - Removes country code prefixes (+2, +20, 2, 002)
    - Returns None if null or empty
    """

    cleaned = F.regexp_replace(F.trim(column), r"[\s\-\.\(\)/]", "")

    standardized = F.regexp_replace(
        cleaned,
        r"^(\+?20?|0020?)?(01\d{9})$",
        "$2"
    )

    return (
        F.when(column.isNull() | (cleaned == ""), None)
        .otherwise(standardized)
    )


def standardize_status(column):

    """
    Standardize status-like columns:
    - Trim, title-case
    - Returns None if empty
    """

    cleaned = F.trim(column)

    return (
        F.when(cleaned == "", None)
        .otherwise(F.initcap(cleaned))
    )


def standardize_payment_method(column):

    """
    Standardize payment method strings:
    - Trim, title-case
    - Returns None if empty
    """

    cleaned = F.trim(column)

    return (
        F.when(cleaned == "", None)
        .otherwise(F.initcap(cleaned))
    )


def standardize_encounter_type(column):

    """
    Standardize encounter type:
    - Trim, title-case  (e.g. "outpatient" → "Outpatient")
    - Returns None if empty
    """

    cleaned = F.trim(column)

    return (
        F.when(cleaned == "", None)
        .otherwise(F.initcap(cleaned))
    )


def standardize_admission_type(column):

    """
    Standardize admission type:
    - Trim, title-case (e.g. "elective" → "Elective")
    - Returns None if empty
    """

    cleaned = F.trim(column)

    return (
        F.when(cleaned == "", None)
        .otherwise(F.initcap(cleaned))
    )


def standardize_abnormal_flag(column):

    """
    Standardize lab abnormal flag to uppercase single char:
    - N = Normal, H = High, L = Low
    - Returns None if empty or null
    """

    value = F.upper(F.trim(column))

    return (
        F.when(value.isin("N", "NORMAL"), "N")
        .when(value.isin("H", "HIGH"), "H")
        .when(value.isin("L", "LOW"), "L")
        .otherwise(None)
    )
