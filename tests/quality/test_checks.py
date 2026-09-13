from pyspark.sql import SparkSession

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
    check_date_parsable,
    check_timestamp_parsable,
    check_decimal_parsable,
    check_integer_parsable,
    check_boolean_parsable,
)


def get_spark():

    return (
        SparkSession.builder
        .master("local[2]")
        .appName("DQTests")
        .getOrCreate()
    )


# ─────────────────────────────────────────────
# Original checks
# ─────────────────────────────────────────────


def test_not_null():

    spark = get_spark()

    data = [
        ("P001",),
        (None,),
        ("P003",)
    ]

    df = spark.createDataFrame(
        data,
        ["patient_id"]
    )

    result = check_not_null(
        df,
        "patient_id"
    )

    assert result.count() == 1


def test_not_blank():

    spark = get_spark()

    data = [
        ("P001",),
        ("",),
        ("  ",),
        (None,)
    ]

    df = spark.createDataFrame(
        data,
        ["name"]
    )

    result = check_not_blank(df, "name")
    assert result.count() == 3


def test_allowed_values():

    spark = get_spark()

    data = [
        ("M",),
        ("F",),
        ("X",)
    ]

    df = spark.createDataFrame(
        data,
        ["gender"]
    )

    result = check_allowed_values(
        df,
        "gender",
        ["M", "F", "O"]
    )

    assert result.count() == 1


def test_duplicates():

    spark = get_spark()

    data = [
        ("P001",),
        ("P001",),
        ("P002",)
    ]

    df = spark.createDataFrame(
        data,
        ["patient_id"]
    )

    result = check_duplicates(
        df,
        ["patient_id"]
    )

    assert result.count() == 2


def test_regex():

    spark = get_spark()

    data = [
        ("ABC-123",),
        ("abc-456",),
        ("INVALID",)
    ]

    df = spark.createDataFrame(
        data,
        ["code"]
    )

    result = check_regex(
        df,
        "code",
        r"^[A-Z]{3}-\d{3}$"
    )

    # "abc-456" (lowercase) and "INVALID" fail
    assert result.count() == 2


# ─────────────────────────────────────────────
# Type-safe numeric checks on string columns
# ─────────────────────────────────────────────


def test_range_with_string_column():

    spark = get_spark()

    data = [
        ("50",),
        ("101",),
        ("-5",),
        ("abc",),
        (None,)
    ]

    df = spark.createDataFrame(
        data,
        ["score"]
    )

    result = check_range(
        df,
        "score",
        min_value=0,
        max_value=100
    )

    # "101" > 100, "-5" < 0, "abc" un-castable
    assert result.count() == 3


def test_positive_with_string_column():

    spark = get_spark()

    data = [
        ("10.5",),
        ("-3",),
        ("0",),
        ("abc",),
        (None,)
    ]

    df = spark.createDataFrame(
        data,
        ["amount"]
    )

    result = check_positive(df, "amount")

    # "-3" <= 0, "0" <= 0, "abc" not numeric, None
    assert result.count() == 4


# ─────────────────────────────────────────────
# Type-safe date checks on string columns
# ─────────────────────────────────────────────


def test_date_order_with_string_dates():

    spark = get_spark()

    data = [
        ("2026-01-10", "2026-01-15"),
        ("2026-01-20", "2026-01-10"),
        ("15/01/2026", "20/01/2026"),
        ("20/01/2026", "10/01/2026"),
        (None, "2026-01-10"),
    ]

    df = spark.createDataFrame(
        data,
        ["start_date", "end_date"]
    )

    result = check_date_order(
        df,
        "start_date",
        "end_date"
    )

    # row 2: 2026-01-20 > 2026-01-10
    # row 4: 20/01/2026 > 10/01/2026
    assert result.count() == 2


def test_not_future_with_string_dates():

    spark = get_spark()

    data = [
        ("2020-01-01",),
        ("01/01/2099",),
        ("not-a-date",),
        (None,),
    ]

    df = spark.createDataFrame(
        data,
        ["event_date"]
    )

    result = check_not_future(df, "event_date")

    # Only "01/01/2099" is in the future
    # "not-a-date" parses to null so is NOT flagged
    assert result.count() == 1


# ─────────────────────────────────────────────
# Date parsable checks
# ─────────────────────────────────────────────


def test_date_parsable_valid():

    spark = get_spark()

    data = [
        ("2026-08-20",),
        ("20/08/2026",),
        ("08-20-2026",),
        ("20-Aug-2026",),
    ]

    df = spark.createDataFrame(
        data,
        ["dob"]
    )

    result = check_date_parsable(df, "dob")
    assert result.count() == 0


def test_date_parsable_invalid():

    spark = get_spark()

    data = [
        ("not-a-date",),
        ("2026-13-40",),
        ("",),
    ]

    df = spark.createDataFrame(
        data,
        ["dob"]
    )

    result = check_date_parsable(df, "dob")

    # "not-a-date" fails, "2026-13-40" fails
    # "" is treated as blank and skipped
    assert result.count() == 2


def test_date_parsable_nulls_ignored():

    spark = get_spark()

    data = [
        (None,),
        ("2026-01-01",),
    ]

    df = spark.createDataFrame(
        data,
        ["dob"]
    )

    result = check_date_parsable(df, "dob")
    assert result.count() == 0


# ─────────────────────────────────────────────
# Timestamp parsable checks
# ─────────────────────────────────────────────


def test_timestamp_parsable_valid():

    spark = get_spark()

    data = [
        ("2026-08-20 14:30:00",),
        ("20/08/2026 14:30:00",),
        ("2026-08-20T14:30:00",),
    ]

    df = spark.createDataFrame(
        data,
        ["ts"]
    )

    result = check_timestamp_parsable(df, "ts")
    assert result.count() == 0


def test_timestamp_parsable_invalid():

    spark = get_spark()

    data = [
        ("not-a-timestamp",),
        ("2026-08-20",),
    ]

    df = spark.createDataFrame(
        data,
        ["ts"]
    )

    result = check_timestamp_parsable(df, "ts")

    # "not-a-timestamp" fails
    # "2026-08-20" also fails — no time component
    assert result.count() == 2


# ─────────────────────────────────────────────
# Decimal parsable checks
# ─────────────────────────────────────────────


def test_decimal_parsable_valid():

    spark = get_spark()

    data = [
        ("10.5",),
        ("-3.14",),
        ("0",),
        ("100",),
    ]

    df = spark.createDataFrame(
        data,
        ["price"]
    )

    result = check_decimal_parsable(df, "price")
    assert result.count() == 0


def test_decimal_parsable_invalid():

    spark = get_spark()

    data = [
        ("abc",),
        ("$100",),
        ("12.3.4",),
        (None,),
        ("",),
    ]

    df = spark.createDataFrame(
        data,
        ["price"]
    )

    result = check_decimal_parsable(df, "price")

    # "abc", "$100", "12.3.4" fail
    # None and "" are skipped
    assert result.count() == 3


# ─────────────────────────────────────────────
# Integer parsable checks
# ─────────────────────────────────────────────


def test_integer_parsable_valid():

    spark = get_spark()

    data = [
        ("10",),
        ("-3",),
        ("0",),
    ]

    df = spark.createDataFrame(
        data,
        ["qty"]
    )

    result = check_integer_parsable(df, "qty")
    assert result.count() == 0


def test_integer_parsable_invalid():

    spark = get_spark()

    data = [
        ("abc",),
        ("10.5",),
        (None,),
        ("",),
    ]

    df = spark.createDataFrame(
        data,
        ["qty"]
    )

    result = check_integer_parsable(df, "qty")

    # "abc" fails (can't cast), "10.5" fails
    # (has decimal point)
    # None and "" are skipped
    assert result.count() == 2


def test_integer_parsable_rejects_floats():

    spark = get_spark()

    data = [
        ("3.5",),
        ("7.0",),
    ]

    df = spark.createDataFrame(
        data,
        ["qty"]
    )

    result = check_integer_parsable(df, "qty")

    # Both have decimal points → rejected
    assert result.count() == 2


# ─────────────────────────────────────────────
# Boolean parsable checks
# ─────────────────────────────────────────────


def test_boolean_parsable_valid():

    spark = get_spark()

    data = [
        ("true",),
        ("False",),
        ("YES",),
        ("0",),
        ("N",),
    ]

    df = spark.createDataFrame(
        data,
        ["flag"]
    )

    result = check_boolean_parsable(df, "flag")
    assert result.count() == 0


def test_boolean_parsable_invalid():

    spark = get_spark()

    data = [
        ("maybe",),
        ("2",),
        ("enabled",),
        (None,),
        ("",),
    ]

    df = spark.createDataFrame(
        data,
        ["flag"]
    )

    result = check_boolean_parsable(df, "flag")

    # "maybe", "2", "enabled" fail
    # None and "" are skipped
    assert result.count() == 3


def test_check_duplicates_composite_key():

    spark = get_spark()

    data = [
        ("cairo", "P001"),
        ("giza", "P001"),      # Same ID, different branch -> NOT a duplicate
        ("cairo", "P002"),
        ("cairo", "P001"),     # Duplicate within cairo branch!
    ]

    df = spark.createDataFrame(
        data,
        ["_source_branch", "patient_id"]
    )

    result = check_duplicates(
        df,
        ["_source_branch", "patient_id"]
    )

    # 2 rows for ("cairo", "P001") are flagged
    assert result.count() == 2