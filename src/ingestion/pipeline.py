from src.ingestion.reader import read_csv_autoloader
from src.ingestion.metadata import add_ingestion_metadata
from src.ingestion.writer import write_bronze_stream


def build_landing_path(
    catalog: str,
    landing_schema: str,
    volume: str,
    branch: str, 
    source_system: str, 
    table_name: str
) -> str:
    return (
        f"/Volumes/"
        f"{catalog}/"
        f"{landing_schema}/"
        f"{volume}/"
        f"{branch}/"
        f"{source_system}/"
        f"{table_name}/"
    )


def build_checkpoint_path(
    checkpoint_base_path: str,
    branch: str,
    source_system: str,
    table_name: str
) -> str:
    return f"{checkpoint_base_path}/{branch}/{source_system}/{table_name}"


def build_schema_path(
    schema_base_path: str,
    branch: str,
    source_system: str,
    table_name: str
) -> str:
    return f"{schema_base_path}/{branch}/{source_system}/{table_name}"


def ingest_table(
    spark,
    catalog: str,
    landing_schema: str,
    bronze_schema: str,
    volume: str,
    checkpoint_base_path: str,
    schema_base_path: str,
    branch: str,
    source_system: str,
    table_name: str,
    batch_id: str
):
    source_path = build_landing_path(
        catalog=catalog,
        landing_schema=landing_schema,
        volume=volume,
        branch=branch,
        source_system=source_system,
        table_name=table_name
    )

    checkpoint_location = build_checkpoint_path(
        checkpoint_base_path=checkpoint_base_path,
        branch=branch,
        source_system=source_system,
        table_name=table_name
    )

    schema_location = build_schema_path(
        schema_base_path=schema_base_path,
        branch=branch,
        source_system=source_system,
        table_name=table_name
    )

    bronze_table = (
        f"{catalog}."
        f"{bronze_schema}."
        f"{table_name}"
    )

    df = read_csv_autoloader(
        spark=spark,
        path=source_path,
        schema_location=schema_location
    )

    df = add_ingestion_metadata(
        df=df,
        branch=branch, 
        source_system=source_system,
        batch_id=batch_id
    )

    query = write_bronze_stream(
        df=df,
        table_name=bronze_table,
        checkpoint_location=checkpoint_location
    )

    # Calculate total records ingested across microbatches in this run
    records_processed = 0
    if query.recentProgress:
        records_processed = sum(
            progress.get("numInputRows", 0) for progress in query.recentProgress
        )
    elif query.lastProgress:
        records_processed = query.lastProgress.get("numInputRows", 0)

    return {
        "branch": branch,
        "source_system": source_system,
        "table": table_name,
        "source_path": source_path,
        "bronze_table": bronze_table,
        "checkpoint_location": checkpoint_location,
        "record_count": records_processed,
        "status": "SUCCESS"
    }