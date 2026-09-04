from datetime import datetime

from src.ingestion.reader import read_csv
from src.ingestion.metadata import add_ingestion_metadata
from src.ingestion.writer import write_bronze


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


def ingest_table(
    spark,
    catalog: str,
    landing_schema: str,
    bronze_schema: str,
    volume: str,
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

    bronze_table = (
        f"{catalog}."
        f"{bronze_schema}."
        f"{table_name}"
    )

    df = read_csv(
        spark = spark,
        path = source_path
    )

    df = add_ingestion_metadata(
        df = df,
        branch = branch, 
        source_system = source_system,
        batch_id = batch_id
    )

    write_bronze(
        df = df,
        table_name = bronze_table
    )


    return {
        "branch": branch,
        "source_system": source_system,
        "table": table_name,
        "source_path": source_path,
        "bronze_table": bronze_table,
        "record_count": df.count(),
        "status": "SUCCESS"
    }