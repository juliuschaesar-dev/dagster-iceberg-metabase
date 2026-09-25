import os

import pandas as pd

from shared.trino import connect


def query_df(sql: str) -> pd.DataFrame:
    """Run a query against the datamart schema and return the result as a DataFrame."""
    conn = connect(
        host=os.environ["TRINO_HOST"],
        port=int(os.environ["TRINO_PORT"]),
        user=os.environ["TRINO_USER"],
        catalog=os.environ["TRINO_CATALOG"],
        schema=os.environ["TRINO_SCHEMA_DATAMART"],
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()


def snapshot_dates(table: str) -> list[str]:
    """Every distinct snapshot_date in a datamart table, most recent first."""
    df = query_df(f"SELECT DISTINCT snapshot_date FROM {table} ORDER BY snapshot_date DESC")
    return [str(d) for d in df["snapshot_date"]]


def snapshot(table: str, snapshot_date: str) -> pd.DataFrame:
    """All rows from a datamart table's given snapshot_date."""
    return query_df(f"SELECT * FROM {table} WHERE snapshot_date = DATE '{snapshot_date}'")


def latest_snapshot(table: str) -> pd.DataFrame:
    """All rows from a datamart table's most recent snapshot_date."""
    return query_df(
        f'SELECT * FROM {table} WHERE snapshot_date = (SELECT max(snapshot_date) FROM {table})'
    )
