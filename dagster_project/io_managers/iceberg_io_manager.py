from datetime import date, datetime

import pandas as pd
from dagster import ConfigurableIOManager, InputContext, OutputContext

from dagster_project.constants import SCHEMA_STAGING, SNAPSHOT_DATE_COLUMN
from dagster_project.resources.trino_resource import TrinoResource


def _trino_type(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    # A column of plain datetime.date objects (e.g. snapshot_date, assigned
    # as a Python date rather than a pandas Timestamp) stays object dtype
    # instead of being cast to datetime64, so it needs its own check.
    non_null = series.dropna()
    if not non_null.empty and all(
        isinstance(v, date) and not isinstance(v, datetime) for v in non_null
    ):
        return "DATE"
    return "VARCHAR"


def _sql_literal(value: object) -> str:
    if value is None or (isinstance(value, float) and value != value):  # NaN check
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


class IcebergIOManager(ConfigurableIOManager):
    """Persists pandas DataFrames produced by assets as Iceberg tables via Trino.

    Every write is tagged with a `snapshot_date` column and appended, so the
    table accumulates one dated snapshot per pipeline run instead of losing
    history. Re-running on the same day replaces only that day's snapshot
    (delete + insert), so reruns stay idempotent.
    """

    trino: TrinoResource

    def _table_ref(self, context: OutputContext | InputContext) -> tuple[str, str]:
        table = context.asset_key.path[-1]
        schema = (context.definition_metadata or {}).get("schema", SCHEMA_STAGING)
        return schema, table

    def handle_output(self, context: OutputContext, obj: pd.DataFrame) -> None:
        schema, table = self._table_ref(context)
        catalog = self.trino.catalog
        table_ref = f'{catalog}.{schema}."{table}"'
        snapshot_date = date.today()

        self.trino.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        obj = obj.copy()
        obj[SNAPSHOT_DATE_COLUMN] = snapshot_date
        columns = list(obj.columns)
        col_defs = ", ".join(f'"{col}" {_trino_type(obj[col])}' for col in columns)

        self.trino.execute(f"CREATE TABLE IF NOT EXISTS {table_ref} ({col_defs})")
        # Migrates tables that were created before snapshot_date existed; a
        # no-op once the column is present.
        self.trino.execute(
            f'ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS "{SNAPSHOT_DATE_COLUMN}" DATE'
        )
        self.trino.execute(
            f'DELETE FROM {table_ref} WHERE "{SNAPSHOT_DATE_COLUMN}" = {_sql_literal(snapshot_date)}'
        )

        if obj.empty:
            context.log.info(f"No rows to write to {table_ref} for {snapshot_date}")
            return

        col_list = ", ".join(f'"{col}"' for col in columns)
        value_rows = ", ".join(
            "(" + ", ".join(_sql_literal(v) for v in row) + ")"
            for row in obj.itertuples(index=False, name=None)
        )
        sql = f"INSERT INTO {table_ref} ({col_list}) VALUES {value_rows}"
        self.trino.execute(sql)
        context.log.info(f"Wrote {len(obj)} rows to Iceberg table {table_ref} for {snapshot_date}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        # No asset currently takes an Iceberg-backed asset as a direct Python
        # input - all downstream reads go through TrinoResource SQL instead
        # (see dagster_project/assets/datamart.py). Implemented only to
        # satisfy the IOManager interface.
        raise NotImplementedError(
            "IcebergIOManager.load_input is not supported - read the table via "
            "TrinoResource instead of taking it as an asset input."
        )
