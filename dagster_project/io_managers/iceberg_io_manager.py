from datetime import date, datetime

import polars as pl
from dagster import ConfigurableIOManager, InputContext, OutputContext

from dagster_project.constants import SCHEMA_STAGING, SNAPSHOT_DATE_COLUMN
from dagster_project.resources.trino_resource import TrinoResource


def _trino_type(dtype: pl.DataType) -> str:
    if dtype.is_integer():
        return "BIGINT"
    if dtype.is_float():
        return "DOUBLE"
    if dtype == pl.Boolean:
        return "BOOLEAN"
    if dtype == pl.Date:
        return "DATE"
    if isinstance(dtype, pl.Datetime):
        return "TIMESTAMP"
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
    """Persists Polars DataFrames produced by assets as Iceberg tables via Trino.

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

    def handle_output(self, context: OutputContext, obj: pl.DataFrame) -> None:
        schema, table = self._table_ref(context)
        catalog = self.trino.catalog
        table_ref = f'{catalog}.{schema}."{table}"'
        snapshot_date = date.today()

        self.trino.execute(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

        obj = obj.with_columns(pl.lit(snapshot_date).alias(SNAPSHOT_DATE_COLUMN))
        columns = obj.columns
        col_defs = ", ".join(f'"{col}" {_trino_type(obj[col].dtype)}' for col in columns)

        self.trino.execute(f"CREATE TABLE IF NOT EXISTS {table_ref} ({col_defs})")
        # Migrates tables that were created before snapshot_date existed; a
        # no-op once the column is present.
        self.trino.execute(
            f'ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS "{SNAPSHOT_DATE_COLUMN}" DATE'
        )
        self.trino.execute(
            f'DELETE FROM {table_ref} WHERE "{SNAPSHOT_DATE_COLUMN}" = {_sql_literal(snapshot_date)}'
        )

        if obj.is_empty():
            context.log.info(f"No rows to write to {table_ref} for {snapshot_date}")
            return

        col_list = ", ".join(f'"{col}"' for col in columns)
        value_rows = ", ".join(
            "(" + ", ".join(_sql_literal(v) for v in row) + ")" for row in obj.iter_rows()
        )
        sql = f"INSERT INTO {table_ref} ({col_list}) VALUES {value_rows}"
        self.trino.execute(sql)
        context.log.info(f"Wrote {len(obj)} rows to Iceberg table {table_ref} for {snapshot_date}")

    def load_input(self, context: InputContext) -> pl.DataFrame:
        # No asset currently takes an Iceberg-backed asset as a direct Python
        # input - all downstream reads go through TrinoResource SQL instead
        # (see dagster_project/assets/datamart.py). Implemented only to
        # satisfy the IOManager interface.
        raise NotImplementedError(
            "IcebergIOManager.load_input is not supported - read the table via "
            "TrinoResource instead of taking it as an asset input."
        )
