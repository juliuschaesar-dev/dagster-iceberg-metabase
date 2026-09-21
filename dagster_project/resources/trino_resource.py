from contextlib import contextmanager
from typing import Any, Iterator

import trino
from dagster import ConfigurableResource, EnvVar


class TrinoResource(ConfigurableResource):
    """Connection to Trino, reads config from env."""

    host: str = EnvVar("TRINO_HOST")
    port: int = EnvVar.int("TRINO_PORT")
    user: str = EnvVar("TRINO_USER")
    catalog: str = EnvVar("TRINO_CATALOG")

    @contextmanager
    def _connection(self) -> Iterator["trino.dbapi.Connection"]:
        conn = trino.dbapi.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute(self, sql: str) -> list[list[Any]]:
        """Run a statement and return all result rows (empty for DDL)."""
        with self._connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql)
            return cursor.fetchall()

    def query_scalar(self, sql: str) -> Any:
        rows = self.execute(sql)
        return rows[0][0] if rows and rows[0] else None
