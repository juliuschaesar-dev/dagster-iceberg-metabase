import trino


def connect(
    host: str, port: int, user: str, catalog: str, schema: str | None = None
) -> "trino.dbapi.Connection":
    """Open a Trino connection. Shared by the Dagster TrinoResource and the
    Panel dashboard so both configure the driver the same way."""
    return trino.dbapi.connect(host=host, port=port, user=user, catalog=catalog, schema=schema)
