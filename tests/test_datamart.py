from unittest.mock import MagicMock

import pytest
from dagster import materialize

from dagster_project.assets.datamart import (
    dm_currency_distribution,
    dm_language_distribution,
    dm_population_by_region,
    dm_population_density,
    dm_subregion_summary,
    dm_top_countries_by_population,
)

DATAMART_ASSETS = [
    (dm_population_by_region, "dm_population_by_region"),
    (dm_currency_distribution, "dm_currency_distribution"),
    (dm_language_distribution, "dm_language_distribution"),
    (dm_subregion_summary, "dm_subregion_summary"),
    (dm_population_density, "dm_population_density"),
    (dm_top_countries_by_population, "dm_top_countries_by_population"),
]


@pytest.mark.parametrize("asset_def, table_name", DATAMART_ASSETS)
def test_datamart_asset_runs_ctas_and_reports_row_count(asset_def, table_name):
    trino = MagicMock()
    trino.catalog = "iceberg"
    trino.query_scalar.return_value = 7

    result = materialize([asset_def], resources={"trino": trino})

    assert result.success
    executed = [c.args[0] for c in trino.execute.call_args_list]
    assert any(
        "CREATE TABLE IF NOT EXISTS" in sql and "WITH NO DATA" in sql and table_name in sql
        for sql in executed
    )
    assert any("DELETE FROM" in sql and table_name in sql for sql in executed)
    assert any("INSERT INTO" in sql and table_name in sql for sql in executed)

    materialization = result.asset_materializations_for_node(table_name)[0]
    assert materialization.metadata["row_count"].value == 7
