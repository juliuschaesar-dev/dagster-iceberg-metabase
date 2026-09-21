from dagster import AssetCheckResult, AssetExecutionContext, MaterializeResult, asset, asset_check

from dagster_project.assets.staging import stg_countries
from dagster_project.constants import (
    SCHEMA_DATAMART,
    SCHEMA_STAGING,
    SNAPSHOT_DATE_COLUMN,
    TABLE_DM_CURRENCY_DISTRIBUTION,
    TABLE_DM_LANGUAGE_DISTRIBUTION,
    TABLE_DM_POPULATION_BY_REGION,
    TABLE_DM_POPULATION_DENSITY,
    TABLE_DM_SUBREGION_SUMMARY,
    TABLE_DM_TOP_COUNTRIES_BY_POPULATION,
    TABLE_STAGING_COUNTRIES,
)
from dagster_project.resources.trino_resource import TrinoResource


def _staging_ref(trino: TrinoResource) -> str:
    """Only today's staging snapshot - stg_countries keeps one dated
    snapshot per row instead of just the latest, so datamart queries must
    scope to the current run or they'd double-count history."""
    return (
        f'(SELECT * FROM {trino.catalog}.{SCHEMA_STAGING}.{TABLE_STAGING_COUNTRIES} '
        f'WHERE "{SNAPSHOT_DATE_COLUMN}" = CURRENT_DATE) AS {TABLE_STAGING_COUNTRIES}'
    )


def _create_datamart_table(trino: TrinoResource, table: str, select_sql: str) -> int:
    """Builds one datamart table by appending today's snapshot (delete +
    insert, keyed on snapshot_date), so re-running the pipeline on the same
    day stays idempotent while different days accumulate history. Returns
    today's row count."""
    table_ref = f"{trino.catalog}.{SCHEMA_DATAMART}.{table}"
    # snapshot_date goes last: ALTER TABLE ADD COLUMN (used to migrate tables
    # that predate this column) always appends at the end, and INSERT ...
    # SELECT matches columns positionally, so the two must stay in sync.
    dated_select = f"SELECT *, CURRENT_DATE AS {SNAPSHOT_DATE_COLUMN} FROM ({select_sql}) AS src"

    trino.execute(f"CREATE SCHEMA IF NOT EXISTS {trino.catalog}.{SCHEMA_DATAMART}")
    trino.execute(f"CREATE TABLE IF NOT EXISTS {table_ref} AS {dated_select} WITH NO DATA")
    # Migrates tables that were created before snapshot_date existed; a
    # no-op once the column is present.
    trino.execute(f'ALTER TABLE {table_ref} ADD COLUMN IF NOT EXISTS "{SNAPSHOT_DATE_COLUMN}" DATE')
    trino.execute(f'DELETE FROM {table_ref} WHERE "{SNAPSHOT_DATE_COLUMN}" = CURRENT_DATE')
    trino.execute(f"INSERT INTO {table_ref} {dated_select}")
    return trino.query_scalar(
        f'SELECT count(*) FROM {table_ref} WHERE "{SNAPSHOT_DATE_COLUMN}" = CURRENT_DATE'
    )


def _row_count_check(trino: TrinoResource, table: str) -> AssetCheckResult:
    count = trino.query_scalar(
        f'SELECT count(*) FROM {trino.catalog}.{SCHEMA_DATAMART}.{table} '
        f'WHERE "{SNAPSHOT_DATE_COLUMN}" = CURRENT_DATE'
    )
    return AssetCheckResult(passed=count is not None and count > 0, metadata={"row_count": count})


@asset(group_name="datamart", deps=[stg_countries])
def dm_population_by_region(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Datamart table: total population and country count per region, for the
    'population by region' chart."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_POPULATION_BY_REGION,
        f"""
        SELECT
            region,
            sum(population) AS total_population,
            count(*) AS country_count
        FROM {_staging_ref(trino)}
        WHERE region IS NOT NULL AND region <> ''
        GROUP BY region
        ORDER BY total_population DESC
        """,
    )
    return MaterializeResult(metadata={"row_count": row_count})


@asset_check(asset=dm_population_by_region)
def dm_population_by_region_check(trino: TrinoResource) -> AssetCheckResult:
    return _row_count_check(trino, TABLE_DM_POPULATION_BY_REGION)


@asset(group_name="datamart", deps=[stg_countries])
def dm_currency_distribution(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Datamart table: number of countries using each currency, for the
    'currency distribution' chart."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_CURRENCY_DISTRIBUTION,
        f"""
        SELECT
            trim(currency_code) AS currency_code,
            count(*) AS country_count
        FROM {_staging_ref(trino)}
        CROSS JOIN UNNEST(split(currency_codes, ',')) AS t(currency_code)
        WHERE currency_codes IS NOT NULL AND currency_codes <> ''
        GROUP BY trim(currency_code)
        ORDER BY country_count DESC
        """,
    )
    return MaterializeResult(metadata={"row_count": row_count})


@asset_check(asset=dm_currency_distribution)
def dm_currency_distribution_check(trino: TrinoResource) -> AssetCheckResult:
    return _row_count_check(trino, TABLE_DM_CURRENCY_DISTRIBUTION)


@asset(group_name="datamart", deps=[stg_countries])
def dm_language_distribution(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Datamart table: number of countries speaking each language, for the
    'language distribution' chart."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_LANGUAGE_DISTRIBUTION,
        f"""
        SELECT
            trim(language) AS language,
            count(*) AS country_count
        FROM {_staging_ref(trino)}
        CROSS JOIN UNNEST(split(languages, ',')) AS t(language)
        WHERE languages IS NOT NULL AND languages <> ''
        GROUP BY trim(language)
        ORDER BY country_count DESC
        """,
    )
    return MaterializeResult(metadata={"row_count": row_count})


@asset_check(asset=dm_language_distribution)
def dm_language_distribution_check(trino: TrinoResource) -> AssetCheckResult:
    return _row_count_check(trino, TABLE_DM_LANGUAGE_DISTRIBUTION)


@asset(group_name="datamart", deps=[stg_countries])
def dm_subregion_summary(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Datamart table: total population, total area and country count per
    subregion, for a more granular breakdown than region alone."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_SUBREGION_SUMMARY,
        f"""
        SELECT
            region,
            subregion,
            sum(population) AS total_population,
            sum(area) AS total_area,
            count(*) AS country_count
        FROM {_staging_ref(trino)}
        WHERE subregion IS NOT NULL AND subregion <> ''
        GROUP BY region, subregion
        ORDER BY total_population DESC
        """,
    )
    return MaterializeResult(metadata={"row_count": row_count})


@asset_check(asset=dm_subregion_summary)
def dm_subregion_summary_check(trino: TrinoResource) -> AssetCheckResult:
    return _row_count_check(trino, TABLE_DM_SUBREGION_SUMMARY)


@asset(group_name="datamart", deps=[stg_countries])
def dm_population_density(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Datamart table: population density (people per km²) per country, for
    the 'most densely populated countries' chart."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_POPULATION_DENSITY,
        f"""
        SELECT
            cca3,
            name_common,
            region,
            population,
            area,
            population / area AS population_density
        FROM {_staging_ref(trino)}
        WHERE population IS NOT NULL AND area IS NOT NULL AND area > 0
        ORDER BY population_density DESC
        """,
    )
    return MaterializeResult(metadata={"row_count": row_count})


@asset_check(asset=dm_population_density)
def dm_population_density_check(trino: TrinoResource) -> AssetCheckResult:
    return _row_count_check(trino, TABLE_DM_POPULATION_DENSITY)


@asset(group_name="datamart", deps=[stg_countries])
def dm_top_countries_by_population(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Datamart table: the 20 most populous countries, for a 'top countries'
    chart."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_TOP_COUNTRIES_BY_POPULATION,
        f"""
        SELECT
            cca3,
            name_common,
            region,
            capital,
            population
        FROM {_staging_ref(trino)}
        WHERE population IS NOT NULL
        ORDER BY population DESC
        LIMIT 20
        """,
    )
    return MaterializeResult(metadata={"row_count": row_count})


@asset_check(asset=dm_top_countries_by_population)
def dm_top_countries_by_population_check(trino: TrinoResource) -> AssetCheckResult:
    return _row_count_check(trino, TABLE_DM_TOP_COUNTRIES_BY_POPULATION)
