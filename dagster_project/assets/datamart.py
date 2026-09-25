from dagster import AssetCheckResult, AssetExecutionContext, MaterializeResult, asset, asset_check

from dagster_project.assets.reference import dim_currency
from dagster_project.assets.staging import stg_countries
from dagster_project.constants import (
    SCHEMA_DATAMART,
    SCHEMA_REFERENCE,
    SCHEMA_STAGING,
    SNAPSHOT_DATE_COLUMN,
    TABLE_DIM_CURRENCY,
    TABLE_DM_COUNTRIES,
    TABLE_DM_CURRENCY_DISTRIBUTION,
    TABLE_DM_LANGUAGE_DISTRIBUTION,
    TABLE_DM_SUBREGION_SUMMARY,
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


@asset(group_name="datamart", deps=[stg_countries, dim_currency])
def dm_currency_distribution(
    context: AssetExecutionContext, trino: TrinoResource
) -> MaterializeResult:
    """Datamart table: number of countries using each currency (with the
    currency's full name, joined from the dim_currency reference table),
    for the 'currency distribution' chart."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_CURRENCY_DISTRIBUTION,
        f"""
        SELECT
            trim(t.currency_code) AS currency_code,
            d.currency_name,
            count(*) AS country_count
        FROM {_staging_ref(trino)}
        CROSS JOIN UNNEST(split(currency_codes, ',')) AS t(currency_code)
        LEFT JOIN {trino.catalog}.{SCHEMA_REFERENCE}.{TABLE_DIM_CURRENCY} d
            ON d.currency_code = trim(t.currency_code)
        WHERE currency_codes IS NOT NULL AND currency_codes <> ''
        GROUP BY trim(t.currency_code), d.currency_name
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
    subregion. Also the source for region-level totals (group by region and
    re-sum) - there's no separate per-region table since it'd just be this
    one rolled up further."""
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
def dm_countries(context: AssetExecutionContext, trino: TrinoResource) -> MaterializeResult:
    """Datamart table: one row per country with population, area, density
    and capital - the shared source for both the 'most densely populated'
    and 'top countries by population' charts, which each just sort/limit
    this same table differently at query time instead of needing their own
    materialized tables."""
    row_count = _create_datamart_table(
        trino,
        TABLE_DM_COUNTRIES,
        f"""
        SELECT
            cca3,
            name_common,
            region,
            capital,
            population,
            area,
            CASE WHEN area IS NOT NULL AND area > 0 THEN population / area END AS population_density
        FROM {_staging_ref(trino)}
        WHERE population IS NOT NULL
        ORDER BY population DESC
        """,
    )
    return MaterializeResult(metadata={"row_count": row_count})


@asset_check(asset=dm_countries)
def dm_countries_check(trino: TrinoResource) -> AssetCheckResult:
    return _row_count_check(trino, TABLE_DM_COUNTRIES)
