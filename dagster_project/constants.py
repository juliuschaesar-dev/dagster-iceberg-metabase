"""Single source of truth for Iceberg schema and table names, so every
asset, asset check, and the IO manager reference the same identifiers."""

SCHEMA_STAGING = "staging"
SCHEMA_DATAMART = "datamart"

# Every staging/datamart table carries this column so re-running the
# pipeline appends a new dated snapshot instead of overwriting history.
SNAPSHOT_DATE_COLUMN = "snapshot_date"

TABLE_STAGING_COUNTRIES = "stg_countries"
TABLE_DM_POPULATION_BY_REGION = "dm_population_by_region"
TABLE_DM_CURRENCY_DISTRIBUTION = "dm_currency_distribution"
TABLE_DM_LANGUAGE_DISTRIBUTION = "dm_language_distribution"
TABLE_DM_SUBREGION_SUMMARY = "dm_subregion_summary"
TABLE_DM_POPULATION_DENSITY = "dm_population_density"
TABLE_DM_TOP_COUNTRIES_BY_POPULATION = "dm_top_countries_by_population"
