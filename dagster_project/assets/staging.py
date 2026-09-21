import polars as pl
from dagster import AssetCheckResult, AssetExecutionContext, asset, asset_check

from dagster_project.assets.raw import RAW_OBJECT_KEY, raw_countries
from dagster_project.constants import SCHEMA_STAGING, SNAPSHOT_DATE_COLUMN, TABLE_STAGING_COUNTRIES
from dagster_project.resources.garage_resource import GarageResource
from dagster_project.resources.trino_resource import TrinoResource

REQUIRED_COLUMNS = ["name_common", "region", "population"]
# cca3 is deliberately excluded: this API includes disputed/partially
# recognized territories (e.g. Abkhazia, Northern Cyprus, Somaliland, South
# Ossetia) that have no official ISO 3166-1 alpha-3 code, so the field is
# legitimately null for them rather than indicating bad data.


def _flatten_country(country: dict) -> dict:
    codes = country.get("codes") or {}
    names = country.get("names") or {}
    capitals = country.get("capitals") or []
    currencies = country.get("currencies") or []
    languages = country.get("languages") or []
    coordinates = country.get("coordinates") or {}
    area = country.get("area") or {}

    return {
        "cca2": codes.get("alpha_2") or None,
        "cca3": codes.get("alpha_3") or None,
        "name_common": names.get("common"),
        "name_official": names.get("official"),
        "capital": ", ".join(c["name"] for c in capitals if c.get("name")) or None,
        "region": country.get("region"),
        "subregion": country.get("subregion"),
        "population": country.get("population"),
        "area": area.get("kilometers"),
        "currency_codes": ", ".join(c["code"] for c in currencies if c.get("code")) or None,
        "currency_names": ", ".join(c["name"] for c in currencies if c.get("name")) or None,
        "languages": ", ".join(lang["name"] for lang in languages if lang.get("name")) or None,
        "latitude": coordinates.get("lat"),
        "longitude": coordinates.get("lng"),
    }


@asset(
    group_name="staging",
    deps=[raw_countries],
    io_manager_key="iceberg_io_manager",
    metadata={"schema": SCHEMA_STAGING},
)
def stg_countries(context: AssetExecutionContext, garage: GarageResource) -> pl.DataFrame:
    """Reads the raw countries JSON and flattens nested structures (`name`,
    `currencies`, `languages`, `latlng`) into a flat table ready to be
    persisted as the Iceberg staging table."""
    raw_payload = garage.get_json(bucket=garage.bucket_raw, key=RAW_OBJECT_KEY)

    rows = [_flatten_country(country) for country in raw_payload]
    df = pl.DataFrame(rows)

    context.log.info(f"Flattened {len(df)} countries into staging schema")
    return df


@asset_check(asset=stg_countries)
def stg_countries_row_count_check(trino: TrinoResource) -> AssetCheckResult:
    """Fails if today's staging snapshot has no rows."""
    count = trino.query_scalar(
        f"SELECT count(*) FROM {trino.catalog}.{SCHEMA_STAGING}.{TABLE_STAGING_COUNTRIES} "
        f'WHERE "{SNAPSHOT_DATE_COLUMN}" = CURRENT_DATE'
    )
    return AssetCheckResult(passed=count is not None and count > 0, metadata={"row_count": count})


@asset_check(asset=stg_countries)
def stg_countries_required_columns_not_null_check(trino: TrinoResource) -> AssetCheckResult:
    """Fails if any required column has null values in today's staging snapshot."""
    null_counts = {}
    for column in REQUIRED_COLUMNS:
        count = trino.query_scalar(
            f'SELECT count(*) FROM {trino.catalog}.{SCHEMA_STAGING}.{TABLE_STAGING_COUNTRIES} '
            f'WHERE "{column}" IS NULL AND "{SNAPSHOT_DATE_COLUMN}" = CURRENT_DATE'
        )
        null_counts[column] = count

    passed = all(count == 0 for count in null_counts.values())
    return AssetCheckResult(passed=passed, metadata=null_counts)
