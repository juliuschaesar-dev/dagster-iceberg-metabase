from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset

from dagster_project.resources.api_resource import RestCountriesApiResource
from dagster_project.resources.garage_resource import GarageResource

RAW_OBJECT_KEY = "countries_latest.json"


@asset(group_name="raw")
def raw_countries(
    context: AssetExecutionContext,
    api: RestCountriesApiResource,
    garage: GarageResource,
) -> MaterializeResult:
    """Fetches every country from the REST Countries API and stores the raw
    JSON response, unmodified, in the `raw` Garage bucket."""
    countries = api.fetch_all_countries()

    garage.put_json(bucket=garage.bucket_raw, key=RAW_OBJECT_KEY, payload=countries)

    return MaterializeResult(
        metadata={
            "num_countries": len(countries),
            "bucket": garage.bucket_raw,
            "object_key": RAW_OBJECT_KEY,
            "preview": MetadataValue.json(countries[:2]),
        }
    )
