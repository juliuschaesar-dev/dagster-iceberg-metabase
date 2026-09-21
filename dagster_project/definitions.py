from dagster import (
    AssetSelection,
    Definitions,
    RunFailureSensorContext,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
    run_failure_sensor,
)

from dagster_project.assets import datamart, raw, staging
from dagster_project.io_managers.iceberg_io_manager import IcebergIOManager
from dagster_project.resources.api_resource import RestCountriesApiResource
from dagster_project.resources.garage_resource import GarageResource
from dagster_project.resources.trino_resource import TrinoResource

all_assets = load_assets_from_modules([raw, staging, datamart])

all_asset_checks = [
    staging.stg_countries_row_count_check,
    staging.stg_countries_required_columns_not_null_check,
    datamart.dm_population_by_region_check,
    datamart.dm_currency_distribution_check,
    datamart.dm_language_distribution_check,
    datamart.dm_subregion_summary_check,
    datamart.dm_population_density_check,
    datamart.dm_top_countries_by_population_check,
]

countries_pipeline_job = define_asset_job(
    name="countries_pipeline_job",
    selection=AssetSelection.all(),
)

countries_pipeline_schedule = ScheduleDefinition(
    name="daily_countries_pipeline",
    job=countries_pipeline_job,
    cron_schedule="0 6 * * *",  # daily at 06:00
)


@run_failure_sensor(monitored_jobs=[countries_pipeline_job])
def countries_pipeline_failure_sensor(context: RunFailureSensorContext) -> None:
    """Logs pipeline failures. Wire this to Slack/email by replacing the log
    call with a notification call once credentials are available."""
    context.log.error(
        f"Run {context.dagster_run.run_id} failed: {context.failure_event.message}"
    )


trino_resource = TrinoResource()

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=[countries_pipeline_job],
    schedules=[countries_pipeline_schedule],
    sensors=[countries_pipeline_failure_sensor],
    resources={
        "api": RestCountriesApiResource(),
        "garage": GarageResource(),
        "trino": trino_resource,
        "iceberg_io_manager": IcebergIOManager(trino=trino_resource),
    },
)
