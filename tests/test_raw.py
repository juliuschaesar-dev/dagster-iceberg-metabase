from unittest.mock import MagicMock

from dagster import materialize

from dagster_project.assets.raw import RAW_OBJECT_KEY, raw_countries


def test_raw_countries_saves_json_to_garage():
    fake_countries = [
        {"cca2": "ID", "name": {"common": "Indonesia"}, "region": "Asia", "population": 273523615},
        {"cca2": "SG", "name": {"common": "Singapore"}, "region": "Asia", "population": 5850342},
    ]

    api = MagicMock()
    api.fetch_all_countries.return_value = fake_countries

    garage = MagicMock()
    garage.bucket_raw = "raw"

    result = materialize(
        [raw_countries],
        resources={"api": api, "garage": garage},
    )

    assert result.success
    garage.put_json.assert_called_once_with(
        bucket="raw", key=RAW_OBJECT_KEY, payload=fake_countries
    )

    materialization = result.asset_materializations_for_node("raw_countries")[0]
    assert materialization.metadata["num_countries"].value == 2
