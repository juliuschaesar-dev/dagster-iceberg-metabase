from unittest.mock import MagicMock

from dagster import mem_io_manager, materialize

from dagster_project.assets.raw import raw_countries
from dagster_project.assets.staging import _flatten_country, stg_countries


def test_flatten_country_maps_nested_fields():
    country = {
        "codes": {"alpha_2": "ID", "alpha_3": "IDN"},
        "names": {"common": "Indonesia", "official": "Republic of Indonesia"},
        "capitals": [{"name": "Jakarta"}],
        "region": "Asia",
        "subregion": "South-Eastern Asia",
        "population": 273523615,
        "area": {"kilometers": 1904569.0, "miles": 735358.0},
        "currencies": [{"code": "IDR", "name": "Indonesian rupiah"}],
        "languages": [{"name": "Indonesian"}],
        "coordinates": {"lat": -5.0, "lng": 120.0},
    }

    flat = _flatten_country(country)

    assert flat["cca3"] == "IDN"
    assert flat["name_common"] == "Indonesia"
    assert flat["capital"] == "Jakarta"
    assert flat["currency_codes"] == "IDR"
    assert flat["currency_names"] == "Indonesian rupiah"
    assert flat["languages"] == "Indonesian"
    assert flat["area"] == 1904569.0
    assert flat["latitude"] == -5.0
    assert flat["longitude"] == 120.0


def test_stg_countries_flattens_raw_payload():
    fake_payload = [
        {
            "codes": {"alpha_2": "SG", "alpha_3": "SGP"},
            "names": {"common": "Singapore", "official": "Republic of Singapore"},
            "capitals": [{"name": "Singapore"}],
            "region": "Asia",
            "subregion": "South-Eastern Asia",
            "population": 5850342,
            "area": {"kilometers": 710.0, "miles": 274.2},
            "currencies": [{"code": "SGD", "name": "Singapore dollar"}],
            "languages": [{"name": "English"}],
            "coordinates": {"lat": 1.37, "lng": 103.8},
        }
    ]

    garage = MagicMock()
    garage.get_json.return_value = fake_payload
    garage.bucket_raw = "raw"

    result = materialize(
        [raw_countries, stg_countries],
        resources={
            "api": MagicMock(fetch_all_countries=MagicMock(return_value=fake_payload)),
            "garage": garage,
            "iceberg_io_manager": mem_io_manager,
        },
    )

    assert result.success
    df = result.output_for_node("stg_countries")
    assert len(df) == 1
    assert df["cca3"][0] == "SGP"
