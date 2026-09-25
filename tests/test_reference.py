from dagster import materialize, mem_io_manager

from dagster_project.assets.reference import dim_currency


def test_dim_currency_returns_code_name_lookup():
    result = materialize([dim_currency], resources={"iceberg_io_manager": mem_io_manager})

    assert result.success
    df = result.output_for_node("dim_currency")
    assert set(df.columns) == {"currency_code", "currency_name"}
    assert df.set_index("currency_code").loc["EUR", "currency_name"] == "Euro"
    assert df["currency_code"].is_unique
