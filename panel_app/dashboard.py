import hvplot.pandas  # noqa: F401 (registers the .hvplot accessor)
import panel as pn

from panel_app.trino_client import latest_snapshot

pn.extension(sizing_mode="stretch_width")


def population_by_region_chart():
    # Region totals are a further roll-up of dm_subregion_summary, so there's
    # no separate per-region datamart table - just re-aggregate here.
    df = (
        latest_snapshot("dm_subregion_summary")
        .groupby("region", as_index=False)[["total_population", "country_count"]]
        .sum()
        .sort_values("total_population", ascending=False)
    )
    return df.hvplot.bar(
        x="region", y="total_population", title="Population by Region",
        color="#2563EB", rot=45, height=350,
    )


def currency_distribution_chart():
    df = latest_snapshot("dm_currency_distribution").nlargest(15, "country_count")
    return df.hvplot.barh(
        x="currency_code", y="country_count", title="Top Currencies by Country Count",
        color="#0D9488", height=400,
    )


def language_distribution_chart():
    df = latest_snapshot("dm_language_distribution").nlargest(15, "country_count")
    return df.hvplot.barh(
        x="language", y="country_count", title="Top Languages by Country Count",
        color="#7C3AED", height=400,
    )


def subregion_summary_chart():
    df = latest_snapshot("dm_subregion_summary")
    return df.hvplot.bar(
        x="subregion", y="total_population", by="region", title="Population by Subregion",
        rot=60, height=400, legend="top_right",
    )


def population_density_chart():
    df = latest_snapshot("dm_countries").dropna(subset=["population_density"]).nlargest(20, "population_density")
    return df.hvplot.barh(
        x="name_common", y="population_density", title="Most Densely Populated Countries",
        color="#CA8A04", height=500,
    )


def top_countries_chart():
    df = latest_snapshot("dm_countries").nlargest(20, "population")
    return df.hvplot.barh(
        x="name_common", y="population", title="Top 20 Countries by Population",
        color="#DC2626", height=500,
    )


def build_dashboard() -> pn.template.FastListTemplate:
    template = pn.template.FastListTemplate(
        title="REST Countries Dashboard",
        accent_base_color="#2563EB",
        header_background="#2563EB",
    )
    template.main.extend([
        pn.Row(population_by_region_chart(), subregion_summary_chart()),
        pn.Row(currency_distribution_chart(), language_distribution_chart()),
        pn.Row(population_density_chart(), top_countries_chart()),
    ])
    return template


build_dashboard().servable()
