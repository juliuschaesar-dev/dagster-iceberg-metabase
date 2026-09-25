import html
from datetime import datetime

import pandas as pd
import panel as pn

from panel_app.trino_client import snapshot, snapshot_dates

pn.extension(sizing_mode="stretch_width")

REGION_COLORS = {
    "Asia": "#f2a65a",
    "Africa": "#e2725b",
    "Americas": "#2dd4a7",
    "Europe": "#7c86f0",
    "Oceania": "#9b6bdb",
}
TEAL = "#2dd4a7"
ACCENT_POPULATION = "#f2a65a"
ACCENT_DENSITY = TEAL
ACCENT_CURRENCY = TEAL
ACCENT_LANGUAGE = "#8b7cf6"


def _esc(value: object) -> str:
    return html.escape(str(value))


def _fmt_big(n: float) -> str:
    if n >= 1_000_000_000:
        return f"{n / 1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.0f}M"
    if n >= 1_000:
        return f"{n / 1_000:.0f}K"
    return f"{n:.0f}"


def _fmt_int(n: float) -> str:
    return f"{round(n):,}"


def _coalesce(value: object, fallback: object) -> object:
    return fallback if pd.isna(value) else value


def _pct(value: float, max_value: float) -> float:
    return value / max_value * 100


def _load_data(snapshot_date: str) -> dict[str, pd.DataFrame]:
    return {
        "countries": snapshot("dm_countries", snapshot_date),
        "subregions": snapshot("dm_subregion_summary", snapshot_date),
        "currencies": snapshot("dm_currency_distribution", snapshot_date),
        "languages": snapshot("dm_language_distribution", snapshot_date),
    }


def _card_shell(title: str, subtitle: str, body_html: str, header_extra: str = "") -> str:
    """Shared <div class="card"> wrapper (title + subtitle, optionally with
    something extra - a legend or badge - alongside them) used by every card
    on the page, so each card function only needs to build its own body."""
    if header_extra:
        header = f"""
      <div class="card-header-row">
        <div>
          <div class="card-title">{_esc(title)}</div>
          <div class="card-subtitle">{_esc(subtitle)}</div>
        </div>
        {header_extra}
      </div>"""
    else:
        header = f"""
      <div class="card-title">{_esc(title)}</div>
      <div class="card-subtitle">{_esc(subtitle)}</div>"""
    return f"""
    <div class="card">{header}
      {body_html}
    </div>"""


def _stat_tile(label: str, value: str, subtitle_html: str) -> str:
    return f"""
    <div class="tile">
      <div class="tile-label">{_esc(label)}</div>
      <div class="tile-value">{value}</div>
      <div class="tile-subtitle">{subtitle_html}</div>
    </div>"""


def _stat_tiles_html(data: dict[str, pd.DataFrame]) -> str:
    countries = data["countries"]
    subregions = data["subregions"]
    currencies = data["currencies"]
    languages = data["languages"]

    world_population = countries["population"].sum()
    num_regions = subregions["region"].nunique()

    top_countries = countries.nlargest(2, "population")
    top_country = top_countries.iloc[0]
    runner_up = top_countries.iloc[1] if len(top_countries) > 1 else None

    top_language = languages.nlargest(1, "country_count").iloc[0]
    top_currency = currencies.nlargest(1, "country_count").iloc[0]
    currency_name = _coalesce(top_currency["currency_name"], top_currency["currency_code"])

    population_subtitle = f'<span class="accent-amber">{_fmt_big(top_country["population"])}</span> people'
    if runner_up is not None:
        population_subtitle += f' · {_esc(runner_up["name_common"])} close behind'

    tiles = [
        _stat_tile("World population", _fmt_big(world_population), f"across {num_regions} regions"),
        _stat_tile("Most populous", _esc(top_country["name_common"]), population_subtitle),
        _stat_tile(
            "Most widespread language",
            _esc(top_language["language"]),
            f'official in {_fmt_int(top_language["country_count"])} countries',
        ),
        _stat_tile(
            "Most shared currency",
            _esc(currency_name),
            f'<span class="accent-teal">{_esc(top_currency["currency_code"])}</span> '
            f'used in {_fmt_int(top_currency["country_count"])} countries',
        ),
    ]
    return f'<div class="tile-grid">{"".join(tiles)}</div>'


def _region_share_card(subregions: pd.DataFrame) -> str:
    by_region = (
        subregions.groupby("region", as_index=False)["total_population"]
        .sum()
        .sort_values("total_population", ascending=False)
    )
    world_population = by_region["total_population"].sum()

    segments, rows = [], []
    for _, row in by_region.iterrows():
        color = REGION_COLORS.get(row["region"], "#64748b")
        pct = _pct(row["total_population"], world_population)
        segments.append(f'<div class="segment" style="width:{pct:.2f}%;background:{color}"></div>')
        rows.append(f"""
        <div class="legend-row">
          <span class="dot" style="background:{color}"></span>
          <span class="legend-name">{_esc(row["region"])}</span>
          <span class="legend-value mono">{_fmt_big(row["total_population"])}</span>
          <span class="legend-pct mono">{pct:.1f}%</span>
        </div>""")

    body = f'<div class="segment-bar">{"".join(segments)}</div><div class="legend-list">{"".join(rows)}</div>'
    return _card_shell("Population by region", "Share of world population", body)


def _largest_subregions_card(subregions: pd.DataFrame, top_n: int = 8) -> str:
    top = subregions.nlargest(top_n, "total_population")
    max_value = top["total_population"].max()

    legend = "".join(
        f'<span class="legend-chip"><span class="dot" style="background:{color}"></span>{region}</span>'
        for region, color in REGION_COLORS.items()
    )
    rows = []
    for _, row in top.iterrows():
        color = REGION_COLORS.get(row["region"], "#64748b")
        width_pct = _pct(row["total_population"], max_value)
        rows.append(f"""
        <div class="bar-row">
          <div class="bar-label">{_esc(row["subregion"])}</div>
          <div class="bar-track">
            <div class="bar-fill" style="width:{width_pct:.2f}%;background:{color}"></div>
          </div>
          <div class="bar-value mono">{_fmt_big(row["total_population"])}</div>
        </div>""")

    body = f'<div class="bar-list">{"".join(rows)}</div>'
    legend_html = f'<div class="legend-chips">{legend}</div>'
    return _card_shell(
        "Largest subregions", f"Top {top_n} subregions by population, colored by region", body, legend_html
    )


def _ranked_list_card(
    title: str, subtitle: str, rows_df: pd.DataFrame, label_col: str, value_col: str,
    fmt_value, color: str, badge: str | None = None,
) -> str:
    max_value = rows_df[value_col].max()
    rows = []
    for i, (_, row) in enumerate(rows_df.iterrows(), start=1):
        width_pct = _pct(row[value_col], max_value)
        rows.append(f"""
        <div class="rank-row">
          <div class="rank-index mono">{i:02d}</div>
          <div class="rank-label">{_esc(row[label_col])}</div>
          <div class="rank-track">
            <div class="rank-fill" style="width:{width_pct:.2f}%;background:{color}"></div>
          </div>
          <div class="rank-value mono">{fmt_value(row[value_col])}</div>
        </div>""")

    body = f'<div class="rank-list">{"".join(rows)}</div>'
    badge_html = f'<span class="badge">{_esc(badge)}</span>' if badge else ""
    return _card_shell(title, subtitle, body, badge_html)


def _grid_list_card(title: str, subtitle: str, entries: list[tuple], color: str) -> str:
    max_value = max(count for _, _, count in entries)
    cells = []
    for code_or_name, name, count in entries:
        width_pct = _pct(count, max_value)
        cells.append(f"""
        <div class="grid-cell">
          <div class="grid-cell-top">
            <span class="grid-code" style="color:{color}">{_esc(code_or_name)}</span>
            <span class="grid-name">{_esc(name)}</span>
            <span class="grid-count mono">{_fmt_int(count)}</span>
          </div>
          <div class="grid-track">
            <div class="grid-fill" style="width:{width_pct:.2f}%;background:{color}"></div>
          </div>
        </div>""")

    body = f'<div class="grid-list">{"".join(cells)}</div>'
    return _card_shell(title, subtitle, body)


PAGE_CSS = """
<style>
  #theme-toggle { display: none; }
  .page {
    --bg: #0b0f17;
    --card-bg: #141a24;
    --card-border: rgba(255,255,255,0.07);
    --text-primary: #f1f5f9;
    --text-secondary: #94a3b8;
    --text-tertiary: #64748b;
    --track-bg: rgba(255,255,255,0.07);

    background: var(--bg);
    color: var(--text-primary);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    padding: 28px clamp(16px, 4vw, 40px) 60px;
    min-height: 100vh;
    box-sizing: border-box;
  }
  #theme-toggle:checked ~ .page {
    --bg: #f8fafc;
    --card-bg: #ffffff;
    --card-border: #e5e7eb;
    --text-primary: #0f172a;
    --text-secondary: #475569;
    --text-tertiary: #94a3b8;
    --track-bg: #eef2f6;
  }
  .page * { box-sizing: border-box; }
  .mono { font-family: ui-monospace, "SF Mono", Consolas, monospace; font-variant-numeric: tabular-nums; }
  .serif { font-family: Georgia, "Times New Roman", serif; }

  .header { display: flex; align-items: center; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }
  .header-icon {
    width: 48px; height: 48px; border-radius: 12px; border: 1.5px solid __TEAL__;
    display: flex; align-items: center; justify-content: center; flex-shrink: 0;
  }
  .header-title-group { flex: 1; min-width: 200px; }
  .header-title { font-size: 28px; font-weight: 700; margin: 0; }
  .header-subtitle { color: var(--text-secondary); font-size: 13px; margin-top: 2px; }
  .header-right { display: flex; align-items: center; gap: 10px; }
  .as-of-pill {
    background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 999px;
    padding: 6px 14px; font-size: 12px; color: var(--text-secondary);
  }
  .theme-buttons { display: flex; gap: 4px; background: var(--card-bg); border: 1px solid var(--card-border);
    border-radius: 999px; padding: 4px; }
  .theme-btn { width: 30px; height: 30px; border-radius: 999px; display: flex; align-items: center;
    justify-content: center; cursor: pointer; color: var(--text-secondary); }
  .theme-btn.dark-btn { background: __TEAL__; color: #0b0f17; }
  #theme-toggle:checked ~ .page .theme-btn.dark-btn { background: transparent; color: var(--text-secondary); }
  #theme-toggle:checked ~ .page .theme-btn.light-btn { background: __TEAL__; color: #0b0f17; }

  .tile-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 16px; margin-bottom: 20px; }
  .tile { background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 14px; padding: 18px 20px; }
  .tile-label { font-size: 11px; letter-spacing: 0.08em; text-transform: uppercase; color: var(--text-tertiary); margin-bottom: 8px; }
  .tile-value { font-size: 30px; font-weight: 700; margin-bottom: 6px; }
  .tile-subtitle { font-size: 13px; color: var(--text-secondary); }
  .accent-amber { color: #f2a65a; font-weight: 600; }
  .accent-teal { color: __TEAL__; font-weight: 600; }

  .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px; }
  @media (max-width: 860px) { .grid-2 { grid-template-columns: 1fr; } }

  .card { background: var(--card-bg); border: 1px solid var(--card-border); border-radius: 14px; padding: 22px 24px; }
  .card-title { font-size: 18px; font-weight: 700; }
  .card-subtitle { font-size: 12.5px; color: var(--text-secondary); margin-top: 2px; margin-bottom: 18px; }
  .card-header-row { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; flex-wrap: wrap; }
  .badge { font-size: 11px; font-family: ui-monospace, monospace; color: var(--text-secondary);
    border: 1px solid var(--card-border); border-radius: 6px; padding: 3px 8px; height: fit-content; }

  .segment-bar { display: flex; height: 10px; border-radius: 999px; overflow: hidden; margin-bottom: 18px; }
  .segment:first-child { border-radius: 999px 0 0 999px; }
  .segment:last-child { border-radius: 0 999px 999px 0; }
  .legend-list { display: flex; flex-direction: column; gap: 12px; }
  .legend-row { display: flex; align-items: center; gap: 10px; font-size: 14px; }
  .dot { width: 9px; height: 9px; border-radius: 50%; flex-shrink: 0; }
  .legend-name { flex: 1; font-weight: 600; }
  .legend-value { font-weight: 700; margin-right: 12px; }
  .legend-pct { color: var(--text-tertiary); width: 44px; text-align: right; }

  .legend-chips { display: flex; flex-wrap: wrap; gap: 10px 14px; }
  .legend-chip { font-size: 11.5px; color: var(--text-secondary); display: flex; align-items: center; gap: 6px; }

  .bar-list { display: flex; flex-direction: column; gap: 12px; }
  .bar-row { display: grid; grid-template-columns: 140px 1fr 60px; align-items: center; gap: 12px; }
  .bar-label { font-size: 13.5px; color: var(--text-primary); }
  .bar-track { height: 20px; background: var(--track-bg); border-radius: 6px; overflow: hidden; }
  .bar-fill { height: 100%; border-radius: 6px; }
  .bar-value { font-size: 13px; text-align: right; color: var(--text-secondary); }

  .rank-list { display: flex; flex-direction: column; gap: 12px; }
  .rank-row { display: grid; grid-template-columns: 24px 110px 1fr 64px; align-items: center; gap: 12px; }
  .rank-index { font-size: 12px; color: var(--text-tertiary); }
  .rank-label { font-size: 14px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .rank-track { height: 18px; background: var(--track-bg); border-radius: 5px; overflow: hidden; }
  .rank-fill { height: 100%; border-radius: 5px; }
  .rank-value { font-size: 13px; text-align: right; }

  .grid-list { display: grid; grid-template-columns: 1fr 1fr; gap: 16px 24px; }
  @media (max-width: 560px) { .grid-list { grid-template-columns: 1fr; } }
  .grid-cell-top { display: flex; align-items: baseline; gap: 8px; margin-bottom: 6px; }
  .grid-code { font-size: 13px; font-weight: 700; font-family: ui-monospace, monospace; }
  .grid-name { flex: 1; font-size: 13px; color: var(--text-secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
  .grid-count { font-size: 13px; }
  .grid-track { height: 4px; background: var(--track-bg); border-radius: 3px; overflow: hidden; }
  .grid-fill { height: 100%; border-radius: 3px; }
</style>
""".replace("__TEAL__", TEAL)

HEADER_HTML = """
<div class="header">
  <div class="header-icon">
    <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="{teal}" stroke-width="1.6">
      <circle cx="12" cy="12" r="9.2"/>
      <path d="M2.8 12h18.4M12 2.8c2.6 2.6 4 5.9 4 9.2s-1.4 6.6-4 9.2c-2.6-2.6-4-5.9-4-9.2s1.4-6.6 4-9.2z"/>
    </svg>
  </div>
  <div class="header-title-group">
    <div class="header-title serif">REST Countries Dashboard</div>
    <div class="header-subtitle">A world atlas in numbers &middot; data from the REST Countries API</div>
  </div>
  <div class="header-right">
    <div class="as-of-pill mono">Data as of {as_of}</div>
    <label class="theme-buttons" for="theme-toggle">
      <span class="theme-btn dark-btn">&#9789;</span>
      <span class="theme-btn light-btn">&#9728;</span>
    </label>
  </div>
</div>
"""


def _render_page(snapshot_date: str) -> str:
    data = _load_data(snapshot_date)
    countries, subregions, currencies, languages = (
        data["countries"], data["subregions"], data["currencies"], data["languages"],
    )

    top_currencies = currencies.nlargest(10, "country_count")
    currency_entries = [
        (row["currency_code"], _coalesce(row["currency_name"], row["currency_code"]), row["country_count"])
        for _, row in top_currencies.iterrows()
    ]
    top_languages = languages.nlargest(10, "country_count")
    language_entries = [
        (row["language"], "", row["country_count"]) for _, row in top_languages.iterrows()
    ]

    as_of = datetime.strptime(snapshot_date, "%Y-%m-%d").strftime("%b %d, %Y")

    return f"""
    {PAGE_CSS}
    <input type="checkbox" id="theme-toggle" />
    <div class="page">
      {HEADER_HTML.format(as_of=as_of, teal=TEAL)}
      {_stat_tiles_html(data)}
      <div class="grid-2">
        {_region_share_card(subregions)}
        {_largest_subregions_card(subregions)}
      </div>
      <div class="grid-2">
        {_ranked_list_card(
            "Most populous countries", "Top 10 by population", countries.nlargest(10, "population"),
            "name_common", "population", _fmt_big, ACCENT_POPULATION, badge="people",
        )}
        {_ranked_list_card(
            "Most densely populated", "Top 10 by people per km²",
            countries.dropna(subset=["population_density"]).nlargest(10, "population_density"),
            "name_common", "population_density", _fmt_int, ACCENT_DENSITY, badge="per km²",
        )}
      </div>
      <div class="grid-2">
        {_grid_list_card(
            "Most shared currencies", "Number of countries using each currency",
            currency_entries, ACCENT_CURRENCY,
        )}
        {_grid_list_card(
            "Most widespread languages", "Number of countries with each official language",
            language_entries, ACCENT_LANGUAGE,
        )}
      </div>
    </div>
    """


TOOLBAR_CSS = f"""
select {{
  background: #141a24; color: #f1f5f9; border: 1px solid rgba(255,255,255,0.15);
  border-radius: 8px; padding: 5px 10px; font-family: ui-monospace, monospace; font-size: 13px;
}}
label {{ color: #94a3b8; font-family: -apple-system, sans-serif; font-size: 12px; }}
"""


def build_dashboard() -> pn.Column:
    available_dates = snapshot_dates("dm_countries")
    date_select = pn.widgets.Select(
        name="Snapshot date", options=available_dates, value=available_dates[0],
        width=200, stylesheets=[TOOLBAR_CSS],
    )
    toolbar = pn.Row(
        date_select, sizing_mode="stretch_width",
        styles={"background": "#0b0f17", "padding": "14px 28px 0"},
    )

    content = pn.pane.HTML(
        pn.bind(_render_page, date_select), sizing_mode="stretch_width",
    )
    return pn.Column(
        toolbar, content, sizing_mode="stretch_width", styles={"background": "#0b0f17"},
    )


build_dashboard().servable(title="REST Countries Dashboard")
