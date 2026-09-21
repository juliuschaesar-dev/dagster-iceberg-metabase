# Dagster Iceberg Metabase

Dagster pipeline that fetches country data from the [REST Countries API](https://restcountries.com),
lands it raw in Garage (S3-compatible object storage), cleans it into an
Iceberg staging table (via Lakekeeper + Trino), and builds Iceberg datamart
tables for a Metabase dashboard.

Layers: **raw** (JSON in Garage) → **staging** (Iceberg table) → **datamart** (Iceberg tables).

## Architecture

![Architecture](docs/architecture.svg)

## Prerequisites

- Docker + Docker Compose
- Python 3.10+ (needed to run `scripts/init_lakekeeper_warehouse.py`, and for
  Dagster/tests outside Docker). Use a virtualenv so these don't pollute your
  global Python:
  ```bash
  python -m venv .venv
  # Windows:
  .venv\Scripts\activate
  # macOS/Linux:
  source .venv/bin/activate

  pip install -e .
  ```

## Run from scratch

1. Copy the env template and fill in real credentials:
   ```bash
   cp .env.example .env
   ```
   `GARAGE_ACCESS_KEY_ID` must follow Garage's own format: `GK` followed by
   24 hex characters (e.g. generate one with `echo "GK$(openssl rand -hex 12)"`)
   — Garage rejects any other format when the key is registered in step 4.
2. Start every service:
   ```bash
   docker compose up -d
   ```
3. Confirm all services are healthy:
   ```bash
   docker compose ps
   ```
4. Bootstrap the cluster: assigns the Garage layout, registers the access key,
   creates the raw/staging/datamart buckets, and creates the Lakekeeper
   warehouse (reads `.env` itself, works the same in PowerShell or bash):
   ```bash
   python scripts/init_lakekeeper_warehouse.py
   ```
5. Verify Trino can see the Iceberg catalog:
   ```bash
   docker compose exec trino trino --execute "SHOW CATALOGS"
   ```
6. Open the Dagster UI at http://localhost:3000 and materialize all assets
   (or click "Materialize all" on the asset graph), or run:
   ```bash
   docker compose exec dagster dagster job execute -m dagster_project -j countries_pipeline_job
   ```
7. Open Metabase at http://localhost:3001 and complete the first-run setup
   wizard (creates your own admin account — no default credentials). When
   asked to add a database, choose **Starburst** (Metabase's built-in Trino
   driver — no plugin install needed) with:
   - Host: `trino`
   - Port: `8080`
   - Catalog: `iceberg`
   - User: `dagster`
   - No password / SSL

   Then build questions (charts) on the datamart tables below and arrange
   them into a dashboard:
   - `datamart.dm_population_by_region` — population and country count per region
   - `datamart.dm_currency_distribution` — countries per currency
   - `datamart.dm_language_distribution` — countries per language
   - `datamart.dm_subregion_summary` — population/area/country count per subregion
   - `datamart.dm_population_density` — population density (people/km²) per country
   - `datamart.dm_top_countries_by_population` — the 20 most populous countries

The daily schedule (`daily_countries_pipeline`, 06:00) re-runs the full
raw → staging → datamart pipeline automatically once enabled in the Dagster UI.

## Re-running the pipeline

- **Full refresh (fresh raw data):** materialize `raw_countries` (overwrites
  the same raw JSON object in Garage) and let its downstream assets
  (`stg_countries`, `dm_*`) re-materialize.
- **Partial run (staging/datamart only):** select just `stg_countries` and
  the `dm_*` assets in the Dagster UI to reprocess the existing raw JSON
  without re-hitting the API.

Every staging/datamart table carries a `snapshot_date` column and is written
as one dated snapshot per run (delete + insert, keyed on `snapshot_date`), so
history accumulates across days instead of being overwritten — re-running
multiple times on the *same* day just replaces that day's snapshot, staying
idempotent. Datamart queries (and the daily schedule) only ever read the
current day's staging snapshot, so numbers reflect one point in time rather
than the full history.

## Testing

```bash
# activate the venv from Prerequisites first
pip install -e ".[dev]"
pytest
```

Every asset's external resources (`api`, `garage`, `trino`) are Dagster
resources injected at runtime, so tests mock them directly instead of hitting
real services.

## Stopping

```bash
docker compose down
```

Stops and removes the containers, but keeps all data (Postgres, Garage,
Metabase, Dagster home) in their named volumes — a later `docker compose up -d`
picks up right where you left off.

To also wipe all data (raw/staging/datamart tables, the Metabase dashboard,
everything) and start clean:

```bash
docker compose down -v
```
