# EarthquakeFlow 🌍

> A production-ready ELT pipeline that ingests real-time earthquake data from the USGS API, stores it in Google BigQuery, transforms it using dbt, orchestrates it with Apache Airflow, and visualizes it in Looker Studio — all running inside Docker.

---

## Architecture

```
USGS Earthquake API (live, every hour)
              ↓
       src/ingest_raw.py          ← Python ingestion layer
              ↓
  bronze.raw_earthquakes          ← Raw JSON snapshot (BigQuery)
              ↓ dbt (via Cosmos)
  dbt_silver.stg_earthquakes      ← Cleaned, parsed, deduplicated
              ↓ dbt
  dbt_gold.dim_locations          ← Location dimension
  dbt_gold.dim_time               ← Time dimension
  dbt_gold.fct_earthquakes        ← Fact table (Star Schema)
  dbt_gold.fct_earthquakes_enriched ← Enriched view (JOIN)
              ↓
       Looker Studio               ← Live dashboard
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.12 + google-cloud-bigquery |
| Storage | Google BigQuery |
| Transformation | dbt Core 1.11 + dbt-bigquery |
| Orchestration | Apache Airflow 3.x + Astronomer Cosmos |
| Containerization | Docker + Docker Compose |
| Visualization | Looker Studio |
| Authentication | GCP Service Account JSON (via env vars) |
| Package Manager | uv |

---

## Project Structure

```
usgs-earthquake-pipeline-v2/
├── airflow-docker/
│   ├── dags/
│   │   └── first_dag_elt.py      ← Airflow DAG (Cosmos + @task)
│   ├── config/
│   ├── plugins/
│   ├── logs/
│   └── Dockerfile                ← Custom Airflow image
├── dbt_earthquake_project/
│   ├── models/
│   │   ├── silver/
│   │   │   ├── sources.yml
│   │   │   ├── stg_earthquakes.sql
│   │   │   └── schema.yml
│   │   └── gold/
│   │       ├── dim_locations.sql
│   │       ├── dim_time.sql
│   │       ├── fct_earthquakes.sql
│   │       └── fct_earthquakes_enriched.sql
│   ├── macros/
│   ├── packages.yml
│   ├── dbt_project.yml
│   └── profiles.yml
├── src/
│   └── ingest_raw.py             ← Ingestion script
├── keys/                         ← GCP credentials (gitignored)
├── docker-compose.yaml
├── pyproject.toml
├── .env.example
└── README.md
```

---

## Data Model

### Bronze Layer
Raw GeoJSON snapshot from USGS API. One row per ingestion run.

| Column | Type | Description |
|---|---|---|
| raw_payload | JSON | Full GeoJSON response |
| source_url | STRING | API endpoint used |
| ingested_at | TIMESTAMP | Ingestion timestamp |
| record_count | INTEGER | Number of earthquakes in snapshot |

---

### Silver Layer — `stg_earthquakes`

Unpacked, cleaned, and deduplicated earthquake records.

**Key transformations:**
- `SAFE.PARSE_JSON(TO_JSON_STRING(raw_payload))` → handles BigQuery STRUCT/JSON coercion
- `UNNEST(JSON_QUERY_ARRAY(..., '$.features'))` → one row per earthquake
- `SAFE_CAST` on all numeric fields → no pipeline crashes on bad data
- `TIMESTAMP_MILLIS` → converts Unix ms to proper timestamps
- `ROW_NUMBER() OVER(PARTITION BY quake_id)` → deduplication
- Quality checks: magnitude > 0, depth > 0, quake_id IS NOT NULL

| Column | Type |
|---|---|
| quake_id | STRING |
| magnitude | NUMERIC |
| longitude | NUMERIC |
| latitude | NUMERIC |
| depth | NUMERIC |
| earthquake_time | TIMESTAMP |
| updated_at | TIMESTAMP |
| status | STRING |
| place | STRING |
| tsunami | INT64 |
| earthquake_type | STRING |

---

### Gold Layer — Star Schema

```
         dim_time
            ↑
dim_locations ← fct_earthquakes → fct_earthquakes_enriched (view)
```

**`dim_locations`**
| Column | Description |
|---|---|
| location_id | Surrogate key (longitude + latitude hash) |
| place | Location name |
| longitude | Rounded to 6 decimals |
| latitude | Rounded to 6 decimals |
| region | Americas / Europe-Africa / Asia-Pacific |

**`dim_time`**
| Column | Description |
|---|---|
| time_id | Surrogate key (YYYYMMDDHHmmss) |
| earthquake_time | Full timestamp |
| year / month / day / hour / minute | Date parts |
| day_of_week | Monday … Sunday |
| month_name | January … December |
| is_weekend | Boolean |

**`fct_earthquakes`**
| Column | Description |
|---|---|
| quake_id | Natural key |
| location_id | FK → dim_locations |
| time_id | FK → dim_time |
| magnitude | Earthquake magnitude |
| depth | Depth in km |
| magnitude_category | micro / minor / moderate / strong / major / great |
| status | automatic / reviewed |
| tsunami | 0 or 1 |
| earthquake_type | earthquake / quarry blast / etc |

**`fct_earthquakes_enriched`** (view)

Pre-joined view combining fct + dims for direct Looker Studio use.

---

## Airflow DAG

```
load_bronze_earthquakes (@task)
        ↓
dbt_transformations (DbtTaskGroup via Cosmos)
    ├── stg_earthquakes → run → test
    ├── fct_earthquakes → run → test
    ├── dim_locations_run
    ├── dim_time_run
    └── fct_earthquakes_enriched_run
```

**Schedule:** `@hourly`
**Retries:** 3 × 5 min delay

<p align="center">
  <img src="./assets/airflow.png" width="900" alt="Airflow DAG">
</p>
---

## The Hardest Bug 🐛

Airflow crashed when dbt tried to transform Bronze data.

**Root cause:** BigQuery silently stored `raw_payload` as `STRUCT` instead of `JSON STRING`, making `JSON_QUERY_ARRAY` fail with a type coercion error.

**Fix — one line in `stg_earthquakes.sql`:**
```sql
UNNEST(JSON_QUERY_ARRAY(
    SAFE.PARSE_JSON(TO_JSON_STRING(raw_payload)),
    '$.features'
)) AS feature
```

`TO_JSON_STRING` converts STRUCT → STRING, then `SAFE.PARSE_JSON` converts it back to proper JSON. Pipeline unblocked.

---

## Setup

### Prerequisites
- Python 3.12+
- Docker + Docker Compose
- Google Cloud project with BigQuery enabled
- GCP Service Account with BigQuery Admin role
- uv package manager

### Installation

```bash
# Clone the repo
git clone https://github.com/oussama259796/-earthquake-elt-project-v2
cd usgs-earthquake-pipeline-v2

# Install dependencies
uv sync

# Install dbt packages
cd dbt_earthquake_project
dbt deps
```

### Environment Variables

Create a `.env` file at project root:

```dotenv
GCP_PROJECT=your-project-id
GCP_PRIVATE_KEY_ID=your-key-id
GCP_PRIVATE_KEY="-----BEGIN RSA PRIVATE KEY-----\n...\n-----END RSA PRIVATE KEY-----\n"
GCP_CLIENT_EMAIL=your-service-account@project.iam.gserviceaccount.com
GCP_CLIENT_ID=your-client-id
FERNET_KEY=your-fernet-key
API_URL=https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson
```

### Run with Docker

```bash
# Build and start all services
docker compose up --build

# Access Airflow UI
http://localhost:8080
# user: airflow / password: airflow
```

### Run manually (without Docker)

```bash
# Ingest data into Bronze
python src/ingest_raw.py

# Run dbt transformations
cd dbt_earthquake_project
dbt run
dbt test
```

---

## Data Quality Tests

| Test | Model | Column |
|---|---|---|
| unique | stg_earthquakes | quake_id |
| not_null | stg_earthquakes | quake_id |
| accepted_values | stg_earthquakes | status |
| unique | fct_earthquakes | quake_id |
| not_null | fct_earthquakes | quake_id |
| accepted_values | fct_earthquakes | magnitude_category |

---

## Live Dashboard

Built with Looker Studio, connected directly to `fct_earthquakes_enriched`:

- Max / Min / Avg magnitude KPIs
- Total earthquakes per hour
- Global map with magnitude-based color coding
- Filters by region, magnitude category, time

<p align="center">
  <img src="./assets/dashboard.png" width="900" alt="Looker Studio dashboard">
</p>

---

## Key Design Decisions

| Decision | Reason |
|---|---|
| ELT over ETL | Transform inside BigQuery — no Python transformation bottleneck |
| Medallion Architecture | Clear Bronze → Silver → Gold lineage |
| Star Schema | Optimized for Looker Studio and analytical queries |
| SAFE.PARSE_JSON(TO_JSON_STRING()) | Defensive parsing — handles STRUCT/JSON coercion in BigQuery |
| SAFE_CAST everywhere | Pipeline never crashes on malformed API data |
| ROW_NUMBER deduplication | Handles repeated hourly ingestion of same earthquakes |
| Cosmos integration | dbt models appear as individual Airflow tasks with full lineage |
| retries=3 in DAG | Automatic recovery from transient API or BigQuery failures |

---

## Dataset

Live data from [USGS Earthquake Hazards Program](https://earthquake.usgs.gov/earthquakes/feed/v1.0/geojson.php)

> Real-time GeoJSON feed updated every minute. This pipeline uses the `all_month` endpoint (~10,000+ earthquakes per run).

---

## License

MIT