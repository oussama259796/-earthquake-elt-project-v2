# USGS Earthquake ELT Pipeline

> Production‑style ELT pipeline that ingests near‑real‑time earthquake events from the official USGS GeoJSON feeds into BigQuery, transforms them with dbt, orchestrates everything with Airflow, and exposes analytics in Looker Studio.[web:917][web:919]

---

## Table of Contents

1. [Overview](#overview)  
2. [High‑Level Architecture](#high-level-architecture)  
3. [Tech Stack](#tech-stack)  
4. [Project Structure](#project-structure)  
5. [Data Flow & Layers](#data-flow--layers)  
6. [Airflow DAG](#airflow-dag)  
7. [dbt Models](#dbt-models)  
8. [Looker Studio Dashboard](#looker-studio-dashboard)  
9. [Setup & Installation](#setup--installation)  
10. [Configuration](#configuration)  
11. [Security & Secrets](#security--secrets)  
12. [Local Development](#local-development)  
13. [Troubleshooting](#troubleshooting)  
14. [Roadmap](#roadmap)  
15. [License](#license)  

---

## Overview

This repository implements a complete **event‑driven analytics pipeline** for global earthquakes:

- **Source** – USGS GeoJSON summary feeds (e.g. “All earthquakes, past hour”). Each response is a `FeatureCollection` containing an array of earthquake `features` with geometry, magnitude, and rich metadata.[web:917]
- **Storage** – Raw JSON is landed in a **bronze** dataset in BigQuery.
- **Transformations** – A **dbt** project parses, cleans, and models the data into **staging, fact, and dimension** tables optimized for analytics.[web:920][web:616]
- **Orchestration** – An **Airflow** DAG runs ingestion and dbt transformations on a schedule.
- **Analytics** – A **Looker Studio** dashboard visualizes:
  - Max / Min / Average magnitude.
  - Total quakes per hour.
  - A Google Maps bubble map for epicenters (bubble size & color by magnitude).

The result is a small but realistic **modern data stack on GCP** that you can use as a blueprint for other event‑based pipelines.

---

## High‑Level Architecture

```text
           ┌───────────────────────────────────────────────┐
           │             USGS GeoJSON Feed                 │
           │  https://earthquake.usgs.gov/.../geojson      │
           └────────────────────────────┬──────────────────┘
                                        │ HTTP (JSON)
                                        ▼
┌───────────────────────────────┐   ┌───────────────────────────────┐
│          Airflow DAG          │   │           BigQuery            │
│   earthquake_usgs_elt_v3      │   │           (Bronze)            │
│                               │   │  bronze.raw_earthquakes       │
│  -  load_bronze_earthquakes   │──▶│  JSON payload + metadata      │
│  -  dbt_transformations       │   └───────────────┬───────────────┘
└───────────────┬───────────────┘                   │
                │                                   │
                │ triggers dbt                      ▼
                │                           ┌───────────────────────────────┐
                │                           │           BigQuery            │
                │                           │      (Silver / Gold)         │
                │                           │  dbt_silver.stg_earthquakes  │
                │                           │  fct_earthquakes, dims...    │
                │                           └───────────────┬───────────────┘
                │                                           │
                ▼                                           ▼
        ┌────────────────┐                         ┌─────────────────────────┐
        │   dbt + Cosmos │                         │    Looker Studio        │
        │  (BigQuery adapter)                      │  Real‑time dashboard    │
        └────────────────┘                         └─────────────────────────┘
```

---

## Tech Stack

- **Cloud**: Google Cloud Platform (GCP)
- **Warehouse**: BigQuery
- **Orchestration**: Apache Airflow (Docker + Docker Compose)
- **Transformations**: dbt Core (BigQuery adapter) orchestrated via Astronomer Cosmos[web:920]
- **Dashboarding**: Looker Studio (Google Data Studio)
- **Language**: Python 3.12 for ingestion logic & Airflow tasks
- **Containerization**: Docker / Docker Compose

---

## Project Structure

> Adjust paths if your repo differs – this reflects the intended layout.

```text
.
├── airflow-docker/
│   ├── docker-compose.yml       # Airflow services & shared volumes
│   └── Dockerfile               # Extends official Airflow image with dbt etc.
│
├── dbt_earthquake_project/
│   ├── models/
│   │   ├── silver/stg_earthquakes.sql
│   │   ├── fact/fct_earthquakes.sql
│   │   └── dim/{dim_locations.sql, dim_time.sql}
│   ├── seeds/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles/ (optional override)
│
├── src/
│   ├── ingest_raw.py            # USGS → BigQuery bronze ingestion
│   └── utils/                   # helper modules (logging, config, etc.)
│
├── keys/
│   └── gcp-key.json             # GCP service account (NOT in git)
│
├── dags/
│   └── first_dag_elt.py         # DAG definition (earthquake_usgs_elt_v3)
│
├── logs/                        # Airflow logs (gitignored)
├── .env                         # Local configuration (gitignored)
└── README.md
```

---

## Data Flow & Layers

### 1. Bronze Layer – Raw Ingestion

BigQuery dataset (example): **`bronze`**

Table: **`raw_earthquakes`**

Recommended schema:

| Column        | Type     | Description                                      |
|---------------|----------|--------------------------------------------------|
| `raw_payload` | JSON     | Full USGS GeoJSON response for a given run.     |
| `source_url`  | STRING   | Feed URL (e.g. “all_hour.geojson”).             |
| `ingested_at` | TIMESTAMP| UTC ingestion time.                             |
| `record_count`| INT64    | Number of features in the payload.              |

The ingestion code uses the official summary feed, which returns a FeatureCollection where each `feature` corresponds to a single earthquake event.[web:917]

---

### 2. Silver Layer – Staging

Dataset: **`dbt_silver`** (example)

Key model: **`stg_earthquakes`**

Responsibilities:

- UNNEST the `features` array inside `raw_payload`.
- Extract:
  - `id` → `quake_id`
  - Magnitude (`properties.mag`)
  - Coordinates (`geometry.coordinates[longitude, latitude, depth]`)
  - Event timestamps (`properties.time`, `properties.updated`)
  - Attributes (`status`, `place`, `tsunami`, `type`, …)
- Apply data quality rules:
  - Filter out null IDs.
  - Remove non‑positive magnitudes.
  - Clean invalid depths and timestamps.

---

### 3. Gold Layer – Analytics

Typical models:

- **`fct_earthquakes`**
  - Grain: one row per earthquake event.
  - Contains metrics (magnitude, depth, derived severity flags).
  - Joins to time and location dimensions.

- **`dim_time`**
  - Standard time dimension:
    - `date`, `hour`, `weekday`, `month`, `is_weekend`, etc.

- **`dim_locations`**
  - Derived from coordinates / USGS `place` string.
  - Optional enrichment: country, region, tectonic plate, etc.

These models are materialized as BigQuery tables using dbt + the BigQuery adapter.[web:616]

---

## Airflow DAG

**DAG ID**: `earthquake_usgs_elt_v3`

### Tasks

1. **`load_bronze_earthquakes`** (`@task`)
   - Calls the USGS GeoJSON feed defined in `USGS_FEED_URL`.
   - Writes one row into `bronze.raw_earthquakes`.
   - Handles dataset/table creation if they don’t exist.

2. **`dbt_transformations`** (TaskGroup)
   - `stg_earthquakes.run` – `dbt run` for staging model.
   - `stg_earthquakes.test` – `dbt test` for staging tests.
   - `fct_earthquakes.run` / `.test`.
   - `dim_locations_run`
   - `dim_time_run`

### Scheduling

For near‑real‑time monitoring:

```python
schedule_interval="*/15 * * * *"  # every 15 minutes
```

You can tune this interval depending on how fresh you want the earthquakes feed to be (USGS feeds are updated approximately every minute).[web:919]

---

## dbt Models

> The dbt project follows a classic **staging → mart** pattern.

### Example: `stg_earthquakes.sql`

Key ideas:

- Use `UNNEST(raw_payload.features)` because the column is JSON/STRUCT, not a plain string.
- Extract fields via dot notation (`feature.properties.mag`) instead of JSON functions where possible for better performance on BigQuery.

### Example: `dbt_project.yml`

- Configures materialization (`table` for large models, `view` for lightweight ones).
- Uses BigQuery configs like `partition_by` and `cluster_by` where appropriate.[web:616]

### Example: `profiles.yml` (BigQuery, service account)

```yaml
dbt_earthquake_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: "{{ env_var('GOOGLE_CLOUD_PROJECT') }}"
      dataset: dbt
      location: "{{ env_var('BIGQUERY_REGION', 'US') }}"
      threads: 4
      timeout_seconds: 300
      keyfile: "{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}"
```

---

## Looker Studio Dashboard

The dashboard connects directly to `fct_earthquakes` (or `stg_earthquakes`) and exposes:

- **Scorecards**
  - `MAX(magnitude)` – Max mag.
  - `MIN(magnitude)` – Min mag.
  - `AVG(magnitude)` – Avg mag.
  - `COUNT_DISTINCT(quake_id)` filtered to the last hour – Total quakes / H.

- **Google Maps Bubble Map**
  - Location: `latitude`, `longitude`.
  - Bubble size: `magnitude`.
  - Bubble color: `magnitude` (or depth for an extra dimension).

- **Filters**
  - Time‑range control (last 15 minutes / hour / 24 hours).
  - Magnitude slider.

Whenever the Airflow DAG finishes a run, new raw events are ingested, dbt models are rebuilt, and refreshing the report immediately updates all KPIs and maps.

---

## Setup & Installation

### 1. Prerequisites

- GCP project with BigQuery enabled.
- Service account with:
  - `BigQuery Data Editor`
  - `BigQuery Job User` (or equivalent).
- Docker & Docker Compose.
- Optional: local Python environment for running dbt/ingestion outside Docker.

### 2. Clone the Repository

```bash
git clone https://github.com/<your-username>/<your-repo>.git
cd <your-repo>
```

### 3. Configure Secrets & Environment

Create `.env` in the repo root:

```env
GCP_PROJECT=your-gcp-project-id
BIGQUERY_REGION=US
USGS_FEED_URL=https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson
```

Place the GCP service account key at:

```text
keys/gcp-key.json
```

Add to `.gitignore`:

```gitignore
keys/gcp-key.json
.env
```

---

## Configuration

### Docker Compose / Airflow

Share config & credentials with all Airflow services (scheduler, webserver, workers, dag‑processor):

```yaml
x-airflow-common:
  &airflow-common
  env_file:
    - .env
  environment:
    GOOGLE_CLOUD_PROJECT: ${GCP_PROJECT}
    GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/keys/gcp-key.json
  volumes:
    - ./keys/gcp-key.json:/opt/airflow/keys/gcp-key.json:ro
    - ./dags:/opt/airflow/dags
    - ./src:/opt/airflow/src
    - ./dbt_earthquake_project:/opt/airflow/dbt_earthquake_project
```

Then:

```bash
docker compose up -d --build
```

Open Airflow (default `http://localhost:8080`), enable `earthquake_usgs_elt_v3`, and trigger a DAG run.

---

## Security & Secrets

- **Never** commit `gcp-key.json` or `.env`.
- Credentials are mounted as read‑only (`:ro`) into the containers.
- For a production deployment, use:
  - GCP Secret Manager,
  - Docker / Kubernetes secrets,
  - or a Vault provider
  instead of local JSON files.

---

## Local Development

### Run dbt Locally

```bash
cd dbt_earthquake_project
dbt debug          # verify connection
dbt run            # build all models
dbt test           # run tests
```

Make sure your local environment has the same `profiles.yml` configuration and access to `GOOGLE_APPLICATION_CREDENTIALS`.[web:920]

### Run Ingestion Code

You can exercise the ingestion step outside Airflow:

```bash
python -m src.ingest_raw
```

This will hit the USGS feed, write a new row into `bronze.raw_earthquakes`, and help debug schema or authentication issues.

---

## Troubleshooting

### 1. `GOOGLE_APPLICATION_CREDENTIALS` missing / dbt cannot authenticate

- Confirm the env var is set inside the Airflow containers:

```bash
docker compose exec airflow-dag-processor printenv GOOGLE_APPLICATION_CREDENTIALS
```

- It must point to `/opt/airflow/keys/gcp-key.json`, and the file must exist inside the container.

### 2. BigQuery `Access Denied` on dataset

- Ensure the service account from `gcp-key.json` has the necessary IAM roles on:
  - the project `GCP_PROJECT`,
  - and the target datasets (bronze / dbt / mart).

### 3. JSON Parsing Errors on Bronze Load

- If you see “Flat value specified for record field” errors, your BigQuery schema does not match the payload type.
- For this project, keep `raw_payload` defined as **JSON** and pass the Python dict directly to `load_table_from_json`.

### 4. dbt “No matching signature for JSON_QUERY_ARRAY”

- This occurs when calling JSON functions on a `STRUCT` instead of `STRING`/`JSON`.
- Use `UNNEST(raw_payload.features)` and dot notation (`feature.properties.mag`) rather than `JSON_QUERY_ARRAY` when `raw_payload` is already parsed into a structured type.

---

## Roadmap

- Support additional USGS feeds (day / week / month, magnitude thresholds).
- Add advanced dbt tests (schema & data quality, freshness checks).
- Enrich location dimension with external geospatial datasets.
- Add alerting (e.g. send notification when magnitude exceeds a configurable threshold).
- Deploy Airflow & dbt to managed services (Cloud Composer, Cloud Run, etc.).

---

## License

This project is released under the **MIT License** (or update to your preferred license).

---