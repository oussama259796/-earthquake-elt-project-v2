import requests
import os
import logging
import json
from google.cloud import bigquery
from datetime import datetime, timezone
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
                level=logging.INFO,
                filename="logs/app.log",
                format="%(asctime)s - %(levelname)s - %(message)s",
                encoding='utf-8'
            )
logger = logging.getLogger(__name__)

load_dotenv()

PROJECT = os.getenv("GCP_PROJECT")
DATASET = "bronze"
TABLE = "raw_earthquakes"
TABLE_ID = f"{PROJECT}.{DATASET}.{TABLE}"
url = os.getenv("API_URL")

def get_client():
    return bigquery.Client()

def create_infrastructure(client):
    
    # انشاء DATASET
    dataset = bigquery.Dataset(f"{PROJECT}.{DATASET}")
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)
    logger.info(f"Dataset {DATASET} ready") 
    
    # انشاء جدول table
    schema = [
        bigquery.SchemaField("raw_payload", "JSON"),
        bigquery.SchemaField("source_url", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("record_count", "INTEGER")
    ]

    table = bigquery.Table(TABLE_ID, schema=schema)
    client.create_table(table, exists_ok=True)
    logger.info(f"Table {TABLE} ready")

def fetch_earthquakes():

    retry = Retry(
        total=3, 
        backoff_factor=2, 
        status_forcelist=[429, 500, 502, 503, 504]
        )
    adapter = HTTPAdapter(max_retries=retry)
    with requests.Session() as session:
        session.mount("https://", adapter)

        try:
            response = session.get(url, timeout=(2, 10))
            response.raise_for_status()
            raw_data = response.json()
            logger.info("Response received successfully")
            return raw_data
        except requests.exceptions.Timeout:
            logger.error("❌ API timeout")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ HTTP error: {e}")
            raise
        except Exception as e :
            logger.error(f"API Request Failed: {e}")
            raise
        
def load_to_bronze(client , raw_data):
    if not raw_data:
        logger.error("❌ No data received to load into Bronze")
        return

    rows = [{
        'raw_payload': raw_data,
        'source_url': url,
        'ingested_at': datetime.now(timezone.utc).isoformat(),
        'record_count' : len(raw_data.get("features", []))
    }]

    job_config = bigquery.LoadJobConfig(
        write_disposition = "WRITE_APPEND"
    )

    job = client.load_table_from_json(rows, TABLE_ID, job_config=job_config)
    job.result()
    logger.info(f"✅ Loaded {job.output_rows} row to bronze")

def ingest():
    try:
        logger.info("🚀 Starting ingestion pipeline")
        client = get_client()
        create_infrastructure(client)
        raw_data = fetch_earthquakes()

        load_to_bronze(client, raw_data)
        logger.info("✅ Pipeline completed successfully")
    except Exception as e:
        logger.error(f"ngestion pipeline failed: {e}")

if __name__ == "__main__":
    ingest()