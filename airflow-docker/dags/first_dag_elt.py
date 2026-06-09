from airflow.sdk import dag, task
from pendulum import datetime, duration
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    LoadMode,
)
import logging

logger = logging.getLogger(__name__)

DBT_PROJECT_PATH = "/opt/airflow/dbt_earthquake_project"
DBT_PROFILES_PATH = "/opt/airflow/dbt_earthquake_project/profiles.yml"
DBT_EXECUTABLE_PATH = "/opt/airflow/dbt_venv/bin/dbt"

profile_config = ProfileConfig(
    profile_name="dbt_earthquake_project",
    target_name="dev",
    profiles_yml_filepath=DBT_PROFILES_PATH,
)

execution_config = ExecutionConfig(
    dbt_executable_path=DBT_EXECUTABLE_PATH,
)

@dag(
    dag_id="earthquake_usgs_elt_v3",
    schedule="@hourly",
    start_date=datetime(2026, 5, 26),
    catchup=False,
    tags=["production", "elt", "bigquery"],
    default_args={
        "retries": 3,
        "retry_delay": duration(minutes=5),
    },
)
def earthquake_pipeline():

    @task
    def load_bronze_earthquakes():
        from src.ingest_raw import ingest
        logger.info("Starting Bronze ingestion")
        ingest()
        logger.info("Bronze ingestion completed")

    dbt_task = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_PROJECT_PATH),
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            dbt_deps=False,
        ),
        operator_args={
            "install_deps": False,
        },
    )

    load_bronze_earthquakes() >> dbt_task

earthquake_pipeline()