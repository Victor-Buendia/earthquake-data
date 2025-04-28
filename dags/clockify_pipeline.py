import sys
import os
import logging

from airflow.models.dag import DAG
from airflow.decorators import dag

from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.tasks.staging.clockify_ingestion import clockify_ingestion
from src.tasks.staging.clockify_parquet import clockify_parquet

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw"

with DAG(
    dag_id="clockify_pipeline",
    start_date=datetime(2025, 4, 20),
    schedule="@daily",
    catchup=True,
) as dag:

    raw = clockify_ingestion(BUCKET_NAME)
    parquet = clockify_parquet(BUCKET_NAME)

    raw >> parquet
