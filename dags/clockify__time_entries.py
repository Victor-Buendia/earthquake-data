import sys
import os
import logging

from airflow.models.dag import DAG
from airflow.decorators import dag

from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from src.tasks.raw.raw_clockify__time_entries import raw_clockify__time_entries
from src.tasks.raw.raw_clockify__time_entries__parquet import raw_clockify__time_entries__parquet

logger = logging.getLogger(__name__)

BUCKET_NAME = "raw"

with DAG(
    dag_id="clockify__time_entries__pipeline",
    start_date=datetime(2025, 4, 20),
    schedule="@daily",
    catchup=True,
) as dag:

    raw = raw_clockify__time_entries(BUCKET_NAME)
    parquet = raw_clockify__time_entries__parquet(BUCKET_NAME)

    raw >> parquet
