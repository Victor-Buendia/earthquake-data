all:
	docker compose --profile flower -f airflow-docker-compose.yaml -p pipeline --env-file .env up -d --build
	docker compose -f infra-docker-compose.yaml -p pipeline --env-file .env up -d
	docker compose -f spark-docker-compose.yaml -p pipeline --env-file .env up -d
spark:
	docker exec -it $$(docker ps -f name=spark-master -q) spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2 /pyspark/tasks/staging/stg_clockify__time_entries.py