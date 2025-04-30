all:
	docker compose --profile flower -f airflow-docker-compose.yaml -p pipeline --env-file .env up -d --build
	docker compose -f infra-docker-compose.yaml -p pipeline --env-file .env up -d
	docker compose -f spark-docker-compose.yaml -p pipeline --env-file .env up -d
spark:
	docker exec -it --env PYTHONPATH=/:${PYTHONPATH} $$(docker ps -f name=spark-master -q) spark-submit --master spark://spark-master:7077 client /src/tasks/staging/stg_clockify__time_entries.py raw lakehouse