all:
	docker compose --profile flower -f airflow-docker-compose.yaml -p pipeline --env-file .env up -d --build
	docker compose -f infra-docker-compose.yaml -p pipeline --env-file .env up -d
	docker compose -f spark-docker-compose.yaml -p pipeline --env-file .env up -d