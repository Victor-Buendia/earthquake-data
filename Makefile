.PHONY: all
all:
	$(MAKE) airflow hive infra spark

.PHONY: airflow
airflow:
	docker compose --profile flower -f airflow-docker-compose.yaml -p pipeline --env-file .env up -d --build

.PHONY: hive
hive:
	docker compose -f hive-docker-compose.yaml -p pipeline --env-file .env up -d --build

.PHONY: infra
infra:
	docker compose -f infra-docker-compose.yaml -p pipeline --env-file .env up -d --build

.PHONY: spark
spark:
	docker compose -f spark-docker-compose.yaml -p pipeline --env-file .env up -d --build

.PHONY: spark-submit
spark-submit:
	docker exec -it --env PYTHONPATH=/:${PYTHONPATH} $$(docker ps -f name=spark-master -q) spark-submit --master spark://spark-master:7077 client /src/tasks/staging/stg_clockify__time_entries.py raw lakehouse

.PHONY: mvn
mvn:
	mvn dependency:copy-dependencies -DoutputDirectory=./spark/jars