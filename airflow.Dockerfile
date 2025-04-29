FROM apache/airflow:2.10.5

USER root
RUN apt-get update && apt-get install -y procps
# install default-jre
# https://askubuntu.com/questions/1203898/package-openjdk-11-jdk-has-no-installation-candidate
RUN apt-get update && \
    apt-get install -y default-jre && \
    rm -rf /var/lib/apt/lists/*

    USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt