FROM docker.io/bitnami/spark:3.5.0

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY ./spark/jars /opt/bitnami/spark/jars
USER 1001